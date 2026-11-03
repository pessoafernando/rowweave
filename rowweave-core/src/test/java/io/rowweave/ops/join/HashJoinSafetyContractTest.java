package io.rowweave.ops.join;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Standing contract for the production resource bounds of HashJoin and AUTO. */
class HashJoinSafetyContractTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    @TempDir
    Path tempDir;

    @Test
    void unmanagedHashJoinFailsBeforeReadingEitherInput() {
        AtomicInteger leftReads = new AtomicInteger();
        AtomicInteger rightReads = new AtomicInteger();
        RowStream left = countingStream(leftReads, row(1, "left"));
        RowStream right = countingStream(rightReads, row(1, "right"));
        HashJoin join = HashJoin.of(
                LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER);

        assertThrows(IllegalStateException.class,
                () -> join.join(left, right, new CollectingRowSink()));
        assertEquals(0, leftReads.get(), "validation must precede the first left read");
        assertEquals(0, rightReads.get(), "validation must precede the first right read");
    }

    @Test
    void unboundedMemoryReplayStoreFailsBeforeReadingEitherInput() {
        AtomicInteger leftReads = new AtomicInteger();
        AtomicInteger rightReads = new AtomicInteger();
        HashJoin join = HashJoin.of(
                        LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                .withSpill(new MemoryManager(1024 * 1024), new MemorySpillStore(), new HeapPool());

        assertThrows(IllegalStateException.class, () -> join.join(
                countingStream(leftReads, row(1, "left")),
                countingStream(rightReads, row(1, "right")),
                new CollectingRowSink()));
        assertEquals(0, leftReads.get());
        assertEquals(0, rightReads.get());
    }

    @Test
    void hashOverflowReleasesOnlyItsOwnCredits() {
        MemoryManager memory = new MemoryManager(2 * 1024);
        long sentinel = 128L;
        assertTrue(memory.tryReserve(sentinel));
        File spillDir = tempDir.resolve("spill").toFile();
        DiskSpillStore store = new DiskSpillStore(spillDir);
        try {
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store);
            AtomicInteger next = new AtomicInteger();
            RowStream right = () -> {
                int index = next.getAndIncrement();
                return index < 100
                        ? StreamEvent.record(row(index % 20, repeated('r', 96) + index))
                        : null;
            };

            try {
                join.join(() -> null, right, new CollectingRowSink());
            } catch (io.rowweave.mem.MemoryBudgetExceededException | IllegalStateException safeFailure) {
                // Foreign reservations make the root handoff barrier intentionally fail-closed.
            }

            assertEquals(sentinel, memory.reservedBytes(),
                    "HashJoin must not release a reservation owned by its caller/sibling");
        } finally {
            store.close();
            if (memory.reservedBytes() == sentinel) {
                memory.releaseExact(sentinel);
            }
        }
        assertEquals(0L, memory.reservedBytes());
    }

    @Test
    void productionJoinSourcesHaveNoRowListsOrUnbudgetedRecursion() throws Exception {
        String hash = source("HashJoin.java");
        String auto = source("AutoJoinSelector.java");

        assertFalse(hash.contains("List<DataRow>"), "HashJoin must not retain DataRow lists");
        assertFalse(hash.contains("List<BuildRow>"), "HashJoin buckets must not be object lists");
        assertFalse(hash.contains("List<List<DataRow>>"), "HashJoin must not materialize partition matrices");
        assertFalse(hash.contains("MAX_RECURSION"), "HashJoin must not use a recursion-depth budget bypass");
        assertFalse(hash.contains("saltRepartition"), "unpartitionable keys require deterministic fallback");
        assertFalse(auto.contains("List<StreamEvent>"), "AUTO probe must be byte-budgeted, not row-list bounded");
    }

    private static RowStream countingStream(AtomicInteger reads, DataRow row) {
        return new RowStream() {
            private boolean emitted;

            @Override
            public StreamEvent next() {
                reads.incrementAndGet();
                if (emitted) return null;
                emitted = true;
                return StreamEvent.record(row);
            }
        };
    }

    private static DataRow row(int key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static String repeated(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String source(String file) throws Exception {
        Path cwd = Paths.get(System.getProperty("user.dir"));
        Path path = cwd.resolve("src/main/java/io/rowweave/ops/join").resolve(file);
        if (!Files.isRegularFile(path)) {
            path = cwd.resolve("rowweave-core/src/main/java/io/rowweave/ops/join").resolve(file);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
