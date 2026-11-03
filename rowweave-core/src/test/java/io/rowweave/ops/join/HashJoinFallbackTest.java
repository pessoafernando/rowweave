package io.rowweave.ops.join;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.BoundedReplayRun;
import io.rowweave.mem.spill.BoundedReplayStore;
import io.rowweave.mem.spill.BudgetedFrameReader;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Hash overflow must hand off deterministically to SortMergeJoin against the same managed ledger. */
class HashJoinFallbackTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));

    @TempDir
    Path tempDir;

    @Test
    void overflowReplaysResidentCurrentAndTailExactlyOnce() {
        MemoryManager memory = new MemoryManager(128L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("replay").toFile());
        try {
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool);
            CollectingRowSink sink = new CollectingRowSink();
            RowStream build = generated(5_000, i -> row(i, repeated('r', 64) + i));

            join.join(RowStream.fromIterable(Arrays.asList(
                    row(0, "first"), row(1777, "middle"), row(4999, "last"), row(9000, "missing"))),
                    build, sink);

            assertEquals(3, sink.rows().size(), "resident/current/tail rows must each survive replay");
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
            assertTrue(store.runsCreated() > 0, "fixture must execute the disk fallback branch");
            assertTrue(memory.spillCount() > 0, "disk fallback must be visible in managed spill metrics");
        } finally {
            store.close();
            pool.trim();
        }
    }

    @Test
    void oneHundredThousandIdenticalBuildKeysStayInsideTinyBudget() {
        MemoryManager memory = new MemoryManager(128L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("skew").toFile());
        AtomicInteger outputs = new AtomicInteger();
        try {
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool);

            join.join(() -> null,
                    generated(100_000, i -> row(7, "same-key-" + i)),
                    event -> { if (event.isRecord()) outputs.incrementAndGet(); });

            assertEquals(0, outputs.get());
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
            assertTrue(memory.peakReservedBytes() <= memory.budget());
            assertTrue(store.runsCreated() > 1, "fallback plus external sort must use real disk runs");
            assertTrue(memory.spillCount() > 0, "disk fallback must be visible in managed spill metrics");
        } finally {
            store.close();
            pool.trim();
        }
    }

    @Test
    void fallbackOutputFailureClosesReaderRunSegmentsAndBothChildren() {
        MemoryManager memory = new MemoryManager(128L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("output-failure").toFile());
        try {
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool);

            assertThrows(IllegalStateException.class, () -> join.join(
                    RowStream.fromIterable(Arrays.asList(row(4999, "probe"))),
                    generated(5_000, i -> row(i, repeated('x', 64))),
                    event -> { if (event.isRecord()) throw new IllegalStateException("output failed"); }));

            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
            assertEquals(0, store.currentOpenReaders());
            assertEquals(0, store.currentOpenWriters());
        } finally {
            store.close();
        }
    }

    @Test
    void buildTailErrorAfterOverflowProducesNoPartialJoin() {
        MemoryManager memory = new MemoryManager(128L * 1024);
        HeapPool pool = new HeapPool();
        DiskSpillStore store = new DiskSpillStore(tempDir.resolve("tail-error").toFile());
        AtomicInteger records = new AtomicInteger();
        AtomicInteger errors = new AtomicInteger();
        AtomicInteger index = new AtomicInteger();
        RowStream failingBuild = () -> {
            int current = index.getAndIncrement();
            if (current < 5_000) return StreamEvent.record(row(current, repeated('e', 64)));
            if (current == 5_000) return StreamEvent.error(new IllegalStateException("build failed"));
            return null;
        };
        try {
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool);

            join.join(RowStream.fromIterable(Arrays.asList(row(1, "must-not-run"))), failingBuild,
                    event -> {
                        if (event.isRecord()) records.incrementAndGet();
                        if (event.isError()) errors.incrementAndGet();
                    });

            assertEquals(0, records.get());
            assertEquals(1, errors.get());
            assertEquals(0L, memory.reservedBytes());
            assertEquals(0, pool.outstanding());
            assertEquals(0, store.currentOpenReaders());
            assertEquals(0, store.currentOpenWriters());
        } finally {
            store.close();
        }
    }

    @Test
    void handoffFailurePointsPreserveRootCauseAndReleaseOwnedResources() {
        for (FailurePoint point : FailurePoint.values()) {
            MemoryManager memory = new MemoryManager(128L * 1024);
            HeapPool pool = new HeapPool();
            FailingReplayStore store = new FailingReplayStore(point);
            HashJoin join = HashJoin.of(
                            LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, JoinType.INNER)
                    .withSpill(memory, store, pool);

            IllegalStateException failure = assertThrows(IllegalStateException.class, () -> join.join(
                    RowStream.fromIterable(Arrays.asList(row(4999, "probe"))),
                    generated(5_000, i -> row(i, repeated('f', 64))),
                    event -> { }));

            assertTrue(failure.getMessage().contains(point.name()), point + " root cause");
            assertEquals(0L, memory.reservedBytes(), point + " memory");
            assertEquals(0, pool.outstanding(), point + " segments");
            if (point != FailurePoint.CREATE) {
                assertEquals(1, store.disposals.get(), point + " run dispose");
            }
        }
    }

    private static RowStream generated(int count, IntRowFactory factory) {
        AtomicInteger index = new AtomicInteger();
        return () -> {
            int current = index.getAndIncrement();
            return current < count ? StreamEvent.record(factory.row(current)) : null;
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
        Arrays.fill(chars, value);
        return new String(chars);
    }

    @FunctionalInterface
    private interface IntRowFactory {
        DataRow row(int value);
    }

    private enum FailurePoint { CREATE, APPEND, FINISH, OPEN }

    /** Test-only fault backend; production HashJoin accepts DiskSpillStore as its bounded implementation. */
    private static final class FailingReplayStore implements BoundedReplayStore {
        private final FailurePoint point;
        private final AtomicInteger disposals = new AtomicInteger();

        private FailingReplayStore(FailurePoint point) {
            this.point = point;
        }

        @Override
        public BoundedReplayRun createRun() {
            if (point == FailurePoint.CREATE) throw failure();
            return new BoundedReplayRun() {
                private final List<byte[]> frames = new ArrayList<>();

                @Override public void append(byte[] frame) {
                    if (point == FailurePoint.APPEND) throw failure();
                    frames.add(frame.clone());
                }
                @Override public void finishWriting() {
                    if (point == FailurePoint.FINISH) throw failure();
                }
                @Override public Iterator<byte[]> read() { return frames.iterator(); }
                @Override public BudgetedFrameReader openBudgetedReader(
                        MemoryManager memoryManager, int fieldCount, int maximumFrameLength) {
                    if (point == FailurePoint.OPEN) throw failure();
                    throw new AssertionError("unexpected reader open");
                }
                @Override public void dispose() {
                    frames.clear();
                    disposals.incrementAndGet();
                }
                @Override public void abort() { dispose(); }
            };
        }

        @Override public void close() { }

        private IllegalStateException failure() {
            return new IllegalStateException(point.name() + " injected");
        }
    }
}
