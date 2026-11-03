package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The real row, sort and final-cursor credit boundaries of the synchronous Pointer buffer. */
class PointerSortExactMemoryTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.LongType),
            new Field("v", DataType.StringType));
    private static final SortField[] SORT = {
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};

    @Test
    void fullWorstCaseRowCreditIsReservedBeforeAllocation() {
        DataRow row = row(1, repeat('x', 257));
        RowSerializer serializer = new RowSerializer(META);
        int serialized = serializer.serializedSize(row);
        long estimate = RowSizeEstimator.estimateRow(row);
        long scratch = serializer.serializerScratchCredit(row);
        long required = Math.addExact(
                Math.addExact(PipelinedExternalSortBuffer.rowCredit(serialized, estimate, scratch),
                        PipelinedExternalSortBuffer.BATCH_FIXED_CREDIT),
                Math.addExact(Math.multiplyExact(2L, estimate),
                        PipelinedExternalSortBuffer.finalHeadCredit(serialized, estimate)));
        MemoryManager memory = new MemoryManager(required - 1);
        HeapPool pool = new HeapPool();
        PointerSortBuffer buffer = pointer(memory, pool, 1024);

        assertThrows(MemoryBudgetExceededException.class, () -> buffer.putRow(row));
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding(), "reservation must fail before segment allocation");
        buffer.close();

        MemoryManager exactMemory = new MemoryManager(required);
        HeapPool exactPool = new HeapPool();
        PointerSortBuffer exact = pointer(exactMemory, exactPool, 1024);
        exact.putRow(row);
        assertEquals(required, exactMemory.reservedBytes(), "all row/batch/aux ownership must be visible");
        exact.close();
        assertEquals(0, exactMemory.reservedBytes());
        assertEquals(0, exactPool.outstanding());
    }

    @Test
    void finalCursorCreditLivesUntilEofAndCloseReleasesAbandonedOwnership() {
        MemoryManager memory = new MemoryManager(128 * 1024);
        HeapPool pool = new HeapPool();
        PointerSortBuffer buffer = pointer(memory, pool, 2);
        for (int i = 0; i < 40; i++) {
            buffer.putRow(row(40 - i, repeat('p', 64)));
        }
        buffer.sort();

        assertTrue(memory.reservedBytes() > 0, "final cursor/comparator credit must stay owned after sort");
        int count = 0;
        for (DataRow ignored : buffer) count++;
        assertEquals(40, count);
        assertEquals(0, memory.reservedBytes(), "EOF is the exact aux-credit release boundary");
        buffer.close();
        assertEquals(0, pool.outstanding());

        MemoryManager abandonedMemory = new MemoryManager(128 * 1024);
        HeapPool abandonedPool = new HeapPool();
        PointerSortBuffer abandoned = pointer(abandonedMemory, abandonedPool, 2);
        for (int i = 0; i < 20; i++) abandoned.putRow(row(i, repeat('q', 64)));
        abandoned.sort();
        abandoned.iterator().next();
        assertTrue(abandonedMemory.reservedBytes() > 0);
        abandoned.close();
        assertEquals(0, abandonedMemory.reservedBytes());
        assertEquals(0, abandonedPool.outstanding());
    }

    @Test
    void groupedReplayScratchStaysEscrowedAfterSortCursorEof() {
        ManagedMemory managed = ManagedMemory.ofBytes(256 * 1024);
        MemoryManager root = managed.acquireOwners(
                ManagedMemory.OwnerSpec.partitioned(32 * 1024, 256 * 1024, 2))[0];
        MemoryManager memory = root.partitionEvenly(2)[0];
        HeapPool pool = new HeapPool();
        SortBuffer buffer = ExternalSortBuffers.pointer(
                META, new RowComparator(META, SORT), SORT, pool, memory, 2,
                new MemorySpillStore());
        for (int i = 0; i < 20; i++) {
            buffer.putRow(row(i, repeat('r', 64 + i)));
        }
        buffer.sort();

        long replayScratch = buffer.retainedReplayScratchBytes();
        assertTrue(replayScratch > 0, "group replay must reserve its maximum serialization scratch");
        for (DataRow ignored : buffer) {
            // drain the final sort cursor
        }
        assertEquals(replayScratch, memory.reservedBytes(),
                "cursor EOF must retain scratch for the last equal-key block");

        buffer.close();
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        managed.release(root);
    }

    @Test
    void byteNativeDecodeAndCopyStartOnlyAfterFullCreditReservation() {
        DataRow row = row(7, repeat('b', 91));
        RowSerializer serializer = new RowSerializer(META);
        byte[] encoded = serializer.serialize(row);
        long estimate = RowSizeEstimator.estimateEncodedUpperBound(META.getFieldCount(), encoded.length);
        long required = Math.addExact(
                Math.addExact(ExternalSortMemory.rowCredit(encoded.length, estimate, 0),
                        ExternalSortMemory.BATCH_FIXED_CREDIT),
                Math.addExact(Math.multiplyExact(2L, estimate),
                        ExternalSortMemory.finalHeadCredit(encoded.length, estimate)));
        MemoryManager memory = new MemoryManager(required - 1);
        HeapPool pool = new HeapPool();
        PointerSortBuffer buffer = pointer(memory, pool, 1024);

        assertThrows(MemoryBudgetExceededException.class, () -> buffer.putBytes(encoded));
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
        buffer.close();

        MemoryManager exactMemory = new MemoryManager(required);
        HeapPool exactPool = new HeapPool();
        PointerSortBuffer exact = pointer(exactMemory, exactPool, 1024);
        exact.putBytes(encoded);
        exact.sort();
        DataRow decoded = exact.iterator().next();
        assertEquals(row.get(0), decoded.get(0));
        assertEquals(row.get(1), decoded.get(1));
        exact.close();
        assertEquals(0, exactMemory.reservedBytes());
        assertEquals(0, exactPool.outstanding());
    }

    @Test
    void pointerAndPipelinedMustSharePrimitiveBatchAndMergeFormulaSources() throws Exception {
        String pointer = source("PointerSortBuffer.java");
        String pipelined = source("PipelinedExternalSortBuffer.java");
        String runs = source("SortedRunStore.java");

        assertTrue(pointer.contains("EncodedSortBatch"));
        assertTrue(pipelined.contains("EncodedSortBatch"));
        assertTrue(pointer.contains("ExternalSortMergePlanner"));
        assertTrue(pipelined.contains("ExternalSortMergePlanner"));
        assertFalse(runs.contains("Integer[] order"));
        assertFalse(runs.contains("Arrays.sort(order"));
        assertFalse(pointer.contains("serializer.serialize(row)"));
    }

    @Test
    void synchronousSpillFailureKeepsRootAndCloseReleasesBatchOwnership() {
        RuntimeException root = new RuntimeException("pointer-spill-root");
        AtomicBoolean aborted = new AtomicBoolean();
        SpillStore spill = new SpillStore() {
            @Override public SpillRun createRun() {
                return new SpillRun() {
                    @Override public void append(byte[] frame) { throw root; }
                    @Override public void finishWriting() { }
                    @Override public Iterator<byte[]> read() { return Collections.emptyIterator(); }
                    @Override public void abort() { aborted.set(true); }
                };
            }
            @Override public void close() { }
        };
        MemoryManager memory = new MemoryManager(128 * 1024);
        HeapPool pool = new HeapPool();
        PointerSortBuffer buffer = new PointerSortBuffer(
                META, new RowComparator(META, SORT), SORT, pool, memory, 1, spill);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> buffer.putRow(row(1, "fails")));

        assertEquals(root, actual);
        assertTrue(aborted.get());
        assertTrue(memory.reservedBytes() > 0, "failed batch remains owned until close");
        buffer.close();
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, pool.outstanding());
    }

    private static PointerSortBuffer pointer(MemoryManager memory, HeapPool pool, int threshold) {
        RowComparator comparator = new RowComparator(META, SORT);
        return new PointerSortBuffer(META, comparator, SORT, pool, memory, threshold,
                new MemorySpillStore());
    }

    private static DataRow row(long key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static String repeat(char value, int count) {
        StringBuilder result = new StringBuilder(count);
        for (int i = 0; i < count; i++) result.append(value);
        return result.toString();
    }

    private static String source(String file) throws Exception {
        Path cwd = Paths.get("").toAbsolutePath();
        Path path = cwd.resolve("src/main/java/io/rowweave/mem").resolve(file);
        if (!Files.exists(path)) {
            path = cwd.resolve("rowweave-core/src/main/java/io/rowweave/mem").resolve(file);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
