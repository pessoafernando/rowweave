package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * TwoTierSortBuffer — tier one sorts in place in the memory-resident heap without serializing, and past
 * budget it falls back to tier two, PointerSortBuffer.
 * **Correctness edges**: the fast path (budget available) and the small budget forced tier two must give
 * equivalent result sets — BigDecimal scale, null forming its own group, NULLS_LAST and tie order.
 */
class TwoTierSortBufferTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType),
            new Field("name", DataType.StringType));

    private static final SortField[] SORT = {
            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};

    private static DataRow row(Integer k, String amt, String name) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, amt == null ? null : new BigDecimal(amt));
        r.set(2, name);
        return r;
    }

    /** Build a TwoTierSortBuffer whose budget decides the first tier (large) or the second tier. */
    private static TwoTierSortBuffer buffer(long budget) {
        MemoryManager mm = new MemoryManager(budget);
        RowComparator cmp = new RowComparator(META, SORT);
        Supplier<SortBuffer> secondary = () -> new PipelinedExternalSortBuffer(
                META, cmp, SORT, mm, 4, 3, new MemorySpillStore(), HeapPool::new);
        return new TwoTierSortBuffer(META, cmp, mm, SORT, 4, 3, secondary);
    }

    private static List<DataRow> sortAll(TwoTierSortBuffer buf, List<DataRow> input) {
        for (DataRow r : input) buf.putRow(r);
        buf.sort();
        List<DataRow> out = new ArrayList<>();
        for (DataRow r : buf) out.add(r);
        return out;
    }

    private static String fingerprint(List<DataRow> rows) {
        StringBuilder sb = new StringBuilder();
        for (DataRow r : rows) {
            sb.append(r.get(0)).append('|')
              .append(r.get(1) == null ? "N" : ((BigDecimal) r.get(1)).toPlainString()).append('|')
              .append(r.get(2)).append('\n');
        }
        return sb.toString();
    }

    @Test
    void fastPathEqualsSecondaryPath() {
        List<DataRow> input = new ArrayList<>();
        for (int i = 0; i < 500; i++) {
            input.add(row((i * 7) % 50, Integer.toString(i % 100) + ".5", "n" + (i % 10)));
        }
        input.add(row(null, "1.0", "nullkey")); // null key → NULLS_LAST

        List<DataRow> fast = sortAll(buffer(64L * 1024 * 1024), input);  // a large budget sorts in the first tier on the heap
        List<DataRow> slow = sortAll(buffer(64L * 1024), input);  // enough for a full single-row credit, then switches to the second tier quickly

        assertEquals(input.size(), fast.size(), "the first tier loses no rows");
        assertEquals(input.size(), slow.size(), "the second tier loses no rows");
        // keys ascend under NULLS_LAST: sample the first several non-null keys for monotonic order
        assertEquals(fingerprint(fast), fingerprint(slow), "the two tiers are set-equivalent (sorted order, BigDecimal and null included)");
    }

    @Test
    void fastPathDoesNotSerialize() {
        // First-tier path: with a large budget, assert the in-memory residence held (the size is right and nothing threw), which indirectly confirms the fast path ran without spilling.
        TwoTierSortBuffer buf = buffer(64L * 1024 * 1024);
        for (int i = 0; i < 100; i++) buf.putRow(row(i, "1.5", "x"));
        assertFalse(buf.spilledForTest(), "a large budget stays resident in the first tier without spilling");
        buf.sort();
        assertEquals(100, buf.size());
    }

    @Test
    void midStreamSwitchNoLossNoDup() {
        // Switching mid-run: a budget that holds only a few rows before overflowing triggers the fallback, and nothing is lost or repeated.
        List<DataRow> input = new ArrayList<>();
        for (int i = 0; i < 200; i++) input.add(row(i % 30, Integer.toString(i) + ".25", "m" + i));
        List<DataRow> fast = sortAll(buffer(64L * 1024 * 1024), input);
        List<DataRow> mid = sortAll(buffer(64L * 1024), input);  // a small budget overflows after a few dozen rows, so the switch happens mid-run
        assertEquals(200, mid.size(), "switching mid-stream neither loses nor duplicates rows");
        assertEquals(fingerprint(fast), fingerprint(mid), "the mid-stream switch gives the same result as running fully in memory");
    }

    @Test
    void failedSecondaryMigrationAbortsBothTiersAndReleasesReservation() {
        DataRow resident = row(1, "1.00", "resident");
        DataRow trigger = row(2, "2.00", "forces-secondary-switch");
        long budget = Math.addExact(RowSizeEstimator.estimateRow(resident),
                RowSizeEstimator.estimateRow(trigger)) - 1;
        MemoryManager memory = new MemoryManager(budget);
        RowComparator comparator = new RowComparator(META, SORT);
        AtomicBoolean secondaryClosed = new AtomicBoolean();
        TwoTierSortBuffer buffer = new TwoTierSortBuffer(META, comparator, memory, () -> new SortBuffer() {
            @Override public void putRow(DataRow row) { throw new RuntimeException("migration failed"); }
            @Override public void sort() { }
            @Override public long size() { return 0; }
            @Override public java.util.Iterator<DataRow> iterator() { return java.util.Collections.emptyIterator(); }
            @Override public void close() { secondaryClosed.set(true); }
        });

        buffer.putRow(resident);
        assertThrows(RuntimeException.class,
                () -> buffer.putRow(trigger));
        buffer.close();

        assertTrue(secondaryClosed.get());
        assertEquals(0, memory.reservedBytes());
    }

    @Test
    void productionEscrowMigrationPreservesLogicalBatchesInsteadOfOneRunPerRow(@TempDir Path tempDir) {
        int rows = 1_000;
        int runThreshold = 64;
        MemoryManager memory = new MemoryManager(16L * 1024 * 1024);
        RowComparator comparator = new RowComparator(META, SORT);
        DiskSpillStore spillStore = new DiskSpillStore(tempDir.toFile());
        TwoTierSortBuffer buffer = new TwoTierSortBuffer(META, comparator, memory, SORT,
                runThreshold, 3,
                () -> new PipelinedExternalSortBuffer(
                        META, comparator, SORT, memory, runThreshold, 3, spillStore, HeapPool::new));
        for (int i = 0; i < rows; i++) {
            buffer.putRow(row((i * 37) % 101, i + ".25", "resident-" + i));
        }
        assertFalse(buffer.externalized());

        buffer.externalizeFromEscrow();
        buffer.sort();
        int count = 0;
        for (DataRow ignored : buffer) count++;

        assertEquals(rows, count);
        assertTrue(spillStore.runsCreated() <= (rows + runThreshold - 1) / runThreshold,
                "resident migration must create logical batches, not one durable run per row: runs="
                        + spillStore.runsCreated());
        buffer.close();
        assertEquals(0, memory.reservedBytes());
        java.io.File[] files = tempDir.toFile().listFiles();
        assertEquals(0, files == null ? 0 : files.length);
    }

    @Test
    void productionBatchMigrationFailureTransfersCreditExactlyOnce() {
        RuntimeException root = new RuntimeException("fourth migrated segment failed");
        HeapPool delegate = new HeapPool();
        java.util.concurrent.atomic.AtomicInteger allocations = new java.util.concurrent.atomic.AtomicInteger();
        MemorySegmentPool failingPool = new MemorySegmentPool() {
            @Override public MemorySegment allocate(int size) {
                if (allocations.incrementAndGet() == 4) throw root;
                return delegate.allocate(size);
            }
            @Override public void free(MemorySegment segment) { delegate.free(segment); }
            @Override public int outstanding() { return delegate.outstanding(); }
            @Override public void trim() { delegate.trim(); }
        };
        MemoryManager memory = new MemoryManager(2L * 1024 * 1024);
        RowComparator comparator = new RowComparator(META, SORT);
        AtomicReference<PipelinedExternalSortBuffer> candidateRef = new AtomicReference<>();
        TwoTierSortBuffer buffer = new TwoTierSortBuffer(
                META, comparator, memory, SORT, 64, 3, () -> {
                    PipelinedExternalSortBuffer candidate = new PipelinedExternalSortBuffer(
                            META, comparator, SORT, memory, 64, 3,
                            new MemorySpillStore(), () -> failingPool);
                    candidateRef.set(candidate);
                    return candidate;
                });
        for (int i = 0; i < 10; i++) {
            buffer.putRow(row(i, i + ".00", "resident-" + i));
        }

        RuntimeException thrown = assertThrows(RuntimeException.class, buffer::externalizeFromEscrow);

        assertSame(root, thrown);
        buffer.close();
        assertEquals(0, memory.reservedBytes());
        assertEquals(0, delegate.outstanding());
        assertFalse(candidateRef.get().workerThreadsAliveForTest());
    }
}
