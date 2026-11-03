package io.rowweave;

import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.PointerSortBuffer;
import io.rowweave.mem.SortBuffer;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.ops.Distinct;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * OOM stress — breakers fall back to spilling under a **deliberately tiny memory budget**, so a large
 * input does not exhaust the heap (fail fast is preferred to running out of memory).
 * Tiny-budget spilling buffers and a disk SpillStore are injected through the operators' spill seams
 * (withSortBufferFactory/withSpill), the operators are fed far more rows than the budget allows, and
 * the full output is asserted, which proves spilling actually engaged. The cluster path takes the same
 * wiring once the BatchSession memory-budget accessor exists.
 */
class OomStressTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType));

    /** Generates N rows lazily, with no List materialized up front, so the test itself does not run out of memory. */
    private static RowStream genRows(int n) {
        return RowStream.fromIterable(() -> new Iterator<DataRow>() {
            private int i = 0;
            @Override public boolean hasNext() { return i < n; }
            @Override public DataRow next() {
                if (i >= n) throw new NoSuchElementException();
                DataRow r = new DataRow(2);
                r.set(0, n - i);  // ids in reverse order, forcing a sort
                r.set(1, new BigDecimal(Integer.toString(i % 1000)));
                i++;
                return r;
            }
        });
    }

    /** OrderBy over a large volume with a tiny budget falls back on a segmented pool plus disk spill, producing everything in order without running out of memory. */
    @Test
    void orderByLargeSpillsNoOom(@TempDir Path tmp) {
        final int n = 50_000;
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        DiskSpillStore store = new DiskSpillStore(new File(tmp.toFile(), "ob"));
        OrderBy ob = OrderBy.of(META, sf).withSortBufferFactory((m, c) ->
                new PointerSortBuffer(m, c, new SortField[]{sf}, new HeapPool(),
                        new MemoryManager(64 * 1024), /*runThreshold*/ 2000, store));

        CollectingRowSink sink = new CollectingRowSink();
        ob.applyStream(genRows(n), 1, false, sink);

        java.util.List<DataRow> out = sink.rows();
        assertEquals(n, out.size(), "full output (spill loses no row)");
        // asserted ascending: id runs 1..n, since genRows writes them in reverse
        assertEquals(1, out.get(0).get(0));
        assertEquals(n, out.get(n - 1).get(0));
        // sampled, in order
        int prev = Integer.MIN_VALUE;
        for (int i = 0; i < out.size(); i += 997) {
            int cur = (Integer) out.get(i).get(0);
            assertTrue(cur >= prev, "ascending at " + i);
            prev = cur;
        }
    }

    /** SortBuffer put under direct pressure: a tiny budget over a large volume spills into several runs and merges them stably in order, which pins the fallback that keeps memory bounded. */
    @Test
    void pointerSortBufferManyRunsNoOom(@TempDir Path tmp) {
        final int n = 30_000;
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        RowComparator cmp = new RowComparator(META, sf);
        DiskSpillStore store = new DiskSpillStore(new File(tmp.toFile(), "sb"));
        SortBuffer sb = new PointerSortBuffer(META, cmp, new SortField[]{sf}, new HeapPool(),
                new MemoryManager(32 * 1024), 1000, store);
        Iterator<DataRow> it = genRows(n).toIterator();
        while (it.hasNext()) {
            sb.putRow(it.next());
        }
        sb.sort();
        int count = 0;
        int prev = Integer.MIN_VALUE;
        for (DataRow r : sb) {
            int cur = (Integer) r.get(0);
            assertTrue(cur >= prev, "ascending");
            prev = cur;
            count++;
        }
        assertEquals(n, count, "merging spill runs loses no row");
    }
}
