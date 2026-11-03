package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The external sort merge keeps its fan-in bounded. With runThreshold=1, N rows where N
 * greatly exceeds mergeFanIn produce N spilled runs, and the merge must hold at most
 * mergeFanIn, which is 128, open file handles at once through a multi-pass merge, while the
 * sorted result stays bit-identical to the JDK stable sort.
 */
class BoundedMergeFanInTest {

    private static final DataSchema NUM = new DataSchema(new Field("n", DataType.IntegerType));
    private static final int FANIN = WorkerConfig.mergeFanIn();  // the default is 128

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    private static List<Integer> expectedSorted(List<Integer> input, RowComparator cmp) {
        List<DataRow> rows = new ArrayList<>();
        for (Integer v : input) rows.add(row(v));
        rows.sort(cmp);
        List<Integer> out = new ArrayList<>();
        for (DataRow r : rows) out.add((Integer) r.get(0));
        return out;
    }

    private List<Integer> input(int n) {
        List<Integer> in = new ArrayList<>(n);
        for (int i = 0; i < n; i++) in.add((i * 7919 + 13) % 1000);  // pseudo-random with repeats, to check stability
        return in;
    }

    // Several rows per run, which is more than one: a cursor does not reach EOF right after its first frame, so every handle stays open while the heap is built and the real concurrency peak becomes measurable.
    private static final int ROWS_PER_RUN = 5;

    /** SpillingSegmentSortBuffer: more than FANIN multi-row runs, a handle peak at or below FANIN, and a bit-identical result. */
    @Test
    void segmentSortBufferBoundedFanIn(@TempDir Path tmp) {
        int runs = FANIN * 3 + 17;  // far beyond the fan-in, so several passes are forced
        int n = runs * ROWS_PER_RUN;
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        RowComparator cmp = new RowComparator(NUM, sf);
        DiskSpillStore store = new DiskSpillStore(new File(tmp.toFile(), "seg"));
        SpillingSegmentSortBuffer buf = new SpillingSegmentSortBuffer(
                NUM, cmp, new HeapPool(), new MemoryManager(512 * 1024), ROWS_PER_RUN, store);
        List<Integer> in = input(n);
        for (int v : in) buf.putRow(row(v));
        buf.sort();
        List<Integer> got = new ArrayList<>();
        for (DataRow r : buf) got.add((Integer) r.get(0));
        assertTrue(store.runsCreated() > FANIN, "more runs than the fan-in are created (measured " + store.runsCreated() + ")");
        assertTrue(store.peakOpenReaders() <= FANIN,
                "the peak of handles open at once during the merge stays at or below " + FANIN + ", measured " + store.peakOpenReaders());
        assertEquals(expectedSorted(in, cmp), got, "the multi-pass merge matches the JDK stable sort row for row");
    }

    /**
     * When the merge iterator is abandoned early, before EOF as a downstream LIMIT would,
     * the FrameIterator handles of the RunCursors left in the heap leak until GC. Assert that
     * store.close() shuts every handle still open, so currentOpenReaders returns to zero and
     * no file descriptor leaks.
     */
    @Test
    void abandonedMergeIteratorHandlesClosedOnStoreClose(@TempDir Path tmp) {
        int runs = 20;  // create several runs, whose final merge opens a number of handles at once
        int n = runs * ROWS_PER_RUN;
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        RowComparator cmp = new RowComparator(NUM, sf);
        DiskSpillStore store = new DiskSpillStore(new File(tmp.toFile(), "abandon"));
        SpillingSegmentSortBuffer buf = new SpillingSegmentSortBuffer(
                NUM, cmp, new HeapPool(), new MemoryManager(512 * 1024), ROWS_PER_RUN, store);
        for (int v : input(n)) buf.putRow(row(v));
        buf.sort();
        // read only the first few rows and stop, so the cursors left in the merge heap keep their handles open.
        java.util.Iterator<DataRow> it = buf.iterator();
        for (int i = 0; i < 3 && it.hasNext(); i++) it.next();
        assertTrue(store.currentOpenReaders() > 0, "abandoning early leaves handles still open (measured " + store.currentOpenReaders() + ")");
        store.close();
        assertEquals(0, store.currentOpenReaders(), "store.close() closes every leaked handle");
    }

    /** SpillingSortBuffer: the same assertions. */
    @Test
    void sortBufferBoundedFanIn(@TempDir Path tmp) {
        int runs = FANIN * 3 + 17;
        int n = runs * ROWS_PER_RUN;
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        RowComparator cmp = new RowComparator(NUM, sf);
        DiskSpillStore store = new DiskSpillStore(new File(tmp.toFile(), "plain"));
        SpillingSortBuffer buf = new SpillingSortBuffer(
                NUM, cmp, new MemoryManager(512 * 1024), ROWS_PER_RUN, store);
        List<Integer> in = input(n);
        for (int v : in) buf.putRow(row(v));
        buf.sort();
        List<Integer> got = new ArrayList<>();
        for (DataRow r : buf) got.add((Integer) r.get(0));
        assertTrue(store.runsCreated() > FANIN, "more runs than the fan-in are created");
        assertTrue(store.peakOpenReaders() <= FANIN,
                "the peak of handles open at once during the merge stays at or below " + FANIN + ", measured " + store.peakOpenReaders());
        assertEquals(expectedSorted(in, cmp), got, "the multi-pass merge matches the JDK stable sort row for row");
    }
}
