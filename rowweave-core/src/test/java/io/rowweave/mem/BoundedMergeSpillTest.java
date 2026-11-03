package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillRun;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Running out of file descriptors, which a large-data benchmark exposed:
 * {@link PointerSortBuffer} used to open every spilled run at once in {@code iterator()},
 * one handle each, so tens of thousands of runs from tens of millions of rows under a small
 * budget produced "Too many open files" and broke the no-OOM invariant.
 *
 * <p>Here a handle-counting SpillStore pins down that the number of simultaneously open
 * runs stays bounded, through a bounded multi-pass merge, however many runs exist, and that
 * the merged result is still bit-identical to the authoritative sort.
 */
class BoundedMergeSpillTest {

    private static final DataSchema NUM = new DataSchema(new Field("n", DataType.IntegerType));

    private static DataRow row(int v) {
        DataRow r = new DataRow(1);
        r.set(0, v);
        return r;
    }

    /** Wraps MemorySpillStore and tracks the peak number of runs open at once, meaning read but not exhausted. */
    private static final class CountingSpillStore implements SpillStore {
        private final MemorySpillStore delegate = new MemorySpillStore();
        final AtomicInteger openNow = new AtomicInteger();
        final AtomicInteger openPeak = new AtomicInteger();
        int runsCreated;

        @Override
        public SpillRun createRun() {
            runsCreated++;
            SpillRun inner = delegate.createRun();
            return new SpillRun() {
                @Override public void append(byte[] frame) { inner.append(frame); }
                @Override public void finishWriting() { inner.finishWriting(); }
                @Override
                public Iterator<byte[]> read() {
                    Iterator<byte[]> it = inner.read();
                    int now = openNow.incrementAndGet();
                    openPeak.accumulateAndGet(now, Math::max);
                    return new Iterator<byte[]>() {
                        private boolean closed;
                        @Override public boolean hasNext() {
                            boolean h = it.hasNext();
                            if (!h) { close(); }
                            return h;
                        }
                        @Override public byte[] next() { return it.next(); }
                        private void close() {
                            if (!closed) { closed = true; openNow.decrementAndGet(); }
                        }
                    };
                }
            };
        }

        @Override public void close() { delegate.close(); }
    }

    /**
     * runThreshold=1 with many rows makes one run per row, so thousands of runs. The merge
     * must open them with a bounded peak, some small constant such as 128 with a safety
     * margin, and must never equal runsCreated, which is the pathology of the old single-pass
     * merge. The result stays bit-identical.
     */
    @Test
    void manyRunsMergeKeepsOpenFilesBounded() {
        int n = 5000;  // 5000 runs at runThreshold=1, so opening one handle per run would reach 5000
        SortField sf = new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST);
        RowComparator cmp = new RowComparator(NUM, sf);
        MemorySegmentPool pool = new HeapPool();
        CountingSpillStore store = new CountingSpillStore();
        PointerSortBuffer buf = new PointerSortBuffer(
                NUM, cmp, new SortField[]{sf}, pool, new MemoryManager(512 * 1024), 1, store);

        List<Integer> input = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            int v = (i * 2654435761L % 100000) >= 0 ? (int) (i * 2654435761L % 100000) : 0;
            input.add(v);
            buf.putRow(row(v));
        }
        buf.sort();

        List<Integer> got = new ArrayList<>(n);
        for (DataRow r : buf) {
            got.add((Integer) r.get(0));
        }

        // the result is bit-identical to the authoritative sort
        input.sort(null);
        assertEquals(input, got, "the merged result matches the ordered expectation");
        assertTrue(store.runsCreated >= 1000, "a large number of runs really are produced (runThreshold=1): " + store.runsCreated);
        // The key assertion: the number of open run handles has an upper bound far below the run count, which is the bounded multi-pass merge.
        assertTrue(store.openPeak.get() <= 256,
                "the bounded merge opens at most 256 run handles at once, measured peak=" + store.openPeak.get()
                        + " / total runs=" + store.runsCreated);
        assertEquals(0, store.openNow.get(), "no handle leaks once every run is drained");
    }
}
