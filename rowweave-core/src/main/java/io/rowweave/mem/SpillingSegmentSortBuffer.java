package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillStore;

import java.util.Comparator;
import java.util.Iterator;

/**
 * @deprecated production code uses {@link ExternalSortBuffers}; this class stays for source compatibility and delegates to the
 * {@link PointerSortBuffer} and {@link SortedRunStore} external-sort core.
 */
@Deprecated
public final class SpillingSegmentSortBuffer implements SortBuffer {

    private final PointerSortBuffer delegate;

    public SpillingSegmentSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator,
                                     MemorySegmentPool pool, MemoryManager memoryManager,
                                     int runThreshold) {
        this(dataSchema, comparator, pool, memoryManager, runThreshold, new MemorySpillStore());
    }

    public SpillingSegmentSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator,
                                     MemorySegmentPool pool, MemoryManager memoryManager,
                                     int runThreshold, SpillStore spillStore) {
        this.delegate = PointerSortBuffer.comparatorOnly(
                dataSchema, comparator, pool, memoryManager, runThreshold, spillStore);
    }

    @Override public void putRow(DataRow row) { delegate.putRow(row); }

    @Override public void sort() { delegate.sort(); }

    @Override public long size() { return delegate.size(); }

    @Override public Iterator<DataRow> iterator() { return delegate.iterator(); }

    @Override public void cancel(Throwable cause) { delegate.cancel(cause); }

    @Override public void close() { delegate.close(); }
}
