package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillStore;

import java.util.Comparator;
import java.util.Iterator;

/**
 * @deprecated production code uses {@link ExternalSortBuffers}; this class stays for source compatibility and delegates to the
 * {@link PointerSortBuffer} and {@link SortedRunStore} external-sort core.
 */
@Deprecated
public final class SpillingSortBuffer implements SortBuffer {

    private final HeapPool pool = new HeapPool();
    private final PointerSortBuffer delegate;
    private boolean closed;

    public SpillingSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator,
                              MemoryManager memoryManager, int runThreshold) {
        this(dataSchema, comparator, memoryManager, runThreshold, new MemorySpillStore());
    }

    public SpillingSortBuffer(DataSchema dataSchema, Comparator<DataRow> comparator,
                              MemoryManager memoryManager, int runThreshold, SpillStore spillStore) {
        this.delegate = PointerSortBuffer.comparatorOnly(
                dataSchema, comparator, pool, memoryManager, runThreshold, spillStore);
    }

    @Override public void putRow(DataRow row) { delegate.putRow(row); }

    @Override public void sort() { delegate.sort(); }

    @Override public long size() { return delegate.size(); }

    @Override public Iterator<DataRow> iterator() { return delegate.iterator(); }

    @Override public void cancel(Throwable cause) { delegate.cancel(cause); }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        Throwable failure = null;
        try {
            delegate.close();
        } catch (RuntimeException | Error cleanup) {
            failure = cleanup;
        }
        try {
            pool.trim();
        } catch (RuntimeException | Error cleanup) {
            if (failure == null) {
                failure = cleanup;
            } else if (failure != cleanup) {
                failure.addSuppressed(cleanup);
            }
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
    }
}
