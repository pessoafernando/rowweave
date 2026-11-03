package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.MemorySpillStore;
import io.rowweave.mem.spill.SpillStore;

import java.util.Iterator;
import java.util.function.Supplier;

/**
 * Compatibility entry point for the older segmented group buffer.
 *
 * <p>The production path uses {@link OpenAddressingGroupAggBuffer}. This class keeps the public name,
 * constructor signature and diagnostic probes, and delegates everything inside to the single
 * supported implementation.
 *
 * @deprecated use {@link OpenAddressingGroupAggBuffer} directly; this class exists only for code that already constructs it.
 */
@Deprecated
public final class SpillingSegmentGroupAggBuffer implements GroupAggBuffer, MemorySpillable {

    private final OpenAddressingGroupAggBuffer delegate;

    public SpillingSegmentGroupAggBuffer(DataSchema dataSchema, String[] keyFields,
                                         Supplier<Object[]> accumulatorFactory, MemoryManager memoryManager,
                                         MemorySegmentPool pool) {
        this(dataSchema, keyFields, accumulatorFactory, memoryManager, pool, new MemorySpillStore());
    }

    public SpillingSegmentGroupAggBuffer(DataSchema dataSchema, String[] keyFields,
                                         Supplier<Object[]> accumulatorFactory, MemoryManager memoryManager,
                                         MemorySegmentPool pool, SpillStore spillStore) {
        this.delegate = new OpenAddressingGroupAggBuffer(
                dataSchema, keyFields, accumulatorFactory, memoryManager, pool, spillStore);
    }

    @Override
    public Object[] getOrCreate(byte[] keyBytes) {
        return delegate.getOrCreate(keyBytes);
    }

    @Override
    public long spill() {
        return delegate.spill();
    }

    @Override
    public long groupCount() {
        return delegate.groupCount();
    }

    @Override
    public Iterator<Group> iterator() {
        return delegate.iterator();
    }

    @Override
    public void close() {
        delegate.close();
    }

    /** Diagnostic probe kept for compatibility; its counters mean exactly what the production implementation reports. */
    public int recursiveRepartitions() {
        return delegate.recursiveRepartitions();
    }
}
