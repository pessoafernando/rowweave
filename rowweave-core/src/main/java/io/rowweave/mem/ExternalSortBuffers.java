package io.rowweave.mem;

import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.ops.SortField;

import java.util.Comparator;

/** Single construction entry for the production synchronous breaker; PlanOps selects the Pipelined path for OrderBy separately. */
public final class ExternalSortBuffers {

    private ExternalSortBuffers() {
    }

    public static SortBuffer pointer(DataSchema dataSchema, Comparator<DataRow> comparator, SortField[] sortFields,
                                     MemorySegmentPool pool, MemoryManager memoryManager,
                                     int runThreshold, SpillStore spillStore) {
        return pointer(dataSchema, comparator, sortFields, pool, memoryManager, runThreshold, spillStore, false);
    }

    /** Sort feeding a grouped replay consumer retains one serialization scratch until the buffer closes. */
    public static SortBuffer pointerForGroupedReplay(
            DataSchema dataSchema, Comparator<DataRow> comparator, SortField[] sortFields,
            MemorySegmentPool pool, MemoryManager memoryManager,
            int runThreshold, SpillStore spillStore) {
        return pointer(dataSchema, comparator, sortFields, pool, memoryManager, runThreshold, spillStore, true);
    }

    private static SortBuffer pointer(DataSchema dataSchema, Comparator<DataRow> comparator, SortField[] sortFields,
                                      MemorySegmentPool pool, MemoryManager memoryManager,
                                      int runThreshold, SpillStore spillStore,
                                      boolean groupedReplayConsumer) {
        if (pool == null || memoryManager == null || spillStore == null) {
            throw new IllegalArgumentException("pointer sort requires pool, memoryManager and spillStore");
        }
        return new PointerSortBuffer(dataSchema, comparator, sortFields, pool, memoryManager,
                runThreshold, spillStore, groupedReplayConsumer);
    }

}
