package io.rowweave.mem.spill;

import io.rowweave.mem.MemoryManager;

/** A replay run with a non-prefetching, managed-memory reader. */
public interface BoundedReplayRun extends SpillRun {

    BudgetedFrameReader openBudgetedReader(
            MemoryManager memoryManager, int fieldCount, int maximumFrameLength);
}
