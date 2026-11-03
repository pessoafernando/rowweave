package io.rowweave.mem.spill;

/** Spill capability whose reader reserves and validates every frame before allocation. */
public interface BoundedReplayStore extends SpillStore {

    @Override
    BoundedReplayRun createRun();
}
