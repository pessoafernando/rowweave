package io.rowweave.run;

/** Optional control-plane backend provider; wiring it into the execution path is outside the current product contract. */
public interface RunStateStoreProvider {
    RunStateStore runStateStore();
}
