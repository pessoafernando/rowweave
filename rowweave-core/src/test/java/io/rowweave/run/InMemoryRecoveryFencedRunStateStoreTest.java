package io.rowweave.run;

class InMemoryRecoveryFencedRunStateStoreTest
        extends RecoveryFencedRunStateStoreContract {
    @Override
    protected RunStateStore newStore() {
        return new InMemoryRunStateStore();
    }
}
