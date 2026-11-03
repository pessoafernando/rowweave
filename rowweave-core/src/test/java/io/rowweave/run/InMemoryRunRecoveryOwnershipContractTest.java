package io.rowweave.run;

class InMemoryRunRecoveryOwnershipContractTest extends RunRecoveryOwnershipContract {
    @Override
    protected RunStateStore newStore() {
        return new InMemoryRunStateStore();
    }
}
