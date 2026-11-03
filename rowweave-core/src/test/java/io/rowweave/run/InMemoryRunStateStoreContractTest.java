package io.rowweave.run;

class InMemoryRunStateStoreContractTest extends RunStateStoreContract {
    @Override
    protected RunStateStore newStore() {
        return new InMemoryRunStateStore();
    }
}
