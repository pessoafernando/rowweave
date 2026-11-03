package io.rowweave.run;

class InMemoryRecoverableRunCatalogContractTest extends RecoverableRunCatalogContract {
    @Override
    protected RunStateStore newStore() {
        return new InMemoryRunStateStore();
    }
}
