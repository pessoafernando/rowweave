package io.rowweave.run;

class InMemoryRunCancellationAuthorityContractTest
        extends RunCancellationAuthorityContract {
    @Override
    protected RunStateStore newStore() {
        return new InMemoryRunStateStore();
    }
}
