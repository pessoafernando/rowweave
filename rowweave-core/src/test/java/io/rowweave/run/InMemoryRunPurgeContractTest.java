package io.rowweave.run;

class InMemoryRunPurgeContractTest extends RunPurgeContract {
    @Override
    protected RunStateStore newStore(int lifecycleCapacity) {
        return new InMemoryRunStateStore(
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                lifecycleCapacity, () -> 100L);
    }
}
