package io.rowweave.run;

class InMemoryRunRetentionAuthorityContractTest extends RunRetentionAuthorityContract {
    @Override
    protected RunStateStore newStore() {
        return new InMemoryRunStateStore(
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD, () -> 100L);
    }
}
