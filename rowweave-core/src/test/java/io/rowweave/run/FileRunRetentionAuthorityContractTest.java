package io.rowweave.run;

import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class FileRunRetentionAuthorityContractTest extends RunRetentionAuthorityContract {
    @TempDir
    Path temp;

    @Override
    protected RunStateStore newStore() {
        return new FileRunStateStore(temp.resolve("store-" + System.nanoTime()),
                () -> { }, RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                () -> 100L);
    }
}
