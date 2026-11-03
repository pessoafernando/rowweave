package io.rowweave.run;

import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

class FileRunPurgeContractTest extends RunPurgeContract {
    @TempDir
    Path temp;

    @Override
    protected RunStateStore newStore(int lifecycleCapacity) {
        return new FileRunStateStore(
                temp.resolve("store-" + System.nanoTime()), () -> { },
                lifecycleCapacity, () -> 100L);
    }
}
