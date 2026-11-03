package io.rowweave.run;

import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

class FileRunStateStoreContractTest extends RunStateStoreContract {
    @TempDir
    Path temporary;

    private final AtomicInteger stores = new AtomicInteger();

    @Override
    protected RunStateStore newStore() {
        return new FileRunStateStore(temporary.resolve("store-" + stores.incrementAndGet()));
    }
}
