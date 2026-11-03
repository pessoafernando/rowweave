package io.rowweave.run;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileRunDeadlineAuthorityContractTest extends RunDeadlineAuthorityContract {
    @TempDir
    Path temp;

    private final AtomicInteger stores = new AtomicInteger();

    @Override
    protected RunStateStore newStore(LongSupplier authorityClock) {
        return new FileRunStateStore(
                temp.resolve("store-" + stores.incrementAndGet()), () -> { },
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD, authorityClock);
    }

    @Test
    void acceptedDeadlineReplaysAfterStoreReopen() {
        Path root = temp.resolve("reopen");
        MutableClock clock = new MutableClock(100L);
        RunStateStore first = new FileRunStateStore(root, () -> { },
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD, clock);
        create(first, "run", 100L);
        assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                authority(first).evaluateDeadline("run").disposition());

        clock.set(101L);
        RunStateStore reopened = new FileRunStateStore(root, () -> { },
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD, clock);
        RunDeadlineResult replay = authority(reopened).evaluateDeadline("run");
        assertEquals(RunDeadlineResult.Disposition.ALREADY_REQUESTED,
                replay.disposition());
        assertEquals(2L, replay.revision());
        assertEquals(2, reopened.events("run", 0L, 10).size());
    }
}
