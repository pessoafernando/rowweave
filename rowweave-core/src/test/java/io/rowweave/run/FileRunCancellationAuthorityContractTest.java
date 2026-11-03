package io.rowweave.run;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FileRunCancellationAuthorityContractTest
        extends RunCancellationAuthorityContract {
    @TempDir
    Path temp;

    @Override
    protected RunStateStore newStore() {
        return new FileRunStateStore(temp.resolve("store-" + System.nanoTime()));
    }

    @Test
    void acceptedRequestReplaysAfterStoreReopen() {
        Path root = temp.resolve("reopen");
        RunStateStore first = new FileRunStateStore(root);
        create(first, "run");
        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                authority(first).requestUserCancellation("run", "request").disposition());

        RunStateStore reopened = new FileRunStateStore(root);
        RunCancellationResult replay = authority(reopened)
                .requestUserCancellation("run", "request");
        assertEquals(RunCancellationResult.Disposition.ALREADY_REQUESTED,
                replay.disposition());
        assertEquals(2L, replay.revision());
        assertEquals(2, reopened.events("run", 0L, 10).size());
    }
}
