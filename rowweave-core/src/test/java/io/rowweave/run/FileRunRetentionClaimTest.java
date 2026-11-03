package io.rowweave.run;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FileRunRetentionClaimTest {
    @TempDir
    Path temp;

    @Test
    void forceThenAckLossReopensAsTheSameOwnerReplay() {
        AtomicInteger publications = new AtomicInteger();
        FileRunStateStore first = new FileRunStateStore(temp, () -> {
            if (publications.incrementAndGet() == 3) {
                throw new IllegalStateException("simulated retention ACK loss");
            }
        }, RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD, () -> 100L);
        RunSnapshot terminal = terminal(first, "run");
        RunLifecycleRef ref = first.lifecycle("run");
        RetentionOwner owner = new RetentionOwner("master", 7L);

        assertThrows(IllegalStateException.class, () -> first.claim(
                "run", terminal.revision(), ref.terminalAtMillis(), owner, 0L));

        FileRunStateStore reopened = new FileRunStateStore(temp);
        RunRetentionClaim replay = reopened.claim(
                "run", terminal.revision(), ref.terminalAtMillis(), owner, 0L);
        assertEquals(RunRetentionClaim.Code.REPLAYED, replay.code());
        assertEquals(1L, replay.token().ownershipEpoch());
        assertEquals(replay.token(), reopened.lifecycle("run").retentionToken());
    }

    private static RunSnapshot terminal(RunStateStore store, String runId) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create", runId, "definition", "artifact", 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision canceled = store.apply(RunCommand.cancelReadyRun(
                "cancel", runId, created.snapshot().revision(), 2L,
                "CANCELED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, canceled.code());
        return canceled.snapshot();
    }
}
