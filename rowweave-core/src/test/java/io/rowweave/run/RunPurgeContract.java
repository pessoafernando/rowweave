package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Final purge and DELETING visibility semantics for the InMemory and File backends. */
public abstract class RunPurgeContract {
    protected abstract RunStateStore newStore(int lifecycleCapacity);

    @Test
    void deletingHasExplicitReadWriteContractAndCurrentTokenPurgesAtomically() {
        RunStateStore store = newStore(RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD);
        RunRetentionAuthority retention = (RunRetentionAuthority) store;
        RunSnapshot terminal = terminal(store, "run");
        List<ExecutionEvent> completeHistory = store.events("run", 0L, 1000);
        RunLifecycleRef ref = ((RunLifecycleCatalog) store).lifecycle("run");
        RunRetentionClaim first = retention.claim(
                "run", terminal.revision(), ref.terminalAtMillis(),
                new RetentionOwner("master-a", 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, first.code());

        assertEquals(terminal.revision(), store.get("run").revision());
        assertThrows(RunHistoryRetiringException.class,
                () -> store.events("run", 0L, 1000));
        RunDecision replayDuringDelete = store.apply(RunCommand.cancelReadyRun(
                "cancel-run", "run", 1L, 2L, "CANCELED", "terminal"));
        assertEquals(RunDecision.Code.RETENTION_IN_PROGRESS,
                replayDuringDelete.code());
        assertEquals(terminal.revision(), replayDuringDelete.snapshot().revision());

        RunRetentionClaim replacement = retention.claim(
                "run", terminal.revision(), ref.terminalAtMillis(),
                new RetentionOwner("master-b", 2L), first.token().ownershipEpoch());
        assertEquals(RunRetentionClaim.Code.ACQUIRED, replacement.code());
        assertEquals(RunPurgeProgress.Code.STALE_OWNER,
                retention.purge(first.token(), 1).code());
        assertEquals(RunPurgeProgress.Code.PURGED,
                retention.purge(replacement.token(), 1).code());

        assertNull(store.get("run"));
        assertEquals(java.util.Collections.emptyList(), store.events("run", 0L, 1000));
        assertNull(((RunLifecycleCatalog) store).lifecycle("run"));
        assertFalse(retention.current(replacement.token()));
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.createRun(
                        "recreate", "run", "definition", "artifact-new",
                        1, 0L, 3L)).code());
        assertEquals(2, completeHistory.size());
    }

    @Test
    void finalPurgeReleasesTheExactLifecycleCapacitySlot() {
        List<String> ids = sameShardIds(2);
        RunStateStore store = newStore(1);
        RunRetentionAuthority retention = (RunRetentionAuthority) store;
        RunSnapshot terminal = terminal(store, ids.get(0));
        RunLifecycleRef ref = ((RunLifecycleCatalog) store).lifecycle(ids.get(0));
        assertEquals(RunDecision.Code.LIMIT_EXCEEDED,
                create(store, ids.get(1)).code());
        RunRetentionClaim claim = retention.claim(
                ids.get(0), terminal.revision(), ref.terminalAtMillis(),
                new RetentionOwner("master", 1L), 0L);

        assertEquals(RunPurgeProgress.Code.PURGED,
                retention.purge(claim.token(), 1).code());
        assertEquals(RunDecision.Code.ACCEPTED, create(store, ids.get(1)).code());
    }

    @Test
    void purgeRejectsInvalidWorkBounds() {
        RunRetentionAuthority retention =
                (RunRetentionAuthority) newStore(1);
        RetentionToken token = RetentionToken.create(
                "run", new RetentionOwner("master", 1L), 1L, 1L, 1L);
        assertThrows(IllegalArgumentException.class, () -> retention.purge(null, 1));
        assertThrows(IllegalArgumentException.class, () -> retention.purge(token, 0));
        assertThrows(IllegalArgumentException.class, () -> retention.purge(token, 1001));
    }

    private static RunSnapshot terminal(RunStateStore store, String runId) {
        RunDecision created = create(store, runId);
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision canceled = store.apply(RunCommand.cancelReadyRun(
                "cancel-" + runId, runId, created.snapshot().revision(), 2L,
                "CANCELED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, canceled.code());
        return canceled.snapshot();
    }

    private static RunDecision create(RunStateStore store, String runId) {
        return store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact-" + runId,
                1, 0L, 1L));
    }

    private static List<String> sameShardIds(int count) {
        java.util.ArrayList<String> ids = new java.util.ArrayList<>();
        int target = -1;
        for (int value = 0; ids.size() < count; value++) {
            String id = "purge-capacity-" + value;
            String digest = RunLifecycleCatalogKernel.digestHex(id);
            int shard = RunLifecycleCatalogKernel.shard(digest);
            if (target < 0) target = shard;
            if (shard == target) ids.add(id);
        }
        return ids;
    }
}
