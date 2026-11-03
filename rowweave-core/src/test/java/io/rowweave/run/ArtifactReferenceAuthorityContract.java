package io.rowweave.run;

import io.rowweave.artifact.ArtifactBackfillStatus;
import io.rowweave.artifact.ArtifactReferenceAuthority;
import io.rowweave.artifact.ArtifactReferenceView;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Shared black-box contract for run authority and exact Artifact references. */
public abstract class ArtifactReferenceAuthorityContract {
    protected static final String ARTIFACT_A = repeat('a', 64);
    protected static final String ARTIFACT_B = repeat('b', 64);

    protected abstract RunStateStore newStore();

    @Test
    void createReplaySharedReferencesAndFinalPurgeAreExact() {
        RunStateStore store = newStore();
        ArtifactReferenceAuthority authority = (ArtifactReferenceAuthority) store;
        assertEquals(ArtifactBackfillStatus.Code.READY,
                authority.backfillStatus().code());
        assertEquals(ArtifactReferenceView.Code.READY_UNREFERENCED,
                authority.inspect(ARTIFACT_A).code());

        RunDecision first = store.apply(RunCommand.createRun(
                "create-1", "run-1", "definition", ARTIFACT_A, 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, first.code());
        assertEquals(1L, authority.inspect(ARTIFACT_A).referenceCount());
        assertEquals(RunDecision.Code.ALREADY_APPLIED,
                store.apply(RunCommand.createRun(
                        "create-1", "run-1", "definition", ARTIFACT_A,
                        1, 0L, 1L)).code());
        assertEquals(1L, authority.inspect(ARTIFACT_A).referenceCount());

        RunDecision second = store.apply(RunCommand.createRun(
                "create-2", "run-2", "definition", ARTIFACT_A, 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, second.code());
        assertEquals(2L, authority.inspect(ARTIFACT_A).referenceCount());
        assertEquals(ArtifactReferenceView.Code.READY_UNREFERENCED,
                authority.inspect(ARTIFACT_B).code());

        terminalAndPurge(store, "run-1", first.snapshot().revision(), "cancel-1");
        assertEquals(1L, authority.inspect(ARTIFACT_A).referenceCount());
        terminalAndPurge(store, "run-2", second.snapshot().revision(), "cancel-2");
        assertEquals(ArtifactReferenceView.Code.READY_UNREFERENCED,
                authority.inspect(ARTIFACT_A).code());
    }

    @Test
    void rejectedConflictAndNonCanonicalLegacyIdentityCannotForgeReference() {
        RunStateStore store = newStore();
        ArtifactReferenceAuthority authority = (ArtifactReferenceAuthority) store;
        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.createRun(
                        "legacy", "legacy-run", "definition", "legacy-artifact",
                        1, 0L, 1L)).code());
        assertEquals(ArtifactReferenceView.Code.READY_UNREFERENCED,
                authority.inspect(ARTIFACT_A).code());

        assertEquals(RunDecision.Code.ACCEPTED,
                store.apply(RunCommand.createRun(
                        "canonical", "canonical-run", "definition", ARTIFACT_A,
                        1, 0L, 1L)).code());
        assertEquals(RunDecision.Code.COMMAND_ID_CONFLICT,
                store.apply(RunCommand.createRun(
                        "canonical", "canonical-run", "definition", ARTIFACT_B,
                        1, 0L, 1L)).code());
        assertEquals(1L, authority.inspect(ARTIFACT_A).referenceCount());
        assertThrows(IllegalArgumentException.class,
                () -> authority.inspect("not-content-addressed"));
        assertEquals(ArtifactBackfillStatus.Code.READY,
                authority.advanceBackfill(1).code());
        assertThrows(IllegalArgumentException.class,
                () -> authority.advanceBackfill(0));
    }

    private static void terminalAndPurge(RunStateStore store, String runId,
                                         long revision, String commandId) {
        RunDecision terminal = store.apply(RunCommand.cancelReadyRun(
                commandId, runId, revision, 2L, "CANCELED", "done"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        RunLifecycleRef lifecycle = ((RunLifecycleCatalog) store).lifecycle(runId);
        RunRetentionClaim claim = ((RunRetentionAuthority) store).claim(
                runId, terminal.snapshot().revision(), lifecycle.terminalAtMillis(),
                new RetentionOwner("gc-" + runId, 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        RunPurgeProgress progress = null;
        for (int attempt = 0; attempt < 20; attempt++) {
            progress = ((RunRetentionAuthority) store).purge(claim.token(), 1000);
            if (progress.code() != RunPurgeProgress.Code.IN_PROGRESS) break;
        }
        assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
