package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class InMemoryDurableExecutionEvidenceTest {

    @Test
    void terminalAndDeletingRetainEvidenceUntilAtomicFinalPurge() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = store.apply(RunCommand.createRun(
                "create", "evidence-run", "definition", "artifact", 1, 0L, 1L))
                .snapshot();
        run = store.apply(RunCommand.createAttempt(
                "attempt", run.runId(), run.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 100L, 0, 0, StageAssignmentRole.SINK,
                "worker", "localhost", 7001, -1, "incarnation");
        StageAssignmentSnapshot published = store.publish(
                new StageAssignmentManifest(
                        fence, run.artifactId(), 1L, Collections.singletonList(record)))
                .snapshot();
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                published.token(0), new byte[32], 3L);
        store.accept(acceptance);
        store.recordTerminal(StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 4L, 3L, 4L));

        run = advance(store, run, AttemptStatus.DISPATCHING, "dispatch", 5L, null);
        run = advance(store, run, AttemptStatus.RUNNING, "running", 6L, null);
        run = advance(store, run, AttemptStatus.COMMITTING, "committing", 7L, null);
        run = advance(store, run, AttemptStatus.SUCCEEDED, "succeeded", 8L,
                RunResultSummary.discard(3L));

        DurableExecutionEvidenceView terminal = store.executionEvidence(run.runId());
        assertEquals(DurableExecutionEvidenceView.State.PRESENT, terminal.state());
        assertEquals(run, terminal.snapshot());
        assertEquals(1, terminal.assignments().terminalCount());
        assertNull(store.assignmentSnapshot(fence),
                "terminal evidence must not reopen the mutable assignment authority");

        RunLifecycleRef lifecycle = store.lifecycle(run.runId());
        RunRetentionClaim claim = store.claim(
                run.runId(), run.revision(), lifecycle.terminalAtMillis(),
                new RetentionOwner("master", 10L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        assertEquals(DurableExecutionEvidenceView.State.PRESENT,
                store.executionEvidence(run.runId()).state());

        assertEquals(RunPurgeProgress.Code.PURGED,
                store.purge(claim.token(), 1000).code());
        assertEquals(DurableExecutionEvidenceView.State.MISSING,
                store.executionEvidence(run.runId()).state());
    }

    private static RunSnapshot advance(
            InMemoryRunStateStore store, RunSnapshot run, AttemptStatus status,
            String commandId, long time, RunResultSummary summary) {
        return store.apply(RunCommand.advanceAttempt(
                commandId, run.runId(), run.revision(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken(),
                status, false, time, null, null, summary)).snapshot();
    }
}
