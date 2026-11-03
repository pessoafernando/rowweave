package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.InMemoryRunStateStore;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.CancellationCause;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InheritedRunRecoveryModelTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));

    @Test
    void unsupportedAttemptsRemainPendingAndDeadlineRunRemainsRecoverable() {
        RunSnapshot attempts = createReady("unsupported-attempts", "definition", "artifact", 4, 17L);
        assertEquals(InheritedRunRecovery.Eligibility.PENDING,
                InheritedRunRecovery.eligibility(attempts));

        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot deadline = accepted(store, RunCommand.createRun(
                "create-deadline", "unsupported-deadline", "definition", "artifact",
                1, 100L, 17L));
        assertEquals(InheritedRunRecovery.Eligibility.EXECUTE_READY,
                InheritedRunRecovery.eligibility(deadline));
        assertEquals(deadline.runId(), InheritedRunRecovery.identityFor(deadline).runId());
    }

    @Test
    void readyIdentityIsDeterministicAndIndependentOfOwnerEpoch() {
        RunSnapshot ready = createReady("ready", "definition", "artifact", 1, 17L);
        InheritedRunRecovery.Identity first = InheritedRunRecovery.identityFor(ready);
        InheritedRunRecovery.Identity repeated = InheritedRunRecovery.identityFor(ready);

        assertEquals(InheritedRunRecovery.Eligibility.EXECUTE_READY,
                InheritedRunRecovery.eligibility(ready));
        assertEquals(first, repeated);
        assertTrue(first.attemptId().startsWith("recovery-attempt-v1-"));
        assertTrue(first.publicationId().startsWith("recovery-publication-v1-"));
        assertEquals(ready.updatedAtMillis(), first.eventTimeMillis());
        assertEquals(first.commandId("create-attempt"),
                repeated.commandId("create-attempt"));

        RecoveryToken ownerOne = RecoveryToken.create(
                ready.runId(), new RecoveryOwner("master-a", 1L), 1L, ready.revision());
        RecoveryToken ownerTwo = RecoveryToken.create(
                ready.runId(), new RecoveryOwner("master-b", 2L), 2L, ready.revision());
        assertNotEquals(ownerOne, ownerTwo);
        assertEquals(first, InheritedRunRecovery.identityFor(ready));
    }

    @Test
    void createdIdentityPreservesAttemptPublicationFenceAndStartedTime() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot ready = accepted(store, RunCommand.createRun(
                "create", "created", "definition", "artifact", 1, 0L, 23L));
        RunSnapshot created = accepted(store, RunCommand.createAttempt(
                "attempt", "created", ready.revision(),
                "original-attempt", "original-publication", 29L));

        InheritedRunRecovery.Identity identity =
                InheritedRunRecovery.identityFor(created);

        assertEquals(InheritedRunRecovery.Eligibility.EXECUTE_CREATED,
                InheritedRunRecovery.eligibility(created));
        assertEquals("original-attempt", identity.attemptId());
        assertEquals("original-publication", identity.publicationId());
        assertEquals(created.currentAttempt().fenceToken(), identity.fenceToken());
        assertEquals(created.currentAttempt().createdAtMillis(),
                identity.eventTimeMillis());
    }

    @Test
    void acceptedInFlightAttemptsEnterReconciliationButAreNeverExecutable() {
        for (AttemptStatus target : new AttemptStatus[] {
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING, AttemptStatus.COMMITTING}) {
            InMemoryRunStateStore store = new InMemoryRunStateStore();
            RunSnapshot snapshot = accepted(store, RunCommand.createRun(
                    "create-" + target, "in-flight-" + target, "definition", "artifact",
                    1, 0L, 1L));
            snapshot = accepted(store, RunCommand.createAttempt(
                    "attempt-" + target, snapshot.runId(), snapshot.revision(),
                    "attempt", "publication", 2L));
            for (AttemptStatus step : new AttemptStatus[] {
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING, AttemptStatus.COMMITTING}) {
                snapshot = accepted(store, RunCommand.advanceAttempt(
                        "advance-" + target + '-' + step, snapshot.runId(), snapshot.revision(),
                        "attempt", snapshot.currentAttempt().fenceToken(), step,
                        false, 3L + step.ordinal(), null, null));
                if (step == target) break;
            }

            final RunSnapshot inFlight = snapshot;
            assertEquals(InheritedRunRecovery.Eligibility.RECONCILE_IN_FLIGHT,
                    InheritedRunRecovery.eligibility(inFlight));
            assertThrows(IllegalArgumentException.class,
                    () -> InheritedRunRecovery.identityFor(inFlight),
                    "in-flight authority must never produce a replay identity");
        }
    }

    @Test
    void userCancelingAttemptHasDedicatedReconciliationEligibility() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot snapshot = accepted(store, RunCommand.createRun(
                "create-canceling", "canceling", "definition", "artifact",
                1, 0L, 1L));
        snapshot = accepted(store, RunCommand.createAttempt(
                "attempt-canceling", snapshot.runId(), snapshot.revision(),
                "attempt", "publication", 2L));
        ((RunCancellationAuthority) store).requestUserCancellation(
                snapshot.runId(), "request-canceling");
        RunSnapshot canceling = store.get(snapshot.runId());

        assertEquals(InheritedRunRecovery.Eligibility.RECONCILE_CANCELLATION,
                InheritedRunRecovery.eligibility(canceling));
        assertThrows(IllegalArgumentException.class,
                () -> InheritedRunRecovery.identityFor(canceling));
        assertTrue(InheritedRunRecovery.inFlightCommandId(
                canceling, "user-canceled").startsWith(
                "run-recovery-v1-in-flight-user-canceled-"));
    }

    @Test
    void timeoutCancelingAttemptHasDedicatedReconciliationEligibility() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot snapshot = accepted(store, RunCommand.createRun(
                "create-timeout", "timeout", "definition", "artifact",
                1, 100L, 1L));
        snapshot = accepted(store, RunCommand.createAttempt(
                "attempt-timeout", snapshot.runId(), snapshot.revision(),
                "attempt", "publication", 2L));
        snapshot = accepted(store, RunCommand.advanceAttempt(
                "dispatch-timeout", snapshot.runId(), snapshot.revision(),
                "attempt", snapshot.currentAttempt().fenceToken(),
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        RunSnapshot canceling = accepted(store, RunCommand.requestCancellation(
                "request-timeout", snapshot.runId(), snapshot.revision(),
                "attempt", snapshot.currentAttempt().fenceToken(),
                CancellationCause.TIMEOUT, 4L, "RUN_DEADLINE", null));

        assertEquals(CancellationCause.TIMEOUT,
                canceling.currentAttempt().cancellationCause());
        assertEquals(InheritedRunRecovery.Eligibility.RECONCILE_CANCELLATION,
                InheritedRunRecovery.eligibility(canceling));
        assertTrue(InheritedRunRecovery.inFlightCommandId(
                canceling, "deadline-timed-out").startsWith(
                "run-recovery-v1-in-flight-deadline-timed-out-"));
    }

    @Test
    void candidateRejectsCrossRunTokenAndArtifactIdentityMismatch() {
        PlanArtifact expected = artifact("definition");
        RunSnapshot ready = createReady("candidate", "definition",
                expected.artifactId(), 1, 1L);
        RecoveryToken wrongRun = RecoveryToken.create(
                "other", new RecoveryOwner("master-a", 1L), 1L, 1L);
        RecoveryToken token = RecoveryToken.create(
                ready.runId(), new RecoveryOwner("master-a", 1L), 1L, ready.revision());

        assertThrows(IllegalArgumentException.class, () ->
                new InheritedRunRecovery.Candidate(ready, wrongRun, expected));
        assertThrows(IllegalArgumentException.class, () ->
                new InheritedRunRecovery.Candidate(ready, token, artifact("other-definition")));
        assertThrows(IllegalArgumentException.class,
                () -> InheritedRunRecovery.Result.pending(""));
        assertThrows(IllegalArgumentException.class,
                () -> InheritedRunRecovery.Result.retry(null));
    }

    private static RunSnapshot createReady(String runId, String definitionId,
                                           String artifactId, int maxAttempts,
                                           long eventTime) {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        return accepted(store, RunCommand.createRun(
                "create-" + runId, runId, definitionId, artifactId,
                maxAttempts, 0L, eventTime));
    }

    private static RunSnapshot accepted(InMemoryRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static PlanArtifact artifact(String definitionId) {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList())
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return LegacyPlanArtifactAdapter.wrap(definitionId, graph);
    }
}
