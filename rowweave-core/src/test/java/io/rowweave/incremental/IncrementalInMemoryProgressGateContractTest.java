package io.rowweave.incremental;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** incremental attempts have no generic terminal bypass. */
class IncrementalInMemoryProgressGateContractTest {

    @Test
    void currentActiveAttemptAdvancesOnlyThroughNonTerminalExecutionProgress()
            throws Exception {
        Fixture fixture = claimedFixture();
        RunSnapshot current = fixture.store.runSnapshot(fixture.request.runId());
        assertEquals(AttemptStatus.CREATED, current.currentAttempt().status());

        for (AttemptStatus target : Arrays.asList(
                AttemptStatus.DISPATCHING,
                AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING)) {
            IncrementalAttemptProgressCommand command = progress(
                    fixture, current, target,
                    "progress-" + target.name(), current.updatedAtMillis() + 1L);
            RunDecision accepted = fixture.store.advanceProgress(
                    fixture.token, command);
            assertEquals(RunDecision.Code.ACCEPTED, accepted.code());
            assertEquals(target, accepted.snapshot().currentAttempt().status());
            assertEquals(1L, fixture.store.record("feed").revision(),
                    "Run-only nonterminal progress must not consume frontier revision");

            RunDecision replay = fixture.store.advanceProgress(
                    fixture.token, command);
            assertEquals(RunDecision.Code.ALREADY_APPLIED, replay.code());
            assertEquals(accepted.snapshot().revision(), replay.snapshot().revision());
            current = accepted.snapshot();
        }
    }

    @Test
    void staleOwnerFrontierRevisionFenceAndCommandConflictAreZeroMutation()
            throws Exception {
        Fixture fixture = claimedFixture();
        RunSnapshot before = fixture.store.runSnapshot(fixture.request.runId());
        IncrementalAuthorityRecord frontierBefore = fixture.store.record("feed");
        IncrementalAttemptProgressCommand first = progress(
                fixture, before, AttemptStatus.DISPATCHING,
                "same-progress", before.updatedAtMillis() + 1L);
        RunDecision accepted = fixture.store.advanceProgress(fixture.token, first);
        assertEquals(RunDecision.Code.ACCEPTED, accepted.code());

        RunSnapshot dispatching = accepted.snapshot();
        IncrementalAttemptProgressCommand conflict = progress(
                fixture, dispatching, AttemptStatus.RUNNING,
                "same-progress", dispatching.updatedAtMillis() + 1L);
        assertEquals(RunDecision.Code.COMMAND_ID_CONFLICT,
                fixture.store.advanceProgress(fixture.token, conflict).code());
        assertEquals(dispatching.revision(), fixture.store
                .runSnapshot(fixture.request.runId()).revision());

        IncrementalAttemptProgressCommand wrongFeed =
                new IncrementalAttemptProgressCommand(
                        "other-feed", fixture.request.intervalId(),
                        frontierBefore.revision(), dispatching.revision(),
                        fixture.request.firstAttemptFence(), AttemptStatus.RUNNING,
                        "wrong-feed", dispatching.updatedAtMillis() + 1L);
        assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER,
                fixture.store.advanceProgress(fixture.token, wrongFeed).code());
        assertEquals(dispatching.revision(), fixture.store
                .runSnapshot(fixture.request.runId()).revision());

        IncrementalAttemptProgressCommand staleFrontier =
                new IncrementalAttemptProgressCommand(
                        "feed", fixture.request.intervalId(), 0L,
                        dispatching.revision(), fixture.request.firstAttemptFence(),
                        AttemptStatus.RUNNING, "stale-frontier",
                        dispatching.updatedAtMillis() + 1L);
        assertEquals(RunDecision.Code.STALE_REVISION,
                fixture.store.advanceProgress(fixture.token, staleFrontier).code());

        RunAttemptFence wrongFence = new RunAttemptFence(
                fixture.request.runId(), "other-attempt", 2L);
        IncrementalAttemptProgressCommand staleFence =
                new IncrementalAttemptProgressCommand(
                        "feed", fixture.request.intervalId(), frontierBefore.revision(),
                        dispatching.revision(), wrongFence, AttemptStatus.RUNNING,
                        "stale-fence", dispatching.updatedAtMillis() + 1L);
        assertEquals(RunDecision.Code.STALE_FENCE,
                fixture.store.advanceProgress(fixture.token, staleFence).code());

        FrontierRecoveryToken replacement = fixture.store.claimOwnership(
                "feed", frontierBefore.revision(), 1L,
                new RecoveryOwner("master-b", 2L)).token();
        IncrementalAttemptProgressCommand staleOwner = progress(
                fixture, dispatching, AttemptStatus.RUNNING,
                "stale-owner", dispatching.updatedAtMillis() + 1L);
        assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER,
                fixture.store.advanceProgress(fixture.token, staleOwner).code());
        assertEquals(dispatching.revision(), fixture.store
                .runSnapshot(fixture.request.runId()).revision());
        assertEquals(frontierBefore, fixture.store.record("feed"));

        IncrementalAttemptProgressCommand replacementCommand =
                new IncrementalAttemptProgressCommand(
                        "feed", fixture.request.intervalId(), frontierBefore.revision(),
                        dispatching.revision(), fixture.request.firstAttemptFence(),
                        AttemptStatus.RUNNING, "replacement-progress",
                        dispatching.updatedAtMillis() + 1L);
        assertEquals(RunDecision.Code.ACCEPTED,
                fixture.store.advanceProgress(replacement, replacementCommand).code());
    }

    @Test
    void progressCommandRejectsEveryTerminalOrControlTarget() throws Exception {
        Fixture fixture = claimedFixture();
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        for (AttemptStatus forbidden : Arrays.asList(
                AttemptStatus.CREATED,
                AttemptStatus.RECONCILING,
                AttemptStatus.CANCELING,
                AttemptStatus.SUCCEEDED,
                AttemptStatus.FAILED,
                AttemptStatus.CANCELED,
                AttemptStatus.TIMED_OUT,
                AttemptStatus.FENCED)) {
            assertThrows(IllegalArgumentException.class,
                    () -> progress(fixture, run, forbidden,
                            "forbidden-" + forbidden.name(), 20L));
        }
    }

    private static IncrementalAttemptProgressCommand progress(
            Fixture fixture, RunSnapshot run,
            AttemptStatus target, String commandId, long eventTimeMillis) {
        return new IncrementalAttemptProgressCommand(
                "feed", fixture.request.intervalId(),
                fixture.store.record("feed").revision(), run.revision(),
                fixture.request.firstAttemptFence(), target, commandId,
                eventTimeMillis);
    }

    private static Fixture claimedFixture() throws Exception {
        InMemoryIncrementalAuthorityStore store = new InMemoryIncrementalAuthorityStore();
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                "feed", 1L, sha256("definition"), sha256("source-contract"),
                sha256("frontier-contract"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        IncrementalAuthorityCommand initializeCommand = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.INITIALIZE, "feed", "initialize",
                0L, 1L, null, null, initialize.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        initializeCommand, initialize)).code());
        FrontierOwnershipClaim ownership = store.claimOwnership(
                "feed", 0L, 0L, new RecoveryOwner("master-a", 1L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, ownership.code());
        FrontierRecoveryToken token = ownership.token();

        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "trigger", IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier-contract"),
                "artifact", "attempt-1", "publication-1", 3, 0L, 10L);
        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                store.publishPending(token, request).code());
        IncrementalAuthorityCommand claim = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, "feed", "claim", 0L, 1L,
                request.intervalId(), request.firstAttemptFence(),
                request.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.claim(
                        claim, token, request)).code());
        return new Fixture(store, token, request);
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static final class Fixture {
        private final InMemoryIncrementalAuthorityStore store;
        private final FrontierRecoveryToken token;
        private final IncrementalClaimRequest request;

        private Fixture(
                InMemoryIncrementalAuthorityStore store,
                FrontierRecoveryToken token,
                IncrementalClaimRequest request) {
            this.store = store;
            this.token = token;
            this.request = request;
        }
    }
}
