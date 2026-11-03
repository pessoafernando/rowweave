package io.rowweave.incremental;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.WholeAttemptRetryDecision;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** File terminal WAL exposes only joint before/after states. */
class IncrementalFileAuthorityJointTerminalContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType));

    @Test
    void jointSuccessRecoversAfterForceBeforeAckAndReplaysExactly(
            @TempDir Path root) throws Exception {
        Fixture fixture = prepared(root);
        IncrementalAuthorityMutation mutation = successMutation(
                fixture, "joint-after-force");
        FileIncrementalAuthorityStore crashing =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> {
                    if (phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        throw new InjectedFileFault();
                    }
                });
        assertThrows(InjectedFileFault.class,
                () -> crashing.apply(mutation));

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        IncrementalAuthorityRecord committed = reopened.record("feed");
        RunSnapshot succeeded = reopened.runSnapshot(fixture.request.runId());
        assertEquals(IncrementalFrontierAuthorityState.READY, committed.state());
        assertNull(committed.activeInterval());
        assertEquals(IncrementalFrontier.int64Sequence(10L),
                committed.committedFrontier());
        assertEquals(RunStatus.SUCCEEDED, succeeded.status());
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                reopened.apply(mutation).code());
    }

    @Test
    void beforeCommitFaultLeavesBothDurableProjectionsBefore(
            @TempDir Path root) throws Exception {
        Fixture fixture = prepared(root);
        IncrementalAuthorityMutation mutation = successMutation(
                fixture, "joint-before-force");
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        RunSnapshot runBefore = fixture.store.runSnapshot(fixture.request.runId());
        FileIncrementalAuthorityStore crashing =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> {
                    if (phase == IncrementalJointSuccessPhase.BEFORE_COMMIT) {
                        throw new InjectedFileFault();
                    }
                });
        assertThrows(InjectedFileFault.class,
                () -> crashing.apply(mutation));

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        assertEquals(before, reopened.record("feed"));
        assertEquals(runBefore.revision(),
                reopened.runSnapshot(fixture.request.runId()).revision());
        assertEquals(RunStatus.RUNNING,
                reopened.runSnapshot(fixture.request.runId()).status());
    }

    @Test
    void typedConflictRecoversAfterForceAndRetainsActiveInterval(
            @TempDir Path root) throws Exception {
        Fixture fixture = prepared(root);
        IncrementalAuthorityRecord record = fixture.store.record("feed");
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(
                        record.activeInterval(), fixture.store);
        Files.write(fixture.manifestDomain.resolve("manifests/9.manifest"),
                bytes("durably-corrupt-manifest"));
        VerifiedIncrementalConflictProof proof =
                IncrementalManifestStrictAdapter.verifyConflict(
                        record.activeInterval(), sinkDecision,
                        new ManifestFileStore(fixture.manifestDomain),
                        fixture.token, fixture.runToken);
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.MARK_CONFLICT,
                "feed", "file-conflict", record.revision(), 1L,
                fixture.request.intervalId(),
                fixture.request.firstAttemptFence(), proof.proofDigest());
        IncrementalAuthorityMutation mutation =
                IncrementalAuthorityMutation.markConflict(
                        command, fixture.token, fixture.runToken, proof);
        FileIncrementalAuthorityStore crashing =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> {
                    if (phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        throw new InjectedFileFault();
                    }
                });
        assertThrows(InjectedFileFault.class,
                () -> crashing.apply(mutation));

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        IncrementalAuthorityRecord frozen = reopened.record("feed");
        assertEquals(IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT,
                frozen.state());
        assertEquals(record.activeInterval(), frozen.activeInterval());
        assertEquals(RunStatus.RECONCILING,
                reopened.runSnapshot(fixture.request.runId()).status());
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                reopened.apply(mutation).code());
    }

    @Test
    void abortClosureRecoversFrontierAndFailedRunTogether(
            @TempDir Path root) throws Exception {
        AbortFixture fixture = abortable(root);
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryToken runToken = fixture.store.claim(
                run.runId(), run.revision(), 0L,
                fixture.token.owner()).token();
        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                fixture.request.firstAttemptFence(), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 9L, fixture.request.publicationId(),
                sha256("abort-identity"), "SOURCE_FAILED", 20L);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, runToken, decided);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, runToken, decided.completed(21L));
        VerifiedIncrementalAbortClosureProof proof =
                fixture.store.verifyAbortClosure(
                        fixture.token, runToken,
                        fixture.request.firstAttemptFence(),
                        AttemptStatus.FAILED);
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                "feed", "file-abort-close", before.revision(), 1L,
                fixture.request.intervalId(),
                fixture.request.firstAttemptFence(), proof.proofDigest());
        IncrementalAuthorityMutation mutation =
                IncrementalAuthorityMutation.closeAborted(
                        command, fixture.token, runToken, proof);
        FileIncrementalAuthorityStore crashing =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> {
                    if (phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        throw new InjectedFileFault();
                    }
                });
        assertThrows(InjectedFileFault.class,
                () -> crashing.apply(mutation));

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        assertEquals(IncrementalFrontierAuthorityState.READY,
                reopened.record("feed").state());
        assertNull(reopened.record("feed").activeInterval());
        assertEquals(before.committedFrontier(),
                reopened.record("feed").committedFrontier());
        assertEquals(RunStatus.FAILED,
                reopened.runSnapshot(fixture.request.runId()).status());
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                reopened.apply(mutation).code());
    }

    @Test
    void retryClaimRecoversTheRunAndFrontierFenceTogether(
            @TempDir Path root) throws Exception {
        AtomicLong clock = new AtomicLong(100L);
        AbortFixture fixture = abortable(root, clock);
        StageAssignmentSnapshot assignments = fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence());
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                assignments.token(0), sha256("retry-command"), 99L);
        fixture.store.accept(acceptance);
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryToken runToken = fixture.store.claim(
                run.runId(), run.revision(), 0L,
                fixture.token.owner()).token();
        fixture.store.recordTerminalRecovered(
                fixture.token, runToken, StageTerminalReceipt.lost(
                        acceptance.token(), acceptance.commandDigest(),
                        "HOST_TERMINATED", sha256("retry-lost"), 0L));
        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                fixture.request.firstAttemptFence(), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 9L, fixture.request.publicationId(),
                sha256("retry-abort"), "HOST_TERMINATED", 101L);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, runToken, decided);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, runToken, decided.completed(102L));
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                fixture.store.scheduleRetry(
                        fixture.token, runToken,
                        fixture.request.firstAttemptFence()).code());
        IncrementalActiveInterval frozen =
                fixture.store.record("feed").activeInterval();

        FileIncrementalAuthorityStore waiting =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> { }, authorityClock(clock));
        assertEquals(RunStatus.READY,
                waiting.runSnapshot(fixture.request.runId()).status());
        assertEquals(frozen,
                waiting.record("feed").activeInterval());

        clock.set(1_100L);
        FileIncrementalAuthorityStore crashing =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> {
                    if (phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        throw new InjectedFileFault();
                    }
                }, authorityClock(clock));
        assertThrows(InjectedFileFault.class,
                () -> crashing.claimDueRetry(
                        fixture.token, runToken,
                        fixture.request.runId()));

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> { }, authorityClock(clock));
        assertEquals(2,
                reopened.record("feed").activeInterval().attemptOrdinal());
        assertEquals(reopened.record("feed").activeInterval().fence().attemptId(),
                reopened.runSnapshot(fixture.request.runId())
                        .currentAttempt().attemptId());
        assertEquals(RetryAttemptClaim.Code.ALREADY_CLAIMED,
                reopened.claimDueRetry(
                        fixture.token, runToken,
                        fixture.request.runId()).code());
    }

    @Test
    void cancellationAndDeadlineRecoverReadyRetryClosureTogether(
            @TempDir Path root) throws Exception {
        AtomicLong cancelClock = new AtomicLong(200L);
        RetryFixture canceled = retryWaiting(
                root.resolve("cancel"), cancelClock, 0L);
        FileIncrementalAuthorityStore canceling =
                afterForceFault(root.resolve("cancel"), cancelClock);
        assertThrows(InjectedFileFault.class,
                () -> canceling.requestUserCancellation(
                        canceled.token, canceled.runToken,
                        canceled.request.runId(), "cancel-retry"));
        FileIncrementalAuthorityStore canceledReopen =
                new FileIncrementalAuthorityStore(
                        root.resolve("cancel/authority"), phase -> { },
                        authorityClock(cancelClock));
        assertEquals(IncrementalFrontierAuthorityState.READY,
                canceledReopen.record("feed").state());
        assertNull(canceledReopen.record("feed").activeInterval());
        assertEquals(RunStatus.CANCELED,
                canceledReopen.runSnapshot(canceled.request.runId()).status());

        AtomicLong deadlineClock = new AtomicLong(300L);
        RetryFixture timedOut = retryWaiting(
                root.resolve("deadline"), deadlineClock, 305L);
        deadlineClock.set(305L);
        FileIncrementalAuthorityStore timingOut =
                afterForceFault(root.resolve("deadline"), deadlineClock);
        assertThrows(InjectedFileFault.class,
                () -> timingOut.evaluateDeadline(
                        timedOut.token, timedOut.runToken,
                        timedOut.request.runId()));
        FileIncrementalAuthorityStore deadlineReopen =
                new FileIncrementalAuthorityStore(
                        root.resolve("deadline/authority"), phase -> { },
                        authorityClock(deadlineClock));
        assertEquals(IncrementalFrontierAuthorityState.READY,
                deadlineReopen.record("feed").state());
        assertNull(deadlineReopen.record("feed").activeInterval());
        assertEquals(RunStatus.FAILED,
                deadlineReopen.runSnapshot(timedOut.request.runId()).status());
    }

    @Test
    void completedReceiptCheckpointBoundsLaterHistoryAndRetainsRun(
            @TempDir Path root) throws Exception {
        Fixture fixture = prepared(root);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                fixture.store.apply(successMutation(
                        fixture, "checkpoint-success")).code());
        IncrementalAuthorityRecord expected = fixture.store.record("feed");
        FileIncrementalAuthorityStore history =
                new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> { },
                        System::currentTimeMillis, false);
        long ownershipEpoch = fixture.token.ownershipEpoch();
        for (int i = 0; i < 100; i++) {
            FrontierOwnershipClaim claim = history.claimOwnership(
                    "feed", expected.revision(), ownershipEpoch,
                    new RecoveryOwner("replacement-" + (i & 1), i + 2L));
            assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, claim.code());
            ownershipEpoch = claim.token().ownershipEpoch();
        }
        assertTrue(history.lastReplayFrameCountForTest() <= 64);

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        assertEquals(expected, reopened.record("feed"));
        assertEquals(RunStatus.SUCCEEDED, reopened.runSnapshot(
                expected.lastCommitReceipt().fence().runId()).status());
        assertTrue(reopened.lastReplayFrameCountForTest() <= 64);
    }

    @Test
    void completedRunHistoryRemainsQueryableAcrossCheckpointRotation(
            @TempDir Path root) throws Exception {
        FileIncrementalAuthorityStore store =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                "feed", 1L, sha256("definition"), sha256("source"),
                sha256("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        store.apply(IncrementalAuthorityMutation.initialize(
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.INITIALIZE,
                        "feed", "initialize-history", 0L, 1L,
                        null, null, initialize.transitionDigest()), initialize));
        FrontierRecoveryToken token = store.claimOwnership(
                "feed", 0L, 0L, new RecoveryOwner("master", 1L)).token();
        String firstRunId = closeFailedRun(store, token, 1);
        String secondRunId = closeFailedRun(store, token, 2);

        FileIncrementalAuthorityStore reopened =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        assertEquals(RunStatus.FAILED,
                reopened.runSnapshot(firstRunId).status());
        assertEquals(RunStatus.FAILED,
                reopened.runSnapshot(secondRunId).status());
    }

    private static String closeFailedRun(
            FileIncrementalAuthorityStore store,
            FrontierRecoveryToken token, int cycle) throws Exception {
        IncrementalAuthorityRecord before = store.record("feed");
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source"),
                before.revision(), IncrementalFrontier.begin(),
                "trigger-" + cycle, IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L),
                bytes("snapshot-" + cycle));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier"),
                "artifact-" + cycle, "attempt-" + cycle,
                "publication-" + cycle, 3, 0L, cycle * 1_000L);
        store.publishPending(token, request);
        store.apply(IncrementalAuthorityMutation.claim(
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.CLAIM,
                        "feed", "claim-history-" + cycle,
                        before.revision(), 1L, request.intervalId(),
                        request.firstAttemptFence(), request.transitionDigest()),
                token, request));
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                request.firstAttemptFence(), 1L, 0, cycle, 0, 0,
                StageAssignmentRole.SINK, "worker", "localhost", 7501,
                -1, "worker-incarnation");
        store.publish(new StageAssignmentManifest(
                request.firstAttemptFence(), request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(assignment)));
        RunSnapshot run = store.runSnapshot(request.runId());
        for (AttemptStatus target : new AttemptStatus[]{
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING}) {
            run = store.advanceProgress(
                    token, new IncrementalAttemptProgressCommand(
                            "feed", request.intervalId(),
                            store.record("feed").revision(), run.revision(),
                            request.firstAttemptFence(), target,
                            "history-progress-" + cycle + '-' + target,
                            run.updatedAtMillis() + 1L)).snapshot();
        }
        RecoveryToken runToken = store.claim(
                request.runId(), run.revision(), 0L, token.owner()).token();
        BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                request.firstAttemptFence(), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", cycle, request.publicationId(),
                sha256("history-abort-" + cycle), "SOURCE_FAILED",
                run.updatedAtMillis() + 2L);
        store.recordBatchSinkAbortRecovered(token, runToken, abort);
        store.recordBatchSinkAbortRecovered(
                token, runToken, abort.completed(run.updatedAtMillis() + 3L));
        VerifiedIncrementalAbortClosureProof proof = store.verifyAbortClosure(
                token, runToken, request.firstAttemptFence(),
                AttemptStatus.FAILED);
        IncrementalAuthorityRecord active = store.record("feed");
        store.apply(IncrementalAuthorityMutation.closeAborted(
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                        "feed", "close-history-" + cycle,
                        active.revision(), 1L, request.intervalId(),
                        request.firstAttemptFence(), proof.proofDigest()),
                token, runToken, proof));
        return request.runId();
    }

    private static FileIncrementalAuthorityStore afterForceFault(
            Path root, AtomicLong clock) {
        return new FileIncrementalAuthorityStore(
                root.resolve("authority"), phase -> {
            if (phase
                    == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                throw new InjectedFileFault();
            }
        }, authorityClock(clock));
    }

    private static RetryFixture retryWaiting(
            Path root, AtomicLong clock, long deadline) throws Exception {
        AbortFixture fixture = abortable(root, clock, deadline);
        StageAssignmentSnapshot assignments = fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence());
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                assignments.token(0), sha256("retry-command"), 99L);
        fixture.store.accept(acceptance);
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryToken runToken = fixture.store.claim(
                run.runId(), run.revision(), 0L,
                fixture.token.owner()).token();
        fixture.store.recordTerminalRecovered(
                fixture.token, runToken, StageTerminalReceipt.lost(
                        acceptance.token(), acceptance.commandDigest(),
                        "HOST_TERMINATED", sha256("retry-lost"), 0L));
        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                fixture.request.firstAttemptFence(), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 9L, fixture.request.publicationId(),
                sha256("retry-abort"), "HOST_TERMINATED", 101L);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, runToken, decided);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, runToken, decided.completed(102L));
        assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                fixture.store.scheduleRetry(
                        fixture.token, runToken,
                        fixture.request.firstAttemptFence()).code());
        return new RetryFixture(
                fixture.store, fixture.token, runToken, fixture.request);
    }

    private static AbortFixture abortable(Path root) throws Exception {
        return abortable(root, null);
    }

    private static AbortFixture abortable(
            Path root, AtomicLong clock) throws Exception {
        return abortable(root, clock, 0L);
    }

    private static AbortFixture abortable(
            Path root, AtomicLong clock, long deadline) throws Exception {
        FileIncrementalAuthorityStore store =
                clock == null
                ? new FileIncrementalAuthorityStore(root.resolve("authority"))
                : new FileIncrementalAuthorityStore(
                        root.resolve("authority"), phase -> { }, authorityClock(clock));
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                "feed", 1L, sha256("definition"), sha256("source"),
                sha256("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        store.apply(IncrementalAuthorityMutation.initialize(
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.INITIALIZE,
                        "feed", "initialize", 0L, 1L,
                        null, null, initialize.transitionDigest()), initialize));
        FrontierRecoveryToken token = store.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("master", 1L)).token();
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source"), 0L,
                IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier"), "artifact",
                "attempt-1", "publication", 3, deadline, 10L);
        store.publishPending(token, request);
        store.apply(IncrementalAuthorityMutation.claim(
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.CLAIM,
                        "feed", "claim", 0L, 1L,
                        request.intervalId(), request.firstAttemptFence(),
                        request.transitionDigest()), token, request));
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                request.firstAttemptFence(), 1L, 0, 501L, 0, 0,
                StageAssignmentRole.SINK, "worker", "localhost", 7501,
                -1, "worker-incarnation");
        store.publish(new StageAssignmentManifest(
                request.firstAttemptFence(), request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(assignment)));
        RunSnapshot run = store.runSnapshot(request.runId());
        for (AttemptStatus target : new AttemptStatus[]{
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING}) {
            RunDecision advanced = store.advanceProgress(
                    token, new IncrementalAttemptProgressCommand(
                            "feed", request.intervalId(),
                            store.record("feed").revision(), run.revision(),
                            request.firstAttemptFence(), target,
                            "abort-progress-" + target.name(),
                            run.updatedAtMillis() + 1L));
            run = advanced.snapshot();
        }
        return new AbortFixture(store, token, request);
    }

    static Fixture prepared(Path root) throws Exception {
        FileIncrementalAuthorityStore store =
                new FileIncrementalAuthorityStore(root.resolve("authority"));
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                "feed", 1L, sha256("definition"), sha256("source"),
                sha256("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        IncrementalAuthorityCommand initializeCommand =
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.INITIALIZE,
                        "feed", "initialize", 0L, 1L,
                        null, null, initialize.transitionDigest());
        store.apply(IncrementalAuthorityMutation.initialize(
                initializeCommand, initialize));
        FrontierRecoveryToken token = store.claimOwnership(
                "feed", 0L, 0L,
                new RecoveryOwner("master", 1L)).token();
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source"), 0L,
                IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier"), "artifact",
                "attempt-1", "publication", 3, 0L, 10L);
        store.publishPending(token, request);
        IncrementalAuthorityCommand claim = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM,
                "feed", "claim", 0L, 1L,
                request.intervalId(), request.firstAttemptFence(),
                request.transitionDigest());
        store.apply(IncrementalAuthorityMutation.claim(
                claim, token, request));

        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                request.firstAttemptFence(), 1L, 0, 501L, 0, 0,
                StageAssignmentRole.SINK, "worker", "localhost", 7501,
                -1, "worker-incarnation");
        StageAssignmentManifest assignments = new StageAssignmentManifest(
                request.firstAttemptFence(), request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(assignment));
        StageAssignmentSnapshot published = store.publish(assignments).snapshot();
        byte[] commandDigest = sha256("worker-command");
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                published.token(0), commandDigest, 20L);
        store.accept(acceptance);
        store.recordTerminal(StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 3L, 3L, 21L));

        ManifestFileStore manifestStore =
                new ManifestFileStore(root.resolve("manifest"));
        Path staged = manifestStore.stagingPath(
                request.publicationId(), "part-0");
        Files.write(staged, bytes("data-part-0"));
        manifestStore.forceStaged(staged);
        CommitManifest.Member member = manifestStore.installMember(
                9L, "part-0", "schema", staged);
        PreparedSinkLocator locator = locator(
                manifestStore.domainId(), assignments, assignment, member, 22L);
        store.recordPreparedSink(new PreparedSinkReceipt(
                acceptance.token(), acceptance.commandDigest(), locator));
        store.recordManifestMember(ManifestMemberReceipt.sealed(
                acceptance.token(), acceptance.commandDigest(),
                manifestStore.domainId(), 9L, request.publicationId(),
                member, 22L, locator));

        RunSnapshot run = store.runSnapshot(request.runId());
        for (AttemptStatus target : new AttemptStatus[]{
                AttemptStatus.DISPATCHING,
                AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING}) {
            IncrementalAttemptProgressCommand progress =
                    new IncrementalAttemptProgressCommand(
                            "feed", request.intervalId(),
                            store.record("feed").revision(), run.revision(),
                            request.firstAttemptFence(), target,
                            "progress-" + target.name(),
                            run.updatedAtMillis() + 1L);
            RunDecision advanced = store.advanceProgress(token, progress);
            assertEquals(RunDecision.Code.ACCEPTED, advanced.code());
            run = advanced.snapshot();
        }
        ManifestCommitIntent intent = store.manifestCommitJournal(
                request.firstAttemptFence()).buildIntent();
        store.commitManifestIntent(intent);
        CommitManifest manifest = new CommitManifest(
                manifestStore.domainId(), 9L, request.publicationId(),
                Collections.singletonList(member));
        manifestStore.publish(manifest);

        RecoveryClaim runOwnership = store.claim(
                request.runId(), store.runSnapshot(request.runId()).revision(),
                0L, token.owner());
        RecoveryToken runToken = runOwnership.token();
        IncrementalActiveInterval active = store.record("feed").activeInterval();
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(active, store);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        sinkDecision, manifestStore, token, runToken);
        VerifiedIncrementalJointCommitProof proof =
                IncrementalJointCommitProofAssembler.join(
                        active, sinkDecision, publication);
        return new Fixture(
                store, token, runToken, request, proof,
                root.resolve("manifest"));
    }

    static IncrementalAuthorityMutation successMutation(
            Fixture fixture, String commandId) {
        IncrementalAuthorityRecord record = fixture.store.record("feed");
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.COMMIT,
                "feed", commandId, record.revision(), 1L,
                fixture.request.intervalId(),
                fixture.request.firstAttemptFence(),
                fixture.proof.proofDigest());
        return IncrementalAuthorityMutation.jointTerminal(
                command, fixture.token, fixture.runToken, fixture.proof);
    }

    private static PreparedSinkLocator locator(
            String domainId, StageAssignmentManifest manifest,
            StageAssignmentRecord assignment, CommitManifest.Member member,
            long preparedAtMillis) {
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SchemaFingerprint.of(META),
                "sink:part-0", domainId,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                manifest, assignment, "sink-op-part-0", descriptor);
        return new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                domainId, ManifestCommitWire.encodeMember(member),
                member.length(), member.sha256(), preparedAtMillis);
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static java.util.function.LongSupplier authorityClock(
            AtomicLong clock) {
        return clock::get;
    }

    static final class Fixture {
        private final FileIncrementalAuthorityStore store;
        private final FrontierRecoveryToken token;
        private final RecoveryToken runToken;
        private final IncrementalClaimRequest request;
        private final VerifiedIncrementalJointCommitProof proof;
        private final Path manifestDomain;

        private Fixture(
                FileIncrementalAuthorityStore store,
                FrontierRecoveryToken token, RecoveryToken runToken,
                IncrementalClaimRequest request,
                VerifiedIncrementalJointCommitProof proof,
                Path manifestDomain) {
            this.store = store;
            this.token = token;
            this.runToken = runToken;
            this.request = request;
            this.proof = proof;
            this.manifestDomain = manifestDomain;
        }
    }

    private static final class AbortFixture {
        private final FileIncrementalAuthorityStore store;
        private final FrontierRecoveryToken token;
        private final IncrementalClaimRequest request;

        private AbortFixture(
                FileIncrementalAuthorityStore store,
                FrontierRecoveryToken token,
                IncrementalClaimRequest request) {
            this.store = store;
            this.token = token;
            this.request = request;
        }
    }

    private static final class RetryFixture {
        private final FileIncrementalAuthorityStore store;
        private final FrontierRecoveryToken token;
        private final RecoveryToken runToken;
        private final IncrementalClaimRequest request;

        private RetryFixture(
                FileIncrementalAuthorityStore store,
                FrontierRecoveryToken token, RecoveryToken runToken,
                IncrementalClaimRequest request) {
            this.store = store;
            this.token = token;
            this.runToken = runToken;
            this.request = request;
        }
    }

    private static final class InjectedFileFault extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
