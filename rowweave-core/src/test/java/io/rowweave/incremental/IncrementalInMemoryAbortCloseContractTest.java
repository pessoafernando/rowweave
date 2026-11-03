package io.rowweave.incremental;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.SinkAttemptIdentity;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** abort closure and Run failure share one authority CAS. */
class IncrementalInMemoryAbortCloseContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType));

    @Test
    void authorityMintsOnlyCompletedAbortProofAndClosesFailedRunAtomically()
            throws Exception {
        Fixture fixture = runningFixture();
        IncrementalCommitReceipt retained = injectRetainedBackfill(
                fixture.store, "past-backfill");
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryClaim runOwnership = fixture.store.claim(
                run.runId(), run.revision(), 0L, fixture.token.owner());
        assertEquals(RecoveryClaim.Code.ACQUIRED, runOwnership.code());

        BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                fixture.request.firstAttemptFence(), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 9L, fixture.request.publicationId(),
                sha256("abort-identity"), "SOURCE_FAILED", 20L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbortRecovered(
                        fixture.token, runOwnership.token(), decided).code());
        assertThrows(IllegalArgumentException.class,
                () -> fixture.store.verifyAbortClosure(
                        fixture.token, runOwnership.token(),
                        fixture.request.firstAttemptFence(),
                        AttemptStatus.FAILED));
        assertSame(before, fixture.store.record("feed"));

        BatchSinkAbortRecord completed = decided.completed(21L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbortRecovered(
                        fixture.token, runOwnership.token(), completed).code());
        VerifiedIncrementalAbortClosureProof proof =
                fixture.store.verifyAbortClosure(
                        fixture.token, runOwnership.token(),
                        fixture.request.firstAttemptFence(),
                        AttemptStatus.FAILED);
        assertEquals(AttemptStatus.FAILED, proof.terminalStatus());
        assertSame(before, fixture.store.record("feed"),
                "proof issuance is read-only");
        assertNull(fixture.store.manifestCommitIntentObservation(
                fixture.request.firstAttemptFence()));

        IncrementalAuthorityCommand close = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                "feed", "close-failed", before.revision(), 1L,
                fixture.request.intervalId(), fixture.request.firstAttemptFence(),
                proof.proofDigest());
        IncrementalAuthorityDecision closed = fixture.store.apply(
                IncrementalAuthorityMutation.closeAborted(
                        close, fixture.token, runOwnership.token(), proof));
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED, closed.code());
        assertEquals(before.revision() + 1L, closed.record().revision());
        assertEquals(IncrementalFrontierAuthorityState.READY,
                closed.record().state());
        assertNull(closed.record().activeInterval());
        assertEquals(before.committedFrontier(),
                closed.record().committedFrontier());
        assertSame(before.lastCommitReceipt(), closed.record().lastCommitReceipt());
        assertSame(retained, closed.record().lastBackfillReceipt());
        assertEquals(RunStatus.FAILED, closed.runSnapshot().status());
        assertEquals(AttemptStatus.FAILED,
                closed.runSnapshot().currentAttempt().status());
        assertEquals(0, runFeedCount(fixture.store));
    }

    @Test
    void recoveryFencesOrdinaryAbortCompletion() throws Exception {
        Fixture fixture = runningFixture();
        BatchSinkAbortRecord decided = abortRecord(fixture, "SOURCE_FAILED");
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                fixture.store.recordBatchSinkAbort(decided).code());
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryClaim recovery = fixture.store.claim(
                run.runId(), run.revision(), 0L, fixture.token.owner());
        assertEquals(RecoveryClaim.Code.ACQUIRED, recovery.code());

        assertEquals(BatchSinkAbortDecision.Code.RECOVERY_OWNED,
                fixture.store.recordBatchSinkAbort(decided.completed(21L)).code());
        assertSame(decided, fixture.store.batchSinkAbort(
                fixture.request.firstAttemptFence()),
                "stale ordinary completion must not mutate durable abort state");
    }

    @Test
    void abortDecisionReservesEveryFinalClosureCapacityAndFreezesProgress()
            throws Exception {
        Fixture timeExhausted = runningFixture(Long.MAX_VALUE);
        IncrementalAuthorityRecord timeRecord = timeExhausted.store.record("feed");
        RunSnapshot timeRun = timeExhausted.store.runSnapshot(
                timeExhausted.request.runId());
        assertEquals(BatchSinkAbortDecision.Code.RECORD_CONFLICT,
                timeExhausted.store.recordBatchSinkAbort(
                        abortRecord(timeExhausted, "SOURCE_FAILED")).code());
        assertSame(timeRecord, timeExhausted.store.record("feed"));
        assertSame(timeRun, timeExhausted.store.runSnapshot(
                timeExhausted.request.runId()));
        assertNull(timeExhausted.store.batchSinkAbort(
                timeExhausted.request.firstAttemptFence()));

        Fixture runRevisionExhausted = runningFixture();
        RunSnapshot exhaustedRun = runRevisionExhausted.store.runSnapshot(
                runRevisionExhausted.request.runId());
        long savedRunRevision = exhaustedRun.revision();
        replaceLong(exhaustedRun, "revision", Long.MAX_VALUE);
        assertEquals(BatchSinkAbortDecision.Code.RECORD_CONFLICT,
                runRevisionExhausted.store.recordBatchSinkAbort(
                        abortRecord(runRevisionExhausted, "SOURCE_FAILED")).code());
        assertNull(runRevisionExhausted.store.batchSinkAbort(
                runRevisionExhausted.request.firstAttemptFence()));
        replaceLong(exhaustedRun, "revision", savedRunRevision);

        Fixture frontierRevisionExhausted = runningFixture();
        IncrementalAuthorityRecord exhaustedFrontier =
                frontierRevisionExhausted.store.record("feed");
        long savedFrontierRevision = exhaustedFrontier.revision();
        replaceLong(exhaustedFrontier, "revision", Long.MAX_VALUE);
        assertEquals(BatchSinkAbortDecision.Code.RECORD_CONFLICT,
                frontierRevisionExhausted.store.recordBatchSinkAbort(
                        abortRecord(frontierRevisionExhausted,
                                "SOURCE_FAILED")).code());
        assertNull(frontierRevisionExhausted.store.batchSinkAbort(
                frontierRevisionExhausted.request.firstAttemptFence()));
        replaceLong(exhaustedFrontier, "revision", savedFrontierRevision);

        Fixture frozen = runningFixture();
        BatchSinkAbortRecord decided = abortRecord(frozen, "SOURCE_FAILED");
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                frozen.store.recordBatchSinkAbort(decided).code());
        RunSnapshot beforeProgress = frozen.store.runSnapshot(
                frozen.request.runId());
        RunDecision rejected = frozen.store.advanceProgress(
                frozen.token, new IncrementalAttemptProgressCommand(
                        "feed", frozen.request.intervalId(),
                        frozen.store.record("feed").revision(),
                        beforeProgress.revision(),
                        frozen.request.firstAttemptFence(),
                        AttemptStatus.COMMITTING, "progress-after-abort",
                        beforeProgress.updatedAtMillis() + 1L));
        assertEquals(RunDecision.Code.ILLEGAL_TRANSITION, rejected.code());
        assertSame(beforeProgress, frozen.store.runSnapshot(
                frozen.request.runId()));
    }

    @Test
    void retainedSuccessCommandIdCannotBeReusedForAbortClose()
            throws Exception {
        Fixture fixture = runningFixture();
        IncrementalCommitReceipt retained = injectRetainedBackfill(
                fixture.store, "shared-command");
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        RunSnapshot beforeRun = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryClaim runOwnership = fixture.store.claim(
                beforeRun.runId(), beforeRun.revision(), 0L,
                fixture.token.owner());
        BatchSinkAbortRecord decided = abortRecord(fixture, "SOURCE_FAILED");
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, runOwnership.token(), decided);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, runOwnership.token(), decided.completed(21L));
        VerifiedIncrementalAbortClosureProof proof =
                fixture.store.verifyAbortClosure(
                        fixture.token, runOwnership.token(),
                        fixture.request.firstAttemptFence(),
                        AttemptStatus.FAILED);
        IncrementalAuthorityCommand close = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                "feed", "shared-command", before.revision(), 1L,
                fixture.request.intervalId(), fixture.request.firstAttemptFence(),
                proof.proofDigest());

        IncrementalAuthorityDecision rejected = fixture.store.apply(
                IncrementalAuthorityMutation.closeAborted(
                        close, fixture.token, runOwnership.token(), proof));
        assertEquals(IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                rejected.code());
        assertSame(before, fixture.store.record("feed"));
        assertSame(beforeRun, fixture.store.runSnapshot(fixture.request.runId()));
        assertSame(retained, before.lastBackfillReceipt());
    }

    @Test
    void abortDecisionRejectsLatePreparedOutputAndKeepsProofFresh()
            throws Exception {
        Fixture fixture = runningFixture();
        StageAssignmentSnapshot assignments = fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence());
        byte[] workerDigest = sha256("worker-command");
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                assignments.token(0), workerDigest, 15L);
        fixture.store.accept(acceptance);
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                acceptance.token(), workerDigest, 1L, 1L, 16L);
        fixture.store.recordTerminal(terminal);
        CommitManifest.Member member = new CommitManifest.Member(
                "member", "data/9/member.data", "schema", 3L,
                sha256("member"));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SchemaFingerprint.of(META),
                "domain:member", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                assignments.manifest(), assignments.manifest().assignment(0),
                "sink-op", descriptor);
        PreparedSinkLocator locator = new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", ManifestCommitWire.encodeMember(member),
                member.length(), member.sha256(), 17L);
        PreparedSinkReceipt late = new PreparedSinkReceipt(
                acceptance.token(), workerDigest, locator);
        ManifestMemberReceipt sealed = ManifestMemberReceipt.sealed(
                acceptance.token(), workerDigest, "domain", 9L,
                fixture.request.publicationId(), member, 18L);
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                fixture.store.recordManifestMember(sealed).code());

        IncrementalAuthorityRecord before = fixture.store.record("feed");
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryClaim recovery = fixture.store.claim(
                run.runId(), run.revision(), 0L, fixture.token.owner());
        BatchSinkAbortRecord decided = abortRecord(fixture, "SOURCE_FAILED");
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, recovery.token(), decided);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, recovery.token(), decided.completed(21L));
        VerifiedIncrementalAbortClosureProof proof =
                fixture.store.verifyAbortClosure(
                        fixture.token, recovery.token(),
                        fixture.request.firstAttemptFence(),
                        AttemptStatus.FAILED);

        assertEquals(io.rowweave.run.StageAcceptanceDecision.Code.ALREADY_ACCEPTED,
                fixture.store.acceptRecovered(
                        fixture.token, recovery.token(), acceptance).code());
        assertEquals(io.rowweave.run.StageTerminalDecision.Code.ALREADY_RECORDED,
                fixture.store.recordTerminalRecovered(
                        fixture.token, recovery.token(), terminal).code());
        assertEquals(ManifestMemberDecision.Code.ALREADY_RECORDED,
                fixture.store.recordManifestMemberRecovered(
                        fixture.token, recovery.token(), sealed).code());
        assertSame(before, fixture.store.record("feed"));
        assertEquals(PreparedSinkDecision.Code.LOCATOR_CONFLICT,
                fixture.store.recordPreparedSinkRecovered(
                        fixture.token, recovery.token(), late).code());
        assertNull(fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence()).preparedSink(0));
        assertSame(before, fixture.store.record("feed"));

        IncrementalAuthorityCommand close = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                "feed", "close-after-late-prepared", before.revision(), 1L,
                fixture.request.intervalId(), fixture.request.firstAttemptFence(),
                proof.proofDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                fixture.store.apply(IncrementalAuthorityMutation.closeAborted(
                        close, fixture.token, recovery.token(), proof)).code());
    }

    @Test
    void abortCloseFaultsExposeOnlyJointBeforeOrAfterState() throws Exception {
        InMemoryIncrementalAuthorityStore beforeStore =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (phase == IncrementalJointSuccessPhase.BEFORE_COMMIT) {
                        throw new InjectedJointFault();
                    }
                });
        Fixture before = runningFixture(beforeStore, -1L);
        IncrementalAuthorityMutation beforeMutation = abortCloseMutation(
                before, "before-abort-fault");
        IncrementalAuthorityRecord beforeRecord = before.store.record("feed");
        RunSnapshot beforeRun = before.store.runSnapshot(before.request.runId());
        assertThrows(InjectedJointFault.class,
                () -> before.store.apply(beforeMutation));
        assertSame(beforeRecord, before.store.record("feed"));
        assertSame(beforeRun, before.store.runSnapshot(before.request.runId()));

        InMemoryIncrementalAuthorityStore afterStore =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        throw new InjectedJointFault();
                    }
                });
        Fixture after = runningFixture(afterStore, -1L);
        IncrementalAuthorityMutation afterMutation = abortCloseMutation(
                after, "after-abort-fault");
        assertThrows(InjectedJointFault.class,
                () -> after.store.apply(afterMutation));
        IncrementalAuthorityRecord closed = after.store.record("feed");
        RunSnapshot terminal = after.store.runSnapshot(after.request.runId());
        assertEquals(IncrementalFrontierAuthorityState.READY, closed.state());
        assertNull(closed.activeInterval());
        assertEquals(RunStatus.FAILED, terminal.status());
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                after.store.apply(afterMutation).code());
        assertSame(closed, after.store.record("feed"));
        assertSame(terminal, after.store.runSnapshot(after.request.runId()));
    }

    private static Fixture runningFixture() throws Exception {
        return runningFixture(-1L);
    }

    private static Fixture runningFixture(long runningEventTime) throws Exception {
        return runningFixture(
                new InMemoryIncrementalAuthorityStore(), runningEventTime);
    }

    private static Fixture runningFixture(
            InMemoryIncrementalAuthorityStore store, long runningEventTime)
            throws Exception {
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                "feed", 1L, sha256("definition"), sha256("source-contract"),
                sha256("frontier-contract"),
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        IncrementalAuthorityCommand initializeCommand =
                new IncrementalAuthorityCommand(
                        IncrementalAuthorityCommandKind.INITIALIZE,
                        "feed", "initialize", 0L, 1L,
                        null, null, initialize.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.initialize(
                        initializeCommand, initialize)).code());
        FrontierOwnershipClaim ownership = store.claimOwnership(
                "feed", 0L, 0L, new RecoveryOwner("master", 1L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, ownership.code());

        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier-contract"),
                "artifact", "attempt-1", "publication-1", 3, 0L, 10L);
        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                store.publishPending(ownership.token(), request).code());
        IncrementalAuthorityCommand claim = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, "feed", "claim",
                0L, 1L, request.intervalId(), request.firstAttemptFence(),
                request.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.claim(
                        claim, ownership.token(), request)).code());

        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                request.firstAttemptFence(), 1L, 0, 101L, 0, 0,
                StageAssignmentRole.SINK, "worker", "localhost", 7001,
                -1, "worker-incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                request.firstAttemptFence(), request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(assignment));
        assertEquals(io.rowweave.run.StageManifestPublication.Code.PUBLISHED,
                store.publish(manifest).code());

        RunSnapshot run = store.runSnapshot(request.runId());
        for (AttemptStatus target : new AttemptStatus[]{
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING}) {
            long eventTime = target == AttemptStatus.RUNNING
                    && runningEventTime >= 0L
                    ? runningEventTime : run.updatedAtMillis() + 1L;
            IncrementalAttemptProgressCommand progress =
                    new IncrementalAttemptProgressCommand(
                            "feed", request.intervalId(),
                            store.record("feed").revision(), run.revision(),
                            request.firstAttemptFence(), target,
                            "progress-" + target.name(),
                            eventTime);
            RunDecision advanced = store.advanceProgress(
                    ownership.token(), progress);
            assertEquals(RunDecision.Code.ACCEPTED, advanced.code());
            run = advanced.snapshot();
        }

        return new Fixture(store, ownership.token(), request);
    }

    private static IncrementalAuthorityMutation abortCloseMutation(
            Fixture fixture, String commandId) throws Exception {
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryClaim ownership = fixture.store.claim(
                run.runId(), run.revision(), 0L, fixture.token.owner());
        BatchSinkAbortRecord decided = abortRecord(fixture, "SOURCE_FAILED");
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, ownership.token(), decided);
        fixture.store.recordBatchSinkAbortRecovered(
                fixture.token, ownership.token(), decided.completed(21L));
        VerifiedIncrementalAbortClosureProof proof =
                fixture.store.verifyAbortClosure(
                        fixture.token, ownership.token(),
                        fixture.request.firstAttemptFence(),
                        AttemptStatus.FAILED);
        IncrementalAuthorityRecord record = fixture.store.record("feed");
        IncrementalAuthorityCommand close = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                "feed", commandId, record.revision(), 1L,
                fixture.request.intervalId(), fixture.request.firstAttemptFence(),
                proof.proofDigest());
        return IncrementalAuthorityMutation.closeAborted(
                close, fixture.token, ownership.token(), proof);
    }

    private static BatchSinkAbortRecord abortRecord(
            Fixture fixture, String reasonCode) throws Exception {
        return BatchSinkAbortRecord.decided(
                fixture.request.firstAttemptFence(), 1L,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 9L, fixture.request.publicationId(),
                sha256("abort-identity"), reasonCode, 20L);
    }

    private static IncrementalCommitReceipt injectRetainedBackfill(
            InMemoryIncrementalAuthorityStore store, String commandId)
            throws Exception {
        IncrementalAuthorityRecord current = store.record("feed");
        CapturedTargetReceipt capture = CapturedTargetReceipt.backfill(
                1L, current.definitionDigest(), current.sourceContractDigest(),
                1L, IncrementalFrontier.int64Sequence(1L),
                IncrementalFrontier.int64Sequence(2L), "past-backfill",
                bytes("past-snapshot"));
        IncrementalActiveInterval committed = new IncrementalActiveInterval(
                IncrementalIntervalIdentity.from(capture), capture,
                "past-artifact", 0L,
                new io.rowweave.run.RunAttemptFence(
                        IncrementalIntervalIdentity.from(capture).candidateRunId(),
                        "past-attempt", 1L),
                1, IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                sha256("past-decision"));
        IncrementalCommitReceipt receipt = IncrementalCommitReceipt.committed(
                committed, "past-publication", 8L,
                sha256("past-manifest"), commandId,
                sha256("past-command"));
        IncrementalAuthorityRecord replacement = new IncrementalAuthorityRecord(
                current.revision(), current.feedId(), current.feedIncarnation(),
                current.definitionDigest(), current.sourceContractDigest(),
                current.frontierContractDigest(), current.frontierKind(),
                current.committedFrontier(), current.activeInterval(),
                current.lastCommitReceipt(), 1L, receipt,
                current.state(), current.conflictDigest());
        replaceRecord(store, replacement);
        return receipt;
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    @SuppressWarnings("unchecked")
    private static int runFeedCount(
            InMemoryIncrementalAuthorityStore store) throws Exception {
        java.lang.reflect.Field field =
                InMemoryIncrementalAuthorityStore.class.getDeclaredField("runFeeds");
        field.setAccessible(true);
        return ((Map<String, String>) field.get(store)).size();
    }

    @SuppressWarnings("unchecked")
    private static void replaceRecord(
            InMemoryIncrementalAuthorityStore store,
            IncrementalAuthorityRecord replacement) throws Exception {
        java.lang.reflect.Field feeds =
                InMemoryIncrementalAuthorityStore.class.getDeclaredField("feeds");
        feeds.setAccessible(true);
        Object state = ((Map<String, Object>) feeds.get(store)).get("feed");
        java.lang.reflect.Field record = state.getClass().getDeclaredField("record");
        record.setAccessible(true);
        record.set(state, replacement);
    }

    private static void replaceLong(
            Object target, String fieldName, long replacement) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.setLong(target, replacement);
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

    private static final class InjectedJointFault extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
