package io.rowweave.incremental;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitIntentFixtures;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunRecoveryOwnership;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/** Sink journal writes and active phase share one backend CAS. */
class IncrementalInMemorySinkPhaseContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType));

    @Test
    void completePreparedClosureAndIntentProjectMonotonicActivePhase()
            throws Exception {
        Fixture fixture = claimedFixture();
        DurableStageAssignmentAuthority authority = fixture.store;
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fixture.request.firstAttemptFence(), 1L, 0, 101L, 0, 0,
                StageAssignmentRole.SINK, "worker", "localhost", 7001,
                -1, "worker-incarnation");
        StageAssignmentManifest assignments = new StageAssignmentManifest(
                fixture.request.firstAttemptFence(), fixture.request.artifactId(),
                1L, AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(assignment));
        StageAssignmentManifest legacy = new StageAssignmentManifest(
                fixture.request.firstAttemptFence(), fixture.request.artifactId(),
                1L, Collections.singletonList(assignment));
        assertEquals(StageManifestPublication.Code.CONFLICT,
                authority.publish(legacy).code());
        assertNull(authority.assignmentSnapshot(
                fixture.request.firstAttemptFence()));
        StageManifestPublication published = authority.publish(assignments);
        assertEquals(StageManifestPublication.Code.PUBLISHED, published.code());
        StageAssignmentSnapshot snapshot = published.snapshot();
        byte[] workerCommandDigest = sha256("worker-command");
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                snapshot.token(0), workerCommandDigest, 20L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                authority.accept(acceptance).code());
        assertEquals(StageTerminalDecision.Code.RECORDED,
                authority.recordTerminal(StageTerminalReceipt.succeeded(
                        acceptance.token(), acceptance.commandDigest(),
                        7L, 7L, 21L)).code());

        CommitManifest.Member member = new CommitManifest.Member(
                "member", "data/9/member.data", "schema", 3L,
                sha256("data"));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SchemaFingerprint.of(META),
                "domain:member", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity sinkIdentity = SinkAttemptIdentity.forAssignment(
                assignments, assignment, "sink-op", descriptor);
        PreparedSinkLocator locator = new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, sinkIdentity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", ManifestCommitWire.encodeMember(member),
                member.length(), member.sha256(), 22L);
        PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                acceptance.token(), acceptance.commandDigest(), locator);
        assertEquals(PreparedSinkDecision.Code.RECORDED,
                authority.recordPreparedSink(prepared).code());
        assertEquals(IncrementalAttemptCommitPhase.CLAIMED,
                fixture.store.record("feed").activeInterval().phase(),
                "one prepared receipt is not the complete sealed member closure");

        ManifestMemberReceipt sealed = ManifestMemberReceipt.sealed(
                acceptance.token(), acceptance.commandDigest(), "domain", 9L,
                fixture.request.publicationId(), member, 22L, locator);
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                authority.recordManifestMember(sealed).code());
        IncrementalAuthorityRecord preparedRecord = fixture.store.record("feed");
        assertEquals(2L, preparedRecord.revision());
        assertEquals(IncrementalAttemptCommitPhase.PREPARED,
                preparedRecord.activeInterval().phase());
        assertEquals(ManifestMemberDecision.Code.ALREADY_RECORDED,
                authority.recordManifestMember(sealed).code());
        assertSame(preparedRecord, fixture.store.record("feed"),
                "exact member ACK replay must not consume another frontier revision");

        long runUpdatedAtBeforeCommitting =
                advanceToCommitting(fixture, Long.MAX_VALUE);

        ManifestCommitIntent intent = authority.manifestCommitJournal(
                fixture.request.firstAttemptFence()).buildIntent();
        IncrementalAuthorityRecord exhausted = atRevision(
                preparedRecord, Long.MAX_VALUE - 1L);
        replaceRecord(fixture.store, "feed", exhausted);
        assertEquals(ManifestIntentDecision.Code.CORRUPT,
                authority.commitManifestIntent(intent).code());
        assertSame(exhausted, fixture.store.record("feed"));
        assertNull(authority.manifestCommitIntentObservation(
                fixture.request.firstAttemptFence()),
                "joint-success revision must be reserved before durable intent is written");
        replaceRecord(fixture.store, "feed", preparedRecord);

        RunSnapshot currentRun = fixture.store.runSnapshot(fixture.request.runId());
        long currentRunRevision = currentRun.revision();
        replaceRunRevision(currentRun, Long.MAX_VALUE);
        assertEquals(ManifestIntentDecision.Code.CORRUPT,
                authority.commitManifestIntent(intent).code());
        assertNull(authority.manifestCommitIntentObservation(
                fixture.request.firstAttemptFence()),
                "joint-success Run revision must be reserved before durable intent");
        replaceRunRevision(currentRun, currentRunRevision);

        assertEquals(Long.MAX_VALUE, currentRun.updatedAtMillis(),
                "the counterexample must be reached through the progress API");
        assertEquals(ManifestIntentDecision.Code.CORRUPT,
                authority.commitManifestIntent(intent).code());
        assertNull(authority.manifestCommitIntentObservation(
                fixture.request.firstAttemptFence()),
                "joint-success event time must be reserved before durable intent");
        assertSame(preparedRecord, fixture.store.record("feed"));
        replaceRunUpdatedAt(currentRun, runUpdatedAtBeforeCommitting);

        ManifestCommitIntent wrong = ManifestCommitIntentFixtures.ordinary(
                new io.rowweave.run.RunAttemptFence(
                        "other-run", "attempt-1", 1L),
                intent.domainId(), intent.generation(),
                intent.publicationId(), intent.manifestHash());
        assertEquals(ManifestIntentDecision.Code.STALE_RUN,
                authority.commitManifestIntent(wrong).code());
        assertSame(preparedRecord, fixture.store.record("feed"));
        assertNull(authority.manifestCommitIntentObservation(
                fixture.request.firstAttemptFence()));

        assertEquals(ManifestIntentDecision.Code.COMMITTED,
                authority.commitManifestIntent(intent).code());
        IncrementalAuthorityRecord decidedRecord = fixture.store.record("feed");
        assertEquals(3L, decidedRecord.revision());
        assertEquals(IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                decidedRecord.activeInterval().phase());
        assertArrayEquals(sha256(io.rowweave.run.ManifestCommitJournalCodec
                        .encodeIntent(intent)),
                decidedRecord.activeInterval().commitDecisionDigest());
        assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                authority.commitManifestIntent(intent).code());
        assertSame(decidedRecord, fixture.store.record("feed"),
                "intent ACK replay must not consume another frontier revision");

        VerifiedIncrementalSinkCommitDecision verified =
                IncrementalSinkIntentStrictAdapter.verify(
                        decidedRecord.activeInterval(), authority);
        assertEquals(intent.publicationId(), verified.publicationId());
        assertArrayEquals(intent.manifestHash(), verified.manifestHash());
    }

    @Test
    void replacementRecoveryOwnsEverySinkWriteAndPhaseProjection()
            throws Exception {
        Fixture fixture = claimedFixture();
        RunRecoveryOwnership runOwnership = fixture.store;
        RecoveryFencedIncrementalSinkAuthority recovered = fixture.store;
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        RecoveryClaim first = runOwnership.claim(
                run.runId(), run.revision(), 0L,
                new RecoveryOwner("foreign-recovery", 10L));
        assertEquals(RecoveryClaim.Code.ACQUIRED, first.code());

        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fixture.request.firstAttemptFence(), 1L, 0, 202L, 0, 0,
                StageAssignmentRole.SINK, "worker", "localhost", 7002,
                -1, "worker-incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fixture.request.firstAttemptFence(), fixture.request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(assignment));
        assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                fixture.store.publish(manifest).code());
        assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                recovered.publishRecovered(
                        fixture.token, first.token(), manifest).code());

        FrontierOwnershipClaim frontierReplacement = fixture.store.claimOwnership(
                "feed", fixture.store.record("feed").revision(), 1L,
                new RecoveryOwner("master-b", 2L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED,
                frontierReplacement.code());
        RecoveryClaim replacement = runOwnership.claim(
                run.runId(), run.revision(), 1L,
                new RecoveryOwner("master-b", 2L));
        assertEquals(RecoveryClaim.Code.ACQUIRED, replacement.code());
        FrontierOwnershipClaim frontierRefresh = fixture.store.claimOwnership(
                "feed", fixture.store.record("feed").revision(), 2L,
                new RecoveryOwner("master-b", 2L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED,
                frontierRefresh.code());
        assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                recovered.publishRecovered(
                        frontierReplacement.token(), replacement.token(),
                        manifest).code());
        assertNull(fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence()));
        StageManifestPublication published = recovered.publishRecovered(
                frontierRefresh.token(), replacement.token(), manifest);
        assertEquals(StageManifestPublication.Code.PUBLISHED, published.code());

        byte[] commandDigest = sha256("recovered-worker");
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                published.snapshot().token(0), commandDigest, 30L);
        assertEquals(StageAcceptanceDecision.Code.RECOVERY_OWNED,
                fixture.store.accept(acceptance).code());
        assertEquals(StageAcceptanceDecision.Code.RECOVERY_OWNED,
                recovered.acceptRecovered(
                        frontierReplacement.token(), replacement.token(),
                        acceptance).code());
        assertNull(fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence()).acceptance(0));
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                recovered.acceptRecovered(
                        frontierRefresh.token(), replacement.token(),
                        acceptance).code());
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 3L, 3L, 31L);
        assertEquals(StageTerminalDecision.Code.RECOVERY_OWNED,
                fixture.store.recordTerminal(terminal).code());
        assertEquals(StageTerminalDecision.Code.RECOVERY_OWNED,
                recovered.recordTerminalRecovered(
                        frontierReplacement.token(), replacement.token(),
                        terminal).code());
        assertNull(fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence()).terminal(0));
        assertEquals(StageTerminalDecision.Code.RECORDED,
                recovered.recordTerminalRecovered(
                        frontierRefresh.token(), replacement.token(),
                        terminal).code());

        CommitManifest.Member member = member("recovered", 9L);
        PreparedSinkLocator locator = locator(
                manifest, assignment, member, 32L);
        PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                acceptance.token(), acceptance.commandDigest(), locator);
        assertEquals(PreparedSinkDecision.Code.RECOVERY_OWNED,
                fixture.store.recordPreparedSink(prepared).code());
        assertEquals(PreparedSinkDecision.Code.RECOVERY_OWNED,
                recovered.recordPreparedSinkRecovered(
                        frontierReplacement.token(), replacement.token(),
                        prepared).code());
        assertNull(fixture.store.assignmentSnapshot(
                fixture.request.firstAttemptFence()).preparedSink(0));
        assertEquals(PreparedSinkDecision.Code.RECORDED,
                recovered.recordPreparedSinkRecovered(
                        frontierRefresh.token(), replacement.token(),
                        prepared).code());
        ManifestMemberReceipt sealed = ManifestMemberReceipt.sealed(
                acceptance.token(), acceptance.commandDigest(), "domain", 9L,
                fixture.request.publicationId(), member, 32L, locator);
        assertEquals(ManifestMemberDecision.Code.RECOVERY_OWNED,
                recovered.recordManifestMemberRecovered(
                        frontierReplacement.token(), replacement.token(),
                        sealed).code());
        assertNull(fixture.store.manifestCommitJournal(
                fixture.request.firstAttemptFence()).member(0));
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                recovered.recordManifestMemberRecovered(
                        frontierRefresh.token(), replacement.token(),
                        sealed).code());
        IncrementalAuthorityRecord preparedRecord = fixture.store.record("feed");
        assertEquals(IncrementalAttemptCommitPhase.PREPARED,
                preparedRecord.activeInterval().phase());

        run = fixture.store.runSnapshot(fixture.request.runId());
        for (AttemptStatus target : new AttemptStatus[]{
                AttemptStatus.DISPATCHING,
                AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING}) {
            IncrementalAttemptProgressCommand progress =
                    new IncrementalAttemptProgressCommand(
                            "feed", fixture.request.intervalId(),
                            fixture.store.record("feed").revision(), run.revision(),
                            fixture.request.firstAttemptFence(), target,
                            "recovered-progress-" + target.name(),
                            run.updatedAtMillis() + 1L);
            assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER,
                    fixture.store.advanceProgress(fixture.token, progress).code());
            assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER,
                    fixture.store.advanceProgress(
                            frontierReplacement.token(), first.token(), progress).code());
            RunDecision advanced = fixture.store.advanceProgress(
                    frontierRefresh.token(), replacement.token(), progress);
            assertEquals(RunDecision.Code.ACCEPTED, advanced.code());
            run = advanced.snapshot();
        }

        ManifestCommitIntent intent = recovered.manifestCommitJournal(
                fixture.request.firstAttemptFence()).buildIntent();
        assertEquals(ManifestIntentDecision.Code.RECOVERY_OWNED,
                fixture.store.commitManifestIntent(intent).code());
        assertEquals(ManifestIntentDecision.Code.RECOVERY_OWNED,
                recovered.commitManifestIntentRecovered(
                        frontierReplacement.token(), replacement.token(),
                        intent).code());
        assertNull(fixture.store.manifestCommitJournal(
                fixture.request.firstAttemptFence()).intent());
        assertEquals(IncrementalAttemptCommitPhase.PREPARED,
                fixture.store.record("feed").activeInterval().phase());
        assertEquals(ManifestIntentDecision.Code.COMMITTED,
                recovered.commitManifestIntentRecovered(
                        frontierRefresh.token(), replacement.token(),
                        intent).code());
        IncrementalAuthorityRecord decided = fixture.store.record("feed");
        assertEquals(IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                decided.activeInterval().phase());
        assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                recovered.commitManifestIntentRecovered(
                        frontierRefresh.token(), replacement.token(),
                        intent).code());
        assertSame(decided, fixture.store.record("feed"));
    }

    @Test
    void partialMultiOutputClosureAndRowsOverflowRejectBeforeIntent() throws Exception {
        Fixture fixture = claimedFixture();
        StageAssignmentRecord first = StageAssignmentRecord.create(
                fixture.request.firstAttemptFence(), 1L, 0, 301L, 0, 0,
                StageAssignmentRole.SINK, "worker-a", "localhost", 7101,
                -1, "worker-a-incarnation");
        StageAssignmentRecord second = StageAssignmentRecord.create(
                fixture.request.firstAttemptFence(), 1L, 1, 302L, 1, 0,
                StageAssignmentRole.SINK, "worker-b", "localhost", 7102,
                -1, "worker-b-incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fixture.request.firstAttemptFence(), fixture.request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                java.util.Arrays.asList(first, second));
        StageAssignmentSnapshot snapshot = fixture.store.publish(manifest).snapshot();
        byte[] digest = sha256("partial-worker");
        StageAssignmentAcceptance accepted = new StageAssignmentAcceptance(
                snapshot.token(0), digest, 40L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                fixture.store.accept(accepted).code());
        assertEquals(StageTerminalDecision.Code.RECORDED,
                fixture.store.recordTerminal(StageTerminalReceipt.succeeded(
                        accepted.token(), digest, 1L, Long.MAX_VALUE, 41L)).code());
        CommitManifest.Member member = member("partial", 9L);
        PreparedSinkLocator locator = locator(manifest, first, member, 42L);
        assertEquals(PreparedSinkDecision.Code.RECORDED,
                fixture.store.recordPreparedSink(new PreparedSinkReceipt(
                        accepted.token(), digest, locator)).code());
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                fixture.store.recordManifestMember(ManifestMemberReceipt.sealed(
                        accepted.token(), digest, "domain", 9L,
                        fixture.request.publicationId(), member, 42L, locator)).code());

        assertEquals(IncrementalAttemptCommitPhase.CLAIMED,
                fixture.store.record("feed").activeInterval().phase());
        assertFalse(fixture.store.manifestCommitJournal(
                fixture.request.firstAttemptFence()).allExpectedMembersSealed());

        StageAssignmentAcceptance secondAcceptance = new StageAssignmentAcceptance(
                snapshot.token(1), digest, 43L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                fixture.store.accept(secondAcceptance).code());
        assertEquals(StageTerminalDecision.Code.RECORDED,
                fixture.store.recordTerminal(StageTerminalReceipt.succeeded(
                        secondAcceptance.token(), digest, 1L, 1L, 44L)).code());
        CommitManifest.Member secondMember = member("partial-second", 9L);
        PreparedSinkLocator secondLocator = locator(
                manifest, second, secondMember, 45L);
        assertEquals(PreparedSinkDecision.Code.RECORDED,
                fixture.store.recordPreparedSink(new PreparedSinkReceipt(
                        secondAcceptance.token(), digest, secondLocator)).code());
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                fixture.store.recordManifestMember(ManifestMemberReceipt.sealed(
                        secondAcceptance.token(), digest, "domain", 9L,
                        fixture.request.publicationId(), secondMember,
                        45L, secondLocator)).code());
        assertEquals(IncrementalAttemptCommitPhase.PREPARED,
                fixture.store.record("feed").activeInterval().phase());
        advanceToCommitting(fixture);
        ManifestCommitIntent overflowing = fixture.store.manifestCommitJournal(
                fixture.request.firstAttemptFence()).buildIntent();
        assertEquals(ManifestIntentDecision.Code.CORRUPT,
                fixture.store.commitManifestIntent(overflowing).code());
        assertNull(fixture.store.manifestCommitIntentObservation(
                fixture.request.firstAttemptFence()));
    }

    @Test
    void memberLocatorDriftCannotProjectPreparedOrCommitIntent() throws Exception {
        Fixture fixture = claimedFixture();
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fixture.request.firstAttemptFence(), 1L, 0, 401L, 0, 0,
                StageAssignmentRole.SINK, "worker", "localhost", 7201,
                -1, "worker-incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fixture.request.firstAttemptFence(), fixture.request.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(assignment));
        StageAssignmentSnapshot snapshot = fixture.store.publish(manifest).snapshot();
        byte[] digest = sha256("locator-worker");
        StageAssignmentAcceptance accepted = new StageAssignmentAcceptance(
                snapshot.token(0), digest, 50L);
        fixture.store.accept(accepted);
        fixture.store.recordTerminal(StageTerminalReceipt.succeeded(
                accepted.token(), digest, 1L, 1L, 51L));
        CommitManifest.Member member = member("locator", 9L);
        PreparedSinkLocator durable = locator(manifest, assignment, member, 52L);
        fixture.store.recordPreparedSink(new PreparedSinkReceipt(
                accepted.token(), digest, durable));

        byte[] payload = ManifestCommitWire.encodeMember(member);
        PreparedSinkLocator drift = new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, durable.attemptIdentity(),
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", new DurableSinkProviderKey(
                "alternate-provider", 1,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1),
                new DurableSinkCodecDescriptor(
                        "alternate-locator", 1,
                        PreparedSinkLocator.MAX_PAYLOAD_BYTES),
                payload, sha256(payload), member.length(), member.sha256(), 52L);
        assertEquals(ManifestMemberDecision.Code.RECORDED,
                fixture.store.recordManifestMember(ManifestMemberReceipt.sealed(
                        accepted.token(), digest, "domain", 9L,
                        fixture.request.publicationId(), member, 52L, drift)).code());
        assertEquals(IncrementalAttemptCommitPhase.CLAIMED,
                fixture.store.record("feed").activeInterval().phase());

        advanceToCommitting(fixture);
        ManifestCommitIntent intent = fixture.store.manifestCommitJournal(
                fixture.request.firstAttemptFence()).buildIntent();
        assertEquals(ManifestIntentDecision.Code.INCOMPLETE_MEMBERS,
                fixture.store.commitManifestIntent(intent).code());
        assertNull(fixture.store.manifestCommitIntentObservation(
                fixture.request.firstAttemptFence()));
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
                "feed", 0L, 0L, new RecoveryOwner("master", 1L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, ownership.code());

        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 0L,
                IncrementalFrontier.begin(), "trigger", IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier-contract"),
                "artifact", "attempt-1", "publication-1", 3, 0L, 10L);
        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                store.publishPending(ownership.token(), request).code());
        IncrementalAuthorityCommand claim = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, "feed", "claim", 0L, 1L,
                request.intervalId(), request.firstAttemptFence(),
                request.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.claim(
                        claim, ownership.token(), request)).code());

        return new Fixture(store, ownership.token(), request);
    }

    private static void advanceToCommitting(Fixture fixture) {
        advanceToCommitting(fixture, -1L);
    }

    private static long advanceToCommitting(
            Fixture fixture, long committingEventTime) {
        RunSnapshot run = fixture.store.runSnapshot(fixture.request.runId());
        long updatedAtBeforeCommitting = run.updatedAtMillis();
        for (AttemptStatus target : new AttemptStatus[]{
                AttemptStatus.DISPATCHING,
                AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING}) {
            updatedAtBeforeCommitting = run.updatedAtMillis();
            long eventTime = target == AttemptStatus.COMMITTING
                    && committingEventTime >= 0L
                    ? committingEventTime : run.updatedAtMillis() + 1L;
            IncrementalAttemptProgressCommand progress =
                    new IncrementalAttemptProgressCommand(
                            "feed", fixture.request.intervalId(),
                            fixture.store.record("feed").revision(), run.revision(),
                            fixture.request.firstAttemptFence(), target,
                            "progress-" + target.name(),
                            eventTime);
            RunDecision decision = fixture.store.advanceProgress(
                    fixture.token, progress);
            assertEquals(RunDecision.Code.ACCEPTED, decision.code());
            run = decision.snapshot();
        }
        return updatedAtBeforeCommitting;
    }

    private static byte[] sha256(String value) throws Exception {
        return sha256(bytes(value));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static CommitManifest.Member member(String name, long generation)
            throws Exception {
        return new CommitManifest.Member(
                name, "data/" + generation + '/' + name + ".data",
                "schema", 3L, sha256("data-" + name));
    }

    private static PreparedSinkLocator locator(
            StageAssignmentManifest manifest, StageAssignmentRecord assignment,
            CommitManifest.Member member, long preparedAtMillis) {
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SchemaFingerprint.of(META),
                "domain:" + member.name(), "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                manifest, assignment, "sink-op-" + member.name(), descriptor);
        return new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", ManifestCommitWire.encodeMember(member),
                member.length(), member.sha256(), preparedAtMillis);
    }

    private static IncrementalAuthorityRecord atRevision(
            IncrementalAuthorityRecord record, long revision) {
        return new IncrementalAuthorityRecord(
                revision, record.feedId(), record.feedIncarnation(),
                record.definitionDigest(), record.sourceContractDigest(),
                record.frontierContractDigest(), record.frontierKind(),
                record.committedFrontier(), record.activeInterval(),
                record.lastCommitReceipt(), record.lastCompletedBackfillOrdinal(),
                record.lastBackfillReceipt(), record.state(), record.conflictDigest());
    }

    @SuppressWarnings("unchecked")
    private static void replaceRecord(
            InMemoryIncrementalAuthorityStore store, String feedId,
            IncrementalAuthorityRecord replacement) throws Exception {
        java.lang.reflect.Field feedsField =
                InMemoryIncrementalAuthorityStore.class.getDeclaredField("feeds");
        feedsField.setAccessible(true);
        Map<String, Object> feeds = (Map<String, Object>) feedsField.get(store);
        Object state = feeds.get(feedId);
        java.lang.reflect.Field recordField = state.getClass().getDeclaredField("record");
        recordField.setAccessible(true);
        recordField.set(state, replacement);
    }

    private static void replaceRunRevision(
            RunSnapshot snapshot, long replacement) throws Exception {
        java.lang.reflect.Field revision =
                RunSnapshot.class.getDeclaredField("revision");
        revision.setAccessible(true);
        revision.setLong(snapshot, replacement);
    }

    private static void replaceRunUpdatedAt(
            RunSnapshot snapshot, long replacement) throws Exception {
        java.lang.reflect.Field updatedAtMillis =
                RunSnapshot.class.getDeclaredField("updatedAtMillis");
        updatedAtMillis.setAccessible(true);
        updatedAtMillis.setLong(snapshot, replacement);
    }

    private static final class Fixture {
        private final InMemoryIncrementalAuthorityStore store;
        private final FrontierRecoveryToken token;
        private final IncrementalClaimRequest request;

        private Fixture(InMemoryIncrementalAuthorityStore store,
                        FrontierRecoveryToken token,
                        IncrementalClaimRequest request) {
            this.store = store;
            this.token = token;
            this.request = request;
        }
    }
}
