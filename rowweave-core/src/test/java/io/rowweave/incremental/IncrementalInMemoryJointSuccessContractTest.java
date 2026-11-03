package io.rowweave.incremental;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunResultSummary;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
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
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** visible publication and Run success share one CAS. */
class IncrementalInMemoryJointSuccessContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType));

    @Test
    void snapshotIncrementalAndBackfillSuccessAreAtomicReplayableAndSlotIsolated(
            @TempDir Path root) throws Exception {
        Fixture snapshot = preparedFixture(
                root.resolve("snapshot"), IncrementalRunMode.SNAPSHOT_V1);
        assertJointSuccess(snapshot);
        assertJointSuccess(preparedFixture(
                snapshot.store, snapshot.token, root.resolve("incremental"),
                IncrementalRunMode.INCREMENTAL_V1));
        assertJointSuccess(preparedFixture(
                root.resolve("backfill"), IncrementalRunMode.BACKFILL_V1));
    }

    @Test
    void replacedFrontierOrRunOwnershipRejectsWithoutHalfCommit(
            @TempDir Path root) throws Exception {
        Fixture frontierFixture = preparedFixture(
                root.resolve("frontier"), IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityRecord frontierBefore = frontierFixture.store.record("feed");
        RunSnapshot frontierRunBefore = frontierFixture.store.runSnapshot(
                frontierFixture.request.runId());
        IncrementalAuthorityMutation frontierMutation = mutation(
                frontierFixture, frontierFixture.token,
                frontierFixture.runToken, frontierFixture.proof, "stale-frontier");
        FrontierOwnershipClaim replacement = frontierFixture.store.claimOwnership(
                "feed", frontierBefore.revision(),
                frontierFixture.token.ownershipEpoch(), frontierFixture.token.owner());
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, replacement.code());
        IncrementalAuthorityDecision staleFrontier =
                frontierFixture.store.apply(frontierMutation);
        assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                staleFrontier.code());
        assertSame(frontierBefore, frontierFixture.store.record("feed"));
        assertSame(frontierRunBefore, frontierFixture.store.runSnapshot(
                frontierFixture.request.runId()));

        Fixture runFixture = preparedFixture(
                root.resolve("run"), IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityRecord runBefore = runFixture.store.record("feed");
        RunSnapshot runSnapshotBefore = runFixture.store.runSnapshot(
                runFixture.request.runId());
        IncrementalAuthorityMutation runMutation = mutation(
                runFixture, runFixture.token,
                runFixture.runToken, runFixture.proof, "stale-run");
        RecoveryClaim runReplacement = runFixture.store.claim(
                runFixture.request.runId(), runSnapshotBefore.revision(),
                runFixture.runToken.ownershipEpoch(),
                new RecoveryOwner("replacement-master", 2L));
        assertEquals(RecoveryClaim.Code.ACQUIRED, runReplacement.code());
        IncrementalAuthorityDecision staleRun = runFixture.store.apply(runMutation);
        assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                staleRun.code());
        assertSame(runBefore, runFixture.store.record("feed"));
        assertSame(runSnapshotBefore, runFixture.store.runSnapshot(
                runFixture.request.runId()));
    }

    @Test
    void finalCasRereadsDurableIntentBeforeEitherProjection(
            @TempDir Path root) throws Exception {
        Fixture fixture = preparedFixture(root, IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        RunSnapshot runBefore = fixture.store.runSnapshot(fixture.request.runId());
        IncrementalActiveInterval active = before.activeInterval();
        VerifiedIncrementalSinkCommitDecision forgedDecision =
                new VerifiedIncrementalSinkCommitDecision(
                        fixture.proof.domainId(), fixture.proof.fence(),
                        "forged-publication", fixture.proof.generation(),
                        fixture.proof.manifestHash(),
                        fixture.proof.commitDecisionDigest());
        VerifiedIncrementalPublicationProof forgedPublication =
                new VerifiedIncrementalPublicationProof(
                        forgedDecision, fixture.token, fixture.runToken);
        VerifiedIncrementalJointCommitProof forged =
                IncrementalJointCommitProofAssembler.join(
                        active, forgedDecision, forgedPublication);

        IncrementalAuthorityDecision rejected = fixture.store.apply(mutation(
                fixture, fixture.token, fixture.runToken, forged, "forged-intent"));
        assertEquals(IncrementalAuthorityDecision.Code.CONTRACT_CONFLICT,
                rejected.code());
        assertSame(before, rejected.record());
        assertSame(runBefore, rejected.runSnapshot());
        assertSame(before, fixture.store.record("feed"));
        assertSame(runBefore, fixture.store.runSnapshot(fixture.request.runId()));
    }

    @Test
    void cancellationAfterIntentOnlyReconcilesTheSameVisiblePublication(
            @TempDir Path root) throws Exception {
        Fixture fixture = preparedFixture(root, IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        assertEquals(io.rowweave.run.WholeAttemptRetryDecision.Code.TOO_LATE,
                fixture.store.scheduleRetry(
                        fixture.token, fixture.runToken,
                        fixture.request.firstAttemptFence()).code());
        assertSame(before, fixture.store.record("feed"));
        RunCancellationResult cancellation = fixture.store.requestUserCancellation(
                fixture.token, fixture.runToken, fixture.request.runId(),
                "cancel-after-intent");
        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                cancellation.disposition());
        assertEquals(RunStatus.RECONCILING, cancellation.status());
        assertSame(before, fixture.store.record("feed"));

        IncrementalAuthorityDecision committed = fixture.store.apply(mutation(
                fixture, fixture.token, fixture.runToken,
                fixture.proof, "commit-after-cancel"));
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                committed.code());
        assertEquals(RunStatus.SUCCEEDED, committed.runSnapshot().status());
        assertEquals(AttemptStatus.SUCCEEDED,
                committed.runSnapshot().currentAttempt().status());
        assertEquals(fixture.proof.publicationId(),
                committed.record().lastCommitReceipt().publicationId());
    }

    @Test
    void strictConflictProofFreezesActiveFeedAndRunAtomically(
            @TempDir Path root) throws Exception {
        Fixture fixture = preparedFixture(root, IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        RunSnapshot runBefore = fixture.store.runSnapshot(fixture.request.runId());
        ConflictMutation conflict = conflictMutation(
                fixture, root, "freeze-conflict");

        IncrementalAuthorityDecision frozen = fixture.store.apply(conflict.mutation);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED, frozen.code());
        assertEquals(IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT,
                frozen.record().state());
        assertEquals(before.activeInterval(), frozen.record().activeInterval());
        assertEquals(before.committedFrontier(),
                frozen.record().committedFrontier());
        assertArrayEquals(conflict.proof.proofDigest(),
                frozen.record().conflictDigest());
        assertEquals(RunStatus.RECONCILING, frozen.runSnapshot().status());
        assertEquals(AttemptStatus.RECONCILING,
                frozen.runSnapshot().currentAttempt().status());
        assertEquals(runBefore.revision() + 1L,
                frozen.runSnapshot().revision());

        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                fixture.store.apply(conflict.mutation).code());
        assertSame(frozen.record(), fixture.store.record("feed"));
        assertSame(frozen.runSnapshot(),
                fixture.store.runSnapshot(fixture.request.runId()));
    }

    @Test
    void conflictFreezeFaultsExposeOnlyJointBeforeOrAfterState(
            @TempDir Path root) throws Exception {
        InMemoryIncrementalAuthorityStore beforeStore =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (phase == IncrementalJointSuccessPhase.BEFORE_COMMIT) {
                        throw new InjectedJointFault();
                    }
                });
        Path beforeDomain = root.resolve("before-conflict");
        Fixture before = preparedFixture(
                beforeStore, beforeDomain, IncrementalRunMode.SNAPSHOT_V1);
        ConflictMutation beforeConflict = conflictMutation(
                before, beforeDomain, "before-conflict-fault");
        IncrementalAuthorityRecord beforeRecord = before.store.record("feed");
        RunSnapshot beforeRun = before.store.runSnapshot(before.request.runId());
        assertThrows(InjectedJointFault.class,
                () -> before.store.apply(beforeConflict.mutation));
        assertSame(beforeRecord, before.store.record("feed"));
        assertSame(beforeRun, before.store.runSnapshot(before.request.runId()));

        InMemoryIncrementalAuthorityStore afterStore =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        throw new InjectedJointFault();
                    }
                });
        Path afterDomain = root.resolve("after-conflict");
        Fixture after = preparedFixture(
                afterStore, afterDomain, IncrementalRunMode.SNAPSHOT_V1);
        ConflictMutation afterConflict = conflictMutation(
                after, afterDomain, "after-conflict-fault");
        assertThrows(InjectedJointFault.class,
                () -> after.store.apply(afterConflict.mutation));
        IncrementalAuthorityRecord frozen = after.store.record("feed");
        RunSnapshot reconciling = after.store.runSnapshot(after.request.runId());
        assertEquals(IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT,
                frozen.state());
        assertEquals(RunStatus.RECONCILING, reconciling.status());
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                after.store.apply(afterConflict.mutation).code());
        assertSame(frozen, after.store.record("feed"));
        assertSame(reconciling,
                after.store.runSnapshot(after.request.runId()));
    }

    @Test
    void staleConflictProofCannotFreezeReplacementOwnership(
            @TempDir Path root) throws Exception {
        Fixture fixture = preparedFixture(root, IncrementalRunMode.SNAPSHOT_V1);
        ConflictMutation conflict = conflictMutation(
                fixture, root, "stale-conflict-proof");
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        RunSnapshot runBefore = fixture.store.runSnapshot(fixture.request.runId());
        FrontierOwnershipClaim replacement = fixture.store.claimOwnership(
                "feed", before.revision(), fixture.token.ownershipEpoch(),
                new RecoveryOwner("replacement", 2L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, replacement.code());

        IncrementalAuthorityDecision rejected =
                fixture.store.apply(conflict.mutation);
        assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                rejected.code());
        assertSame(before, fixture.store.record("feed"));
        assertSame(runBefore,
                fixture.store.runSnapshot(fixture.request.runId()));
    }

    @Test
    void retainedNormalAndBackfillSlotsReplayAcrossLaterOppositeMode(
            @TempDir Path root) throws Exception {
        Fixture snapshot = preparedFixture(
                root.resolve("snapshot"), IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityMutation snapshotMutation = mutation(
                snapshot, snapshot.token, snapshot.runToken,
                snapshot.proof, "retained-snapshot");
        IncrementalAuthorityDecision snapshotCommit =
                snapshot.store.apply(snapshotMutation);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                snapshotCommit.code());
        IncrementalCommitReceipt snapshotReceipt =
                snapshotCommit.record().lastCommitReceipt();

        Fixture backfill = preparedFixture(
                snapshot.store, snapshot.token, root.resolve("backfill"),
                IncrementalRunMode.BACKFILL_V1);
        IncrementalAuthorityMutation backfillMutation = mutation(
                backfill, backfill.token, backfill.runToken,
                backfill.proof, "retained-backfill");
        IncrementalAuthorityDecision backfillCommit =
                backfill.store.apply(backfillMutation);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                backfillCommit.code());
        assertEquals(snapshotReceipt, backfillCommit.record().lastCommitReceipt());
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                snapshot.store.apply(snapshotMutation).code());

        Fixture incremental = preparedFixture(
                snapshot.store, snapshot.token, root.resolve("incremental"),
                IncrementalRunMode.INCREMENTAL_V1);
        IncrementalAuthorityMutation incrementalMutation = mutation(
                incremental, incremental.token, incremental.runToken,
                incremental.proof, "retained-incremental");
        IncrementalAuthorityDecision incrementalCommit =
                incremental.store.apply(incrementalMutation);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                incrementalCommit.code());
        assertEquals(backfillCommit.record().lastBackfillReceipt(),
                incrementalCommit.record().lastBackfillReceipt());
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                snapshot.store.apply(backfillMutation).code());

        IncrementalAuthorityRecord finalRecord = snapshot.store.record("feed");
        RunSnapshot finalSnapshotRun = snapshot.store.runSnapshot(
                snapshot.request.runId());
        assertEquals(IncrementalAuthorityDecision.Code.STALE_REVISION,
                snapshot.store.apply(snapshotMutation).code());
        assertSame(finalRecord, snapshot.store.record("feed"));
        assertSame(finalSnapshotRun, snapshot.store.runSnapshot(
                snapshot.request.runId()));

        FrontierOwnershipClaim replaced = snapshot.store.claimOwnership(
                "feed", finalRecord.revision(), snapshot.token.ownershipEpoch(),
                new RecoveryOwner("replacement-master", 3L));
        assertEquals(FrontierOwnershipClaim.Code.ACQUIRED, replaced.code());
        assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                snapshot.store.apply(backfillMutation).code());
        assertSame(finalRecord, snapshot.store.record("feed"));
    }

    @Test
    void commandIdCannotBeReusedAcrossRetainedReceiptSlots(
            @TempDir Path root) throws Exception {
        Fixture snapshot = preparedFixture(
                root.resolve("snapshot"), IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityMutation snapshotMutation = mutation(
                snapshot, snapshot.token, snapshot.runToken,
                snapshot.proof, "shared-command-id");
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                snapshot.store.apply(snapshotMutation).code());

        Fixture backfill = preparedFixture(
                snapshot.store, snapshot.token, root.resolve("backfill"),
                IncrementalRunMode.BACKFILL_V1);
        IncrementalAuthorityRecord before = snapshot.store.record("feed");
        RunSnapshot runBefore = snapshot.store.runSnapshot(backfill.request.runId());
        IncrementalAuthorityDecision conflict = snapshot.store.apply(mutation(
                backfill, backfill.token, backfill.runToken,
                backfill.proof, "shared-command-id"));
        assertEquals(IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                conflict.code());
        assertSame(before, snapshot.store.record("feed"));
        assertSame(runBefore, snapshot.store.runSnapshot(backfill.request.runId()));
    }

    @Test
    void jointFaultsExposeOnlyCompleteBeforeOrAfterState(
            @TempDir Path root) throws Exception {
        InMemoryIncrementalAuthorityStore beforeStore =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (phase == IncrementalJointSuccessPhase.BEFORE_COMMIT) {
                        throw new InjectedJointFault();
                    }
                });
        Fixture before = preparedFixture(
                beforeStore, root.resolve("before"), IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityMutation beforeMutation = mutation(
                before, before.token, before.runToken, before.proof, "before-fault");
        IncrementalAuthorityRecord beforeRecord = before.store.record("feed");
        RunSnapshot beforeRun = before.store.runSnapshot(before.request.runId());
        assertThrows(InjectedJointFault.class,
                () -> before.store.apply(beforeMutation));
        assertSame(beforeRecord, before.store.record("feed"));
        assertSame(beforeRun, before.store.runSnapshot(before.request.runId()));
        assertEquals(1, runFeedEntryCount(before.store));

        InMemoryIncrementalAuthorityStore afterStore =
                new InMemoryIncrementalAuthorityStore(phase -> {
                    if (phase
                            == IncrementalJointSuccessPhase.AFTER_COMMIT_BEFORE_ACK) {
                        throw new InjectedJointFault();
                    }
                });
        Fixture after = preparedFixture(
                afterStore, root.resolve("after"), IncrementalRunMode.SNAPSHOT_V1);
        IncrementalAuthorityMutation afterMutation = mutation(
                after, after.token, after.runToken, after.proof, "after-fault");
        assertThrows(InjectedJointFault.class,
                () -> after.store.apply(afterMutation));
        IncrementalAuthorityRecord afterRecord = after.store.record("feed");
        RunSnapshot afterRun = after.store.runSnapshot(after.request.runId());
        assertEquals(IncrementalFrontierAuthorityState.READY, afterRecord.state());
        assertEquals(RunStatus.SUCCEEDED, afterRun.status());
        assertEquals(0, runFeedEntryCount(after.store));
        IncrementalAuthorityDecision replay = after.store.apply(afterMutation);
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED, replay.code());
        assertSame(afterRecord, replay.record());
        assertSame(afterRun, replay.runSnapshot());
    }

    @Test
    void longAlternatingHistoryKeepsRecordAndRunIndexBounded(
            @TempDir Path root) throws Exception {
        Fixture initial = preparedFixture(
                root.resolve("initial"), IncrementalRunMode.SNAPSHOT_V1);
        assertJointSuccess(initial);
        InMemoryIncrementalAuthorityStore store = initial.store;
        FrontierRecoveryToken token = initial.token;
        for (int i = 1; i <= 32; i++) {
            IncrementalRunMode mode = (i & 1) == 1
                    ? IncrementalRunMode.BACKFILL_V1
                    : IncrementalRunMode.INCREMENTAL_V1;
            Fixture next = preparedFixture(
                    store, token, root.resolve("interval-" + i), mode);
            assertJointSuccess(next);
            byte[] frame = IncrementalAuthorityRecordCodec.encode(store.record("feed"));
            assertEquals(true,
                    frame.length <= IncrementalAuthorityRecordCodec.MAX_FRAME_BYTES);
            assertEquals(0, runFeedEntryCount(store));
        }
        assertEquals(16L, store.record("feed").lastCompletedBackfillOrdinal());
        assertEquals(170L, store.record("feed").committedFrontier().sequence());
    }

    @Test
    void exhaustedBackfillOrdinalRejectsBeforePendingRunOrIntent(
            @TempDir Path root) throws Exception {
        Fixture fixture = preparedFixture(root, IncrementalRunMode.BACKFILL_V1);
        assertJointSuccess(fixture);
        IncrementalAuthorityRecord committed = fixture.store.record("feed");
        IncrementalCommitReceipt receipt = committed.lastBackfillReceipt();
        java.lang.reflect.Field expectedSequence =
                IncrementalCommitReceipt.class.getDeclaredField("expectedSequence");
        expectedSequence.setAccessible(true);
        expectedSequence.setLong(receipt, Long.MAX_VALUE);
        IncrementalAuthorityRecord exhausted = new IncrementalAuthorityRecord(
                committed.revision(), committed.feedId(), committed.feedIncarnation(),
                committed.definitionDigest(), committed.sourceContractDigest(),
                committed.frontierContractDigest(), committed.frontierKind(),
                committed.committedFrontier(), null, committed.lastCommitReceipt(),
                Long.MAX_VALUE, receipt, IncrementalFrontierAuthorityState.READY, null);
        replaceRecord(fixture.store, "feed", exhausted);

        CapturedTargetReceipt capture = CapturedTargetReceipt.backfill(
                1L, sha256("definition"), sha256("source-contract"), 1L,
                IncrementalFrontier.int64Sequence(0L),
                IncrementalFrontier.int64Sequence(10L),
                "overflow-backfill", bytes("snapshot"));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier-contract"), "artifact",
                "attempt", "publication", 3, 0L, 10L);
        assertEquals(IncrementalPendingRunDecision.Code.CONTRACT_CONFLICT,
                fixture.store.publishPending(fixture.token, request).code());
        assertNull(fixture.store.pending("feed"));
        assertNull(fixture.store.runSnapshot(request.runId()));
        assertEquals(0, runFeedEntryCount(fixture.store));
    }

    private static void assertJointSuccess(Fixture fixture) throws Exception {
        IncrementalAuthorityRecord before = fixture.store.record("feed");
        RunSnapshot runBefore = fixture.store.runSnapshot(fixture.request.runId());
        IncrementalAuthorityCommandKind kind = fixture.mode
                == IncrementalRunMode.BACKFILL_V1
                ? IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                : IncrementalAuthorityCommandKind.COMMIT;
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                kind, "feed", "joint-" + fixture.mode.name() + '-'
                + fixture.request.intervalId(), before.revision(),
                1L, fixture.request.intervalId(),
                fixture.request.firstAttemptFence(), fixture.proof.proofDigest());
        IncrementalAuthorityMutation mutation =
                IncrementalAuthorityMutation.jointTerminal(
                        command, fixture.token, fixture.runToken, fixture.proof);

        IncrementalAuthorityDecision committed = fixture.store.apply(mutation);
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED, committed.code());
        IncrementalAuthorityRecord after = committed.record();
        RunSnapshot runAfter = committed.runSnapshot();
        assertEquals(before.revision() + 1L, after.revision());
        assertEquals(IncrementalFrontierAuthorityState.READY, after.state());
        assertNull(after.activeInterval());
        assertEquals(RunStatus.SUCCEEDED, runAfter.status());
        assertEquals(AttemptStatus.SUCCEEDED,
                runAfter.currentAttempt().status());
        RunResultSummary summary = runAfter.currentAttempt().resultSummary();
        assertEquals(RunResultSummary.PublicationKind.MANIFEST,
                summary.publicationKind());
        assertEquals(3L, summary.rowsOut());
        assertEquals(Long.valueOf(fixture.proof.generation()),
                summary.manifestGeneration());
        assertArrayEquals(fixture.proof.manifestHash(), summary.manifestSha256());

        IncrementalCommitReceipt receipt = IncrementalCommitReceipt.committed(
                before.activeInterval(), fixture.proof.publicationId(),
                fixture.proof.generation(), fixture.proof.manifestHash(),
                command.commandId(), IncrementalAuthorityCommandCodec.digest(command));
        if (fixture.mode == IncrementalRunMode.BACKFILL_V1) {
            assertEquals(before.committedFrontier(), after.committedFrontier());
            assertEquals(before.activeInterval().capturedTarget().expectedSequence(),
                    after.lastCompletedBackfillOrdinal());
            assertEquals(receipt, after.lastBackfillReceipt());
            assertEquals(before.lastCommitReceipt(), after.lastCommitReceipt());
        } else {
            assertEquals(before.activeInterval().capturedTarget().target(),
                    after.committedFrontier());
            assertEquals(receipt, after.lastCommitReceipt());
            assertEquals(before.lastCompletedBackfillOrdinal(),
                    after.lastCompletedBackfillOrdinal());
            assertEquals(before.lastBackfillReceipt(), after.lastBackfillReceipt());
        }
        assertEquals(0, runFeedEntryCount(fixture.store));

        IncrementalAuthorityDecision replay = fixture.store.apply(mutation);
        assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED, replay.code());
        assertSame(after, replay.record());
        assertSame(runAfter, replay.runSnapshot());

        IncrementalAuthorityCommand drift = new IncrementalAuthorityCommand(
                kind, "feed", command.commandId(), after.revision(), 1L,
                fixture.request.intervalId(), fixture.request.firstAttemptFence(),
                fixture.proof.proofDigest());
        assertEquals(IncrementalAuthorityDecision.Code.COMMAND_ID_CONFLICT,
                fixture.store.apply(IncrementalAuthorityMutation.jointTerminal(
                        drift, fixture.token, fixture.runToken, fixture.proof)).code());
        assertSame(after, fixture.store.record("feed"));
        assertSame(runAfter, fixture.store.runSnapshot(fixture.request.runId()));

        IncrementalAuthorityCommand stale = new IncrementalAuthorityCommand(
                kind, "feed", "stale-" + fixture.mode.name(), before.revision(),
                1L, fixture.request.intervalId(),
                fixture.request.firstAttemptFence(), fixture.proof.proofDigest());
        assertEquals(IncrementalAuthorityDecision.Code.STALE_REVISION,
                fixture.store.apply(IncrementalAuthorityMutation.jointTerminal(
                        stale, fixture.token, fixture.runToken, fixture.proof)).code());
        assertSame(after, fixture.store.record("feed"));
        assertSame(runAfter, fixture.store.runSnapshot(fixture.request.runId()));
        assertEquals(runBefore.revision() + 1L, runAfter.revision());
    }

    private static IncrementalAuthorityMutation mutation(
            Fixture fixture, FrontierRecoveryToken token, RecoveryToken runToken,
            VerifiedIncrementalJointCommitProof proof, String commandId) {
        IncrementalAuthorityCommandKind kind = fixture.mode
                == IncrementalRunMode.BACKFILL_V1
                ? IncrementalAuthorityCommandKind.COMPLETE_BACKFILL
                : IncrementalAuthorityCommandKind.COMMIT;
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                kind, "feed", commandId, fixture.store.record("feed").revision(),
                1L, fixture.request.intervalId(),
                fixture.request.firstAttemptFence(), proof.proofDigest());
        return IncrementalAuthorityMutation.jointTerminal(
                command, token, runToken, proof);
    }

    private static ConflictMutation conflictMutation(
            Fixture fixture, Path domain, String commandId) throws Exception {
        IncrementalAuthorityRecord record = fixture.store.record("feed");
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(
                        record.activeInterval(), fixture.store);
        Files.write(domain.resolve("manifests/9.manifest"),
                bytes("durably-corrupt-manifest"));
        VerifiedIncrementalConflictProof proof =
                IncrementalManifestStrictAdapter.verifyConflict(
                        record.activeInterval(), sinkDecision,
                        new ManifestFileStore(domain),
                        fixture.token, fixture.runToken);
        IncrementalAuthorityCommand unbound = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.MARK_CONFLICT,
                "feed", commandId + "-unbound", record.revision(), 1L,
                null, null, proof.proofDigest());
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalAuthorityMutation.markConflict(
                        unbound, fixture.token, fixture.runToken, proof));
        IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.MARK_CONFLICT,
                "feed", commandId, record.revision(), 1L,
                fixture.request.intervalId(), fixture.request.firstAttemptFence(),
                proof.proofDigest());
        return new ConflictMutation(proof,
                IncrementalAuthorityMutation.markConflict(
                        command, fixture.token, fixture.runToken, proof));
    }

    private static Fixture preparedFixture(Path domain, IncrementalRunMode mode)
            throws Exception {
        InMemoryIncrementalAuthorityStore store =
                new InMemoryIncrementalAuthorityStore();
        return preparedFixture(store, domain, mode);
    }

    private static Fixture preparedFixture(
            InMemoryIncrementalAuthorityStore store, Path domain,
            IncrementalRunMode mode) throws Exception {
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
        return preparedFixture(store, ownership.token(), domain, mode);
    }

    private static Fixture preparedFixture(
            InMemoryIncrementalAuthorityStore store, FrontierRecoveryToken token,
            Path domain, IncrementalRunMode mode) throws Exception {
        IncrementalAuthorityRecord current = store.record("feed");
        CapturedTargetReceipt capture = mode == IncrementalRunMode.BACKFILL_V1
                ? CapturedTargetReceipt.backfill(
                1L, sha256("definition"), sha256("source-contract"),
                current.lastCompletedBackfillOrdinal() + 1L,
                IncrementalFrontier.int64Sequence(0L),
                IncrementalFrontier.int64Sequence(10L),
                "backfill-" + (current.lastCompletedBackfillOrdinal() + 1L),
                bytes("snapshot"))
                : CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"),
                current.revision(), current.committedFrontier(),
                "trigger-" + mode.name(), mode,
                IncrementalFrontier.int64Sequence(
                        mode == IncrementalRunMode.SNAPSHOT_V1 ? 10L
                                : current.committedFrontier().sequence() + 10L),
                bytes("snapshot-" + mode.name()));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed", capture, sha256("frontier-contract"), "artifact",
                "attempt-1", "publication-" + mode.name(), 3, 0L, 10L);
        assertEquals(IncrementalPendingRunDecision.Code.PUBLISHED,
                store.publishPending(token, request).code());
        IncrementalAuthorityCommand claim = new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.CLAIM, "feed",
                "claim-" + mode.name(), current.revision(), 1L,
                request.intervalId(), request.firstAttemptFence(),
                request.transitionDigest());
        assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                store.apply(IncrementalAuthorityMutation.claim(
                        claim, token, request)).code());

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

        ManifestFileStore manifestStore = new ManifestFileStore(domain);
        Path staged = manifestStore.stagingPath(request.publicationId(), "part-0");
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
            RunDecision advanced = store.advanceProgress(
                    token, progress);
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
        assertEquals(RecoveryClaim.Code.ACQUIRED, runOwnership.code());

        IncrementalActiveInterval active = store.record("feed").activeInterval();
        VerifiedIncrementalSinkCommitDecision sinkDecision =
                IncrementalSinkIntentStrictAdapter.verify(active, store);
        VerifiedIncrementalPublicationProof publication =
                IncrementalManifestStrictAdapter.verify(
                        sinkDecision, manifestStore,
                        token, runOwnership.token());
        VerifiedIncrementalJointCommitProof proof =
                IncrementalJointCommitProofAssembler.join(
                        active, sinkDecision, publication);
        return new Fixture(
                store, token, runOwnership.token(), request, mode, proof);
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

    @SuppressWarnings("unchecked")
    private static int runFeedEntryCount(InMemoryIncrementalAuthorityStore store)
            throws Exception {
        java.lang.reflect.Field field =
                InMemoryIncrementalAuthorityStore.class.getDeclaredField("runFeeds");
        field.setAccessible(true);
        return ((Map<String, String>) field.get(store)).size();
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
        java.lang.reflect.Field recordField =
                state.getClass().getDeclaredField("record");
        recordField.setAccessible(true);
        recordField.set(state, replacement);
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
        private final RecoveryToken runToken;
        private final IncrementalClaimRequest request;
        private final IncrementalRunMode mode;
        private final VerifiedIncrementalJointCommitProof proof;

        private Fixture(
                InMemoryIncrementalAuthorityStore store,
                FrontierRecoveryToken token, RecoveryToken runToken,
                IncrementalClaimRequest request,
                IncrementalRunMode mode,
                VerifiedIncrementalJointCommitProof proof) {
            this.store = store;
            this.token = token;
            this.runToken = runToken;
            this.request = request;
            this.mode = mode;
            this.proof = proof;
        }
    }

    private static final class ConflictMutation {
        private final VerifiedIncrementalConflictProof proof;
        private final IncrementalAuthorityMutation mutation;

        private ConflictMutation(
                VerifiedIncrementalConflictProof proof,
                IncrementalAuthorityMutation mutation) {
            this.proof = proof;
            this.mutation = mutation;
        }
    }

    private static final class InjectedJointFault extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
