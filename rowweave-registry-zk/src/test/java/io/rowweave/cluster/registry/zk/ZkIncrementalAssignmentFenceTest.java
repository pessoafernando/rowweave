package io.rowweave.cluster.registry.zk;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.incremental.CapturedTargetReceipt;
import io.rowweave.incremental.FrontierRecoveryToken;
import io.rowweave.incremental.IncrementalAttemptProgressCommand;
import io.rowweave.incremental.IncrementalAttemptCommitPhase;
import io.rowweave.incremental.IncrementalAuthorityCommand;
import io.rowweave.incremental.IncrementalAuthorityCommandKind;
import io.rowweave.incremental.IncrementalAuthorityDecision;
import io.rowweave.incremental.IncrementalClaimRequest;
import io.rowweave.incremental.IncrementalFrontier;
import io.rowweave.incremental.IncrementalFrontierAuthorityState;
import io.rowweave.incremental.IncrementalInitializeRequest;
import io.rowweave.incremental.IncrementalRunMode;
import io.rowweave.incremental.VerifiedIncrementalJointCommitProof;
import io.rowweave.incremental.VerifiedIncrementalPublicationProof;
import io.rowweave.incremental.VerifiedIncrementalSinkCommitDecision;
import io.rowweave.incremental.VerifiedIncrementalConflictProof;
import io.rowweave.incremental.VerifiedIncrementalAbortClosureProof;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalCodec;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RetryAttemptClaim;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.PreparedSinkDecision;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryDecision;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.lang.reflect.Constructor;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** assignment publication shares the frontier ownership fence. */
class ZkIncrementalAssignmentFenceTest {

    @Test
    void replacementInsideStagingCasCannotPublishAssignmentRoot()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            StageAssignmentManifest manifest = manifest(fixture.run);
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.PUBLISH_STAGING_MARKER
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                    first.publishAssignments(fixture.frontier, manifest).code());
            assertNull(replacementRuns.assignmentSnapshot(manifest.fence()));
        }
    }

    @Test
    void currentOwnerPublishesAndReplacementReplaysWithDualTokens()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            StageAssignmentManifest manifest = manifest(fixture.run);
            assertEquals(StageManifestPublication.Code.PUBLISHED,
                    first.publishAssignments(fixture.frontier, manifest).code());
            assertEquals(StageManifestPublication.Code.ALREADY_PUBLISHED,
                    first.publishAssignments(fixture.frontier, manifest).code());

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", 1L, 1L, ownerB).token();
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, fixture.run.runId(), fixture.run.revision(), 0L, ownerB);
            assertEquals(RecoveryClaim.Code.ACQUIRED, runB.code());
            assertEquals(StageManifestPublication.Code.RECOVERY_OWNED,
                    first.publishAssignments(fixture.frontier, manifest).code());
            assertEquals(StageManifestPublication.Code.ALREADY_PUBLISHED,
                    replacement.publishAssignments(
                            frontierB, runB.token(), manifest).code());
        }
    }

    @Test
    void replacementInsideAcceptanceCasRejectsOldLeaderAndNewOwnerCanRecord()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            StageAssignmentManifest manifest = manifest(fixture.run);
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    first.publishAssignments(fixture.frontier, manifest)
                            .snapshot().token(0), filled(7), 11L);
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.ACCEPT
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(StageAcceptanceDecision.Code.RECOVERY_OWNED,
                    first.acceptAssignment(fixture.frontier, acceptance).code());
            assertEquals(0, replacementRuns.assignmentSnapshot(
                    manifest.fence()).acceptedCount());
            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", 1L, 1L, ownerB).token();
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, fixture.run.runId(), fixture.run.revision(), 0L, ownerB);
            StageAssignmentAcceptance recoveredAcceptance =
                    new StageAssignmentAcceptance(
                            replacementRuns.assignmentSnapshot(manifest.fence())
                                    .token(0), filled(7), 11L);
            assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                    replacement.acceptAssignment(
                            frontierB, runB.token(), recoveredAcceptance).code());
        }
    }

    @Test
    void replacementInsideTerminalCasLeavesNoTerminalEvidence()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            StageAssignmentManifest manifest = manifest(fixture.run);
            StageManifestPublication publication =
                    first.publishAssignments(fixture.frontier, manifest);
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    publication.snapshot().token(0), filled(7), 11L);
            first.acceptAssignment(fixture.frontier, acceptance);
            StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                    acceptance.token(), acceptance.commandDigest(), 3L, 2L, 12L);
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.TERMINAL
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(StageTerminalDecision.Code.RECOVERY_OWNED,
                    first.recordStageTerminal(fixture.frontier, terminal).code());
            assertNull(replacementRuns.assignmentSnapshot(
                    manifest.fence()).terminal(0));
        }
    }

    @Test
    void replacementInsidePreparedCasLeavesNoProviderLocator()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            StageAssignmentManifest manifest = manifest(fixture.run);
            StageManifestPublication publication =
                    first.publishAssignments(fixture.frontier, manifest);
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    publication.snapshot().token(0), filled(7), 11L);
            first.acceptAssignment(fixture.frontier, acceptance);
            first.recordStageTerminal(fixture.frontier,
                    StageTerminalReceipt.succeeded(
                            acceptance.token(), acceptance.commandDigest(),
                            3L, 2L, 12L));
            BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                    1, "rowweave.manifest-file", 1, hex(digest("schema")),
                    "domain:member", "domain",
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, manifest.assignment(0), "sink-op", descriptor);
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    "domain", bytes("locator"), 2L, digest("data"), 13L);
            PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                    acceptance.token(), acceptance.commandDigest(), locator);
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.TERMINAL
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(PreparedSinkDecision.Code.RECOVERY_OWNED,
                    first.recordPreparedSink(fixture.frontier, prepared).code());
            assertNull(replacementRuns.assignmentSnapshot(
                    manifest.fence()).preparedSink(0));
        }
    }

    @Test
    void replacementFencesManifestMemberAndCommitIntentTransactions()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            ManifestFixture evidence = preparedEvidence(first, firstRuns, fixture);
            AtomicBoolean replaceMember = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.MANIFEST_MEMBER
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaceMember.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(ManifestMemberDecision.Code.RECOVERY_OWNED,
                    first.recordManifestMember(
                            fixture.frontier, evidence.member).code());
            assertEquals(0, replacementRuns.manifestCommitJournal(
                    evidence.manifest.fence()).memberCount());
            assertEquals(IncrementalAttemptCommitPhase.CLAIMED,
                    replacement.record("feed-assignment")
                            .activeInterval().phase());

            RecoveryOwner ownerC = new RecoveryOwner("master-c", 3L);
            FrontierRecoveryToken frontierC = replacement.claimOwnership(
                    "feed-assignment", 1L, 2L, ownerC).token();
            RunSnapshot current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runC = replacement.claimRunOwnership(
                    frontierC, current.runId(), current.revision(), 0L, ownerC);
            assertEquals(ManifestMemberDecision.Code.RECORDED,
                    replacement.recordManifestMember(
                            frontierC, runC.token(), evidence.member).code());
            advanceToCommitting(replacement, replacementRuns,
                    frontierC, runC.token(), fixture.intervalId);
            ManifestCommitIntent intent = replacementRuns.manifestCommitJournal(
                    evidence.manifest.fence()).buildIntent();
            AtomicBoolean replaceIntent = new AtomicBoolean(true);
            replacementRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.MANIFEST_INTENT
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaceIntent.compareAndSet(true, false)) {
                    first.claimOwnership(
                            "feed-assignment", 2L, 3L,
                            new RecoveryOwner("master-d", 4L));
                }
            });

            assertEquals(ManifestIntentDecision.Code.RECOVERY_OWNED,
                    replacement.commitManifestIntent(
                            frontierC, runC.token(), intent).code());
            assertNull(firstRuns.manifestCommitJournal(
                    evidence.manifest.fence()).intent());
            assertEquals(IncrementalAttemptCommitPhase.PREPARED,
                    first.record("feed-assignment")
                            .activeInterval().phase());
        }
    }

    @Test
    void replacementFencesAbortDecisionAndCleanupCompletion()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            ManifestFixture evidence = preparedEvidence(first, firstRuns, fixture);
            RunSnapshot current = firstRuns.get(fixture.run.runId());
            RecoveryClaim runA = first.claimRunOwnership(
                    fixture.frontier, current.runId(), current.revision(), 0L,
                    fixture.frontier.owner());
            advance(first, firstRuns, fixture.frontier, runA.token(),
                    fixture.intervalId,
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING);
            BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                    evidence.manifest.fence(), evidence.manifest.generation(),
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    "domain", 31L, "publication-assignment",
                    digest("abort-identity"), "SOURCE_FAILED", 30L);
            AtomicBoolean replaceDecision = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.SINK_ABORT
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaceDecision.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(BatchSinkAbortDecision.Code.RECOVERY_OWNED,
                    first.recordBatchSinkAbort(
                            fixture.frontier, runA.token(), decided).code());
            assertNull(replacementRuns.batchSinkAbort(evidence.manifest.fence()));

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", 1L, 1L, ownerB).token();
            current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, current.runId(), current.revision(),
                    runA.token().ownershipEpoch(), ownerB);
            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    replacement.recordBatchSinkAbort(
                            frontierB, runB.token(), decided).code());
            AtomicBoolean replaceCompletion = new AtomicBoolean(true);
            replacementRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.SINK_ABORT
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaceCompletion.compareAndSet(true, false)) {
                    first.claimOwnership(
                            "feed-assignment", 1L, 2L,
                            new RecoveryOwner("master-c", 3L));
                }
            });

            assertEquals(BatchSinkAbortDecision.Code.RECOVERY_OWNED,
                    replacement.recordBatchSinkAbort(
                            frontierB, runB.token(), decided.completed(31L)).code());
            assertEquals(decided,
                    firstRuns.batchSinkAbort(evidence.manifest.fence()));
        }
    }

    @Test
    void memberAndIntentProjectFrontierPhaseAtTheirLinearizationPoints()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore runs = new ZkRunStateStore(client);
            ZkIncrementalFrontierStore store =
                    new ZkIncrementalFrontierStore(client, runs);
            Fixture fixture = claimed(store, runs);
            ManifestFixture evidence = preparedEvidence(store, runs, fixture);

            assertEquals(ManifestMemberDecision.Code.RECORDED,
                    store.recordManifestMember(
                            fixture.frontier, evidence.member).code());
            assertEquals(IncrementalAttemptCommitPhase.PREPARED,
                    store.record("feed-assignment")
                            .activeInterval().phase());
            advanceToCommitting(store, runs, fixture.frontier, null,
                    fixture.intervalId);
            ManifestCommitIntent intent = runs.manifestCommitJournal(
                    evidence.manifest.fence()).buildIntent();

            assertEquals(ManifestIntentDecision.Code.COMMITTED,
                    store.commitManifestIntent(fixture.frontier, intent).code());
            assertEquals(IncrementalAttemptCommitPhase.COMMIT_DECIDED,
                    store.record("feed-assignment")
                            .activeInterval().phase());
            assertArrayEquals(digest(ManifestCommitJournalCodec.encodeIntent(intent)),
                    store.record("feed-assignment")
                            .activeInterval().commitDecisionDigest());
        }
    }

    @Test
    void jointSuccessHasNoHalfStateAcrossFrontierReplacementAndReplay()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            ManifestFixture evidence = preparedEvidence(first, firstRuns, fixture);
            first.recordManifestMember(fixture.frontier, evidence.member);
            RunSnapshot current = firstRuns.get(fixture.run.runId());
            RecoveryClaim runA = first.claimRunOwnership(
                    fixture.frontier, current.runId(), current.revision(), 0L,
                    fixture.frontier.owner());
            advanceToCommitting(first, firstRuns, fixture.frontier, runA.token(),
                    fixture.intervalId);
            ManifestCommitIntent intent = firstRuns.manifestCommitJournal(
                    evidence.manifest.fence()).buildIntent();
            first.commitManifestIntent(fixture.frontier, runA.token(), intent);
            VerifiedIncrementalJointCommitProof proofA = proof(
                    first.record("feed-assignment").activeInterval(), intent,
                    fixture.frontier, runA.token());
            IncrementalAuthorityCommand commandA = jointCommand(
                    first.record("feed-assignment"), proofA, "joint-success");
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 3L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                    first.jointSuccess(
                            fixture.frontier, runA.token(), commandA, proofA).code());
            assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                    replacement.record("feed-assignment").state());
            assertEquals(RunStatus.RUNNING,
                    replacementRuns.get(fixture.run.runId()).status());

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", 3L, 1L, ownerB).token();
            current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, current.runId(), current.revision(),
                    runA.token().ownershipEpoch(), ownerB);
            VerifiedIncrementalJointCommitProof proofB = proof(
                    replacement.record("feed-assignment").activeInterval(), intent,
                    frontierB, runB.token());
            IncrementalAuthorityCommand commandB = jointCommand(
                    replacement.record("feed-assignment"), proofB,
                    "joint-success-recovered");
            AtomicBoolean loseAck = new AtomicBoolean(true);
            replacementRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment
                        == ZkRunStateStore.GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK
                        && loseAck.compareAndSet(true, false)) {
                    throw new org.apache.zookeeper.KeeperException
                            .ConnectionLossException();
                }
            });

            IncrementalAuthorityDecision committed = replacement.jointSuccess(
                    frontierB, runB.token(), commandB, proofB);
            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    committed.code());
            assertEquals(IncrementalFrontierAuthorityState.READY,
                    committed.record().state());
            assertEquals(IncrementalFrontier.int64Sequence(2L),
                    committed.record().committedFrontier());
            assertEquals(RunStatus.SUCCEEDED, committed.runSnapshot().status());
            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    replacement.jointSuccess(
                            frontierB, runB.token(), commandB, proofB).code());
        }
    }

    @Test
    void conflictFreezeHasNoHalfStateAcrossFrontierReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            ManifestFixture evidence = preparedEvidence(first, firstRuns, fixture);
            first.recordManifestMember(fixture.frontier, evidence.member);
            RunSnapshot current = firstRuns.get(fixture.run.runId());
            RecoveryClaim runA = first.claimRunOwnership(
                    fixture.frontier, current.runId(), current.revision(), 0L,
                    fixture.frontier.owner());
            advanceToCommitting(first, firstRuns, fixture.frontier, runA.token(),
                    fixture.intervalId);
            ManifestCommitIntent intent = firstRuns.manifestCommitJournal(
                    evidence.manifest.fence()).buildIntent();
            first.commitManifestIntent(fixture.frontier, runA.token(), intent);
            VerifiedIncrementalConflictProof proofA = conflictProof(
                    first.record("feed-assignment").activeInterval(), intent,
                    fixture.frontier, runA.token());
            IncrementalAuthorityCommand commandA = new IncrementalAuthorityCommand(
                    IncrementalAuthorityCommandKind.MARK_CONFLICT,
                    "feed-assignment", "freeze-conflict", 3L, 1L,
                    proofA.intervalId(), proofA.fence(), proofA.proofDigest());
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 3L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                    first.markConflict(
                            fixture.frontier, runA.token(), commandA, proofA).code());
            assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                    replacement.record("feed-assignment").state());
            assertEquals(AttemptStatus.COMMITTING,
                    replacementRuns.get(fixture.run.runId())
                            .currentAttempt().status());

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", 3L, 1L, ownerB).token();
            current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, current.runId(), current.revision(),
                    runA.token().ownershipEpoch(), ownerB);
            VerifiedIncrementalConflictProof proofB = conflictProof(
                    replacement.record("feed-assignment").activeInterval(), intent,
                    frontierB, runB.token());
            IncrementalAuthorityCommand commandB = new IncrementalAuthorityCommand(
                    IncrementalAuthorityCommandKind.MARK_CONFLICT,
                    "feed-assignment", "freeze-conflict-recovered", 3L, 1L,
                    proofB.intervalId(), proofB.fence(), proofB.proofDigest());

            IncrementalAuthorityDecision frozen = replacement.markConflict(
                    frontierB, runB.token(), commandB, proofB);
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED, frozen.code());
            assertEquals(IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT,
                    frozen.record().state());
            assertEquals(AttemptStatus.RECONCILING,
                    frozen.runSnapshot().currentAttempt().status());
            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    replacement.markConflict(
                            frontierB, runB.token(), commandB, proofB).code());
        }
    }

    @Test
    void conflictFreezeStillCommitsWhenRunIsAlreadyReconciling()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore runs = new ZkRunStateStore(client);
            ZkIncrementalFrontierStore store =
                    new ZkIncrementalFrontierStore(client, runs);
            Fixture fixture = claimed(store, runs);
            ManifestFixture evidence = preparedEvidence(store, runs, fixture);
            store.recordManifestMember(fixture.frontier, evidence.member);
            RunSnapshot current = runs.get(fixture.run.runId());
            RecoveryClaim run = store.claimRunOwnership(
                    fixture.frontier, current.runId(), current.revision(), 0L,
                    fixture.frontier.owner());
            advanceToCommitting(store, runs, fixture.frontier, run.token(),
                    fixture.intervalId);
            ManifestCommitIntent intent = runs.manifestCommitJournal(
                    evidence.manifest.fence()).buildIntent();
            store.commitManifestIntent(fixture.frontier, run.token(), intent);
            current = runs.get(fixture.run.runId());
            assertEquals(io.rowweave.run.RunDecision.Code.ACCEPTED,
                    runs.applyRecovered(run.token(), RunCommand.advanceAttempt(
                            "reconcile-before-freeze", current.runId(),
                            current.revision(),
                            current.currentAttempt().attemptId(),
                            current.currentAttempt().fenceToken(),
                            AttemptStatus.RECONCILING, false,
                            current.updatedAtMillis() + 1L,
                            "COMMIT_ACK_LOST", null)).code());

            VerifiedIncrementalConflictProof proof = conflictProof(
                    store.record("feed-assignment").activeInterval(), intent,
                    fixture.frontier, run.token());
            IncrementalAuthorityCommand command = new IncrementalAuthorityCommand(
                    IncrementalAuthorityCommandKind.MARK_CONFLICT,
                    "feed-assignment", "freeze-after-reconciling",
                    store.record("feed-assignment").revision(), 1L,
                    proof.intervalId(), proof.fence(), proof.proofDigest());

            IncrementalAuthorityDecision frozen = store.markConflict(
                    fixture.frontier, run.token(), command, proof);
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED,
                    frozen.code());
            assertEquals(IncrementalFrontierAuthorityState.CORRUPT_OR_CONFLICT,
                    frozen.record().state());
            assertEquals(AttemptStatus.RECONCILING,
                    frozen.runSnapshot().currentAttempt().status());
            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    store.markConflict(
                            fixture.frontier, run.token(), command, proof).code());
        }
    }

    @Test
    void abortClosureProofAndFailedCloseAreJointlyFrontierFenced()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            ManifestFixture evidence = preparedEvidence(first, firstRuns, fixture);
            RunSnapshot current = firstRuns.get(fixture.run.runId());
            RecoveryClaim runA = first.claimRunOwnership(
                    fixture.frontier, current.runId(), current.revision(), 0L,
                    fixture.frontier.owner());
            advance(first, firstRuns, fixture.frontier, runA.token(),
                    fixture.intervalId,
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING);
            BatchSinkAbortRecord decided = BatchSinkAbortRecord.decided(
                    evidence.manifest.fence(), evidence.manifest.generation(),
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    "domain", 31L, "publication-assignment",
                    digest("abort-close-identity"), "SOURCE_FAILED", 30L);
            first.recordBatchSinkAbort(fixture.frontier, runA.token(), decided);
            first.recordBatchSinkAbort(
                    fixture.frontier, runA.token(), decided.completed(31L));
            VerifiedIncrementalAbortClosureProof proofA =
                    first.verifyAbortClosure(
                            fixture.frontier, runA.token(),
                            evidence.manifest.fence(), AttemptStatus.FAILED);
            IncrementalAuthorityCommand commandA = new IncrementalAuthorityCommand(
                    IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                    "feed-assignment", "close-failed", 1L, 1L,
                    proofA.intervalId(), proofA.fence(), proofA.proofDigest());
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", 1L, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(IncrementalAuthorityDecision.Code.STALE_OWNERSHIP,
                    first.closeAborted(
                            fixture.frontier, runA.token(), commandA, proofA).code());
            assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                    replacement.record("feed-assignment").state());
            assertEquals(RunStatus.RUNNING,
                    replacementRuns.get(fixture.run.runId()).status());

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", 1L, 1L, ownerB).token();
            current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, current.runId(), current.revision(),
                    runA.token().ownershipEpoch(), ownerB);
            VerifiedIncrementalAbortClosureProof proofB =
                    replacement.verifyAbortClosure(
                            frontierB, runB.token(), evidence.manifest.fence(),
                            AttemptStatus.FAILED);
            IncrementalAuthorityCommand commandB = new IncrementalAuthorityCommand(
                    IncrementalAuthorityCommandKind.CLOSE_ABORTED_RUN,
                    "feed-assignment", "close-failed-recovered", 1L, 1L,
                    proofB.intervalId(), proofB.fence(), proofB.proofDigest());

            IncrementalAuthorityDecision closed = replacement.closeAborted(
                    frontierB, runB.token(), commandB, proofB);
            assertEquals(IncrementalAuthorityDecision.Code.ACCEPTED, closed.code());
            assertEquals(IncrementalFrontierAuthorityState.READY,
                    closed.record().state());
            assertEquals(IncrementalFrontier.begin(),
                    closed.record().committedFrontier());
            assertEquals(RunStatus.FAILED, closed.runSnapshot().status());
            assertEquals(IncrementalAuthorityDecision.Code.ALREADY_APPLIED,
                    replacement.closeAborted(
                            frontierB, runB.token(), commandB, proofB).code());
        }
    }

    @Test
    void retryScheduleAndClaimStayJointlyFrontierFencedAcrossReplacement()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            RetryFixture retry = retryableEvidence(first, firstRuns, fixture);
            long frontierRevision = first.record("feed-assignment").revision();
            AtomicBoolean replaceSchedule = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RETRY_SCHEDULE
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaceSchedule.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", frontierRevision, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(WholeAttemptRetryDecision.Code.STALE_RECOVERY_OWNER,
                    first.scheduleRetry(
                            fixture.frontier, retry.runToken,
                            fixture.runFence()).code());
            assertEquals(RunStatus.RUNNING,
                    replacementRuns.get(fixture.run.runId()).status());
            assertEquals(fixture.runFence(), replacement.record("feed-assignment")
                    .activeInterval().fence());

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", frontierRevision, 1L, ownerB).token();
            RunSnapshot current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, current.runId(), current.revision(),
                    retry.runToken.ownershipEpoch(), ownerB);
            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    replacement.scheduleRetry(
                            frontierB, runB.token(), fixture.runFence()).code());
            assertEquals(RunStatus.READY,
                    replacementRuns.get(fixture.run.runId()).status());
            long dueTimeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (replacementRuns.evaluateRetryReady(current.runId()).code()
                    != io.rowweave.run.RetryReadyEvaluation.Code.DUE
                    && System.nanoTime() < dueTimeout) {
                TimeUnit.MILLISECONDS.sleep(10L);
            }

            AtomicBoolean replaceClaim = new AtomicBoolean(true);
            replacementRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.RETRY_CLAIM
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replaceClaim.compareAndSet(true, false)) {
                    first.claimOwnership(
                            "feed-assignment", frontierRevision, 2L,
                            new RecoveryOwner("master-c", 3L));
                }
            });

            assertEquals(RetryAttemptClaim.Code.STALE_RECOVERY_OWNER,
                    replacement.claimDueRetry(
                            frontierB, runB.token(), current.runId()).code());
            assertEquals(RunStatus.READY, firstRuns.get(current.runId()).status());
            assertEquals(1, first.record("feed-assignment")
                    .activeInterval().attemptOrdinal());
            assertEquals(fixture.runFence(), first.record("feed-assignment")
                    .activeInterval().fence());

            RecoveryOwner ownerC = new RecoveryOwner("master-c", 3L);
            FrontierRecoveryToken frontierC = first.claimOwnership(
                    "feed-assignment", frontierRevision, 2L, ownerC).token();
            current = firstRuns.get(current.runId());
            RecoveryClaim runC = first.claimRunOwnership(
                    frontierC, current.runId(), current.revision(),
                    runB.token().ownershipEpoch(), ownerC);
            RetryAttemptClaim claimed = first.claimDueRetry(
                    frontierC, runC.token(), current.runId());
            assertEquals(RetryAttemptClaim.Code.CLAIMED, claimed.code());
            assertEquals(claimed.snapshot().currentAttempt().attemptId(),
                    first.record("feed-assignment")
                            .activeInterval().fence().attemptId());
            assertEquals(claimed.snapshot().currentAttempt().fenceToken(),
                    first.record("feed-assignment")
                            .activeInterval().fence().fenceToken());
            assertEquals(2, first.record("feed-assignment")
                    .activeInterval().attemptOrdinal());
            assertNull(first.batchSinkAbort(fixture.runFence()));
        }
    }

    @Test
    void readyRetryCancellationClosesRunAndFrontierInOneFencedTransaction()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            RetryFixture retry = retryableEvidence(first, firstRuns, fixture);
            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    first.scheduleRetry(
                            fixture.frontier, retry.runToken,
                            fixture.runFence()).code());
            long frontierRevision = first.record("feed-assignment").revision();
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", frontierRevision, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(RunCancellationResult.Disposition.TOO_LATE,
                    first.requestUserCancellation(
                            fixture.frontier, retry.runToken,
                            fixture.run.runId(), "cancel-retry").disposition());
            assertEquals(RunStatus.READY,
                    replacementRuns.get(fixture.run.runId()).status());
            assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                    replacement.record("feed-assignment").state());

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", frontierRevision, 1L, ownerB).token();
            RunSnapshot current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, current.runId(), current.revision(),
                    retry.runToken.ownershipEpoch(), ownerB);
            RunCancellationResult canceled = replacement.requestUserCancellation(
                    frontierB, runB.token(), current.runId(), "cancel-retry");
            assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                    canceled.disposition());
            assertEquals(RunStatus.CANCELED, canceled.status());
            assertEquals(IncrementalFrontierAuthorityState.READY,
                    replacement.record("feed-assignment").state());
            assertNull(replacement.record("feed-assignment").activeInterval());
        }
    }

    @Test
    void dueDeadlineRequestIsDualOwnerFencedWithoutPrematureFrontierClose()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns, 11L);
            RetryFixture running = retryableEvidence(
                    first, firstRuns, fixture, false);
            long frontierRevision = first.record("feed-assignment").revision();
            AtomicBoolean replace = new AtomicBoolean(true);
            firstRuns.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.APPLY_EXISTING
                        && moment == ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT
                        && replace.compareAndSet(true, false)) {
                    replacement.claimOwnership(
                            "feed-assignment", frontierRevision, 1L,
                            new RecoveryOwner("master-b", 2L));
                }
            });

            assertEquals(RunDeadlineResult.Disposition.TOO_LATE,
                    first.evaluateDeadline(
                            fixture.frontier, running.runToken,
                            fixture.run.runId()).disposition());
            assertEquals(RunStatus.RUNNING,
                    replacementRuns.get(fixture.run.runId()).status());
            assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                    replacement.record("feed-assignment").state());

            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", frontierRevision, 1L, ownerB).token();
            RunSnapshot current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, current.runId(), current.revision(),
                    running.runToken.ownershipEpoch(), ownerB);
            RunDeadlineResult timedOut = replacement.evaluateDeadline(
                    frontierB, runB.token(), current.runId());
            assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                    timedOut.disposition());
            assertEquals(RunStatus.CANCELING, timedOut.status());
            assertEquals(IncrementalFrontierAuthorityState.ACTIVE,
                    replacement.record("feed-assignment").state());
            assertEquals(fixture.runFence(), replacement.record("feed-assignment")
                    .activeInterval().fence());
        }
    }

    @Test
    void retryReadinessRequiresOneStableDualOwnerView() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = connectedClient(server);
             CuratorFramework replacementClient = connectedClient(server)) {
            ZkRunStateStore firstRuns = new ZkRunStateStore(firstClient);
            ZkRunStateStore replacementRuns = new ZkRunStateStore(replacementClient);
            ZkIncrementalFrontierStore first =
                    new ZkIncrementalFrontierStore(firstClient, firstRuns);
            ZkIncrementalFrontierStore replacement =
                    new ZkIncrementalFrontierStore(replacementClient, replacementRuns);
            Fixture fixture = claimed(first, firstRuns);
            RetryFixture retry = retryableEvidence(first, firstRuns, fixture);
            first.scheduleRetry(
                    fixture.frontier, retry.runToken, fixture.runFence());

            long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            RetryReadyEvaluation ready;
            do {
                ready = first.evaluateRetryReady(
                        fixture.frontier, retry.runToken,
                        fixture.run.runId());
                if (ready.code() == RetryReadyEvaluation.Code.DUE) break;
                TimeUnit.MILLISECONDS.sleep(10L);
            } while (System.nanoTime() < timeout);
            assertEquals(RetryReadyEvaluation.Code.DUE, ready.code());

            long revision = first.record("feed-assignment").revision();
            RecoveryOwner ownerB = new RecoveryOwner("master-b", 2L);
            FrontierRecoveryToken frontierB = replacement.claimOwnership(
                    "feed-assignment", revision, 1L, ownerB).token();
            assertEquals(RetryReadyEvaluation.Code.STALE_RECOVERY_OWNER,
                    first.evaluateRetryReady(
                            fixture.frontier, retry.runToken,
                            fixture.run.runId()).code());
            RunSnapshot current = replacementRuns.get(fixture.run.runId());
            RecoveryClaim runB = replacement.claimRunOwnership(
                    frontierB, current.runId(), current.revision(),
                    retry.runToken.ownershipEpoch(), ownerB);
            assertEquals(RetryReadyEvaluation.Code.DUE,
                    replacement.evaluateRetryReady(
                            frontierB, runB.token(), current.runId()).code());
        }
    }

    @Test
    void exhaustedRetryClosesRunAndFrontierTogether() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore runs = new ZkRunStateStore(client);
            ZkIncrementalFrontierStore store =
                    new ZkIncrementalFrontierStore(client, runs);
            Fixture fixture = claimed(store, runs, 0L, 1);
            RetryFixture retry = retryableEvidence(store, runs, fixture);

            WholeAttemptRetryDecision exhausted = store.scheduleRetry(
                    fixture.frontier, retry.runToken, fixture.runFence());
            assertEquals(WholeAttemptRetryDecision.Code.EXHAUSTED,
                    exhausted.code());
            assertEquals(RunStatus.FAILED, exhausted.snapshot().status());
            assertEquals(IncrementalFrontierAuthorityState.READY,
                    store.record("feed-assignment").state());
            assertNull(store.record("feed-assignment").activeInterval());
            assertEquals(IncrementalFrontier.begin(),
                    store.record("feed-assignment").committedFrontier());
        }
    }

    @Test
    void dueDeadlineClosesReadyRetryRunAndFrontierTogether()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore runs = new ZkRunStateStore(client);
            ZkIncrementalFrontierStore store =
                    new ZkIncrementalFrontierStore(client, runs);
            long deadline = System.currentTimeMillis() + 5_000L;
            Fixture fixture = claimed(store, runs, deadline);
            RetryFixture retry = retryableEvidence(store, runs, fixture);
            assertEquals(WholeAttemptRetryDecision.Code.SCHEDULED,
                    store.scheduleRetry(
                            fixture.frontier, retry.runToken,
                            fixture.runFence()).code());
            long timeout = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
            while (runs.incrementalAuthorityTime() < deadline
                    && System.nanoTime() < timeout) {
                TimeUnit.MILLISECONDS.sleep(10L);
            }

            RunDeadlineResult timedOut = store.evaluateDeadline(
                    fixture.frontier, retry.runToken, fixture.run.runId());
            assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                    timedOut.disposition());
            assertEquals(RunStatus.FAILED, timedOut.status());
            assertEquals(IncrementalFrontierAuthorityState.READY,
                    store.record("feed-assignment").state());
            assertNull(store.record("feed-assignment").activeInterval());
            assertEquals(RunDeadlineResult.Disposition.ALREADY_TERMINAL,
                    store.evaluateDeadline(
                            fixture.frontier, retry.runToken,
                            fixture.run.runId()).disposition());
        }
    }

    private static Fixture claimed(
            ZkIncrementalFrontierStore store, ZkRunStateStore runs)
            throws Exception {
        return claimed(store, runs, 0L);
    }

    private static Fixture claimed(
            ZkIncrementalFrontierStore store, ZkRunStateStore runs,
            long deadlineEpochMillis) throws Exception {
        return claimed(store, runs, deadlineEpochMillis, 3);
    }

    private static Fixture claimed(
            ZkIncrementalFrontierStore store, ZkRunStateStore runs,
            long deadlineEpochMillis, int maxAttempts) throws Exception {
        IncrementalInitializeRequest initialize = new IncrementalInitializeRequest(
                "feed-assignment", 1L, digest("definition"), digest("source"),
                digest("frontier"), IncrementalFrontier.Kind.INT64_SEQUENCE_V1);
        store.initialize("initialize-assignment", initialize);
        RecoveryOwner owner = new RecoveryOwner("master-a", 1L);
        FrontierRecoveryToken frontier = store.claimOwnership(
                "feed-assignment", 0L, 0L, owner).token();
        CapturedTargetReceipt capture = CapturedTargetReceipt.normal(
                1L, digest("definition"), digest("source"), 0L,
                IncrementalFrontier.begin(), "trigger-assignment",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(2L), bytes("snapshot"));
        IncrementalClaimRequest request = new IncrementalClaimRequest(
                "feed-assignment", capture, digest("frontier"),
                hex(digest("artifact")), "attempt-assignment",
                "publication-assignment", maxAttempts,
                deadlineEpochMillis, 10L);
        store.publishPending(frontier, request);
        store.claimInterval("claim-assignment", frontier, request);
        return new Fixture(frontier, runs.get(request.runId()), request.intervalId());
    }

    private static ManifestFixture preparedEvidence(
            ZkIncrementalFrontierStore store, ZkRunStateStore runs,
            Fixture fixture) throws Exception {
        StageAssignmentManifest manifest = manifest(fixture.run);
        StageManifestPublication publication =
                store.publishAssignments(fixture.frontier, manifest);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                publication.snapshot().token(0), filled(7), 11L);
        store.acceptAssignment(fixture.frontier, acceptance);
        store.recordStageTerminal(fixture.frontier, StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 3L, 2L, 12L));
        CommitManifest.Member member = new CommitManifest.Member(
                "member-0", "data/31/member-0.data", "schema-0",
                2L, digest("member-data"));
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, hex(digest("schema")),
                "domain:member", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                manifest, manifest.assignment(0), "sink-op", descriptor);
        PreparedSinkLocator locator = new PreparedSinkLocator(
                PreparedSinkLocator.CURRENT_VERSION, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", ManifestCommitWire.encodeMember(member),
                member.length(), member.sha256(), 13L);
        PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                acceptance.token(), acceptance.commandDigest(), locator);
        assertEquals(PreparedSinkDecision.Code.RECORDED,
                store.recordPreparedSink(fixture.frontier, prepared).code());
        ManifestMemberReceipt receipt = ManifestMemberReceipt.sealed(
                acceptance.token(), acceptance.commandDigest(), "domain",
                31L, "publication-assignment", member, 13L, locator);
        return new ManifestFixture(manifest, receipt);
    }

    private static RetryFixture retryableEvidence(
            ZkIncrementalFrontierStore store, ZkRunStateStore runs,
            Fixture fixture) throws Exception {
        return retryableEvidence(store, runs, fixture, true);
    }

    private static RetryFixture retryableEvidence(
            ZkIncrementalFrontierStore store, ZkRunStateStore runs,
            Fixture fixture, boolean closeAttempt) throws Exception {
        StageAssignmentManifest manifest = manifest(fixture.run);
        StageManifestPublication publication =
                store.publishAssignments(fixture.frontier, manifest);
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                publication.snapshot().token(0), filled(7), 11L);
        store.acceptAssignment(fixture.frontier, acceptance);
        advance(store, runs, fixture.frontier, null, fixture.intervalId,
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING);
        RunSnapshot current = runs.get(fixture.run.runId());
        RecoveryClaim run = store.claimRunOwnership(
                fixture.frontier, current.runId(), current.revision(), 0L,
                fixture.frontier.owner());
        if (!closeAttempt) {
            return new RetryFixture(run.token());
        }
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordStageTerminal(
                        fixture.frontier, run.token(), StageTerminalReceipt.lost(
                                acceptance.token(), acceptance.commandDigest(),
                                "HOST_TERMINATED", digest("lost"), 0L)).code());
        BatchSinkAbortRecord abort = BatchSinkAbortRecord.decided(
                fixture.runFence(), manifest.generation(),
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", 31L, "publication-assignment",
                digest("retry-abort"), "RETRY_ROLLBACK", 13L);
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                store.recordBatchSinkAbort(
                        fixture.frontier, run.token(), abort).code());
        assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                store.recordBatchSinkAbort(
                        fixture.frontier, run.token(), abort.completed(14L)).code());
        return new RetryFixture(run.token());
    }

    private static void advanceToCommitting(
            ZkIncrementalFrontierStore store, ZkRunStateStore runs,
            FrontierRecoveryToken frontier,
            io.rowweave.run.RecoveryToken runToken, String intervalId) {
        advance(store, runs, frontier, runToken, intervalId,
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING);
    }

    private static void advance(
            ZkIncrementalFrontierStore store, ZkRunStateStore runs,
            FrontierRecoveryToken frontier,
            io.rowweave.run.RecoveryToken runToken, String intervalId,
            AttemptStatus... targets) {
        for (AttemptStatus target : targets) {
            RunSnapshot current = runs.get(store.record(frontier.feedId())
                    .activeInterval().fence().runId());
            RunAttemptFence fence = new RunAttemptFence(
                    current.runId(), current.currentAttempt().attemptId(),
                    current.currentAttempt().fenceToken());
            IncrementalAttemptProgressCommand command =
                    new IncrementalAttemptProgressCommand(
                            frontier.feedId(), intervalId,
                            store.record(frontier.feedId()).revision(),
                            current.revision(), fence, target,
                            "advance-" + target, 20L + target.ordinal());
            assertEquals(io.rowweave.run.RunDecision.Code.ACCEPTED,
                    (runToken == null
                            ? store.advanceProgress(frontier, command)
                            : store.advanceProgress(frontier, runToken, command))
                            .code());
        }
    }

    private static StageAssignmentManifest manifest(RunSnapshot run) {
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 1L, 0, 0, StageAssignmentRole.SINK,
                "worker-1", "127.0.0.1", 7001, -1, "inc-1");
        return new StageAssignmentManifest(
                fence, run.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1,
                Collections.singletonList(record));
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] digest(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static IncrementalAuthorityCommand jointCommand(
            io.rowweave.incremental.IncrementalAuthorityRecord record,
            VerifiedIncrementalJointCommitProof proof, String commandId) {
        return new IncrementalAuthorityCommand(
                IncrementalAuthorityCommandKind.COMMIT, record.feedId(), commandId,
                record.revision(), record.feedIncarnation(), proof.intervalId(),
                proof.fence(), proof.proofDigest());
    }

    private static VerifiedIncrementalJointCommitProof proof(
            io.rowweave.incremental.IncrementalActiveInterval active,
            ManifestCommitIntent intent, FrontierRecoveryToken frontier,
            io.rowweave.run.RecoveryToken runToken) throws Exception {
        VerifiedIncrementalSinkCommitDecision decision = sinkDecision(intent);
        Constructor<VerifiedIncrementalPublicationProof> publicationConstructor =
                VerifiedIncrementalPublicationProof.class.getDeclaredConstructor(
                        VerifiedIncrementalSinkCommitDecision.class,
                        FrontierRecoveryToken.class,
                        io.rowweave.run.RecoveryToken.class);
        publicationConstructor.setAccessible(true);
        VerifiedIncrementalPublicationProof publication =
                publicationConstructor.newInstance(decision, frontier, runToken);
        Constructor<VerifiedIncrementalJointCommitProof> proofConstructor =
                VerifiedIncrementalJointCommitProof.class.getDeclaredConstructor(
                        io.rowweave.incremental.IncrementalActiveInterval.class,
                        VerifiedIncrementalPublicationProof.class);
        proofConstructor.setAccessible(true);
        return proofConstructor.newInstance(active, publication);
    }

    private static VerifiedIncrementalConflictProof conflictProof(
            io.rowweave.incremental.IncrementalActiveInterval active,
            ManifestCommitIntent intent, FrontierRecoveryToken frontier,
            io.rowweave.run.RecoveryToken runToken) throws Exception {
        Constructor<VerifiedIncrementalConflictProof> constructor =
                VerifiedIncrementalConflictProof.class.getDeclaredConstructor(
                        io.rowweave.incremental.IncrementalActiveInterval.class,
                        VerifiedIncrementalSinkCommitDecision.class,
                        FrontierRecoveryToken.class,
                        io.rowweave.run.RecoveryToken.class);
        constructor.setAccessible(true);
        return constructor.newInstance(
                active, sinkDecision(intent), frontier, runToken);
    }

    private static VerifiedIncrementalSinkCommitDecision sinkDecision(
            ManifestCommitIntent intent) throws Exception {
        byte[] decisionDigest = digest(
                ManifestCommitJournalCodec.encodeIntent(intent));
        Constructor<VerifiedIncrementalSinkCommitDecision> constructor =
                VerifiedIncrementalSinkCommitDecision.class.getDeclaredConstructor(
                        String.class, RunAttemptFence.class, String.class,
                        long.class, byte[].class, byte[].class);
        constructor.setAccessible(true);
        return constructor.newInstance(
                intent.domainId(), intent.fence(), intent.publicationId(),
                intent.generation(), intent.manifestHash(), decisionDigest);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] filled(int value) {
        byte[] digest = new byte[32];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] value = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int next = bytes[i] & 0xff;
            value[i * 2] = digits[next >>> 4];
            value[i * 2 + 1] = digits[next & 0x0f];
        }
        return new String(value);
    }

    private static CuratorFramework connectedClient(TestingServer server) {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(20, 10));
        client.start();
        try {
            if (!client.blockUntilConnected(30, TimeUnit.SECONDS)) {
                client.close();
                throw new IllegalStateException("Curator did not connect");
            }
        } catch (InterruptedException interrupted) {
            client.close();
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interrupted);
        }
        return client;
    }

    private static final class Fixture {
        private final FrontierRecoveryToken frontier;
        private final RunSnapshot run;
        private final String intervalId;

        private Fixture(
                FrontierRecoveryToken frontier, RunSnapshot run,
                String intervalId) {
            this.frontier = frontier;
            this.run = run;
            this.intervalId = intervalId;
        }

        private RunAttemptFence runFence() {
            return new RunAttemptFence(
                    run.runId(), run.currentAttempt().attemptId(),
                    run.currentAttempt().fenceToken());
        }
    }

    private static final class RetryFixture {
        private final io.rowweave.run.RecoveryToken runToken;

        private RetryFixture(io.rowweave.run.RecoveryToken runToken) {
            this.runToken = runToken;
        }
    }

    private static final class ManifestFixture {
        private final StageAssignmentManifest manifest;
        private final ManifestMemberReceipt member;

        private ManifestFixture(
                StageAssignmentManifest manifest,
                ManifestMemberReceipt member) {
            this.manifest = manifest;
            this.member = member;
        }
    }
}
