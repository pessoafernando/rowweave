package io.rowweave.cluster.registry.zk;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;
import io.rowweave.QualityGateDecision;
import io.rowweave.QualityGateDecisionCodec;
import io.rowweave.QualityGateDecisionResult;
import io.rowweave.QualityGatePolicy;
import io.rowweave.QualityGatePolicyCodec;
import io.rowweave.QualityGateTestFixtures;
import io.rowweave.QualityExemplar;
import io.rowweave.QualityReasonKey;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.cluster.rpc.RpcServer;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.run.AttemptOutputProfile;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestCommitIntent;
import io.rowweave.run.ManifestCommitJournalSnapshot;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.QualityGateAssignmentBinding;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RetentionToken;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RetryAttemptIdentity;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import io.rowweave.run.WholeAttemptRetryClass;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ZK owns the same bounded quality-decision slot as intent. */
class ZkQualityGateAuthorityTest {

    private static final List<String> ROLES = QualityGateTestFixtures.roles();

    @Test
    void postCommitRootReplacementReturnsStaleAndDoesNotTouchReplacement()
            throws Exception {
        assertRootReplacementIsStale(
                ZkRunStateStore.GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK);
    }

    @Test
    void preCommitRootReplacementReturnsStaleAndDoesNotTouchReplacement()
            throws Exception {
        assertRootReplacementIsStale(
                ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT);
    }

    @Test
    void recoveredPostCommitRootReplacementReturnsStale()
            throws Exception {
        assertRootReplacementIsStale(
                ZkRunStateStore.GuardedMutationMoment.AFTER_COMMIT_BEFORE_ACK,
                true);
    }

    @Test
    void recoveredPreCommitRootReplacementReturnsStale()
            throws Exception {
        assertRootReplacementIsStale(
                ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT, true);
    }

    private static void assertRootReplacementIsStale(
            ZkRunStateStore.GuardedMutationMoment replacementMoment)
            throws Exception {
        assertRootReplacementIsStale(replacementMoment, false);
    }

    private static void assertRootReplacementIsStale(
            ZkRunStateStore.GuardedMutationMoment replacementMoment,
            boolean recovered) throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore reader = new ZkRunStateStore(firstClient);
            ZkRunStateStore writer = new ZkRunStateStore(secondClient);
            String runId = "run-zk-quality-post-commit-replacement";
            Fixture original = fixture(writer, runId);
            original.sealAll(writer);
            RecoveryToken recoveryToken = null;
            if (recovered) {
                recoveryToken = writer.claim(
                        original.run.runId(), original.run.revision(), 0L,
                        new RecoveryOwner("master-root-replacement", 94L)).token();
            }
            original.advanceToCommitting(writer, recoveryToken);
            QualityGateDecision decision = original.decision(9L, 1L);
            AtomicBoolean replaced = new AtomicBoolean();
            Fixture[] replacement = new Fixture[1];
            RecoveryToken finalRecoveryToken = recoveryToken;
            reader.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.QUALITY_DECISION
                        && moment == replacementMoment
                        && replaced.compareAndSet(false, true)) {
                    retireAndPurge(writer, original, finalRecoveryToken);
                    replacement[0] = fixture(writer, runId);
                }
            });

            QualityGateDecisionResult result = recovered
                    ? reader.recordQualityGateDecisionRecovered(
                    recoveryToken, decision)
                    : reader.recordQualityGateDecision(decision);

            assertTrue(replaced.get());
            assertEquals(QualityGateDecisionResult.Code.STALE_RUN,
                    result.code());
            assertNotNull(replacement[0]);
            assertNull(writer.qualityGateDecision(replacement[0].fence));
            assertEquals(replacement[0].fence,
                    writer.get(runId).currentAttempt() == null ? null
                            : new RunAttemptFence(runId,
                            writer.get(runId).currentAttempt().attemptId(),
                            writer.get(runId).currentAttempt().fenceToken()));
        }
    }

    @Test
    void twoSessionsShareFirstDecisionAndQualityIntentRetainsItsBytes()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            Fixture fixture = fixture(first, "run-zk-quality-decision");
            fixture.sealAll(first);
            fixture.advanceToCommitting(first);
            QualityGateDecision decision = fixture.decision(9L, 1L);

            assertEquals(QualityGateDecisionResult.Code.RECORDED,
                    first.recordQualityGateDecision(decision).code());
            assertEquals(QualityGateDecisionResult.Code.ALREADY_RECORDED,
                    second.recordQualityGateDecision(decision).code());
            assertEquals(QualityGateDecisionResult.Code.DECISION_CONFLICT,
                    second.recordQualityGateDecision(fixture.decision(8L, 2L)).code());
            assertEquals(decision, second.qualityGateDecision(fixture.fence));

            ManifestCommitJournalSnapshot decided = second
                    .manifestCommitJournal(fixture.fence);
            assertEquals(decision, decided.qualityDecision());
            assertNull(decided.intent());
            ManifestCommitIntent intent = decided.buildIntent();
            assertTrue(intent.hasQualityDecision());
            assertArrayEquals(QualityGateDecisionCodec.encode(decision),
                    intent.qualityDecisionBytes());
            assertEquals(ManifestIntentDecision.Code.COMMITTED,
                    first.commitManifestIntent(intent).code());
            assertEquals(decision, second
                    .manifestCommitJournal(fixture.fence).qualityDecision());
        }
    }

    @Test
    void committedDecisionAckLossReconcilesFromTheSharedSlot() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store, "run-zk-quality-ack-loss");
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            QualityGateDecision decision = fixture.decision(9L, 1L);
            AtomicBoolean loseAck = new AtomicBoolean(true);
            store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.QUALITY_DECISION
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && loseAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertEquals(QualityGateDecisionResult.Code.ALREADY_RECORDED,
                    store.recordQualityGateDecision(decision).code());
            assertEquals(decision, store.qualityGateDecision(fixture.fence));
        }
    }

    @Test
    void committedRecoveredDecisionAckLossReconcilesFromTheSharedSlot()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(
                    store, "run-zk-quality-recovered-ack-loss");
            fixture.sealAll(store);
            RecoveryClaim owner = store.claim(
                    fixture.run.runId(), fixture.run.revision(), 0L,
                    new RecoveryOwner("master-ack-loss", 92L));
            fixture.advanceToCommitting(store, owner.token());
            QualityGateDecision decision = fixture.decision(9L, 1L);
            AtomicBoolean loseAck = new AtomicBoolean(true);
            store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.QUALITY_DECISION
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && loseAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertEquals(QualityGateDecisionResult.Code.ALREADY_RECORDED,
                    store.recordQualityGateDecisionRecovered(
                            owner.token(), decision).code());
            assertEquals(decision, store.qualityGateDecision(fixture.fence));
        }
    }

    @Test
    void unknownDecisionOutcomeFailsClosedWhenCommittedEvidenceIsCorrupt()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store,
                    "run-zk-quality-operation-timeout-corrupt-evidence");
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            QualityGateDecision decision = fixture.decision(9L, 1L);
            AtomicBoolean corruptOnce = new AtomicBoolean(true);
            store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                if (kind == ZkRunStateStore.GuardedMutationKind.QUALITY_DECISION
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && corruptOnce.compareAndSet(true, false)) {
                    client.setData().forPath(runPath(fixture.run.runId())
                            + "/stage-assignment/manifest-member-shards/000",
                            new byte[] {1, 2, 3});
                    throw new KeeperException.OperationTimeoutException();
                }
            });

            assertThrows(IllegalArgumentException.class,
                    () -> store.recordQualityGateDecision(decision));
            assertTrue(corruptOnce.get() == false);
        }
    }

    @Test
    void qualityIntentRepairCannotEraseARecordedDecision() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store, "run-zk-quality-slot-repair");
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            assertEquals(QualityGateDecisionResult.Code.RECORDED,
                    store.recordQualityGateDecision(
                            fixture.decision(9L, 1L)).code());
            String slot = runPath(fixture.run.runId())
                    + "/stage-assignment/manifest-intent";
            client.delete().forPath(slot);

            assertThrows(IllegalStateException.class,
                    () -> store.publish(fixture.manifest));
            assertNull(client.checkExists().forPath(slot),
                    "quality slot loss must not be repaired to an empty legacy guard");
        }
    }

    @Test
    void failedDecisionOnlyAcceptsItsExactQualityAbort() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            QualityGatePolicy rejecting = QualityGatePolicy.builder()
                    .maxRejectedRows(0L).build();
            Fixture fixture = fixture(
                    store, "run-zk-quality-fail-abort", rejecting);
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            QualityGateDecision decision = fixture.decision(9L, 1L);
            assertEquals(QualityGateDecision.Outcome.FAIL, decision.outcome());
            assertEquals(QualityGateDecisionResult.Code.RECORDED,
                    store.recordQualityGateDecision(decision).code());

            assertEquals(BatchSinkAbortDecision.Code.RECORD_CONFLICT,
                    store.recordBatchSinkAbort(fixture.abort(
                            digest(88), "QUALITY_GATE_REJECTED")).code());
            assertEquals(BatchSinkAbortDecision.Code.RECORD_CONFLICT,
                    store.recordBatchSinkAbort(fixture.abort(
                            QualityGateDecisionCodec.digest(decision),
                            "PREPUBLICATION_FAILURE")).code());
            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    store.recordBatchSinkAbort(fixture.abort(
                            QualityGateDecisionCodec.digest(decision),
                            "QUALITY_GATE_REJECTED")).code());
        }
    }

    @Test
    void qualityAbortTransactionIsConstantAtMaximumAssignments()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            QualityGatePolicy rejecting = QualityGatePolicy.builder()
                    .maxRejectedRows(0L).build();
            for (boolean recovered : new boolean[] {false, true}) {
                Fixture fixture = fixture(store,
                        "run-zk-quality-max-abort-" + recovered, rejecting,
                        StageAssignmentManifest.MAX_ASSIGNMENTS,
                        new int[] {0, 2048, 4095});
                fixture.sealAll(store);
                RecoveryToken token = null;
                if (recovered) {
                    token = store.claim(
                            fixture.run.runId(), fixture.run.revision(), 0L,
                            new RecoveryOwner("master-max-abort", 95L)).token();
                }
                fixture.advanceToCommitting(store, token);
                QualityGateDecision decision = fixture.decision(9L, 1L);
                assertEquals(QualityGateDecision.Outcome.FAIL, decision.outcome());
                assertEquals(QualityGateDecisionResult.Code.RECORDED,
                        recovered
                                ? store.recordQualityGateDecisionRecovered(
                                token, decision).code()
                                : store.recordQualityGateDecision(decision).code());
                AtomicInteger qualityReads = new AtomicInteger();
                AtomicInteger totalMemberShardReads = new AtomicInteger();
                AtomicInteger operations = new AtomicInteger();
                AtomicInteger requestBytes = new AtomicInteger();
                store.qualityJournalReadObserverForTest(shards -> {
                    qualityReads.incrementAndGet();
                    totalMemberShardReads.addAndGet(shards);
                });
                store.transactionWriteObserverForTest(
                        (kind, ordinal, count, bytes) -> {
                            if (kind == ZkRunStateStore.GuardedMutationKind.SINK_ABORT) {
                                operations.set(count);
                                requestBytes.set(bytes);
                            }
                        });
                BatchSinkAbortRecord abort = fixture.abort(
                        QualityGateDecisionCodec.digest(decision),
                        "QUALITY_GATE_REJECTED");

                BatchSinkAbortDecision result = recovered
                        ? store.recordBatchSinkAbortRecovered(token, abort)
                        : store.recordBatchSinkAbort(abort);

                assertEquals(BatchSinkAbortDecision.Code.RECORDED, result.code());
                assertEquals(1, qualityReads.get());
                assertEquals(3, totalMemberShardReads.get());
                assertEquals(10 + (recovered ? 1 : 0), operations.get());
                assertEquals(recovered ? 1639 : 1516, requestBytes.get());
            }
        }
    }

    @Test
    void qualityAbortRejectsValidDecisionBoundToDifferentRootPolicy()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store,
                    "run-zk-quality-abort-policy-corruption");
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            QualityGatePolicy foreignPolicy = QualityGatePolicy.builder()
                    .maxRejectedRows(0L).build();
            QualityGateDecision foreignDecision = QualityGateTestFixtures.decision(
                    fixture.run.artifactId(), fixture.fence,
                    fixture.run.currentAttempt().publicationId(), foreignPolicy,
                    9L, 1L, fixture.receipts);
            assertEquals(QualityGateDecision.Outcome.FAIL,
                    foreignDecision.outcome());
            client.setData().forPath(runPath(fixture.run.runId())
                            + "/stage-assignment/manifest-intent",
                    io.rowweave.run.ManifestCommitJournalCodec
                            .encodeQualityDecision(foreignDecision));
            BatchSinkAbortRecord abort = fixture.abort(
                    QualityGateDecisionCodec.digest(foreignDecision),
                    "QUALITY_GATE_REJECTED");

            assertThrows(IllegalArgumentException.class,
                    () -> store.recordBatchSinkAbort(abort));
            assertNull(store.batchSinkAbort(fixture.fence));
        }
    }

    @Test
    void qualityQueryRejectsValidDecisionBoundToForeignPublication()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            QualityGatePolicy rejecting = QualityGatePolicy.builder()
                    .maxRejectedRows(0L).build();
            Fixture fixture = fixture(store,
                    "run-zk-quality-query-publication-corruption", rejecting);
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            QualityGateDecision foreignDecision = QualityGateTestFixtures.decision(
                    fixture.run.artifactId(), fixture.fence,
                    "foreign-publication", rejecting,
                    9L, 1L, fixture.receipts);
            client.setData().forPath(runPath(fixture.run.runId())
                            + "/stage-assignment/manifest-intent",
                    io.rowweave.run.ManifestCommitJournalCodec
                            .encodeQualityDecision(foreignDecision));

            assertThrows(IllegalStateException.class,
                    () -> store.qualityGateDecision(fixture.fence));
        }
    }

    @Test
    void passDecisionFreezesMembersButAllowsOrdinaryPrepublicationAbort()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store, "run-zk-quality-pass-abort");
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            QualityGateDecision decision = fixture.decision(9L, 1L);
            assertEquals(QualityGateDecision.Outcome.PASS, decision.outcome());
            assertEquals(QualityGateDecisionResult.Code.RECORDED,
                    store.recordQualityGateDecision(decision).code());

            assertEquals(ManifestMemberDecision.Code.MEMBER_CONFLICT,
                    store.recordManifestMember(
                            fixture.changedReceipt(0, "changed-valid")).code());
            assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                    store.recordBatchSinkAbort(fixture.abort(
                            digest(77), "PREPUBLICATION_FAILURE")).code());
            assertEquals(decision, store.qualityGateDecision(fixture.fence));
        }
    }

    @Test
    void recoveryOwnerFencesDecisionButExactRecoveredWriteSucceeds()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store, "run-zk-quality-recovery-owner");
            fixture.sealAll(store);
            RecoveryClaim owner = store.claim(
                    fixture.run.runId(), fixture.run.revision(), 0L,
                    new RecoveryOwner("master-quality", 90L));
            fixture.advanceToCommitting(store, owner.token());
            QualityGateDecision decision = fixture.decision(9L, 1L);

            assertEquals(QualityGateDecisionResult.Code.RECOVERY_OWNED,
                    store.recordQualityGateDecision(decision).code());
            assertEquals(QualityGateDecisionResult.Code.RECORDED,
                    store.recordQualityGateDecisionRecovered(
                            owner.token(), decision).code());
            assertEquals(QualityGateDecisionResult.Code.ALREADY_RECORDED,
                    store.recordQualityGateDecision(decision).code());
        }
    }

    @Test
    void qualityDecisionTransactionIsConstantAtMaximumAssignments()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            for (boolean recovered : new boolean[] {false, true}) {
                Fixture fixture = fixture(store,
                        "run-zk-quality-max-p-" + recovered,
                        QualityGatePolicy.allowAll(),
                        StageAssignmentManifest.MAX_ASSIGNMENTS,
                        new int[] {0, 2048, 4095});
                fixture.sealAll(store);
                RecoveryToken token = null;
                if (recovered) {
                    RecoveryClaim owner = store.claim(
                            fixture.run.runId(), fixture.run.revision(), 0L,
                            new RecoveryOwner("master-formula", 91L));
                    token = owner.token();
                }
                fixture.advanceToCommitting(store, token);
                AtomicReference<ZkRunStateStore.GuardedMutationKind> kind =
                        new AtomicReference<>();
                AtomicInteger operations = new AtomicInteger();
                AtomicInteger requestBytes = new AtomicInteger();
                AtomicInteger memberShardReads = new AtomicInteger();
                store.transactionWriteObserverForTest(
                        (observedKind, ordinal, count, bytes) -> {
                            kind.set(observedKind);
                            operations.set(count);
                            requestBytes.set(bytes);
                        });
                store.qualityJournalReadObserverForTest(
                        memberShardReads::set);

                QualityGateDecisionResult decision = recovered
                        ? store.recordQualityGateDecisionRecovered(
                        token, fixture.decision(9L, 1L))
                        : store.recordQualityGateDecision(
                        fixture.decision(9L, 1L));

                assertEquals(QualityGateDecisionResult.Code.RECORDED,
                        decision.code());
                assertEquals(ZkRunStateStore.GuardedMutationKind
                                .QUALITY_DECISION,
                        kind.get());
                assertEquals(8 + 3
                                + (recovered ? 1 : 0),
                        operations.get());
                assertEquals(3, memberShardReads.get());
                assertTrue(requestBytes.get() > 0);
                assertTrue(requestBytes.get() < 1024 * 1024);
            }
        }
    }

    @Test
    void qualityIntentTransactionIsConstantAtMaximumAssignments()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            for (boolean recovered : new boolean[] {false, true}) {
                Fixture fixture = fixture(store,
                        "run-zk-quality-intent-max-p-" + recovered,
                        maximumShapePolicy(),
                        StageAssignmentManifest.MAX_ASSIGNMENTS,
                        new int[] {0, 2048, 4095});
                fixture.sealAll(store);
                RecoveryToken token = null;
                if (recovered) {
                    token = store.claim(
                            fixture.run.runId(), fixture.run.revision(), 0L,
                            new RecoveryOwner("master-intent-formula", 92L)).token();
                }
                fixture.advanceToCommitting(store, token);
                QualityGateDecision quality = maximumShapeDecision(fixture);
                QualityGateDecisionResult qualityResult = recovered
                        ? store.recordQualityGateDecisionRecovered(token, quality)
                        : store.recordQualityGateDecision(quality);
                assertEquals(QualityGateDecisionResult.Code.RECORDED,
                        qualityResult.code());
                AtomicInteger qualityReads = new AtomicInteger();
                AtomicInteger totalMemberShardReads = new AtomicInteger();
                store.qualityJournalReadObserverForTest(shards -> {
                    qualityReads.incrementAndGet();
                    totalMemberShardReads.addAndGet(shards);
                });
                ManifestCommitIntent intent = store
                        .qualityGateCommitJournal(fixture.fence)
                        .buildIntent();
                assertEquals(3, intent.memberShardCount(),
                        "quality intent must project only the selected member shards");
                assertEquals(1, qualityReads.get());
                assertEquals(3, totalMemberShardReads.get());

                AtomicReference<ZkRunStateStore.GuardedMutationKind> kind =
                        new AtomicReference<>();
                AtomicInteger operations = new AtomicInteger();
                AtomicInteger requestBytes = new AtomicInteger();
                store.transactionWriteObserverForTest(
                        (observedKind, ordinal, count, bytes) -> {
                            kind.set(observedKind);
                            operations.set(count);
                            requestBytes.set(bytes);
                        });
                AtomicBoolean loseAck = new AtomicBoolean(true);
                store.guardedMutationObserverForTest((observedKind, ordinal,
                                                       moment) -> {
                    if (observedKind == ZkRunStateStore.GuardedMutationKind
                            .MANIFEST_INTENT
                            && moment == ZkRunStateStore.GuardedMutationMoment
                            .AFTER_COMMIT_BEFORE_ACK
                            && loseAck.compareAndSet(true, false)) {
                        throw new KeeperException.OperationTimeoutException();
                    }
                });
                ManifestIntentDecision result = recovered
                        ? store.commitManifestIntentRecovered(token, intent)
                        : store.commitManifestIntent(intent);

                assertEquals(ManifestIntentDecision.Code.ALREADY_COMMITTED,
                        result.code());
                assertEquals(ZkRunStateStore.GuardedMutationKind.MANIFEST_INTENT,
                        kind.get());
                assertEquals(10 + (recovered ? 1 : 0), operations.get());
                assertEquals(3, qualityReads.get());
                assertEquals(9, totalMemberShardReads.get());
                assertEquals(recovered ? 72_615 : 72_493,
                        requestBytes.get());
                assertTrue(requestBytes.get() < 1024 * 1024);
            }
        }
    }

    @Test
    void maximumShapeDecisionHasExactBoundedNormalAndRecoveredRequestBytes()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            QualityGatePolicy policy = maximumShapePolicy();
            for (boolean recovered : new boolean[] {false, true}) {
                Fixture fixture = fixture(store,
                        "run-zk-quality-max-shape-" + recovered, policy,
                        StageAssignmentManifest.MAX_ASSIGNMENTS,
                        new int[] {0, 2048, 4095});
                fixture.sealAll(store);
                RecoveryToken token = null;
                if (recovered) {
                    token = store.claim(
                            fixture.run.runId(), fixture.run.revision(), 0L,
                            new RecoveryOwner("master-max-shape", 93L)).token();
                }
                fixture.advanceToCommitting(store, token);
                QualityGateDecision decision = maximumShapeDecision(fixture);
                AtomicInteger operations = new AtomicInteger();
                AtomicInteger requestBytes = new AtomicInteger();
                store.transactionWriteObserverForTest(
                        (kind, ordinal, count, bytes) -> {
                            if (kind == ZkRunStateStore.GuardedMutationKind
                                    .QUALITY_DECISION) {
                                operations.set(count);
                                requestBytes.set(bytes);
                            }
                        });

                QualityGateDecisionResult result = recovered
                        ? store.recordQualityGateDecisionRecovered(token, decision)
                        : store.recordQualityGateDecision(decision);

                assertEquals(QualityGateDecisionResult.Code.RECORDED,
                        result.code());
                assertEquals(11 + (recovered ? 1 : 0), operations.get());
                byte[] slot = io.rowweave.run.ManifestCommitJournalCodec
                        .encodeQualityDecision(decision);
                int fixedBytes = requestBytes.get() - slot.length;
                assertEquals(recovered ? 1553 : 1429, fixedBytes);
                assertTrue(slot.length > 64 * 1024);
                assertTrue(fixedBytes
                        + io.rowweave.run.ManifestCommitJournalCodec
                        .MAX_QUALITY_INTENT_FRAME_BYTES < 1024 * 1024);
                assertEquals(decision,
                        store.qualityGateDecision(fixture.fence));
                assertTrue(QualityGateDecisionCodec.encode(decision).length
                        <= QualityGateDecisionCodec.MAX_FRAME_BYTES);
            }
        }
    }

    @Test
    void terminalDecisionRemainsQueryableUntilFinalPurge() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store, "run-zk-quality-query-retention");
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            QualityGateDecision decision = fixture.decision(9L, 1L);
            assertEquals(QualityGateDecisionResult.Code.RECORDED,
                    store.recordQualityGateDecision(decision).code());

            RunSnapshot terminal = terminalize(store, fixture, null);
            assertEquals(decision, store.qualityGateDecision(fixture.fence));
            RunLifecycleRef lifecycle = store.lifecycle(fixture.run.runId());
            RunRetentionClaim claim = store.claim(
                    fixture.run.runId(), terminal.revision(),
                    lifecycle.terminalAtMillis(),
                    new RetentionOwner("retention-query", 1L), 0L);
            assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
            RunPurgeProgress progress;
            do {
                progress = store.purge(claim.token(), 1000);
            } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
            assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
            assertNull(store.qualityGateDecision(fixture.fence));
        }
    }

    @Test
    void currentAttemptRolloverNeverReturnsThePreviousDecision()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            Fixture fixture = fixture(store, "run-zk-quality-query-rollover",
                    QualityGatePolicy.allowAll(), 3,
                    new int[] {0, 1, 2}, 2);
            fixture.sealAll(store);
            fixture.advanceToCommitting(store, null);
            QualityGateDecision oldDecision = fixture.decision(9L, 1L);
            assertEquals(QualityGateDecisionResult.Code.RECORDED,
                    store.recordQualityGateDecision(oldDecision).code());
            RunSnapshot current = store.get(fixture.run.runId());
            current = accepted(store, RunCommand.advanceAttempt(
                    "reconciling-rollover", current.runId(), current.revision(),
                    current.currentAttempt().attemptId(),
                    current.currentAttempt().fenceToken(),
                    AttemptStatus.RECONCILING, false, 80L,
                    "COMMIT_ACK_LOST", "retry current attempt"));
            current = accepted(store, retryAuthorityAdvance(
                    current, WholeAttemptRetryClass.WORKER_LOST, 81L));
            RetryAttemptIdentity retry = RetryAttemptIdentity.derive(
                    current.runId(), current.artifactId(), 2);
            current = accepted(store, retryAuthorityCreateAttempt(
                    current, retry, 82L));
            RunAttemptFence currentFence = new RunAttemptFence(
                    current.runId(), current.currentAttempt().attemptId(),
                    current.currentAttempt().fenceToken());

            assertNull(store.qualityGateDecision(currentFence));
            assertNull(store.qualityGateDecision(fixture.fence));
        }
    }

    @Test
    void maximumDecisionRoundTripsThroughPublicZkClient() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework authorityClient = client(server);
             ZkServiceRegistry registry =
                     new ZkServiceRegistry(server.getConnectString())) {
            Fixture fixture = fixture(new ZkRunStateStore(authorityClient),
                    "run-zk-quality-max-public-query", maximumShapePolicy());
            QualityGateDecision decision = maximumShapeDecision(fixture);
            byte[] encoded = QualityGateDecisionCodec.encode(decision);
            assertTrue(encoded.length > 64 * 1024);
            try (RpcServer rpc = new RpcServer(request -> {
                     assertEquals(RpcMessage.Type.QUERY_QUALITY_GATE_DECISION,
                             request.type());
                     return RpcMessage.qualityGateDecision(
                             request.runId(), encoded);
                 });
                 MasterElection election = new MasterElection(registry,
                         new Endpoint("127.0.0.1", rpc.port()));
                 ZkRemoteJobClient queryClient = new ZkRemoteJobClient(
                         server.getConnectString())) {
                election.start();
                assertTrue(election.isLeader());
                assertEquals(decision,
                        queryClient.queryQualityGateDecision(
                                fixture.run.runId()));
            }
        }
    }

    private static QualityGatePolicy maximumShapePolicy() {
        QualityGatePolicy.Builder builder = QualityGatePolicy.builder()
                .maxRejectedRows(Long.MAX_VALUE)
                .maxRejectedPartsPerMillion(1_000_000)
                .exemplarLimit(QualityGatePolicy.MAX_EXEMPLARS);
        for (int i = 0; i < QualityGatePolicy.MAX_REASON_THRESHOLDS; i++) {
            builder.reasonThreshold(maximumReason(i), Long.MAX_VALUE);
        }
        return builder.build();
    }

    private static QualityGateDecision maximumShapeDecision(Fixture fixture) {
        Map<QualityReasonKey, Long> observations = new TreeMap<>();
        for (int i = 0; i < QualityGatePolicy.MAX_REASON_THRESHOLDS; i++) {
            observations.put(maximumReason(i), (long) i);
        }
        List<QualityExemplar> exemplars = new ArrayList<>();
        for (int i = 0; i < QualityGatePolicy.MAX_EXEMPLARS; i++) {
            exemplars.add(QualityExemplar.of(
                    repeat('s', 256), String.format("%064x", i), i,
                    "E" + repeat('9', 63)));
        }
        return QualityGateTestFixtures.decision(
                fixture.run.artifactId(), fixture.fence,
                fixture.run.currentAttempt().publicationId(), fixture.policy,
                1L, 255L, fixture.receipts, observations, exemplars);
    }

    private static QualityReasonKey maximumReason(int ordinal) {
        return QualityReasonKey.of(
                String.format("E%063d", ordinal), repeat('r', 64),
                ordinal, "STRING_UTF8_BYTES_RANGE_CLOSED");
    }

    private static Fixture fixture(ZkRunStateStore store, String runId) {
        return fixture(store, runId, QualityGatePolicy.allowAll());
    }

    private static Fixture fixture(
            ZkRunStateStore store, String runId, QualityGatePolicy policy) {
        return fixture(store, runId, policy, 3, new int[] {0, 1, 2});
    }

    private static Fixture fixture(
            ZkRunStateStore store, String runId, QualityGatePolicy policy,
            int assignmentCount, int[] qualityOrdinals) {
        return fixture(store, runId, policy, assignmentCount,
                qualityOrdinals, 1);
    }

    private static Fixture fixture(
            ZkRunStateStore store, String runId, QualityGatePolicy policy,
            int assignmentCount, int[] qualityOrdinals, int maxAttempts) {
        RunSnapshot created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact-quality",
                maxAttempts, 0L, 1L)).snapshot();
        RunSnapshot run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.revision(),
                "attempt-1", "publication-quality", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>();
        List<QualityGateAssignmentBinding.MemberBinding> bindings = new ArrayList<>();
        for (int i = 0; i < assignmentCount; i++) {
            int qualityRole = indexOf(qualityOrdinals, i);
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0,
                    qualityRole < 0
                            ? StageAssignmentRole.PRODUCER
                            : StageAssignmentRole.SINK,
                    "worker-" + i, "127.0.0.1", 7001 + i, -1,
                    "inc-" + i));
        }
        for (int i = 0; i < ROLES.size(); i++) {
            bindings.add(new QualityGateAssignmentBinding.MemberBinding(
                    qualityOrdinals[i], ROLES.get(i), BatchSinkDescriptorCodec.digest(
                    sinkDescriptor(i, policy))));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L,
                AttemptOutputProfile.MANIFEST_ATOMIC_GROUP_V1, records,
                new QualityGateAssignmentBinding(
                        QualityGatePolicyCodec.digest(policy), bindings));
        StageAssignmentSnapshot assignments = store.publish(manifest).snapshot();
        for (int i = 0; i < ROLES.size(); i++) {
            int ordinal = qualityOrdinals[i];
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    assignments.token(ordinal), digest(i + 1), 10L + i);
            store.accept(acceptance);
            store.recordTerminal(StageTerminalReceipt.succeeded(
                    acceptance.token(), acceptance.commandDigest(), i, i, 20L + i));
        }
        return new Fixture(run, fence, manifest,
                store.assignmentSnapshot(fence), policy, qualityOrdinals);
    }

    private static RunSnapshot accepted(
            ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static RunCommand retryAuthorityAdvance(
            RunSnapshot current, WholeAttemptRetryClass retryClass,
            long eventTimeMillis) throws Exception {
        java.lang.reflect.Method method = RunCommand.class.getDeclaredMethod(
                "retryAuthorityAdvance", String.class, String.class,
                long.class, String.class, long.class, long.class,
                WholeAttemptRetryClass.class, boolean.class);
        method.setAccessible(true);
        return (RunCommand) method.invoke(null, "ready-rollover",
                current.runId(), current.revision(),
                current.currentAttempt().attemptId(),
                current.currentAttempt().fenceToken(), eventTimeMillis,
                retryClass, false);
    }

    private static RunCommand retryAuthorityCreateAttempt(
            RunSnapshot current, RetryAttemptIdentity retry,
            long eventTimeMillis) throws Exception {
        java.lang.reflect.Method method = RunCommand.class.getDeclaredMethod(
                "retryAuthorityCreateAttempt", String.class, String.class,
                long.class, RetryAttemptIdentity.class, long.class);
        method.setAccessible(true);
        return (RunCommand) method.invoke(null, retry.commandId("create"),
                current.runId(), current.revision(), retry, eventTimeMillis);
    }

    private static int indexOf(int[] values, int target) {
        for (int i = 0; i < values.length; i++) {
            if (values[i] == target) return i;
        }
        return -1;
    }

    private static CuratorFramework client(TestingServer server) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(server.getConnectString())
                .sessionTimeoutMs(1_000)
                .connectionTimeoutMs(250)
                .retryPolicy(new ExponentialBackoffRetry(10, 1))
                .build();
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }

    private static void retireAndPurge(
            ZkRunStateStore store, Fixture fixture) {
        retireAndPurge(store, fixture, null);
    }

    private static void retireAndPurge(
            ZkRunStateStore store, Fixture fixture,
            RecoveryToken recoveryToken) {
        RunSnapshot terminal = terminalize(store, fixture, recoveryToken);
        RunLifecycleRef lifecycle = store.lifecycle(fixture.run.runId());
        RunRetentionClaim claim = store.claim(
                fixture.run.runId(), terminal.revision(),
                lifecycle.terminalAtMillis(),
                new RetentionOwner(
                        "retention-" + fixture.run.runId(), 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        RetentionToken token = claim.token();
        RunPurgeProgress progress;
        do {
            progress = store.purge(token, 1000);
        } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
        assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
    }

    private static RunSnapshot terminalize(
            ZkRunStateStore store, Fixture fixture,
            RecoveryToken recoveryToken) {
        RunSnapshot current = store.get(fixture.run.runId());
        RunCommand reconcileCommand = RunCommand.advanceAttempt(
                "reconcile-" + fixture.run.runId(), fixture.run.runId(),
                current.revision(), fixture.fence.attemptId(),
                fixture.fence.fenceToken(), AttemptStatus.RECONCILING,
                false, current.updatedAtMillis() + 1L,
                "FENCED", "retention setup");
        RunDecision reconciling = recoveryToken == null
                ? store.apply(reconcileCommand)
                : store.applyRecovered(recoveryToken, reconcileCommand);
        assertEquals(RunDecision.Code.ACCEPTED, reconciling.code());
        current = reconciling.snapshot();
        RunCommand terminalCommand = RunCommand.advanceAttempt(
                "terminal-" + fixture.run.runId(), fixture.run.runId(),
                current.revision(), fixture.fence.attemptId(),
                fixture.fence.fenceToken(), AttemptStatus.FAILED,
                false, current.updatedAtMillis() + 1L,
                "FAILED", "retention setup");
        RunDecision terminal = recoveryToken == null
                ? store.apply(terminalCommand)
                : store.applyRecovered(recoveryToken, terminalCommand);
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        return terminal.snapshot();
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[CommitManifest.SHA256_BYTES];
        Arrays.fill(digest, (byte) value);
        return digest;
    }

    private static String runPath(String runId) {
        try {
            return "/rowweave/run-state-v1/runs/" + hex(
                    java.security.MessageDigest.getInstance("SHA-256")
                            .digest(runId.getBytes(StandardCharsets.UTF_8)));
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static BatchSinkDescriptor sinkDescriptor(
            int ordinal, QualityGatePolicy policy) {
        return new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, repeat('0', 64),
                "quality-member-" + ordinal, "domain-quality",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "quality-gate-v1:" + ROLES.get(ordinal) + ':'
                        + hex(QualityGatePolicyCodec.digest(policy)));
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String hex(byte[] bytes) {
        StringBuilder out = new StringBuilder(bytes.length * 2);
        char[] alphabet = "0123456789abcdef".toCharArray();
        for (byte value : bytes) {
            out.append(alphabet[(value >>> 4) & 0xf])
                    .append(alphabet[value & 0xf]);
        }
        return out.toString();
    }

    private static final class Fixture {
        final RunSnapshot run;
        final RunAttemptFence fence;
        final StageAssignmentManifest manifest;
        final StageAssignmentSnapshot assignments;
        final QualityGatePolicy policy;
        final int[] qualityOrdinals;
        final List<ManifestMemberReceipt> receipts = new ArrayList<>();

        Fixture(RunSnapshot run, RunAttemptFence fence,
                StageAssignmentManifest manifest,
                StageAssignmentSnapshot assignments,
                QualityGatePolicy policy, int[] qualityOrdinals) {
            this.run = run;
            this.fence = fence;
            this.manifest = manifest;
            this.assignments = assignments;
            this.policy = policy;
            this.qualityOrdinals = qualityOrdinals.clone();
            for (int i = 0; i < ROLES.size(); i++) {
                receipts.add(receipt(i));
            }
        }

        void sealAll(ZkRunStateStore store) {
            for (ManifestMemberReceipt receipt : receipts) {
                assertEquals(ManifestMemberDecision.Code.RECORDED,
                        store.recordManifestMember(receipt).code());
            }
        }

        void advanceToCommitting(ZkRunStateStore store) {
            advanceToCommitting(store, null);
        }

        void advanceToCommitting(
                ZkRunStateStore store, RecoveryToken recoveryToken) {
            for (AttemptStatus next : Arrays.asList(
                    AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                    AttemptStatus.COMMITTING)) {
                RunSnapshot current = store.get(run.runId());
                RunCommand command = RunCommand.advanceAttempt(
                        "advance-" + next + '-' + run.runId(), current.runId(),
                        current.revision(), current.currentAttempt().attemptId(),
                        current.currentAttempt().fenceToken(), next, false,
                        40L + next.ordinal(), next.name(), next.name());
                RunDecision decision = recoveryToken == null
                        ? store.apply(command)
                        : store.applyRecovered(recoveryToken, command);
                assertEquals(RunDecision.Code.ACCEPTED, decision.code());
            }
        }

        BatchSinkAbortRecord abort(byte[] identity, String reason) {
            return BatchSinkAbortRecord.decided(
                    fence, manifest.generation(),
                    BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    "domain-quality", 71L,
                    run.currentAttempt().publicationId(), identity,
                    reason, 60L);
        }

        QualityGateDecision decision(long accepted, long rejected) {
            return QualityGateTestFixtures.decision(
                    run.artifactId(), fence,
                    run.currentAttempt().publicationId(), policy,
                    accepted, rejected, receipts);
        }

        private ManifestMemberReceipt receipt(int ordinal) {
            return receipt(ordinal, "member-" + ordinal);
        }

        ManifestMemberReceipt changedReceipt(int ordinal, String name) {
            return receipt(ordinal, name);
        }

        private ManifestMemberReceipt receipt(int ordinal, String name) {
            int physicalOrdinal = qualityOrdinals[ordinal];
            CommitManifest.Member member = new CommitManifest.Member(
                    name, "data/71/" + name + ".data", "schema-" + ordinal,
                    10L + ordinal, digest(20 + ordinal));
            BatchSinkDescriptor sink = sinkDescriptor(ordinal, policy);
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, manifest.assignment(physicalOrdinal),
                    ROLES.get(ordinal), sink);
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                    "domain-quality", ManifestCommitWire.encodeMember(member),
                    member.length(), member.sha256(), 30L + ordinal);
            return ManifestMemberReceipt.sealed(
                    assignments.acceptance(physicalOrdinal).token(),
                    assignments.acceptance(physicalOrdinal).commandDigest(),
                    "domain-quality", 71L,
                    run.currentAttempt().publicationId(), member,
                    30L + ordinal, locator);
        }

    }
}
