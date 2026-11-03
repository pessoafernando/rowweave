package io.rowweave.cluster.registry.zk;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestCommitWire;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.BatchSinkAbortDecision;
import io.rowweave.run.BatchSinkAbortRecord;
import io.rowweave.run.ManifestIntentDecision;
import io.rowweave.run.ManifestMemberDecision;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** real-ZK member-journal write profile through 4096 Sinks. */
@Tag("profile")
class ZkBatchSinkJournalScaleProfileTest {
    private static final int[] SINK_COUNTS = {1, 64, 512, 4096};
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType));
    private static final String SCHEMA = SchemaFingerprint.of(META);
    private static final String DOMAIN = "sink-v2-zk-profile-domain";

    @Test
    @Timeout(value = 6, unit = TimeUnit.MINUTES)
    void memberAndIntentTransactionsRemainBoundedThrough4096Sinks()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            RecordingWrites writes = new RecordingWrites();
            ZkRunStateStore store = new ZkRunStateStore(
                    client, () -> { }, () -> { }, writes);
            for (int sinkCount : SINK_COUNTS) {
                Fixture fixture = fixture(store, sinkCount);
                writes.reset();
                long started = System.nanoTime();
                for (int ordinal = 0; ordinal < sinkCount; ordinal++) {
                    ManifestMemberDecision decision = store.recordManifestMember(
                            fixture.member(ordinal));
                    assertEquals(ManifestMemberDecision.Code.RECORDED,
                            decision.code());
                }
                long membersNanos = System.nanoTime() - started;
                assertEquals(sinkCount, writes.transactions);
                assertEquals(10 * sinkCount, writes.operations,
                        "member writes add the incarnation guard and lifecycle fence");
                assertTrue(writes.maxRequestPayloadBytes < 900_000,
                        "one member shard rewrite must stay below ZK 1 MiB");
                int memberMaxRequestPayloadBytes =
                        writes.maxRequestPayloadBytes;

                io.rowweave.run.ManifestCommitJournalSnapshot journal =
                        store.manifestCommitJournal(fixture.fence);
                assertEquals(sinkCount, journal.memberCount());
                writes.reset();
                ManifestIntentDecision intent = store.commitManifestIntent(
                        journal.buildIntent());
                assertEquals(ManifestIntentDecision.Code.COMMITTED, intent.code());
                assertEquals(1, writes.transactions);
                assertEquals(7 + fixture.manifest.shardCount(),
                        writes.operations,
                        "intent writes add the incarnation guard and lifecycle fence");
                assertTrue(writes.maxRequestPayloadBytes < 900_000);

                System.out.println("SINK_V2_ZK sinks=" + sinkCount
                        + " shards=" + fixture.manifest.shardCount()
                        + " member_all_ms=" + millis(membersNanos)
                        + " member_tx=" + sinkCount
                        + " member_ops=" + (10 * sinkCount)
                        + " member_max_request_payload_bytes="
                        + memberMaxRequestPayloadBytes
                        + " intent_max_request_payload_bytes="
                        + writes.maxRequestPayloadBytes);
            }
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void recoveredJournalAndAbortUseIncarnationGuardedFormulas()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            RecordingWrites writes = new RecordingWrites();
            ZkRunStateStore store = new ZkRunStateStore(
                    client, () -> { }, () -> { }, writes);
            Fixture recovered = fixture(
                    store, 1, true, "journal-formula-recovered");

            writes.reset();
            ManifestMemberDecision member = store.recordManifestMemberRecovered(
                    recovered.recoveryToken, recovered.member(0));
            assertEquals(ManifestMemberDecision.Code.RECORDED, member.code());
            assertEquals(1, writes.transactions);
            assertEquals(11, writes.operations);
            assertTrue(writes.maxRequestPayloadBytes < 900_000);

            writes.reset();
            ManifestIntentDecision intent = store.commitManifestIntentRecovered(
                    recovered.recoveryToken,
                    store.manifestCommitJournal(recovered.fence).buildIntent());
            assertEquals(ManifestIntentDecision.Code.COMMITTED, intent.code());
            assertEquals(1, writes.transactions);
            assertEquals(8 + recovered.manifest.shardCount(),
                    writes.operations);
            assertTrue(writes.maxRequestPayloadBytes < 900_000);

            for (boolean recoveredCall : new boolean[] { false, true }) {
                Fixture abortFixture = fixture(
                        store, 1, recoveredCall,
                        "abort-formula-" + recoveredCall);
                BatchSinkAbortRecord record = BatchSinkAbortRecord.decided(
                        abortFixture.fence,
                        abortFixture.manifest.generation(),
                        BatchSinkDescriptor.CapabilityProfile
                                .MANIFEST_ATOMIC_GROUP_V1,
                        DOMAIN, 31L, "abort-publication", digest(99),
                        "PREPUBLICATION_FAILURE", 40L);
                writes.reset();
                BatchSinkAbortDecision decision = recoveredCall
                        ? store.recordBatchSinkAbortRecovered(
                                abortFixture.recoveryToken, record)
                        : store.recordBatchSinkAbort(record);
                assertEquals(BatchSinkAbortDecision.Code.RECORDED,
                        decision.code());
                assertEquals(1, writes.transactions);
                assertEquals(recoveredCall ? 8 : 7, writes.operations);
                assertTrue(writes.maxRequestPayloadBytes < 900_000);
            }
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void intentFormulaIncludesThreeShardFanoutForOrdinaryAndRecoveredCalls()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            for (boolean recoveredCall : new boolean[] { false, true }) {
                Fixture fixture = fixture(
                        store, 129, recoveredCall,
                        "intent-formula-" + recoveredCall + "-129");
                for (int ordinal = 0; ordinal < 129; ordinal++) {
                    ManifestMemberDecision member = recoveredCall
                            ? store.recordManifestMemberRecovered(
                                    fixture.recoveryToken,
                                    fixture.member(ordinal))
                            : store.recordManifestMember(fixture.member(ordinal));
                    assertEquals(ManifestMemberDecision.Code.RECORDED,
                            member.code());
                }

                TransactionWrites writes = new TransactionWrites();
                store.transactionWriteObserverForTest(writes);
                ManifestIntentDecision intent = recoveredCall
                        ? store.commitManifestIntentRecovered(
                                fixture.recoveryToken,
                                store.manifestCommitJournal(fixture.fence)
                                        .buildIntent())
                        : store.commitManifestIntent(
                                store.manifestCommitJournal(fixture.fence)
                                        .buildIntent());

                assertEquals(ManifestIntentDecision.Code.COMMITTED,
                        intent.code());
                assertEquals(1, writes.records.size());
                TransactionWrite write = writes.records.get(0);
                assertEquals(ZkRunStateStore.GuardedMutationKind.MANIFEST_INTENT,
                        write.kind);
                assertEquals(-1, write.ordinal);
                assertEquals((recoveredCall ? 8 : 7)
                                + fixture.manifest.shardCount(),
                        write.operationCount,
                        "intent formula must grow with the three real shards");
                assertTrue(write.requestBytes > 0);
                assertTrue(write.requestBytes < 900_000);
            }
        }
    }

    private static Fixture fixture(ZkRunStateStore store, int sinkCount)
            throws Exception {
        return fixture(store, sinkCount, false,
                "sink-v2-zk-profile-" + sinkCount);
    }

    private static Fixture fixture(
            ZkRunStateStore store, int sinkCount, boolean recovered,
            String runId) throws Exception {
        RunSnapshot created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                1, 0L, 1L)).snapshot();
        RunSnapshot run = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.revision(),
                "attempt-1", "publication-1", 2L)).snapshot();
        RunAttemptFence fence = new RunAttemptFence(
                runId, run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        RecoveryToken recoveryToken = null;
        if (recovered) {
            RecoveryClaim claim = store.claim(
                    runId, run.revision(), 0L,
                    new RecoveryOwner("profile-owner-" + runId, 1L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
            recoveryToken = claim.token();
        }
        List<StageAssignmentRecord> records = new ArrayList<>(sinkCount);
        for (int ordinal = 0; ordinal < sinkCount; ordinal++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, ordinal, ordinal + 1L, ordinal, 0,
                    StageAssignmentRole.SINK, "worker", "127.0.0.1",
                    7100, -1, "incarnation"));
        }
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, run.artifactId(), 1L, records);
        StageAssignmentSnapshot assignments = recovered
                ? store.publishRecovered(recoveryToken, manifest).snapshot()
                : store.publish(manifest).snapshot();
        List<byte[]> commandDigests = new ArrayList<>(sinkCount);
        for (int ordinal = 0; ordinal < sinkCount; ordinal++) {
            byte[] commandDigest = digest(ordinal);
            commandDigests.add(commandDigest);
            StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                    assignments.token(ordinal), commandDigest, ordinal);
            StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                    assignments.token(ordinal), commandDigest,
                    1L, 1L, ordinal);
            if (recovered) {
                store.acceptRecovered(recoveryToken, acceptance);
                store.recordTerminalRecovered(recoveryToken, terminal);
            } else {
                store.accept(acceptance);
                store.recordTerminal(terminal);
            }
        }
        for (AttemptStatus status : new AttemptStatus[] {
                AttemptStatus.DISPATCHING,
                AttemptStatus.RUNNING,
                AttemptStatus.COMMITTING}) {
            RunSnapshot current = store.get(runId);
            RunCommand command = RunCommand.advanceAttempt(
                    "advance-" + status + "-" + runId,
                    runId, current.revision(),
                    current.currentAttempt().attemptId(),
                    current.currentAttempt().fenceToken(),
                    status, false, 3L + status.ordinal(),
                    status.name(), status.name());
            RunDecision decision = recovered
                    ? store.applyRecovered(recoveryToken, command)
                    : store.apply(command);
            assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        }
        return new Fixture(
                manifest, assignments, commandDigests, recoveryToken);
    }

    private static CuratorFramework client(TestingServer server) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }

    private static BatchSinkDescriptor descriptor(int ordinal) {
        return new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION,
                "rowweave.manifest-file", 1, SCHEMA,
                "destination-" + ordinal, DOMAIN,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
    }

    private static CommitManifest.Member memberDescriptor(int ordinal)
            throws Exception {
        String name = "member-" + ordinal;
        return new CommitManifest.Member(
                name, "data/1/" + name + ".data", SCHEMA,
                1L, digest(ordinal));
    }

    private static byte[] digest(int value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                new byte[] {(byte) (value >>> 24), (byte) (value >>> 16),
                        (byte) (value >>> 8), (byte) value});
    }

    private static double millis(long nanos) {
        return Math.round(nanos / 1000.0) / 1000.0;
    }

    private static final class Fixture {
        final StageAssignmentManifest manifest;
        final StageAssignmentSnapshot assignments;
        final List<byte[]> commandDigests;
        final RunAttemptFence fence;
        final RecoveryToken recoveryToken;

        Fixture(StageAssignmentManifest manifest,
                StageAssignmentSnapshot assignments,
                List<byte[]> commandDigests,
                RecoveryToken recoveryToken) {
            this.manifest = manifest;
            this.assignments = assignments;
            this.commandDigests = commandDigests;
            this.fence = manifest.fence();
            this.recoveryToken = recoveryToken;
        }

        ManifestMemberReceipt member(int ordinal) throws Exception {
            StageAssignmentRecord record = manifest.assignment(ordinal);
            BatchSinkDescriptor descriptor = descriptor(ordinal);
            CommitManifest.Member member = memberDescriptor(ordinal);
            SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                    manifest, record, "output-" + ordinal, descriptor);
            PreparedSinkLocator locator = new PreparedSinkLocator(
                    PreparedSinkLocator.CURRENT_VERSION, identity,
                    BatchSinkDescriptor.CapabilityProfile
                            .MANIFEST_ATOMIC_GROUP_V1,
                    DOMAIN, ManifestCommitWire.encodeMember(member),
                    member.length(), member.sha256(), ordinal);
            return ManifestMemberReceipt.sealed(
                    assignments.token(ordinal), commandDigests.get(ordinal),
                    DOMAIN, 1L, "publication-1", member, ordinal, locator);
        }
    }

    private static final class RecordingWrites
            implements ZkRunStateStore.AssignmentWriteObserver {
        long payloadBytes;
        int transactions;
        int operations;
        int maxRequestPayloadBytes;

        @Override
        public void onWrite(int operationCount, int requestPayloadBytes) {
            transactions++;
            operations += operationCount;
            payloadBytes += requestPayloadBytes;
            maxRequestPayloadBytes = Math.max(
                    maxRequestPayloadBytes, requestPayloadBytes);
        }

        void reset() {
            payloadBytes = 0L;
            transactions = 0;
            operations = 0;
            maxRequestPayloadBytes = 0;
        }
    }

    private static final class TransactionWrites
            implements ZkRunStateStore.TransactionWriteObserver {
        final List<TransactionWrite> records = new ArrayList<>();

        @Override
        public void onWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes) {
            records.add(new TransactionWrite(
                    kind, ordinal, operationCount, requestBytes));
        }
    }

    private static final class TransactionWrite {
        final ZkRunStateStore.GuardedMutationKind kind;
        final int ordinal;
        final int operationCount;
        final int requestBytes;

        TransactionWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes) {
            this.kind = kind;
            this.ordinal = ordinal;
            this.operationCount = operationCount;
            this.requestBytes = requestBytes;
        }
    }
}
