package io.rowweave.cluster.registry.zk;

import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.StageAcceptanceDecision;
import io.rowweave.run.StageAssignmentAcceptance;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageManifestPublication;
import io.rowweave.run.StageTerminalDecision;
import io.rowweave.run.StageTerminalReceipt;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** / / real-ZK control-plane gate. */
@Tag("profile")
class ZkStageAssignmentControlPlaneBaselineTest {

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void recoveredPublicationAndReceiptsUseIncarnationGuardedFormulas()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            RecordingWrites writes = new RecordingWrites();
            ZkRunStateStore store = new ZkRunStateStore(client);
            store.transactionWriteObserverForTest(writes);
            for (int count : new int[] { 129, 1 }) {
                RunSnapshot run = attempt(
                        store, "recovered-baseline-" + count);
                RecoveryClaim claim = store.claim(
                        run.runId(), run.revision(), 0L,
                        new RecoveryOwner("profile-owner", count));
                assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
                RecoveryToken token = claim.token();
                StageAssignmentManifest manifest = manifest(run, count);

                writes.reset();
                StageManifestPublication publication =
                        store.publishRecovered(token, manifest);
                assertEquals(StageManifestPublication.Code.PUBLISHED,
                        publication.code());
                RecordingWrites.Snapshot publish = writes.snapshot();
                assertPublishTransactions(
                        publish, manifest.shardCount(), true);
                assertEquals(manifest.shardCount() + 3,
                        publish.transactions);
                assertEquals(22 + 8 * manifest.shardCount(),
                        publish.operations);
                assertTrue(publish.maxRequestPayloadBytes < 900_000);

                writes.reset();
                for (int ordinal = 0; ordinal < count; ordinal++) {
                    byte[] digest = new byte[32];
                    Arrays.fill(digest, (byte) ordinal);
                    StageAcceptanceDecision accepted = store.acceptRecovered(
                            token, new StageAssignmentAcceptance(
                                    publication.snapshot().token(ordinal),
                                    digest, ordinal));
                    assertEquals(StageAcceptanceDecision.Code.ACCEPTED,
                            accepted.code());
                }
                RecordingWrites.Snapshot accept = writes.snapshot();
                assertEquals(count, accept.transactions);
                assertEquals(7 * count, accept.operations);
                assertTrue(accept.maxRequestPayloadBytes < 900_000);
                assertUniformTransactions(
                        accept, ZkRunStateStore.GuardedMutationKind.ACCEPT,
                        count, 7);

                writes.reset();
                for (int ordinal = 0; ordinal < count; ordinal++) {
                    byte[] digest = new byte[32];
                    Arrays.fill(digest, (byte) ordinal);
                    StageTerminalDecision terminal =
                            store.recordTerminalRecovered(
                                    token, StageTerminalReceipt.succeeded(
                                            publication.snapshot().token(ordinal),
                                            digest, ordinal, ordinal, ordinal));
                    assertEquals(StageTerminalDecision.Code.RECORDED,
                            terminal.code());
                }
                RecordingWrites.Snapshot terminal = writes.snapshot();
                assertEquals(count, terminal.transactions);
                assertEquals(8 * count, terminal.operations);
                assertTrue(terminal.maxRequestPayloadBytes < 900_000);
                assertUniformTransactions(
                        terminal, ZkRunStateStore.GuardedMutationKind.TERMINAL,
                        count, 8);
            }
        }
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void recordsOneThroughMaximumAssignmentPublicationAndReceiptCost() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            RecordingWrites writes = new RecordingWrites();
            ZkRunStateStore store = new ZkRunStateStore(client);
            store.transactionWriteObserverForTest(writes);

            for (int count : new int[] { 1, 64, 512, 4096 }) {
                RunSnapshot run = attempt(store, "baseline-" + count);
                StageAssignmentManifest manifest = manifest(run, count);
                writes.reset();
                long publishStarted = System.nanoTime();
                StageManifestPublication publication = store.publish(manifest);
                long publishNanos = System.nanoTime() - publishStarted;
                assertEquals(StageManifestPublication.Code.PUBLISHED, publication.code());
                RecordingWrites.Snapshot publish = writes.snapshot();

                assertPublishTransactions(
                        publish, manifest.shardCount(), false);
                assertEquals(manifest.shardCount() + 3, publish.transactions);
                assertEquals(20 + 8 * manifest.shardCount(), publish.operations,
                        "incarnation-guarded layout, staging and root transactions must "
                                + "match the fresh-publication formula");
                assertTrue(publish.maxRequestPayloadBytes < 900_000,
                        "single ZK write request payload must stay below the 1 MiB frame limit");

                writes.reset();
                long acceptStarted = System.nanoTime();
                for (int ordinal = 0; ordinal < count; ordinal++) {
                    byte[] digest = new byte[32];
                    Arrays.fill(digest, (byte) ordinal);
                    StageAcceptanceDecision decision = store.accept(
                            new StageAssignmentAcceptance(
                                    publication.snapshot().token(ordinal), digest, ordinal));
                    assertEquals(StageAcceptanceDecision.Code.ACCEPTED, decision.code());
                }
                long acceptNanos = System.nanoTime() - acceptStarted;
                RecordingWrites.Snapshot accept = writes.snapshot();
                assertEquals(count, accept.transactions);
                assertEquals(6 * count, accept.operations,
                        "Run/lifecycle/incarnation/root fencing plus one target shard CAS "
                                + "must match the frozen publication formula");
                assertTrue(accept.maxRequestPayloadBytes < 900_000,
                        "one acceptance shard rewrite must stay below the 1 MiB frame limit");
                assertUniformTransactions(
                        accept, ZkRunStateStore.GuardedMutationKind.ACCEPT,
                        count, 6);

                writes.reset();
                long terminalStarted = System.nanoTime();
                for (int ordinal = 0; ordinal < count; ordinal++) {
                    byte[] digest = new byte[32];
                    Arrays.fill(digest, (byte) ordinal);
                    StageTerminalDecision decision = store.recordTerminal(
                            StageTerminalReceipt.succeeded(
                                    publication.snapshot().token(ordinal), digest,
                                    ordinal, ordinal, ordinal));
                    assertEquals(StageTerminalDecision.Code.RECORDED, decision.code());
                }
                long terminalNanos = System.nanoTime() - terminalStarted;
                RecordingWrites.Snapshot terminal = writes.snapshot();
                assertEquals(count, terminal.transactions);
                assertEquals(7 * count, terminal.operations,
                        "Run/lifecycle/incarnation/root/acceptance fencing plus one terminal "
                                + "shard CAS must match the frozen publication formula");
                assertTrue(terminal.maxRequestPayloadBytes < 900_000,
                        "one terminal shard rewrite must stay below the 1 MiB frame limit");
                assertUniformTransactions(
                        terminal, ZkRunStateStore.GuardedMutationKind.TERMINAL,
                        count, 7);

                System.out.println("DAR7_ZK assignments=" + count
                        + " shards=" + manifest.shardCount()
                        + " publish_ms=" + millis(publishNanos)
                        + " publish_tx=" + publish.transactions
                        + " publish_ops=" + publish.operations
                        + " publish_payload_bytes=" + publish.payloadBytes
                        + " publish_max_request_payload_bytes="
                        + publish.maxRequestPayloadBytes
                        + " accept_all_ms=" + millis(acceptNanos)
                        + " accept_tx=" + accept.transactions
                        + " accept_ops=" + accept.operations
                        + " accept_payload_bytes=" + accept.payloadBytes
                        + " accept_max_request_payload_bytes="
                        + accept.maxRequestPayloadBytes
                        + " terminal_all_ms=" + millis(terminalNanos)
                        + " terminal_tx=" + terminal.transactions
                        + " terminal_ops=" + terminal.operations
                        + " terminal_payload_bytes=" + terminal.payloadBytes
                        + " terminal_max_request_payload_bytes="
                        + terminal.maxRequestPayloadBytes);
            }
        }
    }

    private static double millis(long nanos) {
        return Math.round(nanos / 1000.0) / 1000.0;
    }

    private static void assertPublishTransactions(
            RecordingWrites.Snapshot snapshot, int shardCount,
            boolean recovered) {
        assertEquals(shardCount + 3, snapshot.records.size());
        assertTransactionIdentity(snapshot.records.get(0),
                ZkRunStateStore.GuardedMutationKind.PUBLISH_LAYOUT,
                -1);
        assertTransactionIdentity(snapshot.records.get(1),
                ZkRunStateStore.GuardedMutationKind.PUBLISH_STAGING_MARKER,
                -1);
        for (int shard = 0; shard < shardCount; shard++) {
            assertTransactionIdentity(snapshot.records.get(shard + 2),
                    ZkRunStateStore.GuardedMutationKind
                            .PUBLISH_STAGING_SHARD,
                    shard);
        }
        assertTransactionIdentity(snapshot.records.get(shardCount + 2),
                ZkRunStateStore.GuardedMutationKind.PUBLISH_ROOT,
                -1);
        assertEquals(9, snapshot.records.get(0).operationCount);
        assertEquals(recovered ? 6 : 5,
                snapshot.records.get(1).operationCount);
        for (int shard = 0; shard < shardCount; shard++) {
            assertEquals(8,
                    snapshot.records.get(shard + 2).operationCount);
        }
        assertEquals(recovered ? 7 : 6,
                snapshot.records.get(shardCount + 2).operationCount);
    }

    private static void assertUniformTransactions(
            RecordingWrites.Snapshot snapshot,
            ZkRunStateStore.GuardedMutationKind kind,
            int transactionCount, int operationCount) {
        assertEquals(transactionCount, snapshot.records.size());
        for (RecordingWrites.Transaction transaction : snapshot.records) {
            assertTransaction(transaction, kind, -1, operationCount);
        }
    }

    private static void assertTransaction(
            RecordingWrites.Transaction transaction,
            ZkRunStateStore.GuardedMutationKind kind,
            int ordinal, int operationCount) {
        assertTransactionIdentity(transaction, kind, ordinal);
        assertEquals(operationCount, transaction.operationCount);
    }

    private static void assertTransactionIdentity(
            RecordingWrites.Transaction transaction,
            ZkRunStateStore.GuardedMutationKind kind, int ordinal) {
        assertEquals(kind, transaction.kind);
        assertEquals(ordinal, transaction.ordinal);
        assertTrue(transaction.requestPayloadBytes > 0,
                "real MultiOperationRecord bytes must be positive");
        assertTrue(transaction.requestPayloadBytes < 900_000,
                "one assignment request must remain below its byte budget");
    }

    private static RunSnapshot attempt(ZkRunStateStore store, String runId) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision attempted = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.snapshot().revision(),
                "attempt-1", "publication-1", 2L));
        assertEquals(RunDecision.Code.ACCEPTED, attempted.code());
        return attempted.snapshot();
    }

    private static StageAssignmentManifest manifest(RunSnapshot run, int count) {
        io.rowweave.run.RunAttemptFence fence = new io.rowweave.run.RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>(count);
        for (int ordinal = 0; ordinal < count; ordinal++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, ordinal, ordinal + 1L, ordinal, 0,
                    ordinal == count - 1
                            ? StageAssignmentRole.SINK : StageAssignmentRole.ROUTER,
                    "worker-" + ordinal, "127.0.0.1", 7001,
                    -1, "incarnation-" + ordinal));
        }
        return new StageAssignmentManifest(fence, run.artifactId(), 1L, records);
    }

    private static CuratorFramework client(TestingServer server) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }

    private static final class RecordingWrites
            implements ZkRunStateStore.TransactionWriteObserver {
        private long payloadBytes;
        private int transactions;
        private int operations;
        private int maxRequestPayloadBytes;
        private final List<Transaction> records = new ArrayList<>();

        @Override
        public void onWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestPayloadBytes) {
            transactions++;
            operations += operationCount;
            payloadBytes += requestPayloadBytes;
            maxRequestPayloadBytes = Math.max(
                    maxRequestPayloadBytes, requestPayloadBytes);
            records.add(new Transaction(
                    kind, ordinal, operationCount, requestPayloadBytes));
        }

        void reset() {
            payloadBytes = 0L;
            transactions = 0;
            operations = 0;
            maxRequestPayloadBytes = 0;
            records.clear();
        }

        Snapshot snapshot() {
            return new Snapshot(payloadBytes, transactions, operations,
                    maxRequestPayloadBytes, new ArrayList<>(records));
        }

        private static final class Transaction {
            final ZkRunStateStore.GuardedMutationKind kind;
            final int ordinal;
            final int operationCount;
            final int requestPayloadBytes;

            Transaction(
                    ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                    int operationCount, int requestPayloadBytes) {
                this.kind = kind;
                this.ordinal = ordinal;
                this.operationCount = operationCount;
                this.requestPayloadBytes = requestPayloadBytes;
            }
        }

        private static final class Snapshot {
            final long payloadBytes;
            final int transactions;
            final int operations;
            final int maxRequestPayloadBytes;
            final List<Transaction> records;

            Snapshot(long payloadBytes, int transactions, int operations,
                     int maxRequestPayloadBytes,
                     List<Transaction> records) {
                this.payloadBytes = payloadBytes;
                this.transactions = transactions;
                this.operations = operations;
                this.maxRequestPayloadBytes = maxRequestPayloadBytes;
                this.records = records;
            }
        }
    }
}
