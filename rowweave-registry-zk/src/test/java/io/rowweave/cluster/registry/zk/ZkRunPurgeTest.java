package io.rowweave.cluster.registry.zk;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RetentionToken;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunHistoryRetiringException;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunLifecycleCatalogKernel;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageManifestPublication;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Minimal end-to-end contract for the ZooKeeper purge state machine. */
class ZkRunPurgeTest {

    @Test
    void deletingHidesSparseHistoryAndPagedPurgePublishesFinalAbsence()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            RetentionToken token = claimed(store, "run");
            RunSnapshot terminal = store.get("run");

            assertEquals(terminal.revision(), store.get("run").revision());
            assertThrows(RunHistoryRetiringException.class,
                    () -> store.events("run", 0L, 1000));
            RunDecision replay = store.apply(RunCommand.cancelReadyRun(
                    "cancel", "run", 1L, 2L, "CANCELED", "terminal"));
            assertEquals(RunDecision.Code.RETENTION_IN_PROGRESS, replay.code());

            RunPurgeProgress progress = null;
            int deleted = 0;
            for (int step = 0; step < 10; step++) {
                progress = store.purge(token, 1);
                deleted += progress.deletedRecords();
                assertTrue(progress.deletedRecords() <= 1);
                if (progress.code() == RunPurgeProgress.Code.PURGED) break;
                assertEquals(RunPurgeProgress.Code.IN_PROGRESS, progress.code());
                assertEquals(1, progress.deletedRecords());
            }

            assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
            assertEquals(4, deleted);
            assertNull(store.get("run"));
            assertEquals(Collections.emptyList(), store.events("run", 0L, 1000));
            assertNull(store.lifecycle("run"));
            assertEquals(RunDecision.Code.ACCEPTED,
                    store.apply(RunCommand.createRun(
                            "recreate", "run", "definition", "artifact-new",
                            1, 0L, 3L)).code());
            assertEquals(RunPurgeProgress.Code.STALE_OWNER,
                    store.purge(token, 1).code());
            assertEquals("artifact-new", store.get("run").artifactId());
        }
    }

    @Test
    void everyBatchAckLossReconcilesAndReplacementOwnerFencesOldToken()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            AtomicInteger committed = new AtomicInteger();
            List<Integer> operationCounts = new ArrayList<>();
            List<Integer> payloadBytes = new ArrayList<>();
            ZkRunStateStore store = new ZkRunStateStore(
                    client,
                    () -> {
                        committed.incrementAndGet();
                        throw new KeeperException.ConnectionLossException();
                    },
                    (operations, bytes) -> {
                        operationCounts.add(operations);
                        payloadBytes.add(bytes);
                    });
            RetentionToken first = claimed(store, "run");

            RunPurgeProgress firstStep = store.purge(first, 1);
            assertEquals(RunPurgeProgress.Code.IN_PROGRESS, firstStep.code());
            assertEquals(1, firstStep.deletedRecords());
            assertEquals(2L, store.get("run").revision());

            RunLifecycleRef deleting = store.lifecycle("run");
            RunRetentionClaim replacement = store.claim(
                    "run", first.terminalRevision(), deleting.terminalAtMillis(),
                    new RetentionOwner("replacement", 2L),
                    first.ownershipEpoch());
            assertEquals(RunRetentionClaim.Code.ACQUIRED, replacement.code());
            assertEquals(RunPurgeProgress.Code.STALE_OWNER,
                    store.purge(first, 1).code());

            RunPurgeProgress progress;
            do {
                progress = store.purge(replacement.token(), 1);
            } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
            assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
            assertTrue(committed.get() >= 5);
            assertEquals(committed.get(), operationCounts.size());
            assertTrue(operationCounts.stream().allMatch(count -> count <= 20));
            assertTrue(payloadBytes.stream().allMatch(bytes -> bytes <= 4096));
            assertNull(store.get("run"));
        }
    }

    @Test
    @Timeout(value = 360, unit = TimeUnit.SECONDS)
    void tenThousandAndOneEventsAndReceiptsStayWithinEveryRequestBound()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            List<Integer> operationCounts = new ArrayList<>();
            List<Integer> payloadBytes = new ArrayList<>();
            ZkRunStateStore store = new ZkRunStateStore(
                    client, () -> { }, (operations, bytes) -> {
                        operationCounts.add(operations);
                        payloadBytes.add(bytes);
                    });
            RetentionToken token = claimedWithEventCount(store, 10_001);

            int deleted = 0;
            List<String> formulaViolations = new ArrayList<>();
            RunPurgeProgress progress;
            do {
                int writesBefore = operationCounts.size();
                progress = store.purge(token, 1000);
                deleted += progress.deletedRecords();
                assertTrue(progress.deletedRecords() <= 1000);
                if (progress.code() == RunPurgeProgress.Code.IN_PROGRESS) {
                    assertTrue(progress.deletedRecords() > 0);
                    if (operationCounts.size() != writesBefore + 1) {
                        formulaViolations.add("batch wrote "
                                + (operationCounts.size() - writesBefore)
                                + " transactions");
                    } else {
                        int actual = operationCounts.get(writesBefore);
                        int expected = progress.deletedRecords() + 4;
                        if (actual != expected) {
                            formulaViolations.add("deleted="
                                    + progress.deletedRecords()
                                    + " expected ops=" + expected
                                    + " actual=" + actual);
                        }
                    }
                }
            } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);

            assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
            assertEquals(20_002, deleted);
            assertEquals(22, operationCounts.size());
            assertEquals(Collections.emptyList(), formulaViolations,
                    "each purge batch must use deletedRecords+4 operations");
            assertTrue(operationCounts.stream().allMatch(count -> count <= 1004));
            assertTrue(payloadBytes.stream().allMatch(bytes -> bytes < 1_000_000));
            assertNull(store.get("large"));
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void finalPurgeFormulaTracksActualAssignmentOwnerAndAbortNodes()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            int[] assignmentCounts = {
                    0, 129, StageAssignmentManifest.MAX_ASSIGNMENTS };
            boolean[] recoveryOwners = { false, true, false };
            int[] exactExpectedOperations = { 9, 29, 272 };
            for (int shape = 0; shape < assignmentCounts.length; shape++) {
                String runId = "final-formula-" + shape;
                RetentionToken token = assignmentCounts[shape] == 0
                        ? claimed(store, runId)
                        : claimedWithAssignments(
                                store, runId, assignmentCounts[shape],
                                recoveryOwners[shape]);
                String root = runRoot(runId);
                int assignmentNodes = countSubtree(
                        client, root + "/stage-assignment");
                int ownerNodes = present(client, root + "/recovery-owner");
                int abortNodes = present(client, root + "/sink-abort");
                int referenceOperations = 0;
                int expectedOperations = 8 + referenceOperations
                        + assignmentNodes + ownerNodes + abortNodes;
                assertEquals(exactExpectedOperations[shape], expectedOperations,
                        "the oracle must be derived from the real optional nodes");

                TransactionWrites writes = new TransactionWrites();
                store.transactionWriteObserverForTest(writes);
                RunPurgeProgress progress;
                do {
                    progress = store.purge(token, 1000);
                } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);

                assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
                TransactionWrite finalWrite = writes.only(
                        ZkRunStateStore.GuardedMutationKind.PURGE_FINAL);
                assertEquals(-1, finalWrite.ordinal);
                assertEquals(expectedOperations, finalWrite.operationCount,
                        "final purge must delete the exact real node shape in one multi");
                assertTrue(finalWrite.requestBytes > 8,
                        "the observer must measure the serialized multi, not a constant");
                assertTrue(finalWrite.requestBytes < 1_000_000,
                        "every real final multi must stay below the ZK jute frame limit");
                assertNull(store.get(runId));
            }
        }
    }

    @Test
    void corruptFinalChildrenAndCursorReturnCorruptWithoutDeletingEvidence()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(client);
            RetentionToken childToken = claimed(store, "child-corrupt");
            assertEquals(RunPurgeProgress.Code.IN_PROGRESS,
                    store.purge(childToken, 1000).code());
            String childRoot = runRoot("child-corrupt");
            String evidence = childRoot + "/events/foreign";
            client.create().forPath(evidence, new byte[] { 1 });

            assertEquals(RunPurgeProgress.Code.CORRUPT,
                    store.purge(childToken, 1000).code());
            assertTrue(client.checkExists().forPath(evidence) != null);
            assertTrue(store.current(childToken));

            RetentionToken cursorToken = claimed(store, "cursor-corrupt");
            assertEquals(RunPurgeProgress.Code.IN_PROGRESS,
                    store.purge(cursorToken, 1).code());
            String cursor = runRoot("cursor-corrupt") + "/retention-purge";
            byte[] encoded = client.getData().forPath(cursor);
            encoded[encoded.length - 1] ^= 1;
            client.setData().forPath(cursor, encoded);

            assertEquals(RunPurgeProgress.Code.CORRUPT,
                    store.purge(cursorToken, 1).code());
            assertTrue(client.checkExists().forPath(cursor) != null);
            assertTrue(store.current(cursorToken));
        }
    }

    @Test
    void newSessionFinishesFinalMultiWithOwnerAndEmptyAssignmentLayout()
            throws Exception {
        try (TestingServer server = new TestingServer(true)) {
            RetentionToken retention;
            String root = runRoot("owned");
            try (CuratorFramework first = connectedClient(server)) {
                ZkRunStateStore store = new ZkRunStateStore(first);
                RunSnapshot created = accepted(store, RunCommand.createRun(
                        "create-owned", "owned", "definition", "artifact",
                        1, 0L, 1L));
                RecoveryClaim recovery = store.claim(
                        "owned", created.revision(), 0L,
                        new RecoveryOwner("master", 1L));
                assertEquals(RecoveryClaim.Code.ACQUIRED, recovery.code());
                RecoveryToken owner = recovery.token();
                createEmptyAssignmentLayout(first, root);
                RunDecision terminal = store.applyRecovered(owner,
                        RunCommand.cancelReadyRun(
                                "cancel-owned", "owned", created.revision(),
                                2L, "CANCELED", "terminal"));
                assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
                RunLifecycleRef ref = store.lifecycle("owned");
                RunRetentionClaim claim = store.claim(
                        "owned", terminal.snapshot().revision(),
                        ref.terminalAtMillis(),
                        new RetentionOwner("retention", 2L), 0L);
                retention = claim.token();
                assertEquals(RunPurgeProgress.Code.IN_PROGRESS,
                        store.purge(retention, 1).code());
            }

            try (CuratorFramework second = connectedClient(server)) {
                ZkRunStateStore reopened = new ZkRunStateStore(second);
                RunPurgeProgress progress;
                do {
                    progress = reopened.purge(retention, 1000);
                } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
                assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
                assertNull(reopened.get("owned"));
                assertNull(reopened.lifecycle("owned"));
                assertNull(second.checkExists().forPath(root));
            }
        }
    }

    private static RetentionToken claimed(ZkRunStateStore store, String runId) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create", runId, "definition", "artifact", 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision terminal = store.apply(RunCommand.cancelReadyRun(
                "cancel", runId, created.snapshot().revision(), 2L,
                "CANCELED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        RunLifecycleRef ref = store.lifecycle(runId);
        RunRetentionClaim claim = store.claim(
                runId, terminal.snapshot().revision(), ref.terminalAtMillis(),
                new RetentionOwner("master", 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static RetentionToken claimedWithEventCount(
            ZkRunStateStore store, int eventCount) {
        RunSnapshot snapshot = accepted(store, RunCommand.createRun(
                "create-large", "large", "definition", "artifact",
                1, 0L, 1L));
        snapshot = accepted(store, RunCommand.createAttempt(
                "attempt-large", "large", snapshot.revision(),
                "attempt", "publication", 2L));
        snapshot = accepted(store, RunCommand.advanceAttempt(
                "dispatch-large", "large", snapshot.revision(),
                "attempt", 1L, AttemptStatus.DISPATCHING, false,
                3L, null, null));
        snapshot = accepted(store, RunCommand.advanceAttempt(
                "running-large", "large", snapshot.revision(),
                "attempt", 1L, AttemptStatus.RUNNING, false,
                4L, null, null));
        snapshot = accepted(store, RunCommand.advanceAttempt(
                "committing-large", "large", snapshot.revision(),
                "attempt", 1L, AttemptStatus.COMMITTING, false,
                5L, null, null));
        snapshot = accepted(store, RunCommand.requestCancellation(
                "reconcile-6", "large", snapshot.revision(),
                "attempt", 1L, CancellationCause.USER,
                6L, "TEST", "build purge history"));
        CancellationCause nextCause = CancellationCause.TIMEOUT;
        while (snapshot.revision() < eventCount - 1L) {
            long nextRevision = snapshot.revision() + 1L;
            snapshot = accepted(store, RunCommand.requestCancellation(
                    "reconcile-" + nextRevision, "large", snapshot.revision(),
                    "attempt", 1L, nextCause, nextRevision,
                    "TEST", "build purge history"));
            nextCause = nextCause == CancellationCause.TIMEOUT
                    ? CancellationCause.USER : CancellationCause.TIMEOUT;
        }
        snapshot = accepted(store, RunCommand.advanceAttempt(
                "terminal-large", "large", snapshot.revision(),
                "attempt", 1L, AttemptStatus.FAILED, false,
                eventCount, "FAILED", "terminal"));
        assertEquals(eventCount, snapshot.revision());
        RunLifecycleRef ref = store.lifecycle("large");
        RunRetentionClaim claim = store.claim(
                "large", snapshot.revision(), ref.terminalAtMillis(),
                new RetentionOwner("master", 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static RetentionToken claimedWithAssignments(
            ZkRunStateStore store, String runId, int assignmentCount,
            boolean recoveryOwner) {
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                1, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, run.revision(),
                "attempt", "publication", 2L));
        RecoveryToken recoveryToken = null;
        if (recoveryOwner) {
            RecoveryClaim recovery = store.claim(
                    runId, run.revision(), 0L,
                    new RecoveryOwner("recovery-" + runId, 1L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, recovery.code());
            recoveryToken = recovery.token();
        }
        StageManifestPublication publication = recoveryOwner
                ? store.publishRecovered(
                        recoveryToken, assignmentManifest(run, assignmentCount))
                : store.publish(assignmentManifest(run, assignmentCount));
        assertEquals(StageManifestPublication.Code.PUBLISHED, publication.code());
        RunCommand terminalCommand = RunCommand.advanceAttempt(
                "terminal-" + runId, runId, run.revision(),
                "attempt", 1L, AttemptStatus.FAILED, false,
                3L, "FAILED", "terminal");
        RunDecision terminal = recoveryOwner
                ? store.applyRecovered(recoveryToken, terminalCommand)
                : store.apply(terminalCommand);
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        RunLifecycleRef lifecycle = store.lifecycle(runId);
        RunRetentionClaim claim = store.claim(
                runId, terminal.snapshot().revision(),
                lifecycle.terminalAtMillis(),
                new RetentionOwner("retention-" + runId, 2L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static RunSnapshot accepted(ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static StageAssignmentManifest assignmentManifest(
            RunSnapshot run, int count) {
        RunAttemptFence fence = new RunAttemptFence(
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

    private static CuratorFramework connectedClient(TestingServer server) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }

    private static String runRoot(String runId) {
        return "/rowweave/run-state-v1/runs/"
                + RunLifecycleCatalogKernel.digestHex(runId);
    }

    private static int countSubtree(CuratorFramework client, String path)
            throws Exception {
        if (client.checkExists().forPath(path) == null) return 0;
        int count = 1;
        for (String child : client.getChildren().forPath(path)) {
            count += countSubtree(client, path + "/" + child);
        }
        return count;
    }

    private static int present(CuratorFramework client, String path)
            throws Exception {
        return client.checkExists().forPath(path) == null ? 0 : 1;
    }

    private static void createEmptyAssignmentLayout(
            CuratorFramework client, String root) throws Exception {
        String stage = root + "/stage-assignment";
        client.create().forPath(stage, new byte[0]);
        client.create().forPath(stage + "/assignment-shards", new byte[0]);
        client.create().forPath(stage + "/acceptance-shards", new byte[0]);
        client.create().forPath(stage + "/terminal-shards", new byte[0]);
        client.create().forPath(stage + "/manifest-member-shards", new byte[0]);
        client.create().forPath(stage + "/manifest-intent", new byte[0]);
    }

    private static final class TransactionWrites
            implements ZkRunStateStore.TransactionWriteObserver {
        private final List<TransactionWrite> records = new ArrayList<>();

        @Override
        public void onWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes) {
            records.add(new TransactionWrite(
                    kind, ordinal, operationCount, requestBytes));
        }

        TransactionWrite only(ZkRunStateStore.GuardedMutationKind kind) {
            TransactionWrite match = null;
            for (TransactionWrite record : records) {
                if (record.kind != kind) continue;
                assertNull(match, "expected exactly one " + kind + " transaction");
                match = record;
            }
            assertTrue(match != null, "missing " + kind + " transaction");
            return match;
        }
    }

    private static final class TransactionWrite {
        private final ZkRunStateStore.GuardedMutationKind kind;
        private final int ordinal;
        private final int operationCount;
        private final int requestBytes;

        private TransactionWrite(
                ZkRunStateStore.GuardedMutationKind kind, int ordinal,
                int operationCount, int requestBytes) {
            this.kind = kind;
            this.ordinal = ordinal;
            this.operationCount = operationCount;
            this.requestBytes = requestBytes;
        }
    }
}
