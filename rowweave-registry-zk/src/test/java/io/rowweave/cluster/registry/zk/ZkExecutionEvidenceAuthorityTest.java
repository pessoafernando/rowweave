package io.rowweave.cluster.registry.zk;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.DurableExecutionEvidenceView;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RetentionToken;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunPurgeProgress;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStoreUnavailableException;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** snapshot-first durable execution-evidence state machine. */
class ZkExecutionEvidenceAuthorityTest {

    @Test
    void initialSnapshotTransportFailureFailsQueryBeforeAnyEvidenceRead()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = terminalStore(client, "initial-failure");
            List<ZkRunStateStore.EvidenceReadPhase> phases = new ArrayList<>();
            store.evidenceReadObserverForTest(phase -> {
                phases.add(phase);
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_INITIAL_SNAPSHOT) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> store.executionEvidence("initial-failure"));
            assertEquals(Arrays.asList(
                    ZkRunStateStore.EvidenceReadPhase.BEFORE_INITIAL_SNAPSHOT), phases,
                    "evidence must never be read before a validated snapshot");
        }
    }

    @Test
    void assignmentTransportFailureAfterSnapshotReturnsSnapshotOnly()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = terminalStore(client, "assignment-failure");
            store.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_ASSIGNMENT) {
                    throw new KeeperException.ConnectionLossException();
                }
            });

            DurableExecutionEvidenceView view =
                    store.executionEvidence("assignment-failure");

            assertEquals(DurableExecutionEvidenceView.State.SNAPSHOT_ONLY, view.state());
            assertEquals("assignment-failure", view.snapshot().runId());
        }
    }

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void confirmationTransportFailureIsUnavailable()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = terminalStore(client, "confirmation-failure");
            store.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_CONFIRMATION) {
                    throw new KeeperException.OperationTimeoutException();
                }
            });

            assertThrows(RunStateStoreUnavailableException.class,
                    () -> store.executionEvidence("confirmation-failure"),
                    "current execution-evidence authority rules forbid projecting a business result when the "
                            + "incarnation confirmation is unavailable");
        }
    }

    @Test
    void retentionClaimRaceRestartsSnapshotAndNeverCreatesPartialNormalState()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = terminalStore(client, "claim-race");
            RunSnapshot terminal = store.get("claim-race");
            RunLifecycleRef lifecycle = store.lifecycle("claim-race");
            AtomicBoolean claimOnce = new AtomicBoolean(true);
            store.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_CONFIRMATION
                        && claimOnce.compareAndSet(true, false)) {
                    RunRetentionClaim claim = store.claim(
                            terminal.runId(), terminal.revision(),
                            lifecycle.terminalAtMillis(),
                            new RetentionOwner("master", 1L), 0L);
                    assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
                }
            });

            DurableExecutionEvidenceView view = store.executionEvidence("claim-race");

            assertEquals(DurableExecutionEvidenceView.State.PRESENT, view.state());
            assertEquals(io.rowweave.run.RunLifecycleState.DELETING,
                    store.lifecycle("claim-race").state());
        }
    }

    @Test
    void confirmedFinalPurgeReturnsMissingAndDiscardsEarlierSnapshotAndEvidence()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = connectedClient(server)) {
            ZkRunStateStore store = terminalStore(client, "purge-race");
            RunSnapshot terminal = store.get("purge-race");
            RunLifecycleRef lifecycle = store.lifecycle("purge-race");
            RunRetentionClaim claim = store.claim(
                    terminal.runId(), terminal.revision(), lifecycle.terminalAtMillis(),
                    new RetentionOwner("master", 1L), 0L);
            assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
            RetentionToken token = claim.token();
            AtomicBoolean purgeOnce = new AtomicBoolean(true);
            store.evidenceReadObserverForTest(phase -> {
                if (phase == ZkRunStateStore.EvidenceReadPhase.BEFORE_CONFIRMATION
                        && purgeOnce.compareAndSet(true, false)) {
                    RunPurgeProgress progress;
                    do {
                        progress = store.purge(token, 1000);
                    } while (progress.code() == RunPurgeProgress.Code.IN_PROGRESS);
                    assertEquals(RunPurgeProgress.Code.PURGED, progress.code());
                }
            });

            DurableExecutionEvidenceView view = store.executionEvidence("purge-race");

            assertEquals(DurableExecutionEvidenceView.State.MISSING, view.state());
            assertTrue(view.snapshot() == null);
            assertTrue(view.assignments() == null);
        }
    }

    private static ZkRunStateStore terminalStore(
            CuratorFramework client, String runId) {
        ZkRunStateStore store = new ZkRunStateStore(client);
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                1, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, run.revision(),
                "attempt", "publication", 2L));
        StageManifestPublication publication = store.publish(manifest(run));
        assertEquals(StageManifestPublication.Code.PUBLISHED, publication.code());
        RunDecision terminal = store.apply(RunCommand.advanceAttempt(
                "terminal-" + runId, runId, run.revision(),
                "attempt", 1L, AttemptStatus.FAILED, false,
                3L, "FAILED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        return store;
    }

    private static StageAssignmentManifest manifest(RunSnapshot run) {
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 1L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 7001, -1, "incarnation");
        return new StageAssignmentManifest(
                fence, run.artifactId(), 1L, Arrays.asList(record));
    }

    private static RunSnapshot accepted(ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static CuratorFramework connectedClient(TestingServer server)
            throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        assertTrue(client.blockUntilConnected(5, TimeUnit.SECONDS));
        return client;
    }
}
