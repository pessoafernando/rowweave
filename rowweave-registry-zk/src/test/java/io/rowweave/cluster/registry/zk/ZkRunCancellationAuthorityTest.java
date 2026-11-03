package io.rowweave.cluster.registry.zk;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkRunCancellationAuthorityTest {

    @Test
    void independentSessionCancelsRecoveryOwnedRunWithoutReleasingOwner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = running(first, "owned");
            RecoveryClaim claim = first.claim("owned", run.revision(), 0L,
                    new RecoveryOwner("master-a", 1L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
            RecoveryToken owner = claim.token();

            RunDecision ordinary = second.apply(RunCommand.advanceAttempt(
                    "ordinary", "owned", run.revision(), "attempt", 1L,
                    AttemptStatus.COMMITTING, false, run.updatedAtMillis() + 1L,
                    null, null));
            assertEquals(RunDecision.Code.RECOVERY_OWNED, ordinary.code());

            RunCancellationResult canceled = ((RunCancellationAuthority) second)
                    .requestUserCancellation("owned", "request");
            assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                    canceled.disposition());
            assertEquals(owner, first.ownership("owned"));
            assertTrue(first.current(owner));
            assertEquals(1L, first.events("owned", 0L, 20).stream()
                    .filter(event -> event.type()
                            == io.rowweave.run.ExecutionEvent.Type.CANCEL_REQUESTED)
                    .count());
        }
    }

    @Test
    void cancellationCommitAckLossReconcilesReceiptWithoutDuplicateEvent() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            AtomicBoolean loseAck = new AtomicBoolean();
            ZkRunStateStore store = new ZkRunStateStore(zk, () -> {
                if (loseAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            running(store, "ack-loss");
            loseAck.set(true);

            RunCancellationResult result = ((RunCancellationAuthority) store)
                    .requestUserCancellation("ack-loss", "request");

            assertEquals(RunCancellationResult.Disposition.ALREADY_REQUESTED,
                    result.disposition());
            assertEquals(1L, store.events("ack-loss", 0L, 20).stream()
                    .filter(event -> event.type()
                            == io.rowweave.run.ExecutionEvent.Type.CANCEL_REQUESTED)
                    .count());
        }
    }

    @Test
    void committingCasAndCancellationCasHaveOneAuthorityWinner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = running(first, "race");
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<RunDecision> commit = new AtomicReference<>();
            AtomicReference<RunCancellationResult> cancel = new AtomicReference<>();
            ExecutorService pool = Executors.newFixedThreadPool(2);
            pool.submit(() -> {
                start.await();
                commit.set(first.apply(RunCommand.advanceAttempt(
                        "commit", "race", run.revision(), "attempt", 1L,
                        AttemptStatus.COMMITTING, false,
                        run.updatedAtMillis() + 1L, null, null)));
                return null;
            });
            pool.submit(() -> {
                start.await();
                cancel.set(((RunCancellationAuthority) second)
                        .requestUserCancellation("race", "request"));
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            if (commit.get().code() == RunDecision.Code.ACCEPTED) {
                assertEquals(RunCancellationResult.Disposition.TOO_LATE,
                        cancel.get().disposition());
                assertEquals(AttemptStatus.COMMITTING,
                        first.get("race").currentAttempt().status());
            } else {
                assertEquals(RunDecision.Code.STALE_REVISION, commit.get().code());
                assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                        cancel.get().disposition());
                assertEquals(AttemptStatus.CANCELING,
                        first.get("race").currentAttempt().status());
            }
        }
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

    private static RunSnapshot running(RunStateStore store, String runId) {
        RunSnapshot snapshot = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact", 1, 0L, 1L));
        snapshot = accepted(store, RunCommand.createAttempt(
                "attempt-" + runId, runId, snapshot.revision(),
                "attempt", "publication", 2L));
        snapshot = accepted(store, RunCommand.advanceAttempt(
                "dispatch-" + runId, runId, snapshot.revision(), "attempt", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null));
        return accepted(store, RunCommand.advanceAttempt(
                "running-" + runId, runId, snapshot.revision(), "attempt", 1L,
                AttemptStatus.RUNNING, false, 4L, null, null));
    }

    private static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        if (decision.code() == RunDecision.Code.ALREADY_APPLIED) {
            assertEquals(command.commandId(), decision.event().commandId());
            return decision.snapshot();
        }
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }
}
