package io.rowweave.cluster.registry.zk;

import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryFencedRunStateStore;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ZkRecoveryFencedMutationTest {

    @Test
    void ackLossUnderSameOwnerReconcilesExactReceiptWithoutDuplicateEvent() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework client = client(server)) {
            AtomicBoolean loseAck = new AtomicBoolean();
            ZkRunStateStore store = new ZkRunStateStore(client, () -> {
                if (loseAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            RunSnapshot run = accepted(store, RunCommand.createRun(
                    "create", "run", "definition", "artifact", 1, 0L, 1L));
            RecoveryToken token = acquired(store.claim(
                    "run", run.revision(), 0L, new RecoveryOwner("master-a", 1L)));
            loseAck.set(true);

            RunDecision result = store.applyRecovered(token, RunCommand.createAttempt(
                    "attempt", "run", run.revision(),
                    "attempt-1", "publication-1", 2L));

            assertEquals(RunDecision.Code.ALREADY_APPLIED, result.code());
            assertEquals(2L, result.snapshot().revision());
            assertEquals(2, store.events("run", 0L, 10).size());
            assertEquals(token, store.ownership("run"));
        }
    }

    @Test
    void ownerReplacementAfterCommitFencesOldAckReconciliation() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            AtomicBoolean replaceAfterCommit = new AtomicBoolean();
            AtomicReference<ZkRunStateStore> secondRef = new AtomicReference<>();
            AtomicReference<RecoveryToken> replacement = new AtomicReference<>();
            ZkRunStateStore first = new ZkRunStateStore(firstClient, () -> {
                if (replaceAfterCommit.compareAndSet(true, false)) {
                    RecoveryClaim claim = secondRef.get().claim(
                            "run", 2L, 1L, new RecoveryOwner("master-b", 2L));
                    replacement.set(acquired(claim));
                    throw new KeeperException.ConnectionLossException();
                }
            });
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            secondRef.set(second);
            RunSnapshot run = accepted(first, RunCommand.createRun(
                    "create", "run", "definition", "artifact", 1, 0L, 1L));
            RecoveryToken firstToken = acquired(first.claim(
                    "run", run.revision(), 0L, new RecoveryOwner("master-a", 1L)));
            RunCommand attempt = RunCommand.createAttempt(
                    "attempt", "run", run.revision(),
                    "attempt-1", "publication-1", 2L);
            replaceAfterCommit.set(true);

            RunDecision oldResult = first.applyRecovered(firstToken, attempt);

            assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER, oldResult.code());
            assertEquals(2L, second.get("run").revision());
            assertEquals(2, second.events("run", 0L, 10).size());
            assertEquals(replacement.get(), second.ownership("run"));
            assertEquals(RunDecision.Code.ALREADY_APPLIED,
                    ((RecoveryFencedRunStateStore) second)
                            .applyRecovered(replacement.get(), attempt).code());
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
        assertTrue(client.blockUntilConnected(5, java.util.concurrent.TimeUnit.SECONDS));
        return client;
    }

    private static RecoveryToken acquired(RecoveryClaim claim) {
        assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static RunSnapshot accepted(ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }
}
