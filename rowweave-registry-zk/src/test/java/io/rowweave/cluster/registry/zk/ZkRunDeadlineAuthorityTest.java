package io.rowweave.cluster.registry.zk;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.CancellationCause;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RetentionOwner;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunCancellationAuthority;
import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunDeadlineAuthority;
import io.rowweave.run.RunDeadlineResult;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunLifecycleRef;
import io.rowweave.run.RunRetentionClaim;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The real multi-session contract for a ZooKeeper server-time deadline authority. */
class ZkRunDeadlineAuthorityTest {
    private static final String TIME_ORACLE = "/rowweave/run-deadline-v1/time";

    @Test
    void serverMtimeControlsMissingDisabledFutureAndDueReadyRun() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            RunDeadlineAuthority authority = authority(store);
            int idleVersion = zk.checkExists().forPath(TIME_ORACLE).getVersion();

            assertEquals(RunDeadlineResult.Disposition.NOT_FOUND,
                    authority.evaluateDeadline("missing").disposition());
            create(store, "disabled", 0L);
            assertEquals(RunDeadlineResult.Disposition.NO_DEADLINE,
                    authority.evaluateDeadline("disabled").disposition());
            assertEquals(idleVersion,
                    zk.checkExists().forPath(TIME_ORACLE).getVersion());

            create(store, "future", Long.MAX_VALUE);
            RunDeadlineResult future = authority.evaluateDeadline("future");
            assertEquals(RunDeadlineResult.Disposition.NOT_DUE, future.disposition());
            assertTrue(future.observedAuthorityTimeMillis() > 0L);
            assertTrue(future.observedAuthorityTimeMillis() < Long.MAX_VALUE);
            assertEquals(idleVersion + 1,
                    zk.checkExists().forPath(TIME_ORACLE).getVersion());

            RunSnapshot ready = create(store, "ready", 2L);
            RunDeadlineResult due = authority.evaluateDeadline("ready");
            assertEquals(RunDeadlineResult.Disposition.ACCEPTED, due.disposition());
            assertEquals(RunStatus.FAILED, due.status());
            assertEquals(ready.revision() + 1L, due.revision());
            assertEquals(2L, due.deadlineEpochMillis());
            assertNull(store.get("ready").currentAttempt());
            assertEquals(1L, store.events("ready", 0L, 10).stream()
                    .filter(event -> event.type() == ExecutionEvent.Type.TIMEOUT_REQUESTED)
                    .count());

            Stat oracle = new Stat();
            zk.getData().storingStatIn(oracle).forPath(TIME_ORACLE);
            assertEquals(oracle.getMtime(), due.observedAuthorityTimeMillis());
        }
    }

    @Test
    void activeDeadlineCommitAckLossReplaysOneTimeoutEvent() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            AtomicBoolean loseAck = new AtomicBoolean();
            ZkRunStateStore store = new ZkRunStateStore(zk, () -> {
                if (loseAck.compareAndSet(true, false)) {
                    throw new KeeperException.ConnectionLossException();
                }
            });
            running(store, "ack-loss", 2L);
            loseAck.set(true);

            RunDeadlineResult result = authority(store).evaluateDeadline("ack-loss");

            assertEquals(RunDeadlineResult.Disposition.ALREADY_REQUESTED,
                    result.disposition());
            assertEquals(CancellationCause.TIMEOUT,
                    store.get("ack-loss").currentAttempt().cancellationCause());
            assertEquals(1L, store.events("ack-loss", 0L, 20).stream()
                    .filter(event -> event.type() == ExecutionEvent.Type.TIMEOUT_REQUESTED)
                    .count());
        }
    }

    @Test
    void independentSessionTimesOutRecoveryOwnedRunWithoutReleasingOwner()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = running(first, "owned", 2L);
            RecoveryClaim claim = first.claim("owned", run.revision(), 0L,
                    new RecoveryOwner("master-a", 1L));
            assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
            RecoveryToken owner = claim.token();

            RunDeadlineResult timedOut = authority(second).evaluateDeadline("owned");

            assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                    timedOut.disposition());
            assertEquals(owner, first.ownership("owned"));
            assertTrue(first.current(owner));
        }
    }

    @Test
    void committingCasAndDeadlineCasHaveExactlyOneWinner() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            RunSnapshot run = running(first, "race", 2L);
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<RunDecision> commit = new AtomicReference<>();
            AtomicReference<RunDeadlineResult> timeout = new AtomicReference<>();
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
                timeout.set(authority(second).evaluateDeadline("race"));
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            if (commit.get().code() == RunDecision.Code.ACCEPTED) {
                assertEquals(RunDeadlineResult.Disposition.TOO_LATE,
                        timeout.get().disposition());
                assertEquals(AttemptStatus.COMMITTING,
                        first.get("race").currentAttempt().status());
            } else {
                assertEquals(RunDecision.Code.STALE_REVISION, commit.get().code());
                assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                        timeout.get().disposition());
                assertEquals(AttemptStatus.CANCELING,
                        first.get("race").currentAttempt().status());
            }
        }
    }

    @Test
    void twoSessionsCommitOneDeadlineMutationAndReplayTheSameReceipt()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            running(first, "dual", 2L);
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<RunDeadlineResult> left = new AtomicReference<>();
            AtomicReference<RunDeadlineResult> right = new AtomicReference<>();
            ExecutorService pool = Executors.newFixedThreadPool(2);
            pool.submit(() -> {
                start.await();
                left.set(authority(first).evaluateDeadline("dual"));
                return null;
            });
            pool.submit(() -> {
                start.await();
                right.set(authority(second).evaluateDeadline("dual"));
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            long accepted = java.util.Arrays.asList(left.get(), right.get()).stream()
                    .filter(result -> result.disposition()
                            == RunDeadlineResult.Disposition.ACCEPTED)
                    .count();
            long replayed = java.util.Arrays.asList(left.get(), right.get()).stream()
                    .filter(result -> result.disposition()
                            == RunDeadlineResult.Disposition.ALREADY_REQUESTED)
                    .count();
            assertEquals(1L, accepted);
            assertEquals(1L, replayed);
            assertEquals(1L, first.events("dual", 0L, 20).stream()
                    .filter(event -> event.type() == ExecutionEvent.Type.TIMEOUT_REQUESTED)
                    .count());
        }
    }

    @Test
    void independentRunsDoNotVersionContendOnTheSharedTimeOracle()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            int initialVersion = zk.checkExists().forPath(TIME_ORACLE).getVersion();
            int concurrency = 32;
            for (int i = 0; i < concurrency; i++) {
                create(store, "parallel-" + i, Long.MAX_VALUE);
            }
            CountDownLatch start = new CountDownLatch(1);
            ExecutorService pool = Executors.newFixedThreadPool(concurrency);
            List<Future<RunDeadlineResult>> results = new ArrayList<>();
            for (int i = 0; i < concurrency; i++) {
                final String runId = "parallel-" + i;
                results.add(pool.submit(() -> {
                    start.await();
                    return authority(store).evaluateDeadline(runId);
                }));
            }
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
            for (Future<RunDeadlineResult> result : results) {
                assertEquals(RunDeadlineResult.Disposition.NOT_DUE,
                        result.get().disposition());
            }
            assertEquals(initialVersion + concurrency,
                    zk.checkExists().forPath(TIME_ORACLE).getVersion());
        }
    }

    @Test
    void userCancellationAndDeadlineCasHaveExactlyOneCauseWinner()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework firstClient = client(server);
             CuratorFramework secondClient = client(server)) {
            ZkRunStateStore first = new ZkRunStateStore(firstClient);
            ZkRunStateStore second = new ZkRunStateStore(secondClient);
            running(first, "cause-race", 2L);
            CountDownLatch start = new CountDownLatch(1);
            AtomicReference<RunCancellationResult> user = new AtomicReference<>();
            AtomicReference<RunDeadlineResult> timeout = new AtomicReference<>();
            ExecutorService pool = Executors.newFixedThreadPool(2);
            pool.submit(() -> {
                start.await();
                user.set(((RunCancellationAuthority) first)
                        .requestUserCancellation("cause-race", "request"));
                return null;
            });
            pool.submit(() -> {
                start.await();
                timeout.set(authority(second).evaluateDeadline("cause-race"));
                return null;
            });
            start.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

            CancellationCause cause = first.get("cause-race")
                    .currentAttempt().cancellationCause();
            if (cause == CancellationCause.USER) {
                assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                        user.get().disposition());
                assertEquals(RunDeadlineResult.Disposition.TOO_LATE,
                        timeout.get().disposition());
            } else {
                assertEquals(CancellationCause.TIMEOUT, cause);
                assertEquals(RunCancellationResult.Disposition.TOO_LATE,
                        user.get().disposition());
                assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                        timeout.get().disposition());
            }
        }
    }

    @Test
    void retentionAndCorruptTimeOracleFailClosed() throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework zk = client(server)) {
            ZkRunStateStore store = new ZkRunStateStore(zk);
            create(store, "retiring", 2L);
            RunDeadlineResult terminal = authority(store).evaluateDeadline("retiring");
            RunLifecycleRef lifecycle = store.lifecycle("retiring");
            RunRetentionClaim claim = store.claim(
                    "retiring", terminal.revision(), lifecycle.terminalAtMillis(),
                    new RetentionOwner("janitor", 1L), 0L);
            assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
            assertEquals(RunDeadlineResult.Disposition.RETIRING,
                    authority(store).evaluateDeadline("retiring").disposition());

            create(store, "corrupt-time", Long.MAX_VALUE);
            zk.setData().forPath(TIME_ORACLE, new byte[] { 99 });
            assertThrows(IllegalStateException.class,
                    () -> authority(store).evaluateDeadline("corrupt-time"));
            assertEquals(RunStatus.READY, store.get("corrupt-time").status());
        }
    }

    private static RunDeadlineAuthority authority(ZkRunStateStore store) {
        return (RunDeadlineAuthority) store;
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

    private static RunSnapshot create(
            ZkRunStateStore store, String runId, long deadline) {
        return accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                1, deadline, 1L));
    }

    private static RunSnapshot running(
            ZkRunStateStore store, String runId, long deadline) {
        RunSnapshot snapshot = create(store, runId, deadline);
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

    private static RunSnapshot accepted(ZkRunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }
}
