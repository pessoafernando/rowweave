package io.rowweave.cluster.registry.zk;

import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunCommand;
import io.rowweave.run.RunDecision;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageManifestPublication;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** every transaction hook must be reachable at exact boundaries. */
class ZkRunIncarnationObserverCoverageTest {

    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    void publicationHooksCoverRepairAndFirstMiddleLastShardTransactions()
            throws Exception {
        try (TestingServer server = new TestingServer(true);
             CuratorFramework owner = connectedClient(server);
             CuratorFramework legacyFixture = connectedClient(server)) {
            ZkRunStateStore store = new ZkRunStateStore(owner);
            RunSnapshot run = active(store, "publication-observers");
            Map<ZkRunStateStore.GuardedMutationKind,
                    EnumSet<ZkRunStateStore.GuardedMutationMoment>> seen =
                    new EnumMap<>(ZkRunStateStore.GuardedMutationKind.class);
            Map<ZkRunStateStore.GuardedMutationMoment, Set<Integer>> shards =
                    new EnumMap<>(ZkRunStateStore.GuardedMutationMoment.class);
            AtomicBoolean removedLegacyIntent = new AtomicBoolean();
            store.guardedMutationObserverForTest((kind, ordinal, moment) -> {
                seen.computeIfAbsent(kind, ignored -> EnumSet.noneOf(
                        ZkRunStateStore.GuardedMutationMoment.class)).add(moment);
                if (kind == ZkRunStateStore.GuardedMutationKind
                        .PUBLISH_STAGING_SHARD) {
                    shards.computeIfAbsent(moment, ignored -> new TreeSet<>())
                            .add(ordinal);
                }
                if (kind == ZkRunStateStore.GuardedMutationKind.PUBLISH_LAYOUT
                        && moment == ZkRunStateStore.GuardedMutationMoment
                        .AFTER_COMMIT_BEFORE_ACK
                        && removedLegacyIntent.compareAndSet(false, true)) {
                    legacyFixture.delete().forPath(
                            runPath(run.runId())
                                    + "/stage-assignment/manifest-intent");
                }
            });

            StageManifestPublication publication =
                    store.publish(manifest(run, 129));

            assertEquals(StageManifestPublication.Code.PUBLISHED,
                    publication.code());
            assertTrue(removedLegacyIntent.get());
            assertBothMoments(seen,
                    ZkRunStateStore.GuardedMutationKind.PUBLISH_LAYOUT,
                    ZkRunStateStore.GuardedMutationKind.PUBLISH_INTENT_REPAIR,
                    ZkRunStateStore.GuardedMutationKind.PUBLISH_STAGING_MARKER,
                    ZkRunStateStore.GuardedMutationKind.PUBLISH_ROOT);
            assertEquals(new TreeSet<>(Arrays.asList(0, 1, 2)),
                    shards.get(ZkRunStateStore.GuardedMutationMoment.BEFORE_COMMIT));
            assertEquals(new TreeSet<>(Arrays.asList(0, 1, 2)),
                    shards.get(ZkRunStateStore.GuardedMutationMoment
                            .AFTER_COMMIT_BEFORE_ACK));
        }
    }

    private static void assertBothMoments(
            Map<ZkRunStateStore.GuardedMutationKind,
                    EnumSet<ZkRunStateStore.GuardedMutationMoment>> seen,
            ZkRunStateStore.GuardedMutationKind... kinds) {
        EnumSet<ZkRunStateStore.GuardedMutationMoment> both =
                EnumSet.allOf(ZkRunStateStore.GuardedMutationMoment.class);
        for (ZkRunStateStore.GuardedMutationKind kind : kinds) {
            assertEquals(both, seen.get(kind), kind + " must expose both moments");
        }
    }

    private static RunSnapshot active(ZkRunStateStore store, String runId) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create", runId, "definition", "artifact", 2, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision active = store.apply(RunCommand.createAttempt(
                "attempt", runId, created.snapshot().revision(),
                "attempt", "publication", 2L));
        assertEquals(RunDecision.Code.ACCEPTED, active.code());
        return active.snapshot();
    }

    private static StageAssignmentManifest manifest(
            RunSnapshot run, int assignmentCount) {
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), run.currentAttempt().attemptId(),
                run.currentAttempt().fenceToken());
        List<StageAssignmentRecord> records = new ArrayList<>(assignmentCount);
        for (int ordinal = 0; ordinal < assignmentCount; ordinal++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, ordinal, ordinal + 1L, ordinal, 0,
                    ordinal == assignmentCount - 1
                            ? StageAssignmentRole.SINK : StageAssignmentRole.ROUTER,
                    "worker-" + ordinal, "127.0.0.1", 7001,
                    -1, "incarnation-" + ordinal));
        }
        return new StageAssignmentManifest(
                fence, run.artifactId(), 1L, records);
    }

    private static String runPath(String runId) {
        return "/rowweave/run-state-v1/runs/" + sha256Hex(runId);
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            char[] alphabet = "0123456789abcdef".toCharArray();
            char[] encoded = new char[digest.length * 2];
            for (int i = 0; i < digest.length; i++) {
                encoded[i * 2] = alphabet[(digest[i] >>> 4) & 0xf];
                encoded[i * 2 + 1] = alphabet[digest[i] & 0xf];
            }
            return new String(encoded);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static CuratorFramework connectedClient(TestingServer server)
            throws InterruptedException {
        CuratorFramework client = CuratorFrameworkFactory.newClient(
                server.getConnectString(), new ExponentialBackoffRetry(10, 3));
        client.start();
        assertTrue(client.blockUntilConnected(10, TimeUnit.SECONDS));
        return client;
    }
}
