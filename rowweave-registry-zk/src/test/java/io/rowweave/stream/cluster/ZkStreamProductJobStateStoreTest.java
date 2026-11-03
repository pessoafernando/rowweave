package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.InMemoryArtifactRepository;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.cluster.registry.zk.ZkServiceRegistry;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import org.apache.curator.test.TestingServer;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ZK job metadata references, rather than embeds, Artifacts. */
class ZkStreamProductJobStateStoreTest {

    @Test
    void replacementSessionReadsSharedLifecycleAndArtifact() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry firstRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry secondRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             InMemoryArtifactRepository artifacts =
                     new InMemoryArtifactRepository(16, 32L * 1024L * 1024L)) {
            FileStreamProductJobAuthority first =
                    new FileStreamProductJobAuthority(
                            ((StreamProductJobStateStoreProvider) firstRegistry)
                                    .streamProductJobStateStore(artifacts));
            StreamSubmissionId submission = StreamSubmissionId.of(
                    "zk-stream-product-job");
            PlanArtifact artifact = artifact();
            StreamProductJobSnapshot created = first.publish(
                    submission, artifact);
            first.transition(created.jobId(), StreamJobStatus.STARTING);
            StreamJobProgress observed = new StreamJobProgress(
                    0, 1L, 17L,
                    io.rowweave.stream.api.StreamCdcPhase.LOG, 0,
                    5L, 4L, 50L, 40L, 3L, 30L,
                    2, 20L, 10L, 1, 1, 9L);
            StreamJobEvidence evidence = new StreamJobEvidence(
                    created.jobId(), "run-zk-evidence",
                    "attempt-zk-evidence", 0, 1L, 2L, 3L,
                    "stream-checkpoint:" + sha256("checkpoint-1"),
                    1L, 7L, 70L, observed, observed,
                    3, 3, 1_000L);
            first.updateEvidence(created.jobId(), evidence);

            FileStreamProductJobAuthority replacement =
                    new FileStreamProductJobAuthority(
                            ((StreamProductJobStateStoreProvider) secondRegistry)
                                    .streamProductJobStateStore(artifacts));
            StreamProductJobSnapshot recovered =
                    replacement.bySubmission(submission);
            assertEquals(StreamJobStatus.STARTING, recovered.status());
            assertEquals(artifact.artifactId(), recovered.artifactId());
            assertArrayEquals(PlanArtifactCodec.encode(artifact),
                    PlanArtifactCodec.encode(recovered.artifact()));
            assertEquals(evidence, recovered.evidence());
            assertEquals(3L,
                    recovered.evidence().attemptProgress().backlogEvents());
            assertEquals(30L,
                    recovered.evidence().lifetimeProgress().backlogBytes());

            replacement.transition(created.jobId(), StreamJobStatus.RUNNING);
            first.acceptCheckpoint(created.jobId(),
                    StreamCommandId.of("first-checkpoint"));
            StreamProductControlException conflict = assertThrows(
                    StreamProductControlException.class,
                    () -> replacement.acceptCheckpoint(created.jobId(),
                            StreamCommandId.of("second-checkpoint")));
            assertEquals(StreamProductControlException.Code.INVALID_STATE,
                    conflict.code());
        }
    }

    @Test
    void replacementSessionRetainsCheckpointTimingEvidence()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry firstRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry secondRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             InMemoryArtifactRepository artifacts =
                     new InMemoryArtifactRepository(16,
                             32L * 1024L * 1024L)) {
            FileStreamProductJobAuthority first =
                    new FileStreamProductJobAuthority(
                            ((StreamProductJobStateStoreProvider) firstRegistry)
                                    .streamProductJobStateStore(artifacts));
            StreamProductJobSnapshot created = first.publish(
                    StreamSubmissionId.of("zk-checkpoint-timing"),
                    artifact());
            first.transition(created.jobId(), StreamJobStatus.STARTING);
            first.transition(created.jobId(), StreamJobStatus.RUNNING);
            StreamCommandId command = StreamCommandId.of(
                    "checkpoint-timing");
            StreamProductJobSnapshot accepted = first.acceptCheckpoint(
                    created.jobId(), command);
            long acceptedAt = accepted.checkpoint(command)
                    .acceptedAtEpochMillis();
            assertTrue(acceptedAt > 0L);
            first.startCheckpoint(created.jobId(), command);
            Thread.sleep(5L);
            StreamProductJobSnapshot completed = first.completeCheckpoint(
                    created.jobId(), command);
            long completedAt = completed.checkpoint(command)
                    .completedAtEpochMillis();
            assertTrue(completedAt > acceptedAt);

            FileStreamProductJobAuthority replacement =
                    new FileStreamProductJobAuthority(
                            ((StreamProductJobStateStoreProvider) secondRegistry)
                                    .streamProductJobStateStore(artifacts));
            StreamProductCheckpointOperation recovered = replacement
                    .byJob(created.jobId()).checkpoint(command);
            assertEquals(acceptedAt, recovered.acceptedAtEpochMillis());
            assertEquals(completedAt, recovered.completedAtEpochMillis());
        }
    }

    @Test
    void replacementSessionRetainsLiveRescaleCommandHistory()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry firstRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry secondRegistry =
                     new ZkServiceRegistry(zk.getConnectString());
             InMemoryArtifactRepository artifacts =
                     new InMemoryArtifactRepository(16,
                             32L * 1024L * 1024L)) {
            FileStreamProductJobAuthority first =
                    new FileStreamProductJobAuthority(
                            ((StreamProductJobStateStoreProvider) firstRegistry)
                                    .streamProductJobStateStore(artifacts));
            StreamProductJobSnapshot created = first.publish(
                    StreamSubmissionId.of("zk-stream-rescale-history"),
                    artifact());
            first.transition(created.jobId(), StreamJobStatus.STARTING);
            first.transition(created.jobId(), StreamJobStatus.RUNNING);
            StreamCommandId command = StreamCommandId.of("scale-to-two");
            first.acceptRescale(created.jobId(), command, 2, 1L);

            FileStreamProductJobAuthority replacement =
                    new FileStreamProductJobAuthority(
                            ((StreamProductJobStateStoreProvider) secondRegistry)
                                    .streamProductJobStateStore(artifacts));
            StreamProductRescaleOperation recovered = replacement
                    .byJob(created.jobId()).rescale(command);
            assertEquals(StreamOperationStatus.ACCEPTED,
                    recovered.status());
            assertEquals(2, recovered.targetParallelism());
            assertEquals(1L, recovered.sourceGeneration());
            assertEquals(2L, recovered.targetGeneration());
        }
    }

    @Test
    void currentReaderAcceptsFrozenVersionOneMetadata() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             CuratorFramework client = CuratorFrameworkFactory.newClient(
                     zk.getConnectString(),
                     new ExponentialBackoffRetry(20, 3));
             InMemoryArtifactRepository artifacts =
                     new InMemoryArtifactRepository(16,
                             32L * 1024L * 1024L)) {
            client.start();
            assertEquals(true, client.blockUntilConnected(
                    5, java.util.concurrent.TimeUnit.SECONDS));
            String root = "/rowweave/test/stream-product-v1";
            ZkStreamProductJobStateStore store =
                    new ZkStreamProductJobStateStore(client, root, artifacts);
            FileStreamProductJobAuthority authority =
                    new FileStreamProductJobAuthority(store);
            StreamSubmissionId submission = StreamSubmissionId.of(
                    "zk-stream-v1-reader");
            StreamProductJobSnapshot created = authority.publish(
                    submission, artifact());
            String child = client.getChildren().forPath(root).get(0);
            String path = root + '/' + child;
            byte[] current = client.getData().forPath(path);
            client.setData().forPath(path, downgrade(current, 1));

            StreamProductJobSnapshot recovered =
                    new FileStreamProductJobAuthority(
                            new ZkStreamProductJobStateStore(
                                    client, root, artifacts))
                            .bySubmission(submission);
            assertEquals(created, recovered);
            assertEquals(Collections.emptyList(), recovered.rescales());
        }
    }

    @Test
    void currentReaderAcceptsFrozenVersionTwoMetadata() throws Exception {
        try (TestingServer zk = new TestingServer(true);
             CuratorFramework client = CuratorFrameworkFactory.newClient(
                     zk.getConnectString(),
                     new ExponentialBackoffRetry(20, 3));
             InMemoryArtifactRepository artifacts =
                     new InMemoryArtifactRepository(16,
                             32L * 1024L * 1024L)) {
            client.start();
            assertEquals(true, client.blockUntilConnected(
                    5, java.util.concurrent.TimeUnit.SECONDS));
            String root = "/rowweave/test/stream-product-v2";
            FileStreamProductJobAuthority authority =
                    new FileStreamProductJobAuthority(
                            new ZkStreamProductJobStateStore(
                                    client, root, artifacts));
            StreamSubmissionId submission = StreamSubmissionId.of(
                    "zk-stream-reader");
            StreamProductJobSnapshot created = authority.publish(
                    submission, artifact());
            String child = client.getChildren().forPath(root).get(0);
            String path = root + '/' + child;
            client.setData().forPath(path, downgrade(
                    client.getData().forPath(path), 2));

            StreamProductJobSnapshot recovered =
                    new FileStreamProductJobAuthority(
                            new ZkStreamProductJobStateStore(
                                    client, root, artifacts))
                            .bySubmission(submission);
            assertEquals(created, recovered);
            assertEquals(false, recovered.evidence().available());
        }
    }

    @Test
    void currentReaderAcceptsFrozenVersionThreeCheckpointMetadata()
            throws Exception {
        try (TestingServer zk = new TestingServer(true);
             CuratorFramework client = CuratorFrameworkFactory.newClient(
                     zk.getConnectString(),
                     new ExponentialBackoffRetry(20, 3));
             InMemoryArtifactRepository artifacts =
                     new InMemoryArtifactRepository(16,
                             32L * 1024L * 1024L)) {
            client.start();
            assertTrue(client.blockUntilConnected(
                    5, java.util.concurrent.TimeUnit.SECONDS));
            String root = "/rowweave/test/stream-product-v3";
            FileStreamProductJobAuthority authority =
                    new FileStreamProductJobAuthority(
                            new ZkStreamProductJobStateStore(
                                    client, root, artifacts));
            StreamSubmissionId submission = StreamSubmissionId.of(
                    "zk-stream-v3-reader");
            StreamProductJobSnapshot created = authority.publish(
                    submission, artifact());
            authority.transition(created.jobId(), StreamJobStatus.STARTING);
            authority.transition(created.jobId(), StreamJobStatus.RUNNING);
            StreamCommandId command = StreamCommandId.of(
                    "v3-checkpoint");
            authority.acceptCheckpoint(created.jobId(), command);
            String child = client.getChildren().forPath(root).get(0);
            String path = root + '/' + child;
            client.setData().forPath(path, downgrade(
                    client.getData().forPath(path), 3));

            StreamProductCheckpointOperation recovered =
                    new FileStreamProductJobAuthority(
                            new ZkStreamProductJobStateStore(
                                    client, root, artifacts))
                            .bySubmission(submission).checkpoint(command);
            assertEquals(StreamOperationStatus.ACCEPTED, recovered.status());
            assertEquals(0L, recovered.acceptedAtEpochMillis());
            assertEquals(0L, recovered.completedAtEpochMillis());
        }
    }

    private static byte[] downgrade(byte[] current, int targetVersion)
            throws Exception {
        ByteBuffer header = ByteBuffer.wrap(current);
        byte[] magic = new byte[8];
        header.get(magic);
        assertArrayEquals("RWXXSPM1".getBytes(StandardCharsets.US_ASCII),
                magic);
        assertEquals(4, header.getInt());
        int bodyLength = header.getInt();
        byte[] body = Arrays.copyOfRange(current, 16, 16 + bodyLength);
        body = withoutCheckpointTiming(body);
        if (targetVersion == 3) {
            return frame(magic, targetVersion, body);
        }
        ByteBuffer value = ByteBuffer.wrap(body);
        skipText(value);
        skipText(value);
        skipText(value);
        value.position(value.position() + Integer.BYTES);
        int progressStart = value.position();
        int backlogStart = progressStart + Integer.BYTES
                + Long.BYTES + Long.BYTES + Integer.BYTES
                + Integer.BYTES + 4 * Long.BYTES;
        byte[] withoutBacklog = new byte[body.length - 2 * Long.BYTES];
        System.arraycopy(body, 0, withoutBacklog, 0, backlogStart);
        System.arraycopy(body, backlogStart + 2 * Long.BYTES,
                withoutBacklog, backlogStart,
                body.length - backlogStart - 2 * Long.BYTES);
        assertEquals(0, withoutBacklog[withoutBacklog.length - 9]);
        byte[] versionTwo = Arrays.copyOf(
                withoutBacklog, withoutBacklog.length - 9);
        byte[] downgraded;
        if (targetVersion == 2) {
            downgraded = versionTwo;
        } else {
            assertEquals(1, targetVersion);
            assertEquals(0, ByteBuffer.wrap(versionTwo,
                    versionTwo.length - Integer.BYTES,
                    Integer.BYTES).getInt(),
                    "the frozen v1 fixture cannot carry v2 rescale history");
            downgraded = Arrays.copyOf(versionTwo,
                    versionTwo.length - Integer.BYTES);
        }
        return frame(magic, targetVersion, downgraded);
    }

    private static byte[] withoutCheckpointTiming(byte[] body) {
        ByteBuffer value = ByteBuffer.wrap(body);
        skipText(value);
        skipText(value);
        skipText(value);
        value.position(value.position() + Integer.BYTES + 112);
        if (value.get() != 0) {
            skipText(value);
            value.position(value.position() + Integer.BYTES);
        }
        int checkpointCount = value.getInt();
        ByteArrayOutputStream result = new ByteArrayOutputStream(
                body.length - checkpointCount * 2 * Long.BYTES);
        int copiedThrough = 0;
        for (int index = 0; index < checkpointCount; index++) {
            skipText(value);
            skipText(value);
            value.position(value.position() + Integer.BYTES);
            int timingStart = value.position();
            result.write(body, copiedThrough, timingStart - copiedThrough);
            value.position(timingStart + 2 * Long.BYTES);
            copiedThrough = value.position();
        }
        result.write(body, copiedThrough, body.length - copiedThrough);
        return result.toByteArray();
    }

    private static byte[] frame(byte[] magic, int version, byte[] body)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream output = new DataOutputStream(bytes)) {
            output.write(magic);
            output.writeInt(version);
            output.writeInt(body.length);
            output.write(body);
            output.write(MessageDigest.getInstance("SHA-256").digest(body));
        }
        return bytes.toByteArray();
    }

    private static void skipText(ByteBuffer input) {
        int length = input.getInt();
        input.position(input.position() + length);
    }

    private static PlanArtifact artifact() {
        StreamSession session = RowWeave.createStreamSession("zk-product-store");
        session.fromSource(source()).uid("source").sinkTo("sink", sink());
        return session.toPlanArtifact();
    }

    private static StreamSourceSpec source() {
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.StringType, false)));
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
