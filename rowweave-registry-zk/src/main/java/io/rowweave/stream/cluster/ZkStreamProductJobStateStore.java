package io.rowweave.stream.cluster;

import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamRecoveryPointId;
import io.rowweave.stream.api.StreamSubmissionId;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.UnaryOperator;

/** ZooKeeper CAS metadata authority with Artifacts kept in shared storage. */
public final class ZkStreamProductJobStateStore
        implements StreamProductJobStateStore {
    private static final byte[] MAGIC =
            "RWXXSPM1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 4;
    private static final int CHECKPOINT_TIMING_VERSION = 4;
    private static final int EVIDENCE_VERSION = 3;
    private static final int RESCALE_VERSION = 2;
    private static final int LEGACY_VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_TEXT_BYTES = 1_024;
    private static final int MAX_METADATA_BYTES = 768 * 1_024;
    private static final int MAX_JOBS = 4_096;

    private final CuratorFramework client;
    private final String root;
    private final ArtifactRepository artifacts;

    public ZkStreamProductJobStateStore(CuratorFramework client,
            String root, ArtifactRepository artifacts) {
        if (client == null || artifacts == null || root == null
                || !root.startsWith("/") || root.length() < 2
                || root.endsWith("/") || root.contains("//")) {
            throw new IllegalArgumentException(
                    "valid ZooKeeper Stream product store configuration is required");
        }
        this.client = client;
        this.root = root;
        this.artifacts = artifacts;
        ensurePath(root);
    }

    @Override
    public StreamProductJobSnapshot createIfAbsent(
            StreamProductJobSnapshot initial) {
        if (initial == null) {
            throw new IllegalArgumentException(
                    "initial Stream product job is required");
        }
        String path = path(initial.jobId());
        byte[] encoded = encode(initial);
        try {
            if (client.getChildren().forPath(root).size() >= MAX_JOBS) {
                throw new IllegalStateException(
                        "ZooKeeper Stream product job capacity is exhausted");
            }
            client.create().withMode(CreateMode.PERSISTENT)
                    .forPath(path, encoded);
            return decode(encoded);
        } catch (KeeperException.NodeExistsException race) {
            return current(initial.jobId());
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw io("ZooKeeper Stream product job publication failed",
                    failure);
        }
    }

    @Override
    public StreamProductJobSnapshot current(StreamJobId jobId) {
        requireJobId(jobId);
        try {
            byte[] data = client.getData().forPath(path(jobId));
            StreamProductJobSnapshot snapshot = decode(data);
            requireIdentity(jobId, snapshot);
            return snapshot;
        } catch (KeeperException.NoNodeException absent) {
            return null;
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw io("ZooKeeper Stream product job read failed", failure);
        }
    }

    @Override
    public List<StreamProductJobSnapshot> all() {
        try {
            List<String> children = client.getChildren().forPath(root);
            if (children.size() > MAX_JOBS) {
                throw new IllegalStateException(
                        "ZooKeeper Stream product catalog exceeds its bound");
            }
            List<StreamProductJobSnapshot> result = new ArrayList<>();
            for (String child : children) {
                byte[] data = client.getData().forPath(root + '/' + child);
                StreamProductJobSnapshot snapshot = decode(data);
                if (!child.equals(key(snapshot.jobId()))) {
                    throw new IllegalStateException(
                            "ZooKeeper Stream product key is noncanonical");
                }
                result.add(snapshot);
            }
            result.sort((left, right) -> left.jobId().value()
                    .compareTo(right.jobId().value()));
            return Collections.unmodifiableList(result);
        } catch (RuntimeException failure) {
            throw failure;
        } catch (Exception failure) {
            throw io("ZooKeeper Stream product catalog read failed", failure);
        }
    }

    @Override
    public StreamProductJobSnapshot update(StreamJobId jobId,
            UnaryOperator<StreamProductJobSnapshot> mutation) {
        requireJobId(jobId);
        if (mutation == null) {
            throw new IllegalArgumentException(
                    "Stream product mutation is required");
        }
        String path = path(jobId);
        while (true) {
            try {
                Stat version = new Stat();
                StreamProductJobSnapshot current = decode(
                        client.getData().storingStatIn(version)
                                .forPath(path));
                requireIdentity(jobId, current);
                StreamProductJobSnapshot next = mutation.apply(current);
                requireIdentity(jobId, next);
                if (next.equals(current)) return current;
                byte[] encoded = encode(next);
                try {
                    client.setData().withVersion(version.getVersion())
                            .forPath(path, encoded);
                    return next;
                } catch (KeeperException.BadVersionException race) {
                    // Re-evaluate the domain mutation against the new version.
                }
            } catch (KeeperException.NoNodeException absent) {
                return null;
            } catch (RuntimeException failure) {
                throw failure;
            } catch (Exception failure) {
                throw io("ZooKeeper Stream product mutation failed", failure);
            }
        }
    }

    private byte[] encode(StreamProductJobSnapshot snapshot) {
        byte[] artifact = PlanArtifactCodec.encode(snapshot.artifact());
        String storedId = artifacts.put(artifact);
        if (!snapshot.artifactId().equals(storedId)) {
            throw new IllegalStateException(
                    "shared Artifact repository changed Stream identity");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                writeText(body, snapshot.submissionId().value());
                writeText(body, snapshot.jobId().value());
                writeText(body, snapshot.artifactId());
                body.writeInt(snapshot.status().wireId());
                writeProgress(body, snapshot.progress(), true);
                body.writeBoolean(snapshot.cancelCommandId() != null);
                if (snapshot.cancelCommandId() != null) {
                    writeText(body, snapshot.cancelCommandId().value());
                    body.writeInt(operationWireId(
                            snapshot.cancelStatusOrNull()));
                }
                body.writeInt(snapshot.checkpoints().size());
                for (StreamProductCheckpointOperation checkpoint
                        : snapshot.checkpoints()) {
                    writeText(body, checkpoint.commandId().value());
                    writeText(body, checkpoint.checkpointId());
                    body.writeInt(operationWireId(checkpoint.status()));
                    body.writeLong(checkpoint.acceptedAtEpochMillis());
                    body.writeLong(checkpoint.completedAtEpochMillis());
                }
                body.writeBoolean(snapshot.savepointOrNull() != null);
                if (snapshot.savepointOrNull() != null) {
                    StreamProductSavepointOperation savepoint =
                            snapshot.savepointOrNull();
                    writeText(body, savepoint.commandId().value());
                    writeText(body, savepoint.savepointId());
                    writeText(body, savepoint.requestDigest());
                    body.writeInt(operationWireId(savepoint.status()));
                }
                body.writeBoolean(snapshot.recoveryOrNull() != null);
                if (snapshot.recoveryOrNull() != null) {
                    StreamProductRecovery recovery = snapshot.recoveryOrNull();
                    writeText(body, recovery.sourceJobId().value());
                    body.writeInt(recovery.recoveryPointId().kind().ordinal());
                    writeText(body, recovery.recoveryPointId().value());
                    body.writeInt(recovery.targetParallelism());
                    body.writeInt(recovery.targetMaxParallelism());
                    body.writeBoolean(recovery.offlineRescale());
                }
                body.writeInt(snapshot.rescales().size());
                for (StreamProductRescaleOperation rescale
                        : snapshot.rescales()) {
                    writeText(body, rescale.commandId().value());
                    writeText(body, rescale.rescaleId());
                    body.writeInt(rescale.targetParallelism());
                    body.writeLong(rescale.sourceGeneration());
                    body.writeLong(rescale.targetGeneration());
                    body.writeInt(operationWireId(rescale.status()));
                    body.writeInt(rescale.phase().wireId());
                }
                writeEvidence(body, snapshot.evidence());
            }
            byte[] body = bodyBytes.toByteArray();
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream();
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(VERSION);
                frame.writeInt(body.length);
                frame.write(body);
                frame.write(sha256(body));
            }
            return bounded(frameBytes.toByteArray());
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private StreamProductJobSnapshot decode(byte[] encoded) {
        byte[] frame = bounded(encoded);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            int version = input.readInt();
            int bodyLength = input.readInt();
            if (!MessageDigest.isEqual(MAGIC, magic)
                    || version < LEGACY_VERSION || version > VERSION
                    || bodyLength < 0 || MAGIC.length + 8L + bodyLength
                    + DIGEST_BYTES != frame.length) {
                throw corrupt("Stream product metadata header is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(body))) {
                throw corrupt("Stream product metadata checksum differs");
            }
            try (DataInputStream value = new DataInputStream(
                    new ByteArrayInputStream(body))) {
                StreamSubmissionId submission = StreamSubmissionId.of(
                        readText(value));
                StreamJobId jobId = StreamJobId.of(readText(value));
                String artifactId = readText(value);
                PlanArtifact artifact = artifact(artifactId);
                StreamJobStatus status = jobStatus(value.readInt());
                StreamJobProgress progress = readProgress(
                        value, version >= EVIDENCE_VERSION);
                StreamCommandId cancelId = null;
                StreamOperationStatus cancelStatus = null;
                if (value.readBoolean()) {
                    cancelId = StreamCommandId.of(readText(value));
                    cancelStatus = operationStatus(value.readInt());
                }
                int checkpointCount = value.readInt();
                if (checkpointCount < 0 || checkpointCount > 257) {
                    throw corrupt("checkpoint command count is invalid");
                }
                List<StreamProductCheckpointOperation> checkpoints =
                        new ArrayList<>(checkpointCount);
                for (int index = 0; index < checkpointCount; index++) {
                    checkpoints.add(new StreamProductCheckpointOperation(
                            StreamCommandId.of(readText(value)),
                            readText(value),
                            operationStatus(value.readInt()),
                            version >= CHECKPOINT_TIMING_VERSION
                                    ? value.readLong() : 0L,
                            version >= CHECKPOINT_TIMING_VERSION
                                    ? value.readLong() : 0L));
                }
                StreamProductSavepointOperation savepoint = null;
                if (value.readBoolean()) {
                    savepoint = new StreamProductSavepointOperation(
                            StreamCommandId.of(readText(value)),
                            readText(value), readText(value),
                            operationStatus(value.readInt()));
                }
                StreamProductRecovery recovery = null;
                if (value.readBoolean()) {
                    StreamJobId source = StreamJobId.of(readText(value));
                    int kind = value.readInt();
                    String point = readText(value);
                    StreamRecoveryPointId recoveryPoint = kind == 0
                            ? StreamRecoveryPointId.checkpoint(point)
                            : kind == 1
                            ? StreamRecoveryPointId.savepoint(point) : null;
                    if (recoveryPoint == null) {
                        throw corrupt("unknown Stream recovery point kind");
                    }
                    recovery = new StreamProductRecovery(source,
                            recoveryPoint, value.readInt(), value.readInt(),
                            value.readBoolean());
                }
                List<StreamProductRescaleOperation> rescales =
                        Collections.emptyList();
                if (version >= RESCALE_VERSION) {
                    int rescaleCount = value.readInt();
                    if (rescaleCount < 0 || rescaleCount > 129) {
                        throw corrupt("rescale command count is invalid");
                    }
                    rescales = new ArrayList<>(rescaleCount);
                    for (int index = 0; index < rescaleCount; index++) {
                        try {
                            rescales.add(new StreamProductRescaleOperation(
                                    StreamCommandId.of(readText(value)),
                                    readText(value), value.readInt(),
                                    value.readLong(), value.readLong(),
                                    operationStatus(value.readInt()),
                                    StreamProductRescaleOperation.Phase
                                            .fromWireId(value.readInt())));
                        } catch (IllegalArgumentException malformed) {
                            throw corrupt(
                                    "Stream rescale metadata is invalid",
                                    malformed);
                        }
                    }
                }
                StreamJobEvidence evidence = version >= EVIDENCE_VERSION
                        ? readEvidence(value, jobId, progress)
                        : StreamJobEvidence.unavailable(jobId, progress, 0L);
                if (value.available() != 0) {
                    throw corrupt("Stream product metadata has trailing bytes");
                }
                return new StreamProductJobSnapshot(submission, jobId,
                        artifact, status, progress, cancelId, cancelStatus,
                        checkpoints, savepoint, recovery, rescales, evidence);
            }
        } catch (StreamProductControlException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw corrupt("Stream product metadata is truncated");
        } catch (IOException | RuntimeException malformed) {
            if (malformed instanceof StreamProductControlException) {
                throw (StreamProductControlException) malformed;
            }
            throw corrupt("Stream product metadata is malformed", malformed);
        }
    }

    private PlanArtifact artifact(String artifactId) {
        byte[] encoded = artifacts.get(artifactId);
        if (encoded == null) {
            throw corrupt("referenced Stream Artifact is unavailable");
        }
        PlanArtifact artifact = PlanArtifactCodec.decode(encoded);
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        if (!artifactId.equals(artifact.artifactId())) {
            throw corrupt("referenced Stream Artifact identity differs");
        }
        return artifact;
    }

    private static void writeProgress(DataOutputStream output,
            StreamJobProgress progress, boolean includesBacklog)
            throws IOException {
        output.writeInt(progress.attempt());
        output.writeLong(progress.completedCheckpoints());
        output.writeLong(progress.operatorWatermark());
        output.writeInt(progress.cdcPhase().wireId());
        output.writeInt(progress.restarts());
        output.writeLong(progress.recordsRead());
        output.writeLong(progress.recordsWritten());
        output.writeLong(progress.bytesRead());
        output.writeLong(progress.bytesWritten());
        if (includesBacklog) {
            output.writeLong(progress.backlogEvents());
            output.writeLong(progress.backlogBytes());
        }
        output.writeInt(progress.mailboxHighWaterEvents());
        output.writeLong(progress.channelHighWaterBytes());
        output.writeLong(progress.managedStateBytes());
        output.writeInt(progress.timerHighWaterCount());
        output.writeInt(progress.windowHighWaterCount());
        output.writeLong(progress.backpressuredMillis());
    }

    private static StreamJobProgress readProgress(DataInputStream input,
            boolean includesBacklog) throws IOException {
        int attempt = input.readInt();
        long completedCheckpoints = input.readLong();
        long operatorWatermark = input.readLong();
        StreamCdcPhase phase = cdcPhase(input.readInt());
        int restarts = input.readInt();
        long recordsRead = input.readLong();
        long recordsWritten = input.readLong();
        long bytesRead = input.readLong();
        long bytesWritten = input.readLong();
        long backlogEvents = includesBacklog ? input.readLong() : 0L;
        long backlogBytes = includesBacklog ? input.readLong() : 0L;
        return new StreamJobProgress(attempt, completedCheckpoints,
                operatorWatermark, phase, restarts, recordsRead,
                recordsWritten, bytesRead, bytesWritten, backlogEvents,
                backlogBytes, input.readInt(), input.readLong(),
                input.readLong(), input.readInt(), input.readInt(),
                input.readLong());
    }

    private static void writeEvidence(DataOutputStream output,
            StreamJobEvidence evidence) throws IOException {
        output.writeBoolean(evidence.available());
        output.writeLong(evidence.observedAtEpochMillis());
        if (!evidence.available()) return;
        writeText(output, evidence.runId());
        writeText(output, evidence.attemptId());
        output.writeInt(evidence.attemptOrdinal());
        output.writeLong(evidence.fenceToken());
        output.writeLong(evidence.coordinatorTerm());
        output.writeLong(evidence.assignmentGeneration());
        output.writeBoolean(evidence.latestCompletedCheckpointId() != null);
        if (evidence.latestCompletedCheckpointId() != null) {
            writeText(output, evidence.latestCompletedCheckpointId());
            output.writeLong(evidence.latestCompletedCheckpointSequence());
            output.writeLong(evidence.latestCheckpointDurationMillis());
            output.writeLong(evidence.latestCheckpointBytes());
        }
        writeProgress(output, evidence.attemptProgress(), true);
        writeProgress(output, evidence.lifetimeProgress(), true);
        output.writeInt(evidence.placementTaskCount());
        output.writeInt(evidence.reportedTaskCount());
    }

    private static StreamJobEvidence readEvidence(DataInputStream input,
            StreamJobId jobId, StreamJobProgress fallbackProgress)
            throws IOException {
        boolean available = input.readBoolean();
        long observedAtEpochMillis = input.readLong();
        if (!available) {
            return StreamJobEvidence.unavailable(jobId,
                    fallbackProgress, observedAtEpochMillis);
        }
        String runId = readText(input);
        String attemptId = readText(input);
        int attemptOrdinal = input.readInt();
        long fenceToken = input.readLong();
        long coordinatorTerm = input.readLong();
        long assignmentGeneration = input.readLong();
        String checkpointId = null;
        long checkpointSequence = 0L;
        long checkpointDurationMillis = 0L;
        long checkpointBytes = 0L;
        if (input.readBoolean()) {
            checkpointId = readText(input);
            checkpointSequence = input.readLong();
            checkpointDurationMillis = input.readLong();
            checkpointBytes = input.readLong();
        }
        StreamJobProgress attemptProgress = readProgress(input, true);
        StreamJobProgress lifetimeProgress = readProgress(input, true);
        return new StreamJobEvidence(jobId, runId, attemptId,
                attemptOrdinal, fenceToken, coordinatorTerm,
                assignmentGeneration, checkpointId, checkpointSequence,
                checkpointDurationMillis, checkpointBytes,
                attemptProgress, lifetimeProgress, input.readInt(),
                input.readInt(), observedAtEpochMillis);
    }

    private static int operationWireId(StreamOperationStatus status) {
        switch (status) {
            case ACCEPTED: return 1;
            case RUNNING: return 2;
            case SUCCEEDED: return 3;
            case FAILED: return 4;
            default: throw new AssertionError(status);
        }
    }

    private static StreamOperationStatus operationStatus(int wireId) {
        switch (wireId) {
            case 1: return StreamOperationStatus.ACCEPTED;
            case 2: return StreamOperationStatus.RUNNING;
            case 3: return StreamOperationStatus.SUCCEEDED;
            case 4: return StreamOperationStatus.FAILED;
            default: throw corrupt("unknown Stream operation status");
        }
    }

    private static StreamJobStatus jobStatus(int wireId) {
        for (StreamJobStatus status : StreamJobStatus.values()) {
            if (status.wireId() == wireId) return status;
        }
        throw corrupt("unknown Stream job status");
    }

    private static StreamCdcPhase cdcPhase(int wireId) {
        for (StreamCdcPhase phase : StreamCdcPhase.values()) {
            if (phase.wireId() == wireId) return phase;
        }
        throw corrupt("unknown Stream CDC phase");
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > MAX_TEXT_BYTES) {
            throw corrupt("Stream product metadata text exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > MAX_TEXT_BYTES) {
            throw corrupt("Stream product metadata text exceeds its bound");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            String value = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            if (!MessageDigest.isEqual(bytes,
                    value.getBytes(StandardCharsets.UTF_8))) {
                throw corrupt("Stream product metadata text is noncanonical");
            }
            return value;
        } catch (java.nio.charset.CharacterCodingException malformed) {
            throw corrupt("Stream product metadata text is invalid UTF-8");
        }
    }

    private static byte[] bounded(byte[] value) {
        if (value == null || value.length < MAGIC.length + 8 + DIGEST_BYTES
                || value.length > MAX_METADATA_BYTES) {
            throw corrupt("Stream product metadata exceeds its bound");
        }
        return value.clone();
    }

    private String path(StreamJobId jobId) {
        return root + '/' + key(jobId);
    }

    private static String key(StreamJobId jobId) {
        requireJobId(jobId);
        return sha256Hex(jobId.value().getBytes(StandardCharsets.UTF_8));
    }

    private static void requireJobId(StreamJobId jobId) {
        if (jobId == null) {
            throw new IllegalArgumentException("Stream job ID is required");
        }
    }

    private static void requireIdentity(StreamJobId expected,
            StreamProductJobSnapshot snapshot) {
        if (snapshot == null || !expected.equals(snapshot.jobId())) {
            throw new IllegalStateException(
                    "Stream product mutation changed its identity");
        }
    }

    private void ensurePath(String path) {
        try {
            if (client.checkExists().forPath(path) == null) {
                try {
                    client.create().creatingParentsIfNeeded()
                            .forPath(path);
                } catch (KeeperException.NodeExistsException race) {
                    // Concurrent constructor established the same root.
                }
            }
        } catch (Exception failure) {
            throw io("ZooKeeper Stream product root is unavailable", failure);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String sha256Hex(byte[] value) {
        byte[] digest = sha256(value);
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[digest.length * 2];
        for (int index = 0; index < digest.length; index++) {
            int current = digest[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0xf];
        }
        return new String(result);
    }

    private static StreamProductControlException corrupt(String message) {
        return new StreamProductControlException(
                StreamProductControlException.Code.INVALID_FRAME, message);
    }

    private static StreamProductControlException corrupt(
            String message, Throwable cause) {
        return new StreamProductControlException(
                StreamProductControlException.Code.INVALID_FRAME,
                message, cause);
    }

    private static IllegalStateException io(String message, Throwable cause) {
        return new IllegalStateException(message, cause);
    }
}
