package io.rowweave.run;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

/** Attempt-scoped immutable physical Stage assignment. */
public final class StageAssignmentRecord {
    private final String stageExecutionId;
    private final int physicalOrdinal;
    private final long runtimeStageId;
    private final int logicalStage;
    private final int instanceIndex;
    private final StageAssignmentRole role;
    private final String workerId;
    private final String host;
    private final int rpcPort;
    private final int dataPort;
    private final String workerIncarnationId;
    private final int sourceSplitCount;
    private final byte[] sourceOwnershipDigest;

    private StageAssignmentRecord(String stageExecutionId, int physicalOrdinal,
                                  long runtimeStageId, int logicalStage, int instanceIndex,
                                  StageAssignmentRole role, String workerId, String host,
                                  int rpcPort, int dataPort, String workerIncarnationId,
                                  int sourceSplitCount, byte[] sourceOwnershipDigest) {
        this.stageExecutionId = requireExecutionId(stageExecutionId);
        if (physicalOrdinal < 0) throw new IllegalArgumentException("physicalOrdinal must be >= 0");
        if (runtimeStageId <= 0L) throw new IllegalArgumentException("runtimeStageId must be positive");
        if (logicalStage < 0) throw new IllegalArgumentException("logicalStage must be >= 0");
        if (instanceIndex < 0) throw new IllegalArgumentException("instanceIndex must be >= 0");
        if (role == null) throw new IllegalArgumentException("role is required");
        this.workerId = RunText.required(workerId, "workerId", RunText.MAX_ID_BYTES);
        this.host = RunText.required(host, "host", RunText.MAX_ID_BYTES);
        if (rpcPort < 1 || rpcPort > 65535) {
            throw new IllegalArgumentException("rpcPort must be in [1,65535]");
        }
        if (dataPort != -1 && (dataPort < 1 || dataPort > 65535)) {
            throw new IllegalArgumentException("dataPort must be -1 or in [1,65535]");
        }
        this.workerIncarnationId = RunText.required(
                workerIncarnationId, "workerIncarnationId", RunText.MAX_ID_BYTES);
        boolean sourceOwner = ownsSource(role);
        if (sourceOwnershipDigest == null) {
            if (sourceSplitCount != 0) {
                throw new IllegalArgumentException(
                        "sourceSplitCount requires sourceOwnershipDigest");
            }
        } else {
            if (!sourceOwner) {
                throw new IllegalArgumentException(
                        "source ownership is invalid for role " + role);
            }
            if (sourceSplitCount <= 0
                    || sourceSplitCount > StageAssignmentManifest.MAX_ASSIGNMENTS) {
                throw new IllegalArgumentException("sourceSplitCount is out of bounds");
            }
            if (sourceOwnershipDigest.length != 32) {
                throw new IllegalArgumentException("sourceOwnershipDigest must be SHA-256");
            }
        }
        this.physicalOrdinal = physicalOrdinal;
        this.runtimeStageId = runtimeStageId;
        this.logicalStage = logicalStage;
        this.instanceIndex = instanceIndex;
        this.role = role;
        this.rpcPort = rpcPort;
        this.dataPort = dataPort;
        this.sourceSplitCount = sourceSplitCount;
        this.sourceOwnershipDigest = sourceOwnershipDigest == null
                ? null : sourceOwnershipDigest.clone();
    }

    public static StageAssignmentRecord create(RunAttemptFence fence, long generation,
                                               int physicalOrdinal, long runtimeStageId,
                                               int logicalStage, int instanceIndex,
                                               StageAssignmentRole role, String workerId,
                                               String host, int rpcPort, int dataPort,
                                               String workerIncarnationId) {
        return new StageAssignmentRecord(deriveExecutionId(fence, generation, physicalOrdinal),
                physicalOrdinal, runtimeStageId, logicalStage, instanceIndex, role,
                workerId, host, rpcPort, dataPort, workerIncarnationId, 0, null);
    }

    public static StageAssignmentRecord create(RunAttemptFence fence, long generation,
                                               int physicalOrdinal, long runtimeStageId,
                                               int logicalStage, int instanceIndex,
                                               StageAssignmentRole role, String workerId,
                                               String host, int rpcPort, int dataPort,
                                               String workerIncarnationId,
                                               SourceSplitAssignment sourceAssignment) {
        if (sourceAssignment == null) {
            throw new IllegalArgumentException("sourceAssignment is required");
        }
        if (!ownsSource(role)) {
            throw new IllegalArgumentException("source ownership is invalid for role " + role);
        }
        if (sourceAssignment.logicalStage() != logicalStage
                || sourceAssignment.instanceIndex() != instanceIndex) {
            throw new IllegalArgumentException(
                    "source assignment physical identity mismatch");
        }
        return new StageAssignmentRecord(deriveExecutionId(fence, generation, physicalOrdinal),
                physicalOrdinal, runtimeStageId, logicalStage, instanceIndex, role,
                workerId, host, rpcPort, dataPort, workerIncarnationId,
                sourceAssignment.sourceSplitCount(), sourceAssignment.ownershipDigest());
    }

    static StageAssignmentRecord decoded(RunAttemptFence fence, long generation,
                                         String stageExecutionId, int physicalOrdinal,
                                         long runtimeStageId, int logicalStage, int instanceIndex,
                                         StageAssignmentRole role, String workerId, String host,
                                         int rpcPort, int dataPort, String workerIncarnationId) {
        String expected = deriveExecutionId(fence, generation, physicalOrdinal);
        if (!expected.equals(stageExecutionId)) {
            throw new IllegalArgumentException("stageExecutionId does not match attempt identity");
        }
        return new StageAssignmentRecord(stageExecutionId, physicalOrdinal, runtimeStageId,
                logicalStage, instanceIndex, role, workerId, host, rpcPort, dataPort,
                workerIncarnationId, 0, null);
    }

    static StageAssignmentRecord decoded(RunAttemptFence fence, long generation,
                                         String stageExecutionId, int physicalOrdinal,
                                         long runtimeStageId, int logicalStage, int instanceIndex,
                                         StageAssignmentRole role, String workerId, String host,
                                         int rpcPort, int dataPort, String workerIncarnationId,
                                         int sourceSplitCount, byte[] sourceOwnershipDigest) {
        String expected = deriveExecutionId(fence, generation, physicalOrdinal);
        if (!expected.equals(stageExecutionId)) {
            throw new IllegalArgumentException("stageExecutionId does not match attempt identity");
        }
        return new StageAssignmentRecord(stageExecutionId, physicalOrdinal, runtimeStageId,
                logicalStage, instanceIndex, role, workerId, host, rpcPort, dataPort,
                workerIncarnationId, sourceSplitCount, sourceOwnershipDigest);
    }

    static boolean ownsSource(StageAssignmentRole role) {
        return role == StageAssignmentRole.SOURCE_STAGE
                || role == StageAssignmentRole.WHOLE_GRAPH;
    }

    static String deriveExecutionId(RunAttemptFence fence, long generation, int physicalOrdinal) {
        if (fence == null) throw new IllegalArgumentException("fence is required");
        if (generation <= 0L) throw new IllegalArgumentException("generation must be positive");
        if (physicalOrdinal < 0) throw new IllegalArgumentException("physicalOrdinal must be >= 0");
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(600);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            RunWire.writeText(out, fence.runId());
            RunWire.writeText(out, fence.attemptId());
            out.writeLong(fence.fenceToken());
            out.writeLong(generation);
            out.writeInt(physicalOrdinal);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return hex(sha256(bytes.toByteArray()));
    }

    private static String requireExecutionId(String value) {
        if (value == null || value.length() != 64) {
            throw new IllegalArgumentException("stageExecutionId must be a full SHA-256 hex string");
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
                throw new IllegalArgumentException("stageExecutionId must be lowercase SHA-256 hex");
            }
        }
        return value;
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String hex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        char[] alphabet = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xff;
            out[i * 2] = alphabet[value >>> 4];
            out[i * 2 + 1] = alphabet[value & 0xf];
        }
        return new String(out);
    }

    public String stageExecutionId() { return stageExecutionId; }
    public int physicalOrdinal() { return physicalOrdinal; }
    public long runtimeStageId() { return runtimeStageId; }
    public int logicalStage() { return logicalStage; }
    public int instanceIndex() { return instanceIndex; }
    public StageAssignmentRole role() { return role; }
    public String workerId() { return workerId; }
    public String host() { return host; }
    public int rpcPort() { return rpcPort; }
    public int dataPort() { return dataPort; }
    public String workerIncarnationId() { return workerIncarnationId; }
    public int sourceSplitCount() { return sourceSplitCount; }
    public byte[] sourceOwnershipDigest() {
        return sourceOwnershipDigest == null ? null : sourceOwnershipDigest.clone();
    }
    public boolean legacySourceOwnership() {
        return ownsSource(role) && sourceOwnershipDigest == null;
    }
    public byte[] canonicalDigest() {
        return StageAssignmentManifestCodec.digestRecordFields(this);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof StageAssignmentRecord)) return false;
        StageAssignmentRecord that = (StageAssignmentRecord) other;
        return physicalOrdinal == that.physicalOrdinal
                && runtimeStageId == that.runtimeStageId
                && logicalStage == that.logicalStage
                && instanceIndex == that.instanceIndex
                && rpcPort == that.rpcPort
                && dataPort == that.dataPort
                && stageExecutionId.equals(that.stageExecutionId)
                && role == that.role
                && workerId.equals(that.workerId)
                && host.equals(that.host)
                && workerIncarnationId.equals(that.workerIncarnationId)
                && sourceSplitCount == that.sourceSplitCount
                && Arrays.equals(sourceOwnershipDigest, that.sourceOwnershipDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stageExecutionId, physicalOrdinal, runtimeStageId, logicalStage,
                instanceIndex, role, workerId, host, rpcPort, dataPort, workerIncarnationId,
                sourceSplitCount, Arrays.hashCode(sourceOwnershipDigest));
    }
}
