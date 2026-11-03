package io.rowweave.run;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Worker acceptance RPC carries this immutable token instead of trusting a runtime long id. */
public final class StageAssignmentToken {
    private final int protocolVersion;
    private final RunAttemptFence fence;
    private final long manifestGeneration;
    private final long acceptanceGeneration;
    private final int physicalOrdinal;
    private final String stageExecutionId;
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
    private final byte[] assignmentRecordDigest;
    private final byte[] tokenDigest;

    private StageAssignmentToken(
            int protocolVersion, RunAttemptFence fence, long manifestGeneration,
            long acceptanceGeneration, int physicalOrdinal, String stageExecutionId,
            long runtimeStageId, int logicalStage, int instanceIndex,
            StageAssignmentRole role, String workerId, String host,
            int rpcPort, int dataPort, String workerIncarnationId,
            int sourceSplitCount, byte[] sourceOwnershipDigest,
            byte[] assignmentRecordDigest, byte[] tokenDigest) {
        if (protocolVersion < 1 || protocolVersion > 2) {
            throw new IllegalArgumentException("unsupported token protocol version");
        }
        if (fence == null) throw new IllegalArgumentException("fence is required");
        if (manifestGeneration <= 0L || acceptanceGeneration <= 0L) {
            throw new IllegalArgumentException("token generations must be positive");
        }
        if (physicalOrdinal < 0) throw new IllegalArgumentException("physicalOrdinal must be >= 0");
        if (runtimeStageId <= 0L) throw new IllegalArgumentException("runtimeStageId must be positive");
        String expectedId = StageAssignmentRecord.deriveExecutionId(
                fence, manifestGeneration, physicalOrdinal);
        if (!expectedId.equals(stageExecutionId)) {
            throw new IllegalArgumentException("stageExecutionId does not match token identity");
        }
        this.workerId = RunText.required(workerId, "workerId", RunText.MAX_ID_BYTES);
        this.workerIncarnationId = RunText.required(
                workerIncarnationId, "workerIncarnationId", RunText.MAX_ID_BYTES);
        if (tokenDigest == null || tokenDigest.length != 32) {
            throw new IllegalArgumentException("tokenDigest must be SHA-256");
        }
        if (protocolVersion == 1) {
            if (role != null || logicalStage != -1 || instanceIndex != -1
                    || host != null || rpcPort != -1 || dataPort != -1
                    || sourceSplitCount != 0 || sourceOwnershipDigest != null
                    || assignmentRecordDigest != null) {
                throw new IllegalArgumentException("v1 token contains v2 assignment fields");
            }
        } else {
            if (logicalStage < 0 || instanceIndex < 0 || role == null) {
                throw new IllegalArgumentException("v2 token stage identity is required");
            }
            RunText.required(host, "host", RunText.MAX_ID_BYTES);
            if (rpcPort < 1 || rpcPort > 65535
                    || dataPort != -1 && (dataPort < 1 || dataPort > 65535)) {
                throw new IllegalArgumentException("v2 token endpoint is invalid");
            }
            if ((sourceOwnershipDigest == null) != (sourceSplitCount == 0)
                    || sourceOwnershipDigest != null && sourceOwnershipDigest.length != 32) {
                throw new IllegalArgumentException("v2 token source ownership is invalid");
            }
            if (assignmentRecordDigest == null || assignmentRecordDigest.length != 32) {
                throw new IllegalArgumentException("assignmentRecordDigest must be SHA-256");
            }
        }
        this.protocolVersion = protocolVersion;
        this.fence = fence;
        this.manifestGeneration = manifestGeneration;
        this.acceptanceGeneration = acceptanceGeneration;
        this.physicalOrdinal = physicalOrdinal;
        this.stageExecutionId = stageExecutionId;
        this.runtimeStageId = runtimeStageId;
        this.logicalStage = logicalStage;
        this.instanceIndex = instanceIndex;
        this.role = role;
        this.host = host;
        this.rpcPort = rpcPort;
        this.dataPort = dataPort;
        this.sourceSplitCount = sourceSplitCount;
        this.sourceOwnershipDigest = cloneOrNull(sourceOwnershipDigest);
        this.assignmentRecordDigest = cloneOrNull(assignmentRecordDigest);
        this.tokenDigest = tokenDigest.clone();
    }

    public static StageAssignmentToken forAssignment(StageAssignmentManifest manifest,
                                                     StageAssignmentRecord assignment,
                                                     long acceptanceGeneration) {
        if (manifest == null || assignment == null) {
            throw new IllegalArgumentException("manifest and assignment are required");
        }
        int ordinal = assignment.physicalOrdinal();
        if (ordinal >= manifest.assignmentCount()
                || manifest.assignment(ordinal) != assignment
                && !manifest.assignment(ordinal).equals(assignment)) {
            throw new IllegalArgumentException("assignment does not belong to manifest");
        }
        byte[] recordDigest = assignment.canonicalDigest();
        byte[] tokenDigest = StageAssignmentManifestCodec.digestTokenFieldsV2(
                manifest.fence(), manifest.generation(), acceptanceGeneration,
                assignment, recordDigest);
        return new StageAssignmentToken(2, manifest.fence(), manifest.generation(),
                acceptanceGeneration, assignment.physicalOrdinal(),
                assignment.stageExecutionId(), assignment.runtimeStageId(),
                assignment.logicalStage(), assignment.instanceIndex(), assignment.role(),
                assignment.workerId(), assignment.host(), assignment.rpcPort(),
                assignment.dataPort(), assignment.workerIncarnationId(),
                assignment.sourceSplitCount(), assignment.sourceOwnershipDigest(),
                recordDigest, tokenDigest);
    }

    static StageAssignmentToken decodedV1(
            RunAttemptFence fence, long manifestGeneration, long acceptanceGeneration,
            int physicalOrdinal, String stageExecutionId, long runtimeStageId,
            String workerId, String workerIncarnationId, byte[] tokenDigest) {
        StageAssignmentToken token = new StageAssignmentToken(
                1, fence, manifestGeneration, acceptanceGeneration,
                physicalOrdinal, stageExecutionId, runtimeStageId,
                -1, -1, null, workerId, null, -1, -1, workerIncarnationId,
                0, null, null, tokenDigest);
        byte[] expected = StageAssignmentManifestCodec.digestTokenFieldsV1(
                fence, manifestGeneration, acceptanceGeneration, physicalOrdinal,
                stageExecutionId, runtimeStageId, workerId, workerIncarnationId);
        if (!MessageDigest.isEqual(expected, tokenDigest)) {
            throw new IllegalArgumentException("stage assignment token digest mismatch");
        }
        return token;
    }

    static StageAssignmentToken decodedV2(
            RunAttemptFence fence, long manifestGeneration, long acceptanceGeneration,
            int physicalOrdinal, String stageExecutionId, long runtimeStageId,
            int logicalStage, int instanceIndex, StageAssignmentRole role,
            String workerId, String host, int rpcPort, int dataPort,
            String workerIncarnationId, int sourceSplitCount,
            byte[] sourceOwnershipDigest, byte[] assignmentRecordDigest,
            byte[] tokenDigest) {
        StageAssignmentToken token = new StageAssignmentToken(
                2, fence, manifestGeneration, acceptanceGeneration,
                physicalOrdinal, stageExecutionId, runtimeStageId,
                logicalStage, instanceIndex, role, workerId, host, rpcPort, dataPort,
                workerIncarnationId, sourceSplitCount, sourceOwnershipDigest,
                assignmentRecordDigest, tokenDigest);
        byte[] expectedRecord = StageAssignmentManifestCodec.digestRecordFields(
                stageExecutionId, physicalOrdinal, runtimeStageId, logicalStage,
                instanceIndex, role, workerId, host, rpcPort, dataPort,
                workerIncarnationId, sourceSplitCount, sourceOwnershipDigest);
        if (!MessageDigest.isEqual(expectedRecord, assignmentRecordDigest)) {
            throw new IllegalArgumentException("assignment record digest mismatch");
        }
        byte[] expectedToken = StageAssignmentManifestCodec.digestTokenFieldsV2(token);
        if (!MessageDigest.isEqual(expectedToken, tokenDigest)) {
            throw new IllegalArgumentException("stage assignment token digest mismatch");
        }
        return token;
    }

    private static byte[] cloneOrNull(byte[] value) {
        return value == null ? null : value.clone();
    }

    public int protocolVersion() { return protocolVersion; }
    public RunAttemptFence fence() { return fence; }
    public long manifestGeneration() { return manifestGeneration; }
    public long acceptanceGeneration() { return acceptanceGeneration; }
    public int physicalOrdinal() { return physicalOrdinal; }
    public String stageExecutionId() { return stageExecutionId; }
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
    public byte[] sourceOwnershipDigest() { return cloneOrNull(sourceOwnershipDigest); }
    public boolean hasAssignmentRecordDigest() { return assignmentRecordDigest != null; }
    public byte[] assignmentRecordDigest() { return cloneOrNull(assignmentRecordDigest); }
    public byte[] tokenDigest() { return tokenDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof StageAssignmentToken)) return false;
        StageAssignmentToken that = (StageAssignmentToken) other;
        return protocolVersion == that.protocolVersion
                && manifestGeneration == that.manifestGeneration
                && acceptanceGeneration == that.acceptanceGeneration
                && physicalOrdinal == that.physicalOrdinal
                && runtimeStageId == that.runtimeStageId
                && logicalStage == that.logicalStage
                && instanceIndex == that.instanceIndex
                && rpcPort == that.rpcPort
                && dataPort == that.dataPort
                && sourceSplitCount == that.sourceSplitCount
                && fence.equals(that.fence)
                && stageExecutionId.equals(that.stageExecutionId)
                && role == that.role
                && workerId.equals(that.workerId)
                && Objects.equals(host, that.host)
                && workerIncarnationId.equals(that.workerIncarnationId)
                && Arrays.equals(sourceOwnershipDigest, that.sourceOwnershipDigest)
                && Arrays.equals(assignmentRecordDigest, that.assignmentRecordDigest)
                && MessageDigest.isEqual(tokenDigest, that.tokenDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(protocolVersion, fence, manifestGeneration,
                acceptanceGeneration, physicalOrdinal, stageExecutionId, runtimeStageId,
                logicalStage, instanceIndex, role, workerId, host, rpcPort, dataPort,
                workerIncarnationId, sourceSplitCount);
        result = 31 * result + Arrays.hashCode(sourceOwnershipDigest);
        result = 31 * result + Arrays.hashCode(assignmentRecordDigest);
        return 31 * result + Arrays.hashCode(tokenDigest);
    }
}
