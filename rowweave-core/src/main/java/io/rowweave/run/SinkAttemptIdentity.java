package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSinkDescriptorCodec;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

/** Immutable idempotency identity for one physical Sink assignment in one fenced attempt. */
public final class SinkAttemptIdentity {

    public static final int CURRENT_VERSION = 1;
    public static final int MAX_OPERATOR_ID_BYTES = 1024;
    private static final int DIGEST_BYTES = 32;

    private final int identityVersion;
    private final String artifactId;
    private final RunAttemptFence fence;
    private final long assignmentGeneration;
    private final String stageExecutionId;
    private final int logicalStage;
    private final int instanceIndex;
    private final String sinkOperatorId;
    private final BatchSinkDescriptor.CapabilityProfile capabilityProfile;
    private final String commitDomainId;
    private final byte[] descriptorDigest;
    private final byte[] identityDigest;

    private SinkAttemptIdentity(
            int identityVersion, String artifactId, RunAttemptFence fence,
            long assignmentGeneration, String stageExecutionId,
            int logicalStage, int instanceIndex, String sinkOperatorId,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile,
            String commitDomainId, byte[] descriptorDigest, byte[] identityDigest) {
        if (identityVersion != CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "sink attempt identityVersion must be " + CURRENT_VERSION);
        }
        this.artifactId = RunText.required(
                artifactId, "artifactId", RunText.MAX_ID_BYTES);
        if (fence == null) throw new IllegalArgumentException("fence is required");
        if (assignmentGeneration <= 0L) {
            throw new IllegalArgumentException("assignmentGeneration must be positive");
        }
        requireHexDigest(stageExecutionId, "stageExecutionId");
        if (logicalStage < 0 || instanceIndex < 0) {
            throw new IllegalArgumentException("sink physical stage identity is invalid");
        }
        this.sinkOperatorId = RunText.required(
                sinkOperatorId, "sinkOperatorId", MAX_OPERATOR_ID_BYTES);
        if (capabilityProfile == null) {
            throw new IllegalArgumentException("capabilityProfile is required");
        }
        this.commitDomainId = RunText.required(
                commitDomainId, "commitDomainId",
                BatchSinkDescriptor.MAX_COMMIT_DOMAIN_ID_BYTES);
        requireDigest(descriptorDigest, "descriptorDigest");
        requireDigest(identityDigest, "identityDigest");
        this.identityVersion = identityVersion;
        this.fence = fence;
        this.assignmentGeneration = assignmentGeneration;
        this.stageExecutionId = stageExecutionId;
        this.logicalStage = logicalStage;
        this.instanceIndex = instanceIndex;
        this.capabilityProfile = capabilityProfile;
        this.descriptorDigest = descriptorDigest.clone();
        this.identityDigest = identityDigest.clone();
        byte[] expected = deriveDigest(
                identityVersion, artifactId, fence, assignmentGeneration,
                stageExecutionId, logicalStage, instanceIndex, sinkOperatorId,
                capabilityProfile, commitDomainId, descriptorDigest);
        if (!MessageDigest.isEqual(expected, identityDigest)) {
            throw new IllegalArgumentException("sink attempt identity digest mismatch");
        }
    }

    public static SinkAttemptIdentity forAssignment(
            StageAssignmentManifest manifest, StageAssignmentRecord assignment,
            String sinkOperatorId, BatchSinkDescriptor descriptor) {
        if (manifest == null || assignment == null || descriptor == null) {
            throw new IllegalArgumentException(
                    "manifest, assignment and descriptor are required");
        }
        int ordinal = assignment.physicalOrdinal();
        if (ordinal < 0 || ordinal >= manifest.assignmentCount()
                || !manifest.assignment(ordinal).equals(assignment)) {
            throw new IllegalArgumentException("assignment does not belong to manifest");
        }
        if (assignment.role() != StageAssignmentRole.SINK
                && assignment.role() != StageAssignmentRole.WHOLE_GRAPH
                && assignment.role() != StageAssignmentRole.SYSTEM_CONTROL) {
            throw new IllegalArgumentException(
                    "sink attempt identity requires an output assignment");
        }
        byte[] descriptorDigest = BatchSinkDescriptorCodec.digest(descriptor);
        byte[] identityDigest = deriveDigest(
                CURRENT_VERSION, manifest.artifactId(), manifest.fence(),
                manifest.generation(), assignment.stageExecutionId(),
                assignment.logicalStage(), assignment.instanceIndex(),
                sinkOperatorId, descriptor.capabilityProfile(),
                descriptor.commitDomainId(), descriptorDigest);
        return new SinkAttemptIdentity(
                CURRENT_VERSION, manifest.artifactId(), manifest.fence(),
                manifest.generation(), assignment.stageExecutionId(),
                assignment.logicalStage(), assignment.instanceIndex(),
                sinkOperatorId, descriptor.capabilityProfile(),
                descriptor.commitDomainId(), descriptorDigest, identityDigest);
    }

    static SinkAttemptIdentity decoded(
            int identityVersion, String artifactId, RunAttemptFence fence,
            long assignmentGeneration, String stageExecutionId,
            int logicalStage, int instanceIndex, String sinkOperatorId,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile,
            String commitDomainId, byte[] descriptorDigest, byte[] identityDigest) {
        return new SinkAttemptIdentity(
                identityVersion, artifactId, fence, assignmentGeneration,
                stageExecutionId, logicalStage, instanceIndex, sinkOperatorId,
                capabilityProfile, commitDomainId, descriptorDigest, identityDigest);
    }

    private static byte[] deriveDigest(
            int identityVersion, String artifactId, RunAttemptFence fence,
            long assignmentGeneration, String stageExecutionId,
            int logicalStage, int instanceIndex, String sinkOperatorId,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile,
            String commitDomainId, byte[] descriptorDigest) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(identityVersion);
            RunWire.writeText(out, artifactId);
            RunWire.writeText(out, fence.runId());
            RunWire.writeText(out, fence.attemptId());
            out.writeLong(fence.fenceToken());
            out.writeLong(assignmentGeneration);
            RunWire.writeText(out, stageExecutionId);
            out.writeInt(logicalStage);
            out.writeInt(instanceIndex);
            RunWire.writeText(out, sinkOperatorId);
            out.writeByte(SinkAttemptIdentityCodec.profileId(capabilityProfile));
            RunWire.writeText(out, commitDomainId);
            out.write(descriptorDigest);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return sha256(bytes.toByteArray());
    }

    private static void requireDigest(byte[] value, String field) {
        if (value == null || value.length != DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
    }

    private static void requireHexDigest(String value, String field) {
        if (value == null || !value.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(field + " must be lowercase SHA-256 hex");
        }
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public int identityVersion() { return identityVersion; }
    public String artifactId() { return artifactId; }
    public RunAttemptFence fence() { return fence; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public String stageExecutionId() { return stageExecutionId; }
    public int logicalStage() { return logicalStage; }
    public int instanceIndex() { return instanceIndex; }
    public String sinkOperatorId() { return sinkOperatorId; }
    public BatchSinkDescriptor.CapabilityProfile capabilityProfile() {
        return capabilityProfile;
    }
    public String commitDomainId() { return commitDomainId; }
    public byte[] descriptorDigest() { return descriptorDigest.clone(); }
    public byte[] identityDigest() { return identityDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof SinkAttemptIdentity)) return false;
        SinkAttemptIdentity that = (SinkAttemptIdentity) other;
        return identityVersion == that.identityVersion
                && assignmentGeneration == that.assignmentGeneration
                && logicalStage == that.logicalStage
                && instanceIndex == that.instanceIndex
                && artifactId.equals(that.artifactId)
                && fence.equals(that.fence)
                && stageExecutionId.equals(that.stageExecutionId)
                && sinkOperatorId.equals(that.sinkOperatorId)
                && capabilityProfile == that.capabilityProfile
                && commitDomainId.equals(that.commitDomainId)
                && Arrays.equals(descriptorDigest, that.descriptorDigest)
                && Arrays.equals(identityDigest, that.identityDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(identityVersion, artifactId, fence,
                assignmentGeneration, stageExecutionId, logicalStage, instanceIndex,
                sinkOperatorId, capabilityProfile, commitDomainId);
        result = 31 * result + Arrays.hashCode(descriptorDigest);
        return 31 * result + Arrays.hashCode(identityDigest);
    }
}
