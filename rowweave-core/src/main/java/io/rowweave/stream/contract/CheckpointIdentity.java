package io.rowweave.stream.contract;

import java.util.Objects;

/** Immutable identity for one fenced checkpoint cut. */
public final class CheckpointIdentity {
    private final String definitionId;
    private final String artifactId;
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long coordinatorTerm;
    private final String coordinatorIncarnation;
    private final long checkpointSequence;
    private final String planDigest;
    private final String deploymentDigest;
    private final int dataProtocolVersion;
    private final int stateFormatVersion;

    public CheckpointIdentity(
            String definitionId, String artifactId, String runId, String attemptId,
            long fenceToken, long coordinatorTerm, String coordinatorIncarnation,
            long checkpointSequence, String planDigest, String deploymentDigest,
            int dataProtocolVersion, int stateFormatVersion) {
        this.definitionId = text(definitionId, "checkpoint.definitionId");
        this.artifactId = StreamContractSupport.digest(artifactId, "checkpoint.artifactId");
        this.runId = text(runId, "checkpoint.runId");
        this.attemptId = text(attemptId, "checkpoint.attemptId");
        this.coordinatorIncarnation = text(coordinatorIncarnation,
                "checkpoint.coordinatorIncarnation");
        this.planDigest = StreamContractSupport.digest(planDigest, "checkpoint.planDigest");
        this.deploymentDigest = StreamContractSupport.digest(
                deploymentDigest, "checkpoint.deploymentDigest");
        if (fenceToken <= 0L || coordinatorTerm <= 0L || checkpointSequence <= 0L
                || dataProtocolVersion <= 0 || stateFormatVersion <= 0) {
            throw StreamContractSupport.failure("INVALID_CHECKPOINT_IDENTITY",
                    "checkpoint", "checkpoint generations must be positive");
        }
        this.fenceToken = fenceToken;
        this.coordinatorTerm = coordinatorTerm;
        this.checkpointSequence = checkpointSequence;
        this.dataProtocolVersion = dataProtocolVersion;
        this.stateFormatVersion = stateFormatVersion;
    }

    private static String text(String value, String path) {
        return StreamContractSupport.text(value, path, 256);
    }

    public String definitionId() { return definitionId; }
    public String artifactId() { return artifactId; }
    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public long fenceToken() { return fenceToken; }
    public long coordinatorTerm() { return coordinatorTerm; }
    public String coordinatorIncarnation() { return coordinatorIncarnation; }
    public long checkpointSequence() { return checkpointSequence; }
    public String planDigest() { return planDigest; }
    public String deploymentDigest() { return deploymentDigest; }
    public int dataProtocolVersion() { return dataProtocolVersion; }
    public int stateFormatVersion() { return stateFormatVersion; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CheckpointIdentity)) return false;
        CheckpointIdentity that = (CheckpointIdentity) other;
        return fenceToken == that.fenceToken && coordinatorTerm == that.coordinatorTerm
                && checkpointSequence == that.checkpointSequence
                && dataProtocolVersion == that.dataProtocolVersion
                && stateFormatVersion == that.stateFormatVersion
                && definitionId.equals(that.definitionId) && artifactId.equals(that.artifactId)
                && runId.equals(that.runId) && attemptId.equals(that.attemptId)
                && coordinatorIncarnation.equals(that.coordinatorIncarnation)
                && planDigest.equals(that.planDigest)
                && deploymentDigest.equals(that.deploymentDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(definitionId, artifactId, runId, attemptId, fenceToken,
                coordinatorTerm, coordinatorIncarnation, checkpointSequence,
                planDigest, deploymentDigest, dataProtocolVersion, stateFormatVersion);
    }
}
