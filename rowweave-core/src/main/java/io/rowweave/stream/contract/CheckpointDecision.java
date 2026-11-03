package io.rowweave.stream.contract;

import java.util.Objects;

/** Immutable observation of the checkpoint reference state machine. */
public final class CheckpointDecision {
    public enum Status {
        COLLECTING(1), COMMIT_DECIDED(2), COMPLETED(3), ABORTED(4);

        private final int wireId;

        Status(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static Status fromWire(int wireId) {
            for (Status value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw StreamContractSupport.failure("UNKNOWN_CHECKPOINT_STATUS",
                    "checkpoint.decision.status", "unknown checkpoint status");
        }
    }

    private final CheckpointIdentity checkpointIdentity;
    private final Status status;
    private final String manifestDigest;

    CheckpointDecision(
            CheckpointIdentity checkpointIdentity, Status status,
            String manifestDigest) {
        if (checkpointIdentity == null || status == null) {
            throw new IllegalArgumentException("checkpoint identity and status are required");
        }
        boolean committed = status == Status.COMMIT_DECIDED || status == Status.COMPLETED;
        if (committed) {
            this.manifestDigest = StreamContractSupport.digest(
                    manifestDigest, "checkpoint.decision.manifestDigest");
        } else {
            if (manifestDigest != null) {
                throw StreamContractSupport.failure("INVALID_CHECKPOINT_DECISION",
                        "checkpoint.decision", "non-commit state cannot carry a manifest");
            }
            this.manifestDigest = null;
        }
        this.checkpointIdentity = checkpointIdentity;
        this.status = status;
    }

    /** Creates the inert abort proposal that an authority may publish. */
    public static CheckpointDecision aborted(CheckpointIdentity checkpoint) {
        return new CheckpointDecision(checkpoint, Status.ABORTED, null);
    }

    /** Creates a completion marker bound to an exact commit decision. */
    public static CheckpointDecision completed(
            CheckpointDecision commitDecision) {
        if (commitDecision == null
                || commitDecision.status != Status.COMMIT_DECIDED) {
            throw new IllegalArgumentException(
                    "completion requires a commit decision");
        }
        return new CheckpointDecision(
                commitDecision.checkpointIdentity, Status.COMPLETED,
                commitDecision.manifestDigest);
    }

    public CheckpointIdentity checkpointIdentity() { return checkpointIdentity; }
    public Status status() { return status; }
    public String manifestDigest() { return manifestDigest; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CheckpointDecision)) return false;
        CheckpointDecision that = (CheckpointDecision) other;
        return checkpointIdentity.equals(that.checkpointIdentity)
                && status == that.status
                && Objects.equals(manifestDigest, that.manifestDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(checkpointIdentity, status, manifestDigest);
    }
}
