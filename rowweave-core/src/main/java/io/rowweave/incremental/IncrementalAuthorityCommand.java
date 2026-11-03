package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.util.Arrays;
import java.util.Objects;

/** Canonical command identity; transitionDigest binds the typed mutation payload/proof. */
public final class IncrementalAuthorityCommand {
    private final IncrementalAuthorityCommandKind kind;
    private final String feedId;
    private final String commandId;
    private final long expectedRevision;
    private final long feedIncarnation;
    private final String intervalId;
    private final RunAttemptFence fence;
    private final byte[] transitionDigest;

    public IncrementalAuthorityCommand(
            IncrementalAuthorityCommandKind kind, String feedId, String commandId,
            long expectedRevision, long feedIncarnation, String intervalId,
            RunAttemptFence fence, byte[] transitionDigest) {
        if (kind == null) throw new IllegalArgumentException("command kind is required");
        if (expectedRevision < 0L || feedIncarnation <= 0L) {
            throw new IllegalArgumentException("command revision/incarnation is invalid");
        }
        this.kind = kind;
        this.feedId = IncrementalAuthorityValue.text(feedId, "command feed id");
        this.commandId = IncrementalAuthorityValue.text(commandId, "command id");
        this.expectedRevision = expectedRevision;
        this.feedIncarnation = feedIncarnation;
        this.intervalId = intervalId == null ? null
                : IncrementalAuthorityValue.hexDigest(intervalId, "command interval id");
        this.fence = fence;
        this.transitionDigest = IncrementalAuthorityValue.digest(
                transitionDigest, "command transition digest");
        validateShape();
    }

    private void validateShape() {
        boolean hasInterval = intervalId != null;
        boolean hasFence = fence != null;
        if (hasInterval != hasFence) {
            throw new IllegalArgumentException("command interval and fence must appear together");
        }
        if (kind == IncrementalAuthorityCommandKind.INITIALIZE && hasInterval) {
            throw new IllegalArgumentException("INITIALIZE cannot bind an interval");
        }
        if (kind != IncrementalAuthorityCommandKind.INITIALIZE
                && kind != IncrementalAuthorityCommandKind.MARK_CONFLICT
                && !hasInterval) {
            throw new IllegalArgumentException(kind + " requires interval and attempt fence");
        }
    }

    public IncrementalAuthorityCommandKind kind() { return kind; }
    public String feedId() { return feedId; }
    public String commandId() { return commandId; }
    public long expectedRevision() { return expectedRevision; }
    public long feedIncarnation() { return feedIncarnation; }
    public String intervalId() { return intervalId; }
    public RunAttemptFence fence() { return fence; }
    public byte[] transitionDigest() { return transitionDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalAuthorityCommand)) return false;
        IncrementalAuthorityCommand that = (IncrementalAuthorityCommand) other;
        return expectedRevision == that.expectedRevision
                && feedIncarnation == that.feedIncarnation && kind == that.kind
                && feedId.equals(that.feedId) && commandId.equals(that.commandId)
                && Objects.equals(intervalId, that.intervalId)
                && Objects.equals(fence, that.fence)
                && Arrays.equals(transitionDigest, that.transitionDigest);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(kind, feedId, commandId, expectedRevision,
                feedIncarnation, intervalId, fence) + Arrays.hashCode(transitionDigest);
    }
}
