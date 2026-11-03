package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.util.Arrays;
import java.util.Objects;

/** Exact retained receipt for the last normal or backfill joint commit. */
public final class IncrementalCommitReceipt {
    private final String intervalId;
    private final String capturedTargetReceiptDigest;
    private final IncrementalRunMode mode;
    private final long expectedSequence;
    private final IncrementalFrontier from;
    private final IncrementalFrontier to;
    private final byte[] definitionFingerprint;
    private final String artifactId;
    private final long claimedRevision;
    private final RunAttemptFence fence;
    private final String publicationId;
    private final long generation;
    private final byte[] manifestHash;
    private final byte[] commitDecisionDigest;
    private final String commandId;
    private final byte[] commandDigest;

    private IncrementalCommitReceipt(
            String intervalId, String capturedTargetReceiptDigest,
            IncrementalRunMode mode, long expectedSequence,
            IncrementalFrontier from, IncrementalFrontier to,
            byte[] definitionFingerprint, String artifactId, long claimedRevision,
            RunAttemptFence fence,
            String publicationId, long generation, byte[] manifestHash,
            byte[] commitDecisionDigest, String commandId, byte[] commandDigest) {
        this.intervalId = IncrementalAuthorityValue.hexDigest(intervalId, "interval id");
        this.capturedTargetReceiptDigest = IncrementalAuthorityValue.hexDigest(
                capturedTargetReceiptDigest, "captured target receipt digest");
        if (mode == null || from == null || to == null || fence == null) {
            throw new IllegalArgumentException("receipt mode, bounds and fence are required");
        }
        if (expectedSequence < 0L) {
            throw new IllegalArgumentException("receipt sequence must be non-negative");
        }
        this.mode = mode;
        this.expectedSequence = expectedSequence;
        this.from = from;
        this.to = to;
        this.definitionFingerprint = IncrementalAuthorityValue.digest(
                definitionFingerprint, "definition fingerprint");
        this.artifactId = IncrementalAuthorityValue.text(artifactId, "artifact id");
        if (claimedRevision < 0L) {
            throw new IllegalArgumentException("receipt claimed revision must be non-negative");
        }
        if (mode != IncrementalRunMode.BACKFILL_V1 && expectedSequence != claimedRevision) {
            throw new IllegalArgumentException("normal receipt revision binding is invalid");
        }
        validateBounds(mode, from, to);
        this.claimedRevision = claimedRevision;
        this.fence = fence;
        this.publicationId = IncrementalAuthorityValue.text(publicationId, "publication id");
        if (generation <= 0L) throw new IllegalArgumentException("generation must be positive");
        this.generation = generation;
        this.manifestHash = IncrementalAuthorityValue.digest(manifestHash, "manifest hash");
        this.commitDecisionDigest = IncrementalAuthorityValue.digest(
                commitDecisionDigest, "commit decision digest");
        this.commandId = IncrementalAuthorityValue.text(commandId, "command id");
        this.commandDigest = IncrementalAuthorityValue.digest(commandDigest, "command digest");
    }

    public static IncrementalCommitReceipt committed(
            IncrementalActiveInterval active, String publicationId, long generation,
            byte[] manifestHash, String commandId, byte[] commandDigest) {
        if (active == null || active.phase() != IncrementalAttemptCommitPhase.COMMIT_DECIDED) {
            throw new IllegalArgumentException("commit receipt requires a commit-decided active attempt");
        }
        CapturedTargetReceipt captured = active.capturedTarget();
        return new IncrementalCommitReceipt(
                active.identity().intervalId(),
                active.identity().capturedTargetReceiptDigest(), captured.mode(),
                captured.expectedSequence(), captured.from(), captured.target(),
                captured.definitionDigest(), active.artifactId(), active.claimedRevision(),
                active.fence(),
                publicationId, generation, manifestHash, active.commitDecisionDigest(),
                commandId, commandDigest);
    }

    static IncrementalCommitReceipt decoded(
            String intervalId, String capturedTargetReceiptDigest,
            IncrementalRunMode mode, long expectedSequence,
            IncrementalFrontier from, IncrementalFrontier to,
            byte[] definitionFingerprint, String artifactId, long claimedRevision,
            RunAttemptFence fence,
            String publicationId, long generation, byte[] manifestHash,
            byte[] commitDecisionDigest, String commandId, byte[] commandDigest) {
        return new IncrementalCommitReceipt(
                intervalId, capturedTargetReceiptDigest, mode, expectedSequence, from, to,
                definitionFingerprint, artifactId, claimedRevision, fence,
                publicationId, generation,
                manifestHash, commitDecisionDigest, commandId, commandDigest);
    }

    public String intervalId() { return intervalId; }
    public String capturedTargetReceiptDigest() { return capturedTargetReceiptDigest; }
    public IncrementalRunMode mode() { return mode; }
    public long expectedSequence() { return expectedSequence; }
    public IncrementalFrontier from() { return from; }
    public IncrementalFrontier to() { return to; }
    public byte[] definitionFingerprint() { return definitionFingerprint.clone(); }
    public String artifactId() { return artifactId; }
    public long claimedRevision() { return claimedRevision; }
    public RunAttemptFence fence() { return fence; }
    public String publicationId() { return publicationId; }
    public long generation() { return generation; }
    public byte[] manifestHash() { return manifestHash.clone(); }
    public byte[] commitDecisionDigest() { return commitDecisionDigest.clone(); }
    public String commandId() { return commandId; }
    public byte[] commandDigest() { return commandDigest.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalCommitReceipt)) return false;
        IncrementalCommitReceipt that = (IncrementalCommitReceipt) other;
        return expectedSequence == that.expectedSequence
                && claimedRevision == that.claimedRevision && generation == that.generation
                && intervalId.equals(that.intervalId)
                && capturedTargetReceiptDigest.equals(that.capturedTargetReceiptDigest)
                && mode == that.mode && from.equals(that.from) && to.equals(that.to)
                && artifactId.equals(that.artifactId) && fence.equals(that.fence)
                && publicationId.equals(that.publicationId)
                && commandId.equals(that.commandId)
                && Arrays.equals(definitionFingerprint, that.definitionFingerprint)
                && Arrays.equals(manifestHash, that.manifestHash)
                && Arrays.equals(commitDecisionDigest, that.commitDecisionDigest)
                && Arrays.equals(commandDigest, that.commandDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(intervalId, capturedTargetReceiptDigest, mode,
                expectedSequence, from, to, artifactId, claimedRevision, fence, publicationId,
                generation, commandId);
        result = 31 * result + Arrays.hashCode(definitionFingerprint);
        result = 31 * result + Arrays.hashCode(manifestHash);
        result = 31 * result + Arrays.hashCode(commitDecisionDigest);
        return 31 * result + Arrays.hashCode(commandDigest);
    }

    private static void validateBounds(
            IncrementalRunMode mode, IncrementalFrontier from, IncrementalFrontier to) {
        if (mode == IncrementalRunMode.SNAPSHOT_V1) {
            IncrementalFrontierOrder.isSnapshotTransition(from, to, to.kind());
            return;
        }
        if (from.kind() == IncrementalFrontier.Kind.BEGIN_V1 || from.kind() != to.kind()
                || IncrementalFrontierOrder.compareConcrete(from, to) >= 0) {
            throw new IllegalArgumentException("commit receipt interval must have positive width");
        }
    }
}
