package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

import java.security.MessageDigest;
import java.util.Arrays;

/** Immutable companion metadata published inside the same visible Manifest. */
public final class IncrementalControlMember {
    private final String feedId;
    private final long feedIncarnation;
    private final byte[] definitionDigest;
    private final String intervalId;
    private final IncrementalRunMode mode;
    private final byte[] capturedTargetFrame;
    private final String artifactId;
    private final RunAttemptFence fence;
    private final String publicationId;
    private final long generation;
    private final byte[] planDescriptorFrame;
    private final byte[] dataMembersDigest;
    private final byte[] decisionInputDigest;

    IncrementalControlMember(
            String feedId, long feedIncarnation, byte[] definitionDigest,
            String intervalId, IncrementalRunMode mode,
            byte[] capturedTargetFrame, String artifactId,
            RunAttemptFence fence, String publicationId, long generation,
            byte[] planDescriptorFrame, byte[] dataMembersDigest,
            byte[] decisionInputDigest) {
        this.feedId = IncrementalAuthorityValue.text(feedId, "feed id");
        if (feedIncarnation <= 0L || mode == null || fence == null
                || generation <= 0L) {
            throw new IllegalArgumentException(
                    "incremental control identity is invalid");
        }
        this.feedIncarnation = feedIncarnation;
        this.definitionDigest = IncrementalAuthorityValue.digest(
                definitionDigest, "definition digest");
        this.intervalId = IncrementalAuthorityValue.text(
                intervalId, "interval id");
        this.mode = mode;
        CapturedTargetReceipt captured =
                CapturedTargetReceiptCodec.decode(capturedTargetFrame);
        if (captured.feedIncarnation() != feedIncarnation
                || captured.mode() != mode
                || !MessageDigest.isEqual(
                captured.definitionDigest(), definitionDigest)
                || !IncrementalIntervalIdentity.from(captured).intervalId()
                .equals(intervalId)
                || !fence.runId().equals(
                IncrementalIntervalIdentity.from(captured).candidateRunId())) {
            throw new IllegalArgumentException(
                    "incremental control capture identity differs");
        }
        this.capturedTargetFrame = capturedTargetFrame.clone();
        this.artifactId = IncrementalAuthorityValue.text(
                artifactId, "artifact id");
        this.fence = fence;
        this.publicationId = IncrementalAuthorityValue.text(
                publicationId, "publication id");
        this.generation = generation;
        IncrementalPlanDescriptorCodec.decode(planDescriptorFrame);
        this.planDescriptorFrame = planDescriptorFrame.clone();
        this.dataMembersDigest = IncrementalAuthorityValue.digest(
                dataMembersDigest, "data members digest");
        this.decisionInputDigest = IncrementalAuthorityValue.digest(
                decisionInputDigest, "decision input digest");
    }

    public String feedId() { return feedId; }
    public long feedIncarnation() { return feedIncarnation; }
    public byte[] definitionDigest() { return definitionDigest.clone(); }
    public String intervalId() { return intervalId; }
    public IncrementalRunMode mode() { return mode; }
    public CapturedTargetReceipt capturedTarget() {
        return CapturedTargetReceiptCodec.decode(capturedTargetFrame);
    }
    byte[] capturedTargetFrame() { return capturedTargetFrame.clone(); }
    public String artifactId() { return artifactId; }
    public RunAttemptFence fence() { return fence; }
    public String runId() { return fence.runId(); }
    public String publicationId() { return publicationId; }
    public long generation() { return generation; }
    public IncrementalPlanDescriptor planDescriptor() {
        return IncrementalPlanDescriptorCodec.decode(planDescriptorFrame);
    }
    byte[] planDescriptorFrame() { return planDescriptorFrame.clone(); }
    public byte[] dataMembersDigest() { return dataMembersDigest.clone(); }
    public byte[] decisionInputDigest() { return decisionInputDigest.clone(); }

    @Override public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalControlMember)) return false;
        IncrementalControlMember that = (IncrementalControlMember) other;
        return feedIncarnation == that.feedIncarnation
                && generation == that.generation
                && feedId.equals(that.feedId)
                && intervalId.equals(that.intervalId)
                && mode == that.mode
                && artifactId.equals(that.artifactId)
                && fence.equals(that.fence)
                && publicationId.equals(that.publicationId)
                && Arrays.equals(definitionDigest, that.definitionDigest)
                && Arrays.equals(capturedTargetFrame, that.capturedTargetFrame)
                && Arrays.equals(planDescriptorFrame, that.planDescriptorFrame)
                && Arrays.equals(dataMembersDigest, that.dataMembersDigest)
                && Arrays.equals(decisionInputDigest, that.decisionInputDigest);
    }

    @Override public int hashCode() {
        int result = feedId.hashCode();
        result = 31 * result + (int) (feedIncarnation ^ (feedIncarnation >>> 32));
        result = 31 * result + Arrays.hashCode(definitionDigest);
        result = 31 * result + intervalId.hashCode();
        result = 31 * result + mode.hashCode();
        result = 31 * result + Arrays.hashCode(capturedTargetFrame);
        result = 31 * result + artifactId.hashCode();
        result = 31 * result + fence.hashCode();
        result = 31 * result + publicationId.hashCode();
        result = 31 * result + (int) (generation ^ (generation >>> 32));
        result = 31 * result + Arrays.hashCode(planDescriptorFrame);
        result = 31 * result + Arrays.hashCode(dataMembersDigest);
        return 31 * result + Arrays.hashCode(decisionInputDigest);
    }
}
