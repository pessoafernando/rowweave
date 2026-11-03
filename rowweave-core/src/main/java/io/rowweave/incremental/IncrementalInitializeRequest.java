package io.rowweave.incremental;

/** Immutable transition payload for first creation of a feed frontier authority. */
public final class IncrementalInitializeRequest {
    private final String feedId;
    private final long feedIncarnation;
    private final byte[] definitionDigest;
    private final byte[] sourceContractDigest;
    private final byte[] frontierContractDigest;
    private final IncrementalFrontier.Kind frontierKind;
    private final byte[] transitionDigest;

    public IncrementalInitializeRequest(
            String feedId, long feedIncarnation, byte[] definitionDigest,
            byte[] sourceContractDigest, byte[] frontierContractDigest,
            IncrementalFrontier.Kind frontierKind) {
        this.feedId = IncrementalAuthorityValue.text(feedId, "feed id");
        if (feedIncarnation <= 0L || frontierKind == null
                || frontierKind == IncrementalFrontier.Kind.BEGIN_V1) {
            throw new IllegalArgumentException(
                    "feed incarnation and concrete frontier kind are required");
        }
        this.feedIncarnation = feedIncarnation;
        this.definitionDigest = IncrementalAuthorityValue.digest(
                definitionDigest, "definition digest");
        this.sourceContractDigest = IncrementalAuthorityValue.digest(
                sourceContractDigest, "source contract digest");
        this.frontierContractDigest = IncrementalAuthorityValue.digest(
                frontierContractDigest, "frontier contract digest");
        this.frontierKind = frontierKind;
        this.transitionDigest = IncrementalTransitionDigest.initialize(this);
    }

    public String feedId() { return feedId; }
    public long feedIncarnation() { return feedIncarnation; }
    public byte[] definitionDigest() { return definitionDigest.clone(); }
    public byte[] sourceContractDigest() { return sourceContractDigest.clone(); }
    public byte[] frontierContractDigest() { return frontierContractDigest.clone(); }
    public IncrementalFrontier.Kind frontierKind() { return frontierKind; }
    public byte[] transitionDigest() { return transitionDigest.clone(); }
}
