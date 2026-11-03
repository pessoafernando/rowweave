package io.rowweave.incremental;

import io.rowweave.run.RunAttemptFence;

/** Authority-only proof that the exact current attempt owns a durable Sink commit intent. */
public final class VerifiedIncrementalSinkCommitDecision {
    private final String domainId;
    private final RunAttemptFence fence;
    private final String publicationId;
    private final long generation;
    private final byte[] manifestHash;
    private final byte[] commitDecisionDigest;

    VerifiedIncrementalSinkCommitDecision(
            String domainId, RunAttemptFence fence, String publicationId,
            long generation, byte[] manifestHash, byte[] commitDecisionDigest) {
        this.domainId = IncrementalAuthorityValue.text(domainId, "commit domain id");
        if (fence == null || generation <= 0L) {
            throw new IllegalArgumentException("sink decision fence/generation is invalid");
        }
        this.fence = fence;
        this.publicationId = IncrementalAuthorityValue.text(publicationId, "publication id");
        this.generation = generation;
        this.manifestHash = IncrementalAuthorityValue.digest(manifestHash, "manifest hash");
        this.commitDecisionDigest = IncrementalAuthorityValue.digest(
                commitDecisionDigest, "commit decision digest");
    }

    public String domainId() { return domainId; }
    public RunAttemptFence fence() { return fence; }
    public String publicationId() { return publicationId; }
    public long generation() { return generation; }
    public byte[] manifestHash() { return manifestHash.clone(); }
    public byte[] commitDecisionDigest() { return commitDecisionDigest.clone(); }
}
