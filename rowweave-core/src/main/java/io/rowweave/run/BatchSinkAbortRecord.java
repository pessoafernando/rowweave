package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Immutable authority record for a durable Sink abort decision and its cleanup completion. */
public final class BatchSinkAbortRecord {

    public enum State { ABORT_DECIDED, ABORTED }

    public static final int DIGEST_BYTES = 32;
    public static final int MAX_REASON_CODE_CHARS = 64;

    private final State state;
    private final RunAttemptFence fence;
    private final long assignmentGeneration;
    private final BatchSinkDescriptor.CapabilityProfile capabilityProfile;
    private final String commitDomainId;
    private final long generation;
    private final String publicationId;
    private final byte[] identityDigest;
    private final String reasonCode;
    private final long decidedAtMillis;
    private final long completedAtMillis;

    private BatchSinkAbortRecord(
            State state, RunAttemptFence fence, long assignmentGeneration,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile,
            String commitDomainId, long generation, String publicationId,
            byte[] identityDigest, String reasonCode,
            long decidedAtMillis, long completedAtMillis) {
        if (state == null || fence == null) {
            throw new IllegalArgumentException("abort state and fence are required");
        }
        if (assignmentGeneration <= 0L || generation <= 0L) {
            throw new IllegalArgumentException("abort generations must be positive");
        }
        if (capabilityProfile
                != BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1) {
            throw new IllegalArgumentException(
                    "durable abort record requires manifest capability profile");
        }
        this.commitDomainId = RunText.required(
                commitDomainId, "commitDomainId",
                BatchSinkDescriptor.MAX_COMMIT_DOMAIN_ID_BYTES);
        this.publicationId = RunText.required(
                publicationId, "publicationId",
                ManifestMemberReceipt.MAX_PUBLICATION_ID_BYTES);
        if (identityDigest == null || identityDigest.length != DIGEST_BYTES) {
            throw new IllegalArgumentException("abort identityDigest must be SHA-256");
        }
        if (reasonCode == null || reasonCode.isEmpty()
                || reasonCode.length() > MAX_REASON_CODE_CHARS
                || !reasonCode.matches("[A-Z0-9_]+")) {
            throw new IllegalArgumentException(
                    "abort reasonCode must be bounded and redacted");
        }
        if (decidedAtMillis < 0L
                || (state == State.ABORT_DECIDED && completedAtMillis != 0L)
                || (state == State.ABORTED && completedAtMillis < decidedAtMillis)) {
            throw new IllegalArgumentException("abort decision timestamps are invalid");
        }
        this.state = state;
        this.fence = fence;
        this.assignmentGeneration = assignmentGeneration;
        this.capabilityProfile = capabilityProfile;
        this.generation = generation;
        this.identityDigest = identityDigest.clone();
        this.reasonCode = reasonCode;
        this.decidedAtMillis = decidedAtMillis;
        this.completedAtMillis = completedAtMillis;
    }

    public static BatchSinkAbortRecord decided(
            RunAttemptFence fence, long assignmentGeneration,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile,
            String commitDomainId, long generation, String publicationId,
            byte[] identityDigest, String reasonCode, long decidedAtMillis) {
        return new BatchSinkAbortRecord(
                State.ABORT_DECIDED, fence, assignmentGeneration,
                capabilityProfile, commitDomainId, generation, publicationId,
                identityDigest, reasonCode, decidedAtMillis, 0L);
    }

    static BatchSinkAbortRecord decoded(
            State state, RunAttemptFence fence, long assignmentGeneration,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile,
            String commitDomainId, long generation, String publicationId,
            byte[] identityDigest, String reasonCode,
            long decidedAtMillis, long completedAtMillis) {
        return new BatchSinkAbortRecord(
                state, fence, assignmentGeneration, capabilityProfile,
                commitDomainId, generation, publicationId, identityDigest,
                reasonCode, decidedAtMillis, completedAtMillis);
    }

    public BatchSinkAbortRecord completed(long completedAtMillis) {
        if (state != State.ABORT_DECIDED) {
            throw new IllegalStateException("abort cleanup is already complete");
        }
        return new BatchSinkAbortRecord(
                State.ABORTED, fence, assignmentGeneration, capabilityProfile,
                commitDomainId, generation, publicationId, identityDigest,
                reasonCode, decidedAtMillis, completedAtMillis);
    }

    public State state() { return state; }
    public RunAttemptFence fence() { return fence; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public BatchSinkDescriptor.CapabilityProfile capabilityProfile() {
        return capabilityProfile;
    }
    public String commitDomainId() { return commitDomainId; }
    public long generation() { return generation; }
    public String publicationId() { return publicationId; }
    public byte[] identityDigest() { return identityDigest.clone(); }
    public String reasonCode() { return reasonCode; }
    public long decidedAtMillis() { return decidedAtMillis; }
    public long completedAtMillis() { return completedAtMillis; }

    public boolean sameDecision(BatchSinkAbortRecord other) {
        return other != null
                && fence.equals(other.fence)
                && assignmentGeneration == other.assignmentGeneration
                && capabilityProfile == other.capabilityProfile
                && commitDomainId.equals(other.commitDomainId)
                && generation == other.generation
                && publicationId.equals(other.publicationId)
                && MessageDigest.isEqual(identityDigest, other.identityDigest)
                && reasonCode.equals(other.reasonCode)
                && decidedAtMillis == other.decidedAtMillis;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof BatchSinkAbortRecord)) return false;
        BatchSinkAbortRecord that = (BatchSinkAbortRecord) other;
        return state == that.state
                && completedAtMillis == that.completedAtMillis
                && sameDecision(that);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(state, fence, assignmentGeneration,
                capabilityProfile, commitDomainId, generation, publicationId,
                reasonCode, decidedAtMillis, completedAtMillis)
                + Arrays.hashCode(identityDigest);
    }
}
