package io.rowweave.stream.contract;

import java.util.Objects;

/** Bounded digest-only evidence; state, offsets and Sink payloads stay out of control state. */
public final class CheckpointEvidence {
    public enum Kind {
        SOURCE_POSITION(1), STATE(2), TRANSACTIONAL_SINK(3),
        EAGER_SINK_CAPABILITY(4), SOURCE_COORDINATOR(5), CHANNEL_STATE(6);

        private final int wireId;

        Kind(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static Kind fromWire(int wireId) {
            for (Kind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw StreamContractSupport.failure("UNKNOWN_EVIDENCE_KIND",
                    "checkpoint.evidence.kind", "unknown evidence kind");
        }

    }

    private final CheckpointEvidenceKey key;
    private final String contentDigest;
    private final long contentBytes;

    public CheckpointEvidence(
            Kind kind, String memberId, String contentDigest, long contentBytes) {
        this.key = new CheckpointEvidenceKey(kind, memberId);
        this.contentDigest = StreamContractSupport.digest(
                contentDigest, "checkpoint.evidence.contentDigest");
        if (contentBytes < 0L) {
            throw StreamContractSupport.failure("INVALID_EVIDENCE_SIZE",
                    "checkpoint.evidence.contentBytes", "evidence size must not be negative");
        }
        this.contentBytes = contentBytes;
    }

    public CheckpointEvidenceKey key() { return key; }
    public Kind kind() { return key.kind(); }
    public String memberId() { return key.memberId(); }
    public String contentDigest() { return contentDigest; }
    public long contentBytes() { return contentBytes; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CheckpointEvidence)) return false;
        CheckpointEvidence that = (CheckpointEvidence) other;
        return contentBytes == that.contentBytes && key.equals(that.key)
                && contentDigest.equals(that.contentDigest);
    }

    @Override
    public int hashCode() { return Objects.hash(key, contentDigest, contentBytes); }
}
