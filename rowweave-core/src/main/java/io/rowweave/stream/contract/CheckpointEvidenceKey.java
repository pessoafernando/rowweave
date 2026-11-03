package io.rowweave.stream.contract;

import java.util.Objects;

/** Stable identity of one required checkpoint evidence member. */
public final class CheckpointEvidenceKey implements Comparable<CheckpointEvidenceKey> {
    private final CheckpointEvidence.Kind kind;
    private final String memberId;

    public CheckpointEvidenceKey(CheckpointEvidence.Kind kind, String memberId) {
        if (kind == null) {
            throw StreamContractSupport.failure("INVALID_EVIDENCE_KEY",
                    "checkpoint.evidence.kind", "evidence kind is required");
        }
        this.kind = kind;
        this.memberId = StreamContractSupport.text(
                memberId, "checkpoint.evidence.memberId", 512);
    }

    public CheckpointEvidence.Kind kind() { return kind; }
    public String memberId() { return memberId; }

    @Override
    public int compareTo(CheckpointEvidenceKey other) {
        int byKind = Integer.compare(kind.wireId(), other.kind.wireId());
        return byKind != 0 ? byKind : memberId.compareTo(other.memberId);
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CheckpointEvidenceKey)) return false;
        CheckpointEvidenceKey that = (CheckpointEvidenceKey) other;
        return kind == that.kind && memberId.equals(that.memberId);
    }

    @Override
    public int hashCode() { return Objects.hash(kind, memberId); }
}
