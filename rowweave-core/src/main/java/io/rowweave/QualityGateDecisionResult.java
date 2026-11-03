package io.rowweave;

/** Closed result set for recording the first durable quality-gate decision. */
public final class QualityGateDecisionResult {
    public enum Code {
        RECORDED,
        ALREADY_RECORDED,
        STALE_RUN,
        ATTEMPT_NOT_COMMITTING,
        INCOMPLETE_MEMBERS,
        IDENTITY_CONFLICT,
        EVIDENCE_CONFLICT,
        DECISION_CONFLICT,
        ABORT_ALREADY_DECIDED,
        RECOVERY_OWNED,
        CORRUPT,
        UNAVAILABLE
    }

    private final Code code;
    private final QualityGateDecision decision;

    private QualityGateDecisionResult(Code code, QualityGateDecision decision) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean recorded = code == Code.RECORDED || code == Code.ALREADY_RECORDED;
        if (recorded != (decision != null)) {
            throw new IllegalArgumentException("decision does not match result code");
        }
        this.code = code;
        this.decision = decision;
    }

    public static QualityGateDecisionResult recorded(QualityGateDecision decision) {
        return new QualityGateDecisionResult(Code.RECORDED, decision);
    }

    public static QualityGateDecisionResult alreadyRecorded(
            QualityGateDecision decision) {
        return new QualityGateDecisionResult(Code.ALREADY_RECORDED, decision);
    }

    public static QualityGateDecisionResult rejected(Code code) {
        if (code == Code.RECORDED || code == Code.ALREADY_RECORDED) {
            throw new IllegalArgumentException("recorded code requires decision");
        }
        return new QualityGateDecisionResult(code, null);
    }

    public Code code() { return code; }
    public QualityGateDecision decision() { return decision; }
}
