package io.rowweave.run;

/** Closed result set for the durable assignment acceptance linearization point. */
public final class StageAcceptanceDecision {
    public enum Code {
        ACCEPTED,
        ALREADY_ACCEPTED,
        STALE_ASSIGNMENT_GENERATION,
        STALE_RUN,
        ASSIGNMENT_CONFLICT,
        RECOVERY_OWNED,
        CORRUPT,
        UNAVAILABLE
    }

    private final Code code;
    private final StageAssignmentAcceptance acceptance;

    private StageAcceptanceDecision(Code code, StageAssignmentAcceptance acceptance) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean hasAcceptance = code == Code.ACCEPTED || code == Code.ALREADY_ACCEPTED;
        if (hasAcceptance != (acceptance != null)) {
            throw new IllegalArgumentException("acceptance does not match decision code");
        }
        this.code = code;
        this.acceptance = acceptance;
    }

    public static StageAcceptanceDecision accepted(StageAssignmentAcceptance acceptance) {
        return new StageAcceptanceDecision(Code.ACCEPTED, acceptance);
    }

    public static StageAcceptanceDecision alreadyAccepted(StageAssignmentAcceptance acceptance) {
        return new StageAcceptanceDecision(Code.ALREADY_ACCEPTED, acceptance);
    }

    public static StageAcceptanceDecision staleGeneration() {
        return rejected(Code.STALE_ASSIGNMENT_GENERATION);
    }

    public static StageAcceptanceDecision rejected(Code code) {
        if (code == Code.ACCEPTED || code == Code.ALREADY_ACCEPTED) {
            throw new IllegalArgumentException("accepted codes require receipt");
        }
        return new StageAcceptanceDecision(code, null);
    }

    public Code code() { return code; }
    public StageAssignmentAcceptance acceptance() { return acceptance; }
    public boolean hasAcceptance() { return acceptance != null; }
}
