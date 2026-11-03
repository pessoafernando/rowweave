package io.rowweave.run;

/** Result of the second, independent prepared-locator authority CAS. */
public final class PreparedSinkDecision {
    public enum Code {
        RECORDED, ALREADY_RECORDED, TERMINAL_REQUIRED,
        ASSIGNMENT_CONFLICT, LOCATOR_CONFLICT,
        STALE_RUN, RECOVERY_OWNED, UNAVAILABLE
    }

    private final Code code;
    private final PreparedSinkReceipt receipt;

    private PreparedSinkDecision(Code code, PreparedSinkReceipt receipt) {
        this.code = code;
        this.receipt = receipt;
    }

    public static PreparedSinkDecision recorded(PreparedSinkReceipt receipt) {
        return new PreparedSinkDecision(Code.RECORDED, receipt);
    }

    public static PreparedSinkDecision alreadyRecorded(PreparedSinkReceipt receipt) {
        return new PreparedSinkDecision(Code.ALREADY_RECORDED, receipt);
    }

    public static PreparedSinkDecision rejected(Code code) {
        if (code == Code.RECORDED || code == Code.ALREADY_RECORDED) {
            throw new IllegalArgumentException("success code requires receipt");
        }
        return new PreparedSinkDecision(code, null);
    }

    public Code code() { return code; }
    public PreparedSinkReceipt receipt() { return receipt; }
}
