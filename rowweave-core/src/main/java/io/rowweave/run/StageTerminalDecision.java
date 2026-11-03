package io.rowweave.run;

/** Closed result set for the durable physical-stage terminal linearization point. */
public final class StageTerminalDecision {
    public enum Code {
        RECORDED,
        ALREADY_RECORDED,
        STALE_RUN,
        ASSIGNMENT_CONFLICT,
        ACCEPTANCE_REQUIRED,
        TERMINAL_CONFLICT,
        RECOVERY_OWNED,
        LOST_REQUIRES_RECOVERY,
        CORRUPT,
        UNAVAILABLE
    }

    private final Code code;
    private final StageTerminalReceipt receipt;

    private StageTerminalDecision(Code code, StageTerminalReceipt receipt) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean hasReceipt = code == Code.RECORDED || code == Code.ALREADY_RECORDED;
        if (hasReceipt != (receipt != null)) {
            throw new IllegalArgumentException("receipt does not match decision code");
        }
        this.code = code;
        this.receipt = receipt;
    }

    public static StageTerminalDecision recorded(StageTerminalReceipt receipt) {
        return new StageTerminalDecision(Code.RECORDED, receipt);
    }

    public static StageTerminalDecision alreadyRecorded(StageTerminalReceipt receipt) {
        return new StageTerminalDecision(Code.ALREADY_RECORDED, receipt);
    }

    public static StageTerminalDecision rejected(Code code) {
        if (code == Code.RECORDED || code == Code.ALREADY_RECORDED) {
            throw new IllegalArgumentException("recorded codes require receipt");
        }
        return new StageTerminalDecision(code, null);
    }

    public Code code() { return code; }
    public StageTerminalReceipt receipt() { return receipt; }
    public boolean hasReceipt() { return receipt != null; }
}
