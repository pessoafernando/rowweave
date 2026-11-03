package io.rowweave.run;

/** Closed result set for one sealed Manifest member receipt. */
public final class ManifestMemberDecision {
    public enum Code {
        RECORDED,
        ALREADY_RECORDED,
        STALE_RUN,
        ASSIGNMENT_CONFLICT,
        TERMINAL_REQUIRED,
        TERMINAL_NOT_SUCCESSFUL,
        COMMIT_IDENTITY_CONFLICT,
        MEMBER_CONFLICT,
        INTENT_ALREADY_COMMITTED,
        RECOVERY_OWNED,
        CORRUPT,
        UNAVAILABLE
    }

    private final Code code;
    private final ManifestMemberReceipt receipt;

    private ManifestMemberDecision(Code code, ManifestMemberReceipt receipt) {
        if (code == null) throw new IllegalArgumentException("code is required");
        boolean hasReceipt = code == Code.RECORDED || code == Code.ALREADY_RECORDED;
        if (hasReceipt != (receipt != null)) {
            throw new IllegalArgumentException("receipt does not match decision code");
        }
        this.code = code;
        this.receipt = receipt;
    }

    public static ManifestMemberDecision recorded(ManifestMemberReceipt receipt) {
        return new ManifestMemberDecision(Code.RECORDED, receipt);
    }

    public static ManifestMemberDecision alreadyRecorded(ManifestMemberReceipt receipt) {
        return new ManifestMemberDecision(Code.ALREADY_RECORDED, receipt);
    }

    public static ManifestMemberDecision rejected(Code code) {
        if (code == Code.RECORDED || code == Code.ALREADY_RECORDED) {
            throw new IllegalArgumentException("recorded code requires receipt");
        }
        return new ManifestMemberDecision(code, null);
    }

    public Code code() { return code; }
    public ManifestMemberReceipt receipt() { return receipt; }
}
