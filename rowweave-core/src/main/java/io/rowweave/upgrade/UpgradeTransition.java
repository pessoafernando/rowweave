package io.rowweave.upgrade;

/** Result of a pure authority-kernel transition. Rejections retain the input record. */
public final class UpgradeTransition {
    public enum Code {
        ACCEPTED,
        REPLAYED,
        ACTIVE_WORK_REMAINS,
        COMMAND_ID_CONFLICT,
        BINDING_MISMATCH,
        DIGEST_DRIFT,
        STALE_OWNER,
        POINTER_VERSION_MISMATCH,
        INVALID_PHASE,
        ROLLBACK_FORBIDDEN,
        DEADLINE_NOT_REACHED,
        EVIDENCE_EXHAUSTED
    }

    private final Code code;
    private final UpgradeAuthorityRecord record;

    private UpgradeTransition(Code code, UpgradeAuthorityRecord record) {
        this.code = code;
        this.record = record;
    }

    static UpgradeTransition of(Code code, UpgradeAuthorityRecord record) {
        return new UpgradeTransition(code, record);
    }

    public Code code() { return code; }
    public UpgradeAuthorityRecord record() { return record; }
}
