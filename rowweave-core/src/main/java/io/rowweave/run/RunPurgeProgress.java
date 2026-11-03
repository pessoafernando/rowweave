package io.rowweave.run;

/** Closed result of one bounded purge step. */
public final class RunPurgeProgress {
    public enum Code { IN_PROGRESS, PURGED, STALE_OWNER, CORRUPT }

    private final Code code;
    private final int deletedRecords;

    private RunPurgeProgress(Code code, int deletedRecords) {
        if (code == null) throw new IllegalArgumentException("code is required");
        if (deletedRecords < 0 || deletedRecords > 1000
                || (code != Code.IN_PROGRESS && deletedRecords != 0)) {
            throw new IllegalArgumentException("invalid deleted record count");
        }
        this.code = code;
        this.deletedRecords = deletedRecords;
    }

    public static RunPurgeProgress inProgress(int deletedRecords) {
        return new RunPurgeProgress(Code.IN_PROGRESS, deletedRecords);
    }
    public static RunPurgeProgress purged() {
        return new RunPurgeProgress(Code.PURGED, 0);
    }
    public static RunPurgeProgress staleOwner() {
        return new RunPurgeProgress(Code.STALE_OWNER, 0);
    }
    public static RunPurgeProgress corrupt() {
        return new RunPurgeProgress(Code.CORRUPT, 0);
    }

    public Code code() { return code; }
    public int deletedRecords() { return deletedRecords; }
}
