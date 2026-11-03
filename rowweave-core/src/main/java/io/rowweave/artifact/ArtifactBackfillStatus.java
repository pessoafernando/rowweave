package io.rowweave.artifact;

/** Bounded, observable state of the durable reference and inventory migration barrier. */
public final class ArtifactBackfillStatus {
    public enum Code { READY, BACKFILLING, CORRUPT }

    private final Code code;
    private final String cursor;
    private final long processedEntries;
    private final String detail;

    private ArtifactBackfillStatus(Code code, String cursor,
                                   long processedEntries, String detail) {
        this.code = code;
        this.cursor = cursor;
        this.processedEntries = processedEntries;
        this.detail = detail;
    }

    public static ArtifactBackfillStatus ready() {
        return new ArtifactBackfillStatus(Code.READY, null, 0L, null);
    }

    public static ArtifactBackfillStatus backfilling(String cursor,
                                                     long processedEntries) {
        if (cursor == null || cursor.isEmpty() || cursor.length() > 1024) {
            throw new IllegalArgumentException("backfill cursor is invalid");
        }
        if (processedEntries < 0L) {
            throw new IllegalArgumentException("processedEntries must be >= 0");
        }
        return new ArtifactBackfillStatus(
                Code.BACKFILLING, cursor, processedEntries, null);
    }

    public static ArtifactBackfillStatus corrupt(String detail) {
        ArtifactLifecycleSupport.requireText(detail, "detail");
        return new ArtifactBackfillStatus(Code.CORRUPT, null, 0L, detail);
    }

    public Code code() { return code; }
    public String cursor() { return cursor; }
    public long processedEntries() { return processedEntries; }
    public String detail() { return detail; }
}
