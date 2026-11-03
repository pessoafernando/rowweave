package io.rowweave.connector.s3;

/** Exact next-record position within a frozen and revalidated object. */
public final class S3SourcePosition {
    private final String splitId;
    private final long completedRecords;

    public S3SourcePosition(String splitId, long completedRecords) {
        S3CodecSupport.parseSha256(splitId, "S3_POSITION_INVALID");
        if (completedRecords < 0) {
            throw S3CodecSupport.invalid("S3_POSITION_INVALID");
        }
        this.splitId = splitId;
        this.completedRecords = completedRecords;
    }

    public String splitId() { return splitId; }
    public long completedRecords() { return completedRecords; }
}
