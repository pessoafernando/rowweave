package io.rowweave;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Objects;
import java.util.regex.Pattern;

/** Immutable bounded references and identities for one exact JDBC snapshot split. */
public final class JdbcSnapshotReadRequest implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Pattern REFERENCE =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,127}");
    private static final Pattern SHA256 = Pattern.compile("[0-9a-f]{64}");

    private final String endpointRef;
    private final String credentialRef;
    private final String datasetRef;
    private final String queryRef;
    private final String partitionRef;
    private final String snapshotRef;
    private final String queryDigest;
    private final String partitionDigest;
    private final String snapshotId;
    private final int fetchSize;
    private final int queryTimeoutSeconds;
    private final int maxCellBytes;

    public JdbcSnapshotReadRequest(
            String endpointRef, String credentialRef, String datasetRef,
            String queryRef, String partitionRef, String snapshotRef,
            String queryDigest, String partitionDigest, String snapshotId,
            int fetchSize, int queryTimeoutSeconds, int maxCellBytes) {
        this.endpointRef = reference(endpointRef, "endpointRef");
        this.credentialRef = reference(credentialRef, "credentialRef");
        this.datasetRef = reference(datasetRef, "datasetRef");
        this.queryRef = reference(queryRef, "queryRef");
        this.partitionRef = reference(partitionRef, "partitionRef");
        this.snapshotRef = reference(snapshotRef, "snapshotRef");
        this.queryDigest = digest(queryDigest, "queryDigest");
        this.partitionDigest = digest(partitionDigest, "partitionDigest");
        this.snapshotId = digest(snapshotId, "snapshotId");
        if (fetchSize < 1 || fetchSize > JdbcSnapshotSource.MAX_FETCH_SIZE) {
            throw invalid("fetchSize");
        }
        if (queryTimeoutSeconds < 1
                || queryTimeoutSeconds > JdbcSnapshotSource.MAX_QUERY_TIMEOUT_SECONDS) {
            throw invalid("queryTimeoutSeconds");
        }
        if (maxCellBytes < JdbcSnapshotSource.MIN_CELL_BYTES
                || maxCellBytes > JdbcSnapshotSource.MAX_CELL_BYTES) {
            throw invalid("maxCellBytes");
        }
        this.fetchSize = fetchSize;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.maxCellBytes = maxCellBytes;
    }

    public String endpointRef() { return endpointRef; }
    public String credentialRef() { return credentialRef; }
    public String datasetRef() { return datasetRef; }
    public String queryRef() { return queryRef; }
    public String partitionRef() { return partitionRef; }
    public String snapshotRef() { return snapshotRef; }
    public String queryDigest() { return queryDigest; }
    public String partitionDigest() { return partitionDigest; }
    public String snapshotId() { return snapshotId; }
    public int fetchSize() { return fetchSize; }
    public int queryTimeoutSeconds() { return queryTimeoutSeconds; }
    public int maxCellBytes() { return maxCellBytes; }

    private Object readResolve() throws ObjectStreamException {
        return new JdbcSnapshotReadRequest(
                endpointRef, credentialRef, datasetRef, queryRef, partitionRef,
                snapshotRef, queryDigest, partitionDigest, snapshotId,
                fetchSize, queryTimeoutSeconds, maxCellBytes);
    }

    private static String reference(String value, String field) {
        if (value == null || !REFERENCE.matcher(value).matches()) {
            throw invalid(field);
        }
        return value;
    }

    private static String digest(String value, String field) {
        if (value == null || !SHA256.matcher(value).matches()) {
            throw invalid(field);
        }
        return value;
    }

    private static IllegalArgumentException invalid(String field) {
        return new IllegalArgumentException(
                "jdbc snapshot " + field + " is invalid or exceeds its hard limit");
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof JdbcSnapshotReadRequest)) return false;
        JdbcSnapshotReadRequest that = (JdbcSnapshotReadRequest) other;
        return fetchSize == that.fetchSize
                && queryTimeoutSeconds == that.queryTimeoutSeconds
                && maxCellBytes == that.maxCellBytes
                && endpointRef.equals(that.endpointRef)
                && credentialRef.equals(that.credentialRef)
                && datasetRef.equals(that.datasetRef)
                && queryRef.equals(that.queryRef)
                && partitionRef.equals(that.partitionRef)
                && snapshotRef.equals(that.snapshotRef)
                && queryDigest.equals(that.queryDigest)
                && partitionDigest.equals(that.partitionDigest)
                && snapshotId.equals(that.snapshotId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(endpointRef, credentialRef, datasetRef, queryRef,
                partitionRef, snapshotRef, queryDigest, partitionDigest,
                snapshotId, fetchSize, queryTimeoutSeconds, maxCellBytes);
    }

    @Override
    public String toString() {
        return "JdbcSnapshotReadRequest{redacted}";
    }
}
