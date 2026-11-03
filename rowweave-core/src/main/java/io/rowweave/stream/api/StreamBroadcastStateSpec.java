package io.rowweave.stream.api;

import java.util.Locale;
import java.util.Objects;

/** Version-frozen broadcast-state authority and hard resource envelope. */
public final class StreamBroadcastStateSpec {
    public static final long MAX_ENTRIES = 100_000_000L;
    public static final long MAX_TOTAL_BYTES = 1L << 40;
    public static final long MAX_UPDATE_BYTES = 16L * 1024L * 1024L;
    public static final int MAX_RETAINED_VERSIONS = 64;

    public enum AuthorityKind { IMMUTABLE_SNAPSHOT, REPLAYABLE_CHANGELOG }

    private final String stateName;
    private final AuthorityKind authorityKind;
    private final String snapshotOrVersionId;
    private final String authorityDigest;
    private final String serializerContractId;
    private final int stateSchemaVersion;
    private final long maximumEntries;
    private final long maximumTotalBytes;
    private final long maximumUpdateBytes;
    private final int maximumRetainedVersions;

    private StreamBroadcastStateSpec(String stateName,
            AuthorityKind authorityKind, String snapshotOrVersionId,
            String authorityDigest, String serializerContractId,
            int stateSchemaVersion, long maximumEntries,
            long maximumTotalBytes, long maximumUpdateBytes,
            int maximumRetainedVersions) {
        if (empty(stateName) || authorityKind == null
                || empty(snapshotOrVersionId) || !digest(authorityDigest)
                || empty(serializerContractId) || stateSchemaVersion <= 0
                || maximumEntries <= 0L || maximumEntries > MAX_ENTRIES
                || maximumTotalBytes <= 0L
                || maximumTotalBytes > MAX_TOTAL_BYTES
                || maximumUpdateBytes <= 0L
                || maximumUpdateBytes > MAX_UPDATE_BYTES
                || maximumUpdateBytes > maximumTotalBytes
                || maximumRetainedVersions <= 0
                || maximumRetainedVersions > MAX_RETAINED_VERSIONS) {
            throw new IllegalArgumentException(
                    "broadcast state authority or bounds are invalid");
        }
        this.stateName = stateName;
        this.authorityKind = authorityKind;
        this.snapshotOrVersionId = snapshotOrVersionId;
        this.authorityDigest = authorityDigest;
        this.serializerContractId = serializerContractId;
        this.stateSchemaVersion = stateSchemaVersion;
        this.maximumEntries = maximumEntries;
        this.maximumTotalBytes = maximumTotalBytes;
        this.maximumUpdateBytes = maximumUpdateBytes;
        this.maximumRetainedVersions = maximumRetainedVersions;
    }

    public static StreamBroadcastStateSpec immutableSnapshot(String stateName,
            String snapshotId, String snapshotDigest,
            String serializerContractId, int stateSchemaVersion,
            long maximumEntries, long maximumTotalBytes,
            long maximumUpdateBytes, int maximumRetainedVersions) {
        return new StreamBroadcastStateSpec(stateName,
                AuthorityKind.IMMUTABLE_SNAPSHOT, snapshotId, snapshotDigest,
                serializerContractId, stateSchemaVersion, maximumEntries,
                maximumTotalBytes, maximumUpdateBytes,
                maximumRetainedVersions);
    }

    public static StreamBroadcastStateSpec replayableChangelog(
            String stateName, String stableVersionId, String versionDigest,
            String serializerContractId, int stateSchemaVersion,
            long maximumEntries, long maximumTotalBytes,
            long maximumUpdateBytes, int maximumRetainedVersions) {
        return new StreamBroadcastStateSpec(stateName,
                AuthorityKind.REPLAYABLE_CHANGELOG, stableVersionId,
                versionDigest, serializerContractId, stateSchemaVersion,
                maximumEntries, maximumTotalBytes, maximumUpdateBytes,
                maximumRetainedVersions);
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }

    private static boolean digest(String value) {
        if (value == null || value.length() != 64
                || !value.equals(value.toLowerCase(Locale.ROOT))) return false;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if ((current < '0' || current > '9')
                    && (current < 'a' || current > 'f')) return false;
        }
        return true;
    }

    public String stateName() { return stateName; }
    public AuthorityKind authorityKind() { return authorityKind; }
    public String snapshotOrVersionId() { return snapshotOrVersionId; }
    public String authorityDigest() { return authorityDigest; }
    public String serializerContractId() { return serializerContractId; }
    public int stateSchemaVersion() { return stateSchemaVersion; }
    public long maximumEntries() { return maximumEntries; }
    public long maximumTotalBytes() { return maximumTotalBytes; }
    public long maximumUpdateBytes() { return maximumUpdateBytes; }
    public int maximumRetainedVersions() {
        return maximumRetainedVersions;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamBroadcastStateSpec)) return false;
        StreamBroadcastStateSpec that = (StreamBroadcastStateSpec) other;
        return stateSchemaVersion == that.stateSchemaVersion
                && maximumEntries == that.maximumEntries
                && maximumTotalBytes == that.maximumTotalBytes
                && maximumUpdateBytes == that.maximumUpdateBytes
                && maximumRetainedVersions == that.maximumRetainedVersions
                && stateName.equals(that.stateName)
                && authorityKind == that.authorityKind
                && snapshotOrVersionId.equals(that.snapshotOrVersionId)
                && authorityDigest.equals(that.authorityDigest)
                && serializerContractId.equals(that.serializerContractId);
    }

    @Override public int hashCode() {
        return Objects.hash(stateName, authorityKind, snapshotOrVersionId,
                authorityDigest, serializerContractId, stateSchemaVersion,
                maximumEntries, maximumTotalBytes, maximumUpdateBytes,
                maximumRetainedVersions);
    }
}
