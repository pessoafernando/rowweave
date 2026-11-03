package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamBroadcastStateSpec;

import java.util.Objects;

/** Portable broadcast authority, serializer and resource profile. */
public final class StreamBroadcastStateDescriptor {
    public enum AuthorityKind {
        IMMUTABLE_SNAPSHOT(1), REPLAYABLE_CHANGELOG(2);
        private final int wireId;
        AuthorityKind(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static AuthorityKind fromWireId(int wireId) {
            for (AuthorityKind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_BROADCAST_AUTHORITY",
                    "unknown broadcast authority kind");
        }
    }

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

    public StreamBroadcastStateDescriptor(String stateName,
            AuthorityKind authorityKind, String snapshotOrVersionId,
            String authorityDigest, String serializerContractId,
            int stateSchemaVersion, long maximumEntries,
            long maximumTotalBytes, long maximumUpdateBytes,
            int maximumRetainedVersions) {
        this.stateName = StreamContractSupport.text(stateName,
                "nodes.broadcast.stateName", 256);
        this.snapshotOrVersionId = StreamContractSupport.text(
                snapshotOrVersionId, "nodes.broadcast.authorityId", 256);
        this.authorityDigest = StreamContractSupport.digest(authorityDigest,
                "nodes.broadcast.authorityDigest");
        this.serializerContractId = StreamContractSupport.text(
                serializerContractId, "nodes.broadcast.serializer", 256);
        if (authorityKind == null || stateSchemaVersion <= 0
                || maximumEntries <= 0L
                || maximumEntries > StreamBroadcastStateSpec.MAX_ENTRIES
                || maximumTotalBytes <= 0L
                || maximumTotalBytes
                > StreamBroadcastStateSpec.MAX_TOTAL_BYTES
                || maximumUpdateBytes <= 0L
                || maximumUpdateBytes
                > StreamBroadcastStateSpec.MAX_UPDATE_BYTES
                || maximumUpdateBytes > maximumTotalBytes
                || maximumRetainedVersions <= 0
                || maximumRetainedVersions
                > StreamBroadcastStateSpec.MAX_RETAINED_VERSIONS) {
            throw failure("INVALID_BROADCAST_RESOURCE_LIMIT",
                    "broadcast state resource limits are invalid");
        }
        this.authorityKind = authorityKind;
        this.stateSchemaVersion = stateSchemaVersion;
        this.maximumEntries = maximumEntries;
        this.maximumTotalBytes = maximumTotalBytes;
        this.maximumUpdateBytes = maximumUpdateBytes;
        this.maximumRetainedVersions = maximumRetainedVersions;
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

    private static StreamContractException failure(String code,
            String message) {
        return StreamContractSupport.failure(code, "nodes.broadcast", message);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamBroadcastStateDescriptor)) return false;
        StreamBroadcastStateDescriptor that =
                (StreamBroadcastStateDescriptor) other;
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
