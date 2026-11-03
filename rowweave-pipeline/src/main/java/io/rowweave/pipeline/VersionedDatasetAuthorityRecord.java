package io.rowweave.pipeline;

/** Immutable opaque record returned by a handoff authority store. */
public final class VersionedDatasetAuthorityRecord {
    private final byte[] bytes;
    private final String versionToken;

    public VersionedDatasetAuthorityRecord(byte[] bytes) {
        this.bytes = VersionedDatasetHandoffSupport.bytes(
                bytes, "authorityRecord.bytes",
                VersionedDatasetHandoffLimits.MAX_RECORD_BYTES);
        this.versionToken = VersionedDatasetHandoffCodec.sha256Hex(this.bytes);
    }

    public byte[] bytes() { return bytes.clone(); }
    public String versionToken() { return versionToken; }
}
