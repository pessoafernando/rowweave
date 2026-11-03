package io.rowweave.upgrade;

import java.util.Objects;

/** Immutable identity and active revision of one durable store namespace. */
public final class StoreFormatHeader {
    public static final int VERSION_1 = 1;

    private final int formatVersion;
    private final String storeId;
    private final int protocolEpoch;
    private final int activeRevision;
    private final String manifestDigest;

    public StoreFormatHeader(int formatVersion, String storeId,
                             int protocolEpoch, int activeRevision,
                             String manifestDigest) {
        if (formatVersion != VERSION_1) {
            throw new IllegalArgumentException("unsupported store header version");
        }
        UpgradeCodecSupport.requireText(storeId, "store id");
        if (protocolEpoch < 1) {
            throw new IllegalArgumentException("protocol epoch must be positive");
        }
        if (activeRevision < 0) {
            throw new IllegalArgumentException("active revision must be non-negative");
        }
        UpgradeCodecSupport.requireDigest(manifestDigest, "manifest digest");
        this.formatVersion = formatVersion;
        this.storeId = storeId;
        this.protocolEpoch = protocolEpoch;
        this.activeRevision = activeRevision;
        this.manifestDigest = manifestDigest;
    }

    public int formatVersion() { return formatVersion; }
    public String storeId() { return storeId; }
    public int protocolEpoch() { return protocolEpoch; }
    public int activeRevision() { return activeRevision; }
    public String manifestDigest() { return manifestDigest; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof StoreFormatHeader)) return false;
        StoreFormatHeader that = (StoreFormatHeader) other;
        return formatVersion == that.formatVersion
                && protocolEpoch == that.protocolEpoch
                && activeRevision == that.activeRevision
                && storeId.equals(that.storeId)
                && manifestDigest.equals(that.manifestDigest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(formatVersion, storeId, protocolEpoch,
                activeRevision, manifestDigest);
    }
}
