package io.rowweave;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Credential-free immutable identity of one sealed quality publication member. */
public final class QualityPreparedMemberEvidence {
    private static final int DIGEST_BYTES = 32;

    private final String role;
    private final String memberName;
    private final long dataLength;
    private final byte[] dataHash;
    private final byte[] preparedLocatorDigest;

    QualityPreparedMemberEvidence(String role, String memberName, long dataLength,
                                  byte[] dataHash, byte[] preparedLocatorDigest) {
        if (!TypedQualityManifest.VALID_SINK_UID.equals(role)
                && !TypedQualityManifest.REJECTED_SINK_UID.equals(role)
                && !TypedQualityManifest.SUMMARY_SINK_UID.equals(role)) {
            throw new IllegalArgumentException("quality member role is not reserved");
        }
        if (memberName == null || memberName.isEmpty()) {
            throw new IllegalArgumentException("quality member name is required");
        }
        if (dataLength < 0L) {
            throw new IllegalArgumentException("quality member data length is negative");
        }
        requireDigest(dataHash, "data hash");
        requireDigest(preparedLocatorDigest, "prepared locator digest");
        this.role = role;
        this.memberName = memberName;
        this.dataLength = dataLength;
        this.dataHash = dataHash.clone();
        this.preparedLocatorDigest = preparedLocatorDigest.clone();
    }

    public String role() { return role; }
    public String memberName() { return memberName; }
    public long dataLength() { return dataLength; }
    public byte[] dataHash() { return dataHash.clone(); }
    public byte[] preparedLocatorDigest() { return preparedLocatorDigest.clone(); }

    private static void requireDigest(byte[] value, String field) {
        if (value == null || value.length != DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof QualityPreparedMemberEvidence)) return false;
        QualityPreparedMemberEvidence that = (QualityPreparedMemberEvidence) other;
        return dataLength == that.dataLength && role.equals(that.role)
                && memberName.equals(that.memberName)
                && MessageDigest.isEqual(dataHash, that.dataHash)
                && MessageDigest.isEqual(preparedLocatorDigest,
                that.preparedLocatorDigest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(role, memberName, dataLength);
        result = 31 * result + Arrays.hashCode(dataHash);
        return 31 * result + Arrays.hashCode(preparedLocatorDigest);
    }
}
