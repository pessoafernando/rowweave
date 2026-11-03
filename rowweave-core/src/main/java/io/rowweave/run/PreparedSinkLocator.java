package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Objects;

/** Durable, credential-free locator for one immutable prepared Sink result. */
public final class PreparedSinkLocator {

    public static final int CURRENT_VERSION = 2;
    public static final int LEGACY_MANIFEST_VERSION = 1;
    public static final int MAX_PAYLOAD_BYTES = 4 * 1024;
    private static final int DIGEST_BYTES = 32;
    private static final DurableSinkProviderKey LEGACY_MANIFEST_PROVIDER =
            new DurableSinkProviderKey("rowweave.manifest-file", 1,
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
    private static final DurableSinkCodecDescriptor LEGACY_MANIFEST_CODEC =
            new DurableSinkCodecDescriptor(
                    "rowweave.manifest-member", 1, MAX_PAYLOAD_BYTES);

    private final int locatorVersion;
    private final SinkAttemptIdentity attemptIdentity;
    private final BatchSinkDescriptor.CapabilityProfile capabilityProfile;
    private final String commitDomainId;
    private final DurableSinkProviderKey providerKey;
    private final DurableSinkCodecDescriptor locatorCodec;
    private final byte[] payload;
    private final byte[] payloadDigest;
    private final long dataLength;
    private final byte[] dataHash;
    private final long preparedAtMillis;

    public PreparedSinkLocator(
            int locatorVersion, SinkAttemptIdentity attemptIdentity,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile,
            String commitDomainId, byte[] payload, long dataLength,
            byte[] dataHash, long preparedAtMillis) {
        this(locatorVersion, attemptIdentity, capabilityProfile, commitDomainId,
                LEGACY_MANIFEST_PROVIDER, LEGACY_MANIFEST_CODEC, payload,
                sha256(payload), dataLength, dataHash, preparedAtMillis);
    }

    public PreparedSinkLocator(
            int locatorVersion, SinkAttemptIdentity attemptIdentity,
            BatchSinkDescriptor.CapabilityProfile capabilityProfile,
            String commitDomainId, DurableSinkProviderKey providerKey,
            DurableSinkCodecDescriptor locatorCodec, byte[] payload,
            byte[] payloadDigest, long dataLength,
            byte[] dataHash, long preparedAtMillis) {
        if (locatorVersion != LEGACY_MANIFEST_VERSION
                && locatorVersion != CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "prepared sink locatorVersion is unsupported");
        }
        if (attemptIdentity == null) {
            throw new IllegalArgumentException("sink attempt identity is required");
        }
        if (capabilityProfile == null
                || capabilityProfile != attemptIdentity.capabilityProfile()) {
            throw new IllegalArgumentException(
                    "prepared locator capability does not match attempt identity");
        }
        if (!attemptIdentity.commitDomainId().equals(commitDomainId)) {
            throw new IllegalArgumentException(
                    "prepared locator commit domain does not match attempt identity");
        }
        if (payload == null || payload.length < 1
                || payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException("prepared locator payload length out of bounds");
        }
        if (providerKey == null || locatorCodec == null
                || providerKey.capabilityProfile() != capabilityProfile) {
            throw new IllegalArgumentException(
                    "prepared locator provider identity is invalid");
        }
        if (locatorVersion == LEGACY_MANIFEST_VERSION
                && (!LEGACY_MANIFEST_PROVIDER.equals(providerKey)
                || !LEGACY_MANIFEST_CODEC.equals(locatorCodec))) {
            throw new IllegalArgumentException(
                    "legacy prepared locator is reserved for built-in Manifest");
        }
        if (payload.length > locatorCodec.maxPayloadBytes()) {
            throw new IllegalArgumentException(
                    "prepared locator payload exceeds codec bound");
        }
        if (payloadDigest == null || payloadDigest.length != DIGEST_BYTES
                || !MessageDigest.isEqual(payloadDigest, sha256(payload))) {
            throw new IllegalArgumentException(
                    "prepared locator payload digest mismatch");
        }
        if (dataLength < 0L) {
            throw new IllegalArgumentException("prepared locator dataLength must be >= 0");
        }
        if (dataHash == null || dataHash.length != DIGEST_BYTES) {
            throw new IllegalArgumentException("prepared locator dataHash must be SHA-256");
        }
        if (preparedAtMillis < 0L) {
            throw new IllegalArgumentException("preparedAtMillis must be >= 0");
        }
        this.locatorVersion = locatorVersion;
        this.attemptIdentity = attemptIdentity;
        this.capabilityProfile = capabilityProfile;
        this.commitDomainId = commitDomainId;
        this.providerKey = providerKey;
        this.locatorCodec = locatorCodec;
        this.payload = payload.clone();
        this.payloadDigest = payloadDigest.clone();
        this.dataLength = dataLength;
        this.dataHash = dataHash.clone();
        this.preparedAtMillis = preparedAtMillis;
    }

    public int locatorVersion() { return locatorVersion; }
    public SinkAttemptIdentity attemptIdentity() { return attemptIdentity; }
    public BatchSinkDescriptor.CapabilityProfile capabilityProfile() {
        return capabilityProfile;
    }
    public String commitDomainId() { return commitDomainId; }
    public DurableSinkProviderKey providerKey() { return providerKey; }
    public DurableSinkCodecDescriptor locatorCodec() { return locatorCodec; }
    public byte[] payload() { return payload.clone(); }
    public byte[] payloadDigest() { return payloadDigest.clone(); }
    public long dataLength() { return dataLength; }
    public byte[] dataHash() { return dataHash.clone(); }
    public long preparedAtMillis() { return preparedAtMillis; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PreparedSinkLocator)) return false;
        PreparedSinkLocator that = (PreparedSinkLocator) other;
        return locatorVersion == that.locatorVersion
                && dataLength == that.dataLength
                && preparedAtMillis == that.preparedAtMillis
                && attemptIdentity.equals(that.attemptIdentity)
                && capabilityProfile == that.capabilityProfile
                && commitDomainId.equals(that.commitDomainId)
                && providerKey.equals(that.providerKey)
                && locatorCodec.equals(that.locatorCodec)
                && MessageDigest.isEqual(payload, that.payload)
                && MessageDigest.isEqual(payloadDigest, that.payloadDigest)
                && MessageDigest.isEqual(dataHash, that.dataHash);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(locatorVersion, attemptIdentity,
                capabilityProfile, commitDomainId, providerKey, locatorCodec,
                dataLength, preparedAtMillis);
        result = 31 * result + Arrays.hashCode(payload);
        result = 31 * result + Arrays.hashCode(payloadDigest);
        return 31 * result + Arrays.hashCode(dataHash);
    }

    /** Deliberately excludes domain and connector payload. */
    @Override
    public String toString() {
        byte[] digest = attemptIdentity.identityDigest();
        StringBuilder prefix = new StringBuilder(16);
        char[] hex = "0123456789abcdef".toCharArray();
        for (int i = 0; i < 8; i++) {
            prefix.append(hex[(digest[i] >>> 4) & 0xf])
                    .append(hex[digest[i] & 0xf]);
        }
        return "PreparedSinkLocator{" + providerKey
                + ", identity=" + prefix + "}";
    }

    private static byte[] sha256(byte[] value) {
        if (value == null) return null;
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
