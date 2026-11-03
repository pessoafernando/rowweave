package io.rowweave;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Immutable bounded-batch Sink identity and publication capability request. */
public final class BatchSinkDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int CURRENT_VERSION = 1;
    public static final int MAX_CONNECTOR_TYPE_BYTES = 128;
    public static final int MAX_DESTINATION_ID_BYTES = 1024;
    public static final int MAX_COMMIT_DOMAIN_ID_BYTES = 512;
    public static final int MAX_BINDING_IDENTITY_BYTES = 256;
    public static final String NO_BINDING_IDENTITY = "none";

    /** Closed profiles prevent contradictory combinations of boolean capability claims. */
    public enum CapabilityProfile {
        LEGACY_PROCESS_LOCAL(1),
        SIDE_EFFECT_FREE_V1(2),
        MANIFEST_ATOMIC_GROUP_V1(3);

        private final int wireId;

        CapabilityProfile(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static CapabilityProfile fromWire(int wireId) {
            for (CapabilityProfile value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw new IllegalArgumentException(
                    "batch sink capabilityProfile is unknown: " + wireId);
        }
    }

    private final int descriptorVersion;
    private final String connectorType;
    private final int connectorVersion;
    private final String schemaFingerprint;
    private final String destinationId;
    private final String commitDomainId;
    private final CapabilityProfile capabilityProfile;
    private final String bindingIdentity;

    public BatchSinkDescriptor(
            int descriptorVersion, String connectorType, int connectorVersion,
            String schemaFingerprint, String destinationId, String commitDomainId,
            CapabilityProfile capabilityProfile) {
        this(descriptorVersion, connectorType, connectorVersion, schemaFingerprint,
                destinationId, commitDomainId, capabilityProfile, NO_BINDING_IDENTITY);
    }

    public BatchSinkDescriptor(
            int descriptorVersion, String connectorType, int connectorVersion,
            String schemaFingerprint, String destinationId, String commitDomainId,
            CapabilityProfile capabilityProfile, String bindingIdentity) {
        if (descriptorVersion != CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "batch sink descriptorVersion must be " + CURRENT_VERSION);
        }
        this.connectorType = boundedText(
                connectorType, "connectorType", MAX_CONNECTOR_TYPE_BYTES);
        if (connectorVersion <= 0) {
            throw new IllegalArgumentException("batch sink connectorVersion must be positive");
        }
        if (schemaFingerprint == null
                || !schemaFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "batch sink schemaFingerprint must be lowercase SHA-256 hex");
        }
        this.destinationId = boundedText(
                destinationId, "destinationId", MAX_DESTINATION_ID_BYTES);
        this.commitDomainId = boundedText(
                commitDomainId, "commitDomainId", MAX_COMMIT_DOMAIN_ID_BYTES);
        if (capabilityProfile == null) {
            throw new IllegalArgumentException("batch sink capabilityProfile is required");
        }
        this.descriptorVersion = descriptorVersion;
        this.connectorVersion = connectorVersion;
        this.schemaFingerprint = schemaFingerprint;
        this.capabilityProfile = capabilityProfile;
        this.bindingIdentity = boundedText(
                bindingIdentity, "bindingIdentity", MAX_BINDING_IDENTITY_BYTES);
    }

    private static String boundedText(String value, String field, int maxBytes) {
        if (value == null || value.isEmpty() || value.indexOf('\0') >= 0) {
            throw new IllegalArgumentException("batch sink " + field + " must not be empty");
        }
        int encodedBytes;
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            encodedBytes = encoded.remaining();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "batch sink " + field + " is not valid UTF-8", invalid);
        }
        if (encodedBytes > maxBytes) {
            throw new IllegalArgumentException(
                    "batch sink " + field + " exceeds UTF-8 byte limit " + maxBytes);
        }
        return value;
    }

    private Object readResolve() throws ObjectStreamException {
        return new BatchSinkDescriptor(
                descriptorVersion, connectorType, connectorVersion,
                schemaFingerprint, destinationId, commitDomainId, capabilityProfile,
                bindingIdentity);
    }

    public int descriptorVersion() { return descriptorVersion; }
    public String connectorType() { return connectorType; }
    public int connectorVersion() { return connectorVersion; }
    public String schemaFingerprint() { return schemaFingerprint; }
    public String destinationId() { return destinationId; }
    public String commitDomainId() { return commitDomainId; }
    public CapabilityProfile capabilityProfile() { return capabilityProfile; }
    public String bindingIdentity() { return bindingIdentity; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof BatchSinkDescriptor)) return false;
        BatchSinkDescriptor that = (BatchSinkDescriptor) other;
        return descriptorVersion == that.descriptorVersion
                && connectorVersion == that.connectorVersion
                && connectorType.equals(that.connectorType)
                && schemaFingerprint.equals(that.schemaFingerprint)
                && destinationId.equals(that.destinationId)
                && commitDomainId.equals(that.commitDomainId)
                && capabilityProfile == that.capabilityProfile
                && bindingIdentity.equals(that.bindingIdentity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptorVersion, connectorType, connectorVersion,
                schemaFingerprint, destinationId, commitDomainId, capabilityProfile,
                bindingIdentity);
    }

    /** Deliberately excludes destination/domain identities. */
    @Override
    public String toString() {
        return "BatchSinkDescriptor{" + connectorType
                + ", capability=" + capabilityProfile + '}';
    }
}
