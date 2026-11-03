package io.rowweave;

import java.io.ObjectStreamException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Immutable identity and replay contract for one application-provided bounded batch split. */
public final class BatchSourceSplitDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final int CURRENT_VERSION = 1;
    public static final int MAX_CONNECTOR_TYPE_BYTES = 128;
    public static final int MAX_SPLIT_ID_BYTES = 256;
    public static final int MAX_SNAPSHOT_ID_BYTES = 1024;

    public enum ReplayGuarantee {
        EXACT_ORDERED_FROM_START(1),
        LEGACY_SINGLE_ATTEMPT(2);

        private final int wireId;

        ReplayGuarantee(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static ReplayGuarantee fromWire(int wireId) {
            for (ReplayGuarantee value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw new IllegalArgumentException(
                    "batch source replayGuarantee is unknown: " + wireId);
        }
    }

    private final int descriptorVersion;
    private final String connectorType;
    private final int connectorVersion;
    private final String splitId;
    private final String schemaFingerprint;
    private final String snapshotId;
    private final ReplayGuarantee replayGuarantee;

    public BatchSourceSplitDescriptor(
            int descriptorVersion, String connectorType, int connectorVersion,
            String splitId, String schemaFingerprint, String snapshotId,
            ReplayGuarantee replayGuarantee) {
        if (descriptorVersion != CURRENT_VERSION) {
            throw new IllegalArgumentException(
                    "batch source descriptorVersion must be " + CURRENT_VERSION);
        }
        this.connectorType = boundedText(
                connectorType, "connectorType", MAX_CONNECTOR_TYPE_BYTES);
        if (connectorVersion <= 0) {
            throw new IllegalArgumentException("batch source connectorVersion must be positive");
        }
        this.splitId = boundedText(splitId, "splitId", MAX_SPLIT_ID_BYTES);
        if (schemaFingerprint == null
                || !schemaFingerprint.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "batch source schemaFingerprint must be lowercase SHA-256 hex");
        }
        this.snapshotId = boundedText(snapshotId, "snapshotId", MAX_SNAPSHOT_ID_BYTES);
        if (replayGuarantee == null) {
            throw new IllegalArgumentException("batch source replayGuarantee is required");
        }
        this.descriptorVersion = descriptorVersion;
        this.connectorVersion = connectorVersion;
        this.schemaFingerprint = schemaFingerprint;
        this.replayGuarantee = replayGuarantee;
    }

    private static String boundedText(String value, String field, int maxBytes) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException("batch source " + field + " must not be empty");
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
                    "batch source " + field + " is not valid UTF-8", invalid);
        }
        if (encodedBytes > maxBytes) {
            throw new IllegalArgumentException(
                    "batch source " + field + " exceeds UTF-8 byte limit " + maxBytes);
        }
        return value;
    }

    private Object readResolve() throws ObjectStreamException {
        return new BatchSourceSplitDescriptor(
                descriptorVersion, connectorType, connectorVersion, splitId,
                schemaFingerprint, snapshotId, replayGuarantee);
    }

    public int descriptorVersion() { return descriptorVersion; }
    public String connectorType() { return connectorType; }
    public int connectorVersion() { return connectorVersion; }
    public String splitId() { return splitId; }
    public String schemaFingerprint() { return schemaFingerprint; }
    public String snapshotId() { return snapshotId; }
    public ReplayGuarantee replayGuarantee() { return replayGuarantee; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof BatchSourceSplitDescriptor)) return false;
        BatchSourceSplitDescriptor that = (BatchSourceSplitDescriptor) other;
        return descriptorVersion == that.descriptorVersion
                && connectorVersion == that.connectorVersion
                && connectorType.equals(that.connectorType)
                && splitId.equals(that.splitId)
                && schemaFingerprint.equals(that.schemaFingerprint)
                && snapshotId.equals(that.snapshotId)
                && replayGuarantee == that.replayGuarantee;
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptorVersion, connectorType, connectorVersion,
                splitId, schemaFingerprint, snapshotId, replayGuarantee);
    }

    @Override
    public String toString() {
        return "BatchSourceSplitDescriptor{" + connectorType + ':' + splitId
                + ", replay=" + replayGuarantee + '}';
    }
}
