package io.rowweave.incremental;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Immutable changelog and source contract carried by the incremental SOURCE. */
public final class IncrementalPlanDescriptor {
    public static final int DIGEST_BYTES = 32;
    public static final int MAX_OPERATOR_UID_CODE_UNITS = 256;
    public static final int MAX_OPERATOR_UID_BYTES = 256;
    public static final int MAX_FIELD_INDEXES = 4_096;

    public enum SinkCommitProfile {
        MANIFEST_ATOMIC_V1(1);

        private final int wireId;

        SinkCommitProfile(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static SinkCommitProfile fromWire(int wireId) {
            for (SinkCommitProfile value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw new IllegalArgumentException("unknown incremental sink commit profile");
        }
    }

    private final String sourceOperatorUid;
    private final byte[] sourceOperatorUidBytes;
    private final byte[] frontierContractDigest;
    private final List<Integer> primaryKeyFieldIndexes;
    private final int operationFieldIndex;
    private final List<Integer> recordPositionFieldIndexes;
    private final byte[] outputSchemaFingerprint;
    private final boolean deleteCapable;
    private final byte[] partitionSetDigest;
    private final SinkCommitProfile sinkCommitProfile;

    public IncrementalPlanDescriptor(
            String sourceOperatorUid, byte[] frontierContractDigest,
            List<Integer> primaryKeyFieldIndexes, int operationFieldIndex,
            List<Integer> recordPositionFieldIndexes, byte[] outputSchemaFingerprint,
            boolean deleteCapable, byte[] partitionSetDigest,
            SinkCommitProfile sinkCommitProfile) {
        this.sourceOperatorUidBytes = canonicalText(sourceOperatorUid);
        this.sourceOperatorUid = sourceOperatorUid;
        this.frontierContractDigest = requireDigest(frontierContractDigest, "frontier contract digest");
        this.primaryKeyFieldIndexes = immutableIndexes(
                primaryKeyFieldIndexes, "primary key", true);
        this.operationFieldIndex = requireIndex(operationFieldIndex, "operation field");
        this.recordPositionFieldIndexes = immutableIndexes(
                recordPositionFieldIndexes, "record position", true);
        this.outputSchemaFingerprint = requireDigest(
                outputSchemaFingerprint, "output schema fingerprint");
        this.deleteCapable = deleteCapable;
        this.partitionSetDigest = requireDigest(partitionSetDigest, "partition set digest");
        if (sinkCommitProfile == null) {
            throw new IllegalArgumentException("sink commit profile is required");
        }
        this.sinkCommitProfile = sinkCommitProfile;
        Set<Integer> occupied = new HashSet<>(this.primaryKeyFieldIndexes);
        if (!occupied.add(operationFieldIndex)) {
            throw new IllegalArgumentException("operation field overlaps primary key");
        }
        for (Integer position : this.recordPositionFieldIndexes) {
            if (!occupied.add(position)) {
                throw new IllegalArgumentException("record position field overlaps another reserved field");
            }
        }
    }

    public String sourceOperatorUid() {
        return sourceOperatorUid;
    }

    byte[] sourceOperatorUidBytes() {
        return sourceOperatorUidBytes.clone();
    }

    public byte[] frontierContractDigest() {
        return frontierContractDigest.clone();
    }

    public List<Integer> primaryKeyFieldIndexes() {
        return primaryKeyFieldIndexes;
    }

    public int operationFieldIndex() {
        return operationFieldIndex;
    }

    public List<Integer> recordPositionFieldIndexes() {
        return recordPositionFieldIndexes;
    }

    public byte[] outputSchemaFingerprint() {
        return outputSchemaFingerprint.clone();
    }

    public boolean deleteCapable() {
        return deleteCapable;
    }

    public byte[] partitionSetDigest() {
        return partitionSetDigest.clone();
    }

    public SinkCommitProfile sinkCommitProfile() {
        return sinkCommitProfile;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalPlanDescriptor)) return false;
        IncrementalPlanDescriptor that = (IncrementalPlanDescriptor) other;
        return operationFieldIndex == that.operationFieldIndex
                && deleteCapable == that.deleteCapable
                && sourceOperatorUid.equals(that.sourceOperatorUid)
                && primaryKeyFieldIndexes.equals(that.primaryKeyFieldIndexes)
                && recordPositionFieldIndexes.equals(that.recordPositionFieldIndexes)
                && sinkCommitProfile == that.sinkCommitProfile
                && Arrays.equals(frontierContractDigest, that.frontierContractDigest)
                && Arrays.equals(outputSchemaFingerprint, that.outputSchemaFingerprint)
                && Arrays.equals(partitionSetDigest, that.partitionSetDigest);
    }

    @Override
    public int hashCode() {
        int result = sourceOperatorUid.hashCode();
        result = 31 * result + Arrays.hashCode(frontierContractDigest);
        result = 31 * result + primaryKeyFieldIndexes.hashCode();
        result = 31 * result + operationFieldIndex;
        result = 31 * result + recordPositionFieldIndexes.hashCode();
        result = 31 * result + Arrays.hashCode(outputSchemaFingerprint);
        result = 31 * result + (deleteCapable ? 1 : 0);
        result = 31 * result + Arrays.hashCode(partitionSetDigest);
        result = 31 * result + sinkCommitProfile.hashCode();
        return result;
    }

    private static List<Integer> immutableIndexes(
            List<Integer> indexes, String field, boolean requireNonEmpty) {
        if (indexes == null || indexes.size() > MAX_FIELD_INDEXES
                || (requireNonEmpty && indexes.isEmpty())) {
            throw new IllegalArgumentException(field + " field indexes have invalid count");
        }
        List<Integer> copy = new ArrayList<>(indexes.size());
        Set<Integer> unique = new HashSet<>();
        for (Integer index : indexes) {
            if (index == null || !unique.add(requireIndex(index, field))) {
                throw new IllegalArgumentException(field + " field indexes must be unique");
            }
            copy.add(index);
        }
        return Collections.unmodifiableList(copy);
    }

    private static int requireIndex(int index, String field) {
        if (index < 0 || index >= MAX_FIELD_INDEXES) {
            throw new IllegalArgumentException(field + " index is out of range");
        }
        return index;
    }

    private static byte[] requireDigest(byte[] value, String field) {
        if (value == null || value.length != DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
        return value.clone();
    }

    private static byte[] canonicalText(String value) {
        if (value == null || value.isEmpty()
                || value.length() > MAX_OPERATOR_UID_CODE_UNITS
                || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException("source operator UID is not canonical bounded text");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > MAX_OPERATOR_UID_BYTES) {
                throw new IllegalArgumentException("source operator UID exceeds byte limit");
            }
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException("source operator UID is invalid UTF-16", malformed);
        }
    }
}
