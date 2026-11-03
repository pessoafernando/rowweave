package io.rowweave.incremental;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Canonical feed definition template; deliberately excludes interval and capture state. */
public final class IncrementalDefinition {
    public static final int DIGEST_BYTES = 32;
    public static final int MAX_TEXT_CODE_UNITS = 256;
    public static final int MAX_TEXT_BYTES = 256;
    static final int MIN_OPERATOR_WIRE_BYTES = 4 + 1 + 1 + 4 + 1 + 32 + 4 + 32;
    public static final int MAX_OPERATORS =
            IncrementalDefinitionCodec.MAX_FRAME_BYTES / MIN_OPERATOR_WIRE_BYTES;
    public static final int MAX_SINK_BINDINGS = 4_094;

    public enum OperatorKind {
        SOURCE_V1(1), MAP_V1(2), SINK_V1(3);

        private final int wireId;

        OperatorKind(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static OperatorKind fromWire(int wireId) {
            for (OperatorKind value : values()) if (value.wireId == wireId) return value;
            throw new IllegalArgumentException("unknown incremental operator kind");
        }
    }

    public static final class OperatorBinding {
        private final String operatorUid;
        private final byte[] operatorUidBytes;
        private final OperatorKind kind;
        private final String typeId;
        private final byte[] typeIdBytes;
        private final byte[] schemaFingerprint;
        private final List<String> inputOperatorUids;
        private final List<byte[]> inputOperatorUidBytes;
        private final byte[] implementationDigest;

        public OperatorBinding(
                String operatorUid, OperatorKind kind, String typeId,
                byte[] schemaFingerprint, List<String> inputOperatorUids,
                byte[] implementationDigest) {
            this.operatorUidBytes = canonicalText(operatorUid, "operator UID");
            this.operatorUid = operatorUid;
            if (kind == null) throw new IllegalArgumentException("operator kind is required");
            this.kind = kind;
            this.typeIdBytes = canonicalText(typeId, "operator type");
            this.typeId = typeId;
            this.schemaFingerprint = requireDigest(schemaFingerprint, "operator schema fingerprint");
            if (inputOperatorUids == null || inputOperatorUids.size() > 1) {
                throw new IllegalArgumentException("incremental operator must have at most one input");
            }
            if (kind == OperatorKind.SOURCE_V1 && !inputOperatorUids.isEmpty()) {
                throw new IllegalArgumentException("incremental SOURCE cannot have an input");
            }
            if (kind != OperatorKind.SOURCE_V1 && inputOperatorUids.size() != 1) {
                throw new IllegalArgumentException("incremental MAP/SINK requires exactly one input");
            }
            List<String> inputs = new ArrayList<>(inputOperatorUids.size());
            List<byte[]> encodedInputs = new ArrayList<>(inputOperatorUids.size());
            for (String input : inputOperatorUids) {
                encodedInputs.add(canonicalText(input, "input operator UID"));
                inputs.add(input);
            }
            this.inputOperatorUids = Collections.unmodifiableList(inputs);
            this.inputOperatorUidBytes = Collections.unmodifiableList(encodedInputs);
            this.implementationDigest = requireDigest(
                    implementationDigest, "operator implementation digest");
        }

        public String operatorUid() { return operatorUid; }
        byte[] operatorUidBytes() { return operatorUidBytes.clone(); }
        public OperatorKind kind() { return kind; }
        public String typeId() { return typeId; }
        byte[] typeIdBytes() { return typeIdBytes.clone(); }
        public byte[] schemaFingerprint() { return schemaFingerprint.clone(); }
        public List<String> inputOperatorUids() { return inputOperatorUids; }
        byte[] inputOperatorUidBytes(int index) { return inputOperatorUidBytes.get(index).clone(); }
        public byte[] implementationDigest() { return implementationDigest.clone(); }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof OperatorBinding)) return false;
            OperatorBinding that = (OperatorBinding) other;
            return kind == that.kind
                    && operatorUid.equals(that.operatorUid)
                    && typeId.equals(that.typeId)
                    && inputOperatorUids.equals(that.inputOperatorUids)
                    && Arrays.equals(schemaFingerprint, that.schemaFingerprint)
                    && Arrays.equals(implementationDigest, that.implementationDigest);
        }

        @Override
        public int hashCode() {
            int result = operatorUid.hashCode();
            result = 31 * result + kind.hashCode();
            result = 31 * result + typeId.hashCode();
            result = 31 * result + Arrays.hashCode(schemaFingerprint);
            result = 31 * result + inputOperatorUids.hashCode();
            result = 31 * result + Arrays.hashCode(implementationDigest);
            return result;
        }
    }

    private final String definitionId;
    private final byte[] definitionIdBytes;
    private final IncrementalPlanDescriptor planDescriptor;
    private final List<OperatorBinding> operators;
    private final List<byte[]> sinkBindingDigests;
    private final byte[] fingerprint;

    public IncrementalDefinition(
            String definitionId, IncrementalPlanDescriptor planDescriptor,
            List<OperatorBinding> operators, List<byte[]> sinkBindingDigests) {
        this.definitionIdBytes = canonicalText(definitionId, "definition ID");
        this.definitionId = definitionId;
        if (planDescriptor == null) {
            throw new IllegalArgumentException("incremental plan descriptor is required");
        }
        this.planDescriptor = planDescriptor;
        if (operators == null || operators.isEmpty() || operators.size() > MAX_OPERATORS) {
            throw new IllegalArgumentException("incremental operator count is invalid");
        }
        List<OperatorBinding> copiedOperators = new ArrayList<>(operators.size());
        Set<String> allIds = new HashSet<>();
        Set<String> preceding = new HashSet<>();
        int sinks = 0;
        boolean sinksStarted = false;
        String linearPredecessor = null;
        byte[] expectedSchema = planDescriptor.outputSchemaFingerprint();
        for (int operatorIndex = 0; operatorIndex < operators.size(); operatorIndex++) {
            OperatorBinding operator = operators.get(operatorIndex);
            if (operator == null || !allIds.add(operator.operatorUid())) {
                throw new IllegalArgumentException("incremental operator UID is null or duplicated");
            }
            for (String input : operator.inputOperatorUids()) {
                if (!preceding.contains(input)) {
                    throw new IllegalArgumentException("incremental topology is not canonical acyclic order");
                }
            }
            if (!MessageDigest.isEqual(expectedSchema, operator.schemaFingerprint())) {
                throw new IllegalArgumentException("incremental operator schema differs from descriptor");
            }
            if (operator.kind() == OperatorKind.SOURCE_V1) {
                if (operatorIndex != 0
                        || !operator.operatorUid().equals(planDescriptor.sourceOperatorUid())) {
                    throw new IllegalArgumentException("definition SOURCE differs from descriptor");
                }
                linearPredecessor = operator.operatorUid();
            } else if (operator.kind() == OperatorKind.MAP_V1) {
                if (sinksStarted
                        || !operator.inputOperatorUids().get(0).equals(linearPredecessor)) {
                    throw new IllegalArgumentException(
                            "incremental MAP operators must form one canonical chain");
                }
                linearPredecessor = operator.operatorUid();
            } else if (operator.kind() == OperatorKind.SINK_V1) {
                if (!operator.inputOperatorUids().get(0).equals(linearPredecessor)) {
                    throw new IllegalArgumentException(
                            "incremental Sink must consume the final SOURCE/MAP directly");
                }
                sinksStarted = true;
                sinks++;
            }
            copiedOperators.add(operator);
            preceding.add(operator.operatorUid());
        }
        if (sinkBindingDigests == null || sinkBindingDigests.isEmpty()
                || sinkBindingDigests.size() > MAX_SINK_BINDINGS
                || sinks != sinkBindingDigests.size()) {
            throw new IllegalArgumentException("definition Sink bindings do not match Sink operators");
        }
        List<byte[]> copiedSinks = new ArrayList<>(sinkBindingDigests.size());
        for (byte[] digest : sinkBindingDigests) {
            copiedSinks.add(requireDigest(digest, "Sink binding digest"));
        }
        this.operators = Collections.unmodifiableList(copiedOperators);
        this.sinkBindingDigests = Collections.unmodifiableList(copiedSinks);
        this.fingerprint = IncrementalDefinitionCodec.sha256(
                IncrementalDefinitionCodec.encode(this));
    }

    public String definitionId() { return definitionId; }
    byte[] definitionIdBytes() { return definitionIdBytes.clone(); }
    public IncrementalPlanDescriptor planDescriptor() { return planDescriptor; }
    public List<OperatorBinding> operators() { return operators; }
    public List<byte[]> sinkBindingDigests() {
        List<byte[]> copy = new ArrayList<>(sinkBindingDigests.size());
        for (byte[] digest : sinkBindingDigests) copy.add(digest.clone());
        return Collections.unmodifiableList(copy);
    }
    byte[] sinkBindingDigest(int index) { return sinkBindingDigests.get(index).clone(); }
    int sinkBindingCount() { return sinkBindingDigests.size(); }
    public byte[] fingerprint() { return fingerprint.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof IncrementalDefinition)) return false;
        IncrementalDefinition that = (IncrementalDefinition) other;
        if (!definitionId.equals(that.definitionId)
                || !planDescriptor.equals(that.planDescriptor)
                || !operators.equals(that.operators)
                || sinkBindingDigests.size() != that.sinkBindingDigests.size()) return false;
        for (int i = 0; i < sinkBindingDigests.size(); i++) {
            if (!Arrays.equals(sinkBindingDigests.get(i), that.sinkBindingDigests.get(i))) return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = definitionId.hashCode();
        result = 31 * result + planDescriptor.hashCode();
        result = 31 * result + operators.hashCode();
        for (byte[] digest : sinkBindingDigests) result = 31 * result + Arrays.hashCode(digest);
        return result;
    }

    private static byte[] requireDigest(byte[] value, String field) {
        if (value == null || value.length != DIGEST_BYTES) {
            throw new IllegalArgumentException(field + " must be SHA-256");
        }
        return value.clone();
    }

    private static byte[] canonicalText(String value, String field) {
        if (value == null || value.isEmpty() || value.length() > MAX_TEXT_CODE_UNITS
                || value.indexOf('\0') >= 0
                || !Normalizer.isNormalized(value, Normalizer.Form.NFC)) {
            throw new IllegalArgumentException(field + " is not canonical bounded text");
        }
        try {
            ByteBuffer encoded = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value));
            if (encoded.remaining() > MAX_TEXT_BYTES) {
                throw new IllegalArgumentException(field + " exceeds byte limit");
            }
            byte[] bytes = new byte[encoded.remaining()];
            encoded.get(bytes);
            return bytes;
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException(field + " is invalid UTF-16", malformed);
        }
    }
}
