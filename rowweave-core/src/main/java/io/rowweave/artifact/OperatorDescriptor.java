package io.rowweave.artifact;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Public operator description that does not depend on PlanGraph array indices. */
public final class OperatorDescriptor {

    public enum Kind {
        SOURCE(1), UNARY(2), BINARY(3), SINK(4);

        private final int wireId;

        Kind(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static Kind fromWire(int id, String path) {
            for (Kind value : values()) {
                if (value.wireId == id) {
                    return value;
                }
            }
            throw ArtifactText.failure("INVALID_SEMANTICS", path, "unknown operator kind: " + id);
        }
    }

    private final String operatorId;
    private final Kind kind;
    private final String typeId;
    private final String schemaFingerprint;
    private final List<String> inputOperatorIds;

    public OperatorDescriptor(
            String operatorId, Kind kind, String typeId, String schemaFingerprint,
            List<String> inputOperatorIds) {
        ArtifactText.encode(operatorId, "operator.operatorId", PlanArtifactLimits.MAX_TEXT_BYTES);
        ArtifactText.encode(typeId, "operator.typeId", PlanArtifactLimits.MAX_TEXT_BYTES);
        ArtifactText.encode(schemaFingerprint, "operator.schemaFingerprint",
                PlanArtifactLimits.MAX_SCHEMA_FINGERPRINT_BYTES);
        if (!schemaFingerprint.matches("[0-9a-f]{64}")) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "operator.schemaFingerprint",
                    "schema fingerprint must be 64 lowercase hex characters");
        }
        if (kind == null) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "operator.kind", "operator kind is required");
        }
        if (inputOperatorIds == null || inputOperatorIds.size() > PlanArtifactLimits.MAX_OPERATOR_INPUTS) {
            throw ArtifactText.failure("LIMIT_EXCEEDED", "operator.inputOperatorIds",
                    "operator input count exceeds limit");
        }
        int expectedInputs = kind == Kind.SOURCE ? 0 : kind == Kind.BINARY ? 2 : 1;
        if (inputOperatorIds.size() != expectedInputs) {
            throw ArtifactText.failure("INVALID_SEMANTICS", "operator.inputOperatorIds",
                    kind + " requires " + expectedInputs + " inputs");
        }
        List<String> copiedInputs = new ArrayList<>(inputOperatorIds.size());
        for (int i = 0; i < inputOperatorIds.size(); i++) {
            String input = inputOperatorIds.get(i);
            ArtifactText.encode(input, "operator.inputOperatorIds[" + i + "]",
                    PlanArtifactLimits.MAX_TEXT_BYTES);
            copiedInputs.add(input);
        }
        this.operatorId = operatorId;
        this.kind = kind;
        this.typeId = typeId;
        this.schemaFingerprint = schemaFingerprint;
        this.inputOperatorIds = Collections.unmodifiableList(copiedInputs);
    }

    public String operatorId() {
        return operatorId;
    }

    public Kind kind() {
        return kind;
    }

    public String typeId() {
        return typeId;
    }

    public String schemaFingerprint() {
        return schemaFingerprint;
    }

    public List<String> inputOperatorIds() {
        return inputOperatorIds;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof OperatorDescriptor)) {
            return false;
        }
        OperatorDescriptor that = (OperatorDescriptor) other;
        return operatorId.equals(that.operatorId) && kind == that.kind
                && typeId.equals(that.typeId) && schemaFingerprint.equals(that.schemaFingerprint)
                && inputOperatorIds.equals(that.inputOperatorIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorId, kind, typeId, schemaFingerprint, inputOperatorIds);
    }
}
