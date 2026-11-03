package io.rowweave.portable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

/** One topologically ordered portable node with no live Java object payload. */
public final class PortablePlanNode {
    public static final int MAX_OPERATOR_ID_BYTES = 256;
    public static final int MAX_PARAMETERS = 64;
    public static final int MAX_LIST_VALUES = 1024;
    public static final int MAX_PARAMETER_KEY_BYTES = 256;
    public static final int MAX_PARAMETER_VALUE_BYTES = 4096;

    private final String operatorId;
    private final PortableOperatorCode opcode;
    private final List<String> inputOperatorIds;
    private final List<String> inputSchemaFingerprints;
    private final PortableRowSchema outputSchema;
    private final SortedMap<String, String> scalarParameters;
    private final SortedMap<String, List<String>> listParameters;
    private final PortableNodeBinding binding;

    public PortablePlanNode(
            String operatorId, PortableOperatorCode opcode,
            List<String> inputOperatorIds,
            List<String> inputSchemaFingerprints,
            PortableRowSchema outputSchema,
            Map<String, String> scalarParameters,
            Map<String, List<String>> listParameters,
            PortableNodeBinding binding) {
        this.operatorId = PortablePlanText.require(
                operatorId, "nodes.operatorId", MAX_OPERATOR_ID_BYTES);
        if (opcode == null) {
            throw PortablePlanText.failure("UNKNOWN_OPCODE", "nodes.opcode",
                    "operator code is required");
        }
        this.opcode = opcode;
        this.inputOperatorIds = inputs(inputOperatorIds, opcode.inputCount());
        this.inputSchemaFingerprints = inputSchemas(
                inputSchemaFingerprints, opcode.inputCount());
        if (outputSchema == null) {
            throw PortablePlanText.failure("INVALID_SCHEMA", "nodes.outputSchema",
                    "output schema is required");
        }
        this.outputSchema = outputSchema;
        this.scalarParameters = scalarParameters(scalarParameters);
        this.listParameters = listParameters(listParameters);
        validateBinding(opcode, binding);
        this.binding = binding;
    }

    private static List<String> inputs(List<String> values, int expected) {
        if (values == null || values.size() != expected) {
            throw PortablePlanText.failure("INVALID_TOPOLOGY", "nodes.inputs",
                    "operator input count does not match its opcode");
        }
        List<String> copied = new ArrayList<>(values.size());
        for (String value : values) {
            copied.add(PortablePlanText.require(
                    value, "nodes.inputs", MAX_OPERATOR_ID_BYTES));
        }
        return Collections.unmodifiableList(copied);
    }

    private static List<String> inputSchemas(List<String> values, int expected) {
        if (values == null || values.size() != expected) {
            throw PortablePlanText.failure("INVALID_TOPOLOGY",
                    "nodes.inputSchemaFingerprints",
                    "input schema count does not match its opcode");
        }
        List<String> copied = new ArrayList<>(values.size());
        for (String value : values) {
            copied.add(PortableBindingSupport.digest(
                    value, "nodes.inputSchemaFingerprints"));
        }
        return Collections.unmodifiableList(copied);
    }

    private static SortedMap<String, String> scalarParameters(
            Map<String, String> values) {
        if (values == null || values.size() > MAX_PARAMETERS) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED",
                    "nodes.scalarParameters",
                    "scalar parameter count exceeds its limit");
        }
        SortedMap<String, String> copied = new TreeMap<>();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            String key = PortablePlanText.require(entry.getKey(),
                    "nodes.scalarParameters.key", MAX_PARAMETER_KEY_BYTES);
            String value = PortablePlanText.value(entry.getValue(),
                    "nodes.scalarParameters.value", MAX_PARAMETER_VALUE_BYTES);
            copied.put(key, value);
        }
        return Collections.unmodifiableSortedMap(copied);
    }

    private static SortedMap<String, List<String>> listParameters(
            Map<String, List<String>> values) {
        if (values == null || values.size() > MAX_PARAMETERS) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "nodes.listParameters",
                    "list parameter count exceeds its limit");
        }
        SortedMap<String, List<String>> copied = new TreeMap<>();
        for (Map.Entry<String, List<String>> entry : values.entrySet()) {
            String key = PortablePlanText.require(entry.getKey(),
                    "nodes.listParameters.key", MAX_PARAMETER_KEY_BYTES);
            List<String> source = entry.getValue();
            if (source == null || source.size() > MAX_LIST_VALUES) {
                throw PortablePlanText.failure("LIMIT_EXCEEDED",
                        "nodes.listParameters.value",
                        "list parameter size exceeds its limit");
            }
            List<String> elements = new ArrayList<>(source.size());
            for (String value : source) {
                elements.add(PortablePlanText.value(value,
                        "nodes.listParameters.value", MAX_PARAMETER_VALUE_BYTES));
            }
            copied.put(key, Collections.unmodifiableList(elements));
        }
        return Collections.unmodifiableSortedMap(copied);
    }

    private static void validateBinding(
            PortableOperatorCode opcode, PortableNodeBinding binding) {
        if (opcode.bindingKind() == PortableOperatorCode.BindingKind.NONE) {
            if (binding != null) bindingMismatch();
            return;
        }
        if (binding == null || binding.kind() != opcode.bindingKind()) {
            bindingMismatch();
        }
        if (opcode.bindingKind() == PortableOperatorCode.BindingKind.SOURCE
                && !(binding instanceof PortableSourceBinding)) {
            bindingMismatch();
        }
        if (opcode.bindingKind() == PortableOperatorCode.BindingKind.SINK
                && !(binding instanceof PortableSinkBinding)) {
            bindingMismatch();
        }
        if (opcode.bindingKind() == PortableOperatorCode.BindingKind.FUNCTION
                && !(binding instanceof FunctionDeploymentReference)) {
            bindingMismatch();
        }
        if (binding instanceof FunctionDeploymentReference
                && ((FunctionDeploymentReference) binding).functionAbi()
                != expectedAbi(opcode)) {
            throw PortablePlanText.failure("FUNCTION_ABI_MISMATCH",
                    "nodes.binding.functionAbi",
                    "function ABI does not match its opcode");
        }
    }

    private static void bindingMismatch() {
        throw PortablePlanText.failure("BINDING_KIND_MISMATCH", "nodes.binding",
                "binding kind does not match its opcode");
    }

    private static FunctionAbi expectedAbi(PortableOperatorCode opcode) {
        switch (opcode) {
            case FUNCTION_FILTER: return FunctionAbi.FILTER_V1;
            case FUNCTION_MAP: return FunctionAbi.MAP_V1;
            case FUNCTION_FLAT_MAP: return FunctionAbi.FLAT_MAP_V1;
            case FUNCTION_REDUCE_GROUP: return FunctionAbi.REDUCE_GROUP_V1;
            case FUNCTION_COMBINE_GROUP: return FunctionAbi.COMBINE_GROUP_V1;
            case FUNCTION_COMBINE_REDUCE_GROUP:
                return FunctionAbi.COMBINE_REDUCE_GROUP_V1;
            case FUNCTION_CO_GROUP: return FunctionAbi.CO_GROUP_V1;
            case FUNCTION_MAP_PARTITION:
                return FunctionAbi.MAP_PARTITION_V1;
            default:
                throw PortablePlanText.failure("BINDING_KIND_MISMATCH",
                        "nodes.binding", "opcode is not a function opcode");
        }
    }

    public String operatorId() { return operatorId; }
    public PortableOperatorCode opcode() { return opcode; }
    public List<String> inputOperatorIds() { return inputOperatorIds; }
    public List<String> inputSchemaFingerprints() { return inputSchemaFingerprints; }
    public PortableRowSchema outputSchema() { return outputSchema; }
    public SortedMap<String, String> scalarParameters() { return scalarParameters; }
    public SortedMap<String, List<String>> listParameters() { return listParameters; }
    public PortableNodeBinding binding() { return binding; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PortablePlanNode)) return false;
        PortablePlanNode that = (PortablePlanNode) other;
        return operatorId.equals(that.operatorId)
                && opcode == that.opcode
                && inputOperatorIds.equals(that.inputOperatorIds)
                && inputSchemaFingerprints.equals(that.inputSchemaFingerprints)
                && outputSchema.equals(that.outputSchema)
                && scalarParameters.equals(that.scalarParameters)
                && listParameters.equals(that.listParameters)
                && Objects.equals(binding, that.binding);
    }

    @Override
    public int hashCode() {
        return Objects.hash(operatorId, opcode, inputOperatorIds,
                inputSchemaFingerprints, outputSchema, scalarParameters,
                listParameters, binding);
    }
}
