package io.rowweave.stream.contract;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;

import java.util.Objects;

/** Stream-specific ABI tag around the shared signed function deployment reference. */
public final class StreamFunctionBinding implements StreamNodeBinding {
    public enum FunctionKind {
        MAP(1, StreamOperatorCode.MAP, FunctionAbi.MAP_V1),
        FILTER(2, StreamOperatorCode.FILTER, FunctionAbi.FILTER_V1),
        KEYED_AGGREGATE(3, StreamOperatorCode.KEYED_AGGREGATE,
                FunctionAbi.STREAM_AGGREGATE_V1),
        TEMPORAL_JOIN(4, StreamOperatorCode.TEMPORAL_JOIN,
                FunctionAbi.CO_GROUP_V1),
        FLAT_MAP(5, StreamOperatorCode.FLAT_MAP, FunctionAbi.FLAT_MAP_V1);

        private final int wireId;
        private final StreamOperatorCode opcode;
        private final FunctionAbi functionAbi;

        FunctionKind(int wireId, StreamOperatorCode opcode, FunctionAbi functionAbi) {
            this.wireId = wireId;
            this.opcode = opcode;
            this.functionAbi = functionAbi;
        }

        int wireId() { return wireId; }
        StreamOperatorCode opcode() { return opcode; }
        FunctionAbi functionAbi() { return functionAbi; }

        static FunctionKind fromWire(int wireId) {
            for (FunctionKind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw StreamContractSupport.failure("UNKNOWN_FUNCTION_KIND",
                    "nodes.function.kind", "unknown Stream function kind");
        }
    }

    private final FunctionKind functionKind;
    private final FunctionDeploymentReference deploymentReference;

    public StreamFunctionBinding(
            FunctionKind functionKind,
            FunctionDeploymentReference deploymentReference) {
        if (functionKind == null || deploymentReference == null) {
            throw StreamContractSupport.failure("INVALID_FUNCTION_BINDING",
                    "nodes.function", "function kind and deployment are required");
        }
        if (!matchesAbi(functionKind,
                deploymentReference.functionAbi())) {
            throw StreamContractSupport.failure("FUNCTION_ABI_MISMATCH",
                    "nodes.function.abi",
                    "shared function ABI does not match the Stream function kind");
        }
        this.functionKind = functionKind;
        this.deploymentReference = deploymentReference;
    }

    private static boolean matchesAbi(FunctionKind kind, FunctionAbi abi) {
        return abi == kind.functionAbi()
                || kind == FunctionKind.KEYED_AGGREGATE
                && abi == FunctionAbi.STREAM_RETRACT_AGGREGATE_V2;
    }

    @Override
    public StreamOperatorCode.BindingKind kind() {
        return StreamOperatorCode.BindingKind.FUNCTION;
    }

    public FunctionKind functionKind() { return functionKind; }
    public FunctionDeploymentReference deploymentReference() {
        return deploymentReference;
    }

    boolean matches(StreamOperatorCode opcode) {
        return functionKind.opcode() == opcode;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamFunctionBinding)) return false;
        StreamFunctionBinding that = (StreamFunctionBinding) other;
        return functionKind == that.functionKind
                && deploymentReference.equals(that.deploymentReference);
    }

    @Override
    public int hashCode() {
        return Objects.hash(functionKind, deploymentReference);
    }
}
