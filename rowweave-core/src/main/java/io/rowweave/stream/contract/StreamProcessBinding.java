package io.rowweave.stream.contract;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamProcessSpec;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Versioned function, state, timer and emission contract for process nodes. */
public final class StreamProcessBinding implements StreamNodeBinding {
    private static final String INTERNAL_TIMER_STATE_PREFIX =
            "__rowweave_timer__/";
    private static final String INTERNAL_BROADCAST_STATE_PREFIX =
            "__rowweave_broadcast__/";
    public enum FunctionKind {
        KEYED_PROCESS(1, StreamOperatorCode.KEYED_PROCESS,
                FunctionAbi.KEYED_PROCESS_V1),
        KEYED_CO_PROCESS(2, StreamOperatorCode.KEYED_CO_PROCESS,
                FunctionAbi.KEYED_CO_PROCESS_V1);

        private final int wireId;
        private final StreamOperatorCode opcode;
        private final FunctionAbi abi;
        FunctionKind(int wireId, StreamOperatorCode opcode, FunctionAbi abi) {
            this.wireId = wireId;
            this.opcode = opcode;
            this.abi = abi;
        }
        int wireId() { return wireId; }
        FunctionAbi abi() { return abi; }
        static FunctionKind fromWireId(int wireId) {
            for (FunctionKind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_PROCESS_FUNCTION_KIND",
                    "unknown process function kind");
        }
    }

    private final FunctionKind functionKind;
    private final FunctionDeploymentReference deploymentReference;
    private final List<StreamManagedStateDescriptor> managedStates;
    private final List<StreamTimerDescriptor> timerFamilies;
    private final int maximumElementOutputs;
    private final long maximumElementOutputBytes;
    private final int maximumRightOutputs;
    private final long maximumRightOutputBytes;
    private final int maximumTimerOutputs;
    private final long maximumTimerOutputBytes;
    private final StreamBroadcastStateDescriptor broadcastState;
    private final List<StreamOutputTagDescriptor> outputTags;

    public StreamProcessBinding(FunctionKind functionKind,
            FunctionDeploymentReference deploymentReference,
            List<StreamManagedStateDescriptor> managedStates,
            List<StreamTimerDescriptor> timerFamilies,
            int maximumElementOutputs, long maximumElementOutputBytes,
            int maximumTimerOutputs, long maximumTimerOutputBytes) {
        this(functionKind, deploymentReference, managedStates, timerFamilies,
                maximumElementOutputs, maximumElementOutputBytes,
                maximumElementOutputs, maximumElementOutputBytes,
                maximumTimerOutputs, maximumTimerOutputBytes, null,
                Collections.<StreamOutputTagDescriptor>emptyList());
    }

    public StreamProcessBinding(FunctionKind functionKind,
            FunctionDeploymentReference deploymentReference,
            List<StreamManagedStateDescriptor> managedStates,
            List<StreamTimerDescriptor> timerFamilies,
            int maximumElementOutputs, long maximumElementOutputBytes,
            int maximumRightOutputs, long maximumRightOutputBytes,
            int maximumTimerOutputs, long maximumTimerOutputBytes) {
        this(functionKind, deploymentReference, managedStates, timerFamilies,
                maximumElementOutputs, maximumElementOutputBytes,
                maximumRightOutputs, maximumRightOutputBytes,
                maximumTimerOutputs, maximumTimerOutputBytes, null,
                Collections.<StreamOutputTagDescriptor>emptyList());
    }

    public StreamProcessBinding(FunctionKind functionKind,
            FunctionDeploymentReference deploymentReference,
            List<StreamManagedStateDescriptor> managedStates,
            List<StreamTimerDescriptor> timerFamilies,
            int maximumElementOutputs, long maximumElementOutputBytes,
            int maximumRightOutputs, long maximumRightOutputBytes,
            int maximumTimerOutputs, long maximumTimerOutputBytes,
            StreamBroadcastStateDescriptor broadcastState) {
        this(functionKind, deploymentReference, managedStates, timerFamilies,
                maximumElementOutputs, maximumElementOutputBytes,
                maximumRightOutputs, maximumRightOutputBytes,
                maximumTimerOutputs, maximumTimerOutputBytes, broadcastState,
                Collections.<StreamOutputTagDescriptor>emptyList());
    }

    public StreamProcessBinding(FunctionKind functionKind,
            FunctionDeploymentReference deploymentReference,
            List<StreamManagedStateDescriptor> managedStates,
            List<StreamTimerDescriptor> timerFamilies,
            int maximumElementOutputs, long maximumElementOutputBytes,
            int maximumRightOutputs, long maximumRightOutputBytes,
            int maximumTimerOutputs, long maximumTimerOutputBytes,
            StreamBroadcastStateDescriptor broadcastState,
            List<StreamOutputTagDescriptor> outputTags) {
        if (functionKind == null || deploymentReference == null) {
            throw failure("INVALID_PROCESS_BINDING",
                    "process function identity is required");
        }
        if (deploymentReference.functionAbi() != functionKind.abi()) {
            throw failure("FUNCTION_ABI_MISMATCH",
                    "process function ABI does not match its kind");
        }
        List<StreamManagedStateDescriptor> checkedStates =
                states(managedStates);
        List<StreamTimerDescriptor> checkedTimers = timers(timerFamilies);
        this.managedStates = checkedStates;
        this.timerFamilies = checkedTimers;
        validateEmission(maximumElementOutputs, maximumElementOutputBytes);
        validateEmission(maximumRightOutputs, maximumRightOutputBytes);
        validateEmission(maximumTimerOutputs, maximumTimerOutputBytes);
        this.functionKind = functionKind;
        this.deploymentReference = deploymentReference;
        this.maximumElementOutputs = maximumElementOutputs;
        this.maximumElementOutputBytes = maximumElementOutputBytes;
        this.maximumRightOutputs = maximumRightOutputs;
        this.maximumRightOutputBytes = maximumRightOutputBytes;
        this.maximumTimerOutputs = maximumTimerOutputs;
        this.maximumTimerOutputBytes = maximumTimerOutputBytes;
        if (broadcastState != null
                && functionKind != FunctionKind.KEYED_CO_PROCESS) {
            throw failure("INVALID_BROADCAST_BINDING",
                    "broadcast state requires keyed co-process");
        }
        this.broadcastState = broadcastState;
        this.outputTags = outputTags(outputTags);
    }

    private static List<StreamManagedStateDescriptor> states(
            List<StreamManagedStateDescriptor> values) {
        if (values == null || values.size() > StreamProcessSpec.MAX_STATES) {
            invalidResources();
        }
        Set<String> names = new HashSet<>();
        List<StreamManagedStateDescriptor> copy = new ArrayList<>(values.size());
        for (StreamManagedStateDescriptor value : values) {
            if (value == null
                    || value.state().stateName().startsWith(
                    INTERNAL_TIMER_STATE_PREFIX)
                    || value.state().stateName().startsWith(
                    INTERNAL_BROADCAST_STATE_PREFIX)
                    || !names.add(value.state().stateName())) {
                invalidResources();
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<StreamTimerDescriptor> timers(
            List<StreamTimerDescriptor> values) {
        if (values == null
                || values.size() > StreamProcessSpec.MAX_TIMER_FAMILIES) {
            invalidResources();
        }
        Set<String> names = new HashSet<>();
        List<StreamTimerDescriptor> copy = new ArrayList<>(values.size());
        for (StreamTimerDescriptor value : values) {
            if (value == null || !names.add(value.timerFamilyId())) {
                invalidResources();
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    private static void validateEmission(int rows, long bytes) {
        if (rows <= 0 || rows > StreamEmissionLimit.MAX_OUTPUTS_PER_INPUT
                || bytes <= 0L
                || bytes > StreamEmissionLimit.MAX_OUTPUT_BYTES_PER_INPUT) {
            invalidResources();
        }
    }

    private static List<StreamOutputTagDescriptor> outputTags(
            List<StreamOutputTagDescriptor> values) {
        if (values == null || values.size() > StreamProcessSpec.MAX_OUTPUT_TAGS) {
            invalidResources();
        }
        Set<String> names = new HashSet<>();
        List<StreamOutputTagDescriptor> copy = new ArrayList<>(values.size());
        for (StreamOutputTagDescriptor value : values) {
            if (value == null || !names.add(value.tagId())) {
                invalidResources();
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    private static void invalidResources() {
        throw failure("INVALID_PROCESS_RESOURCE_LIMIT",
                "process resource declaration is invalid");
    }

    public boolean matches(StreamOperatorCode opcode) {
        return functionKind.opcode == opcode;
    }
    @Override public StreamOperatorCode.BindingKind kind() {
        return StreamOperatorCode.BindingKind.FUNCTION;
    }
    public FunctionKind functionKind() { return functionKind; }
    public FunctionDeploymentReference deploymentReference() {
        return deploymentReference;
    }
    public List<StreamManagedStateDescriptor> managedStates() {
        return managedStates;
    }
    public List<StreamTimerDescriptor> timerFamilies() {
        return timerFamilies;
    }
    public static String timerStateName(String timerFamilyId) {
        if (timerFamilyId == null || timerFamilyId.isEmpty()) {
            throw new IllegalArgumentException("timer family ID is required");
        }
        return INTERNAL_TIMER_STATE_PREFIX + timerFamilyId;
    }
    public int maximumElementOutputs() { return maximumElementOutputs; }
    public int maximumLeftOutputs() { return maximumElementOutputs; }
    public long maximumElementOutputBytes() {
        return maximumElementOutputBytes;
    }
    public int maximumRightOutputs() { return maximumRightOutputs; }
    public long maximumRightOutputBytes() { return maximumRightOutputBytes; }
    public int maximumTimerOutputs() { return maximumTimerOutputs; }
    public long maximumTimerOutputBytes() { return maximumTimerOutputBytes; }
    public StreamBroadcastStateDescriptor broadcastState() {
        return broadcastState;
    }
    public List<StreamOutputTagDescriptor> outputTags() { return outputTags; }

    private static StreamContractException failure(String code,
            String message) {
        return StreamContractSupport.failure(code, "nodes.process", message);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamProcessBinding)) return false;
        StreamProcessBinding that = (StreamProcessBinding) other;
        return maximumElementOutputs == that.maximumElementOutputs
                && maximumElementOutputBytes == that.maximumElementOutputBytes
                && maximumRightOutputs == that.maximumRightOutputs
                && maximumRightOutputBytes == that.maximumRightOutputBytes
                && maximumTimerOutputs == that.maximumTimerOutputs
                && maximumTimerOutputBytes == that.maximumTimerOutputBytes
                && functionKind == that.functionKind
                && deploymentReference.equals(that.deploymentReference)
                && managedStates.equals(that.managedStates)
                && timerFamilies.equals(that.timerFamilies)
                && Objects.equals(broadcastState, that.broadcastState)
                && outputTags.equals(that.outputTags);
    }

    @Override public int hashCode() {
        return Objects.hash(functionKind, deploymentReference, managedStates,
                timerFamilies, maximumElementOutputs,
                maximumElementOutputBytes, maximumRightOutputs,
                maximumRightOutputBytes, maximumTimerOutputs,
                maximumTimerOutputBytes, broadcastState, outputTags);
    }
}
