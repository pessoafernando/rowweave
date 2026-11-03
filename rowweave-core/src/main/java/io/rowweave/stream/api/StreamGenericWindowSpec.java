package io.rowweave.stream.api;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Complete inert declaration for one bounded generic window operator. */
public final class StreamGenericWindowSpec {
    public enum FunctionKind { AGGREGATE, PROCESS }

    private final FunctionKind functionKind;
    private final StreamWindowAssignerSpec assigner;
    private final StreamWindowTriggerSpec trigger;
    private final StreamAccumulationMode accumulationMode;
    private final long allowedLatenessMillis;
    private final FunctionDeploymentReference function;
    private final StreamSchema outputSchema;
    private final List<String> outputStableKeyFieldIds;
    private final StreamStateSpec state;
    private final StreamWindowResourceSpec resources;
    private final StreamOutputTag lateOutput;

    private StreamGenericWindowSpec(FunctionKind functionKind,
            StreamWindowAssignerSpec assigner,
            StreamWindowTriggerSpec trigger,
            StreamAccumulationMode accumulationMode,
            long allowedLatenessMillis,
            FunctionDeploymentReference function, StreamSchema outputSchema,
            List<String> outputStableKeyFieldIds, StreamStateSpec state,
            StreamWindowResourceSpec resources, StreamOutputTag lateOutput) {
        if (functionKind == null || assigner == null || trigger == null
                || accumulationMode == null || function == null
                || outputSchema == null || outputStableKeyFieldIds == null
                || state == null || resources == null
                || state.kind() == StreamStateSpec.Kind.OPERATOR_LIST
                || allowedLatenessMillis < 0L
                || allowedLatenessMillis
                > StreamWindowAssignerSpec.MAX_TIME_MILLIS
                || !assigner.eventTime() && allowedLatenessMillis != 0L
                || !acceptsAbi(functionKind, function.functionAbi())) {
            throw new IllegalArgumentException(
                    "generic window declaration is invalid");
        }
        if (lateOutput != null && !assigner.eventTime()) {
            throw new IllegalArgumentException(
                    "late output requires an event-time window");
        }
        validateTrigger(assigner, trigger);
        this.outputStableKeyFieldIds = stableKeys(
                outputSchema, outputStableKeyFieldIds);
        this.functionKind = functionKind;
        this.assigner = assigner;
        this.trigger = trigger;
        this.accumulationMode = accumulationMode;
        this.allowedLatenessMillis = allowedLatenessMillis;
        this.function = function;
        this.outputSchema = outputSchema;
        this.state = state;
        this.resources = resources;
        this.lateOutput = lateOutput;
    }

    public static StreamGenericWindowSpec aggregate(
            StreamWindowAssignerSpec assigner,
            StreamWindowTriggerSpec trigger,
            StreamAccumulationMode accumulationMode,
            long allowedLatenessMillis,
            FunctionDeploymentReference function, StreamSchema outputSchema,
            List<String> outputStableKeyFieldIds, StreamStateSpec state,
            StreamWindowResourceSpec resources) {
        return new StreamGenericWindowSpec(FunctionKind.AGGREGATE, assigner,
                trigger, accumulationMode, allowedLatenessMillis, function,
                outputSchema, outputStableKeyFieldIds, state, resources, null);
    }

    public static StreamGenericWindowSpec process(
            StreamWindowAssignerSpec assigner,
            StreamWindowTriggerSpec trigger,
            StreamAccumulationMode accumulationMode,
            long allowedLatenessMillis,
            FunctionDeploymentReference function, StreamSchema outputSchema,
            List<String> outputStableKeyFieldIds, StreamStateSpec state,
            StreamWindowResourceSpec resources) {
        return new StreamGenericWindowSpec(FunctionKind.PROCESS, assigner,
                trigger, accumulationMode, allowedLatenessMillis, function,
                outputSchema, outputStableKeyFieldIds, state, resources, null);
    }

    public StreamGenericWindowSpec withLateOutput(StreamOutputTag lateOutput) {
        if (lateOutput == null) {
            throw new IllegalArgumentException("late output tag is required");
        }
        return new StreamGenericWindowSpec(functionKind, assigner, trigger,
                accumulationMode, allowedLatenessMillis, function,
                outputSchema, outputStableKeyFieldIds, state, resources,
                lateOutput);
    }

    private static boolean acceptsAbi(FunctionKind kind, FunctionAbi abi) {
        return kind == FunctionKind.AGGREGATE
                ? abi == FunctionAbi.STREAM_WINDOW_AGGREGATE_V1
                || abi == FunctionAbi.STREAM_RETRACT_WINDOW_AGGREGATE_V2
                : abi == FunctionAbi.STREAM_WINDOW_PROCESS_V1;
    }

    private static void validateTrigger(StreamWindowAssignerSpec assigner,
            StreamWindowTriggerSpec trigger) {
        if (assigner.eventTime()
                && trigger.kind()
                != StreamWindowTriggerSpec.Kind.EVENT_TIME_FINAL
                || assigner.count()
                && trigger.kind() != StreamWindowTriggerSpec.Kind.COUNT
                || !assigner.eventTime() && !assigner.count()
                && trigger.kind()
                != StreamWindowTriggerSpec.Kind.PROCESSING_TIME) {
            throw new IllegalArgumentException(
                    "window assigner and trigger domains differ");
        }
    }

    private static List<String> stableKeys(StreamSchema schema,
            List<String> values) {
        Set<String> unique = new HashSet<>();
        List<String> result = new ArrayList<>(values.size());
        for (String value : values) {
            boolean valid = false;
            for (StreamField field : schema.fields()) {
                if (field.fieldId().equals(value) && !field.nullable()) {
                    valid = true;
                    break;
                }
            }
            if (!valid || !unique.add(value)) {
                throw new IllegalArgumentException(
                        "window output keys must be unique non-null fields");
            }
            result.add(value);
        }
        return Collections.unmodifiableList(result);
    }

    public FunctionKind functionKind() { return functionKind; }
    public StreamWindowAssignerSpec assigner() { return assigner; }
    public StreamWindowTriggerSpec trigger() { return trigger; }
    public StreamAccumulationMode accumulationMode() {
        return accumulationMode;
    }
    public long allowedLatenessMillis() { return allowedLatenessMillis; }
    public FunctionDeploymentReference function() { return function; }
    public StreamSchema outputSchema() { return outputSchema; }
    public List<String> outputStableKeyFieldIds() {
        return outputStableKeyFieldIds;
    }
    public StreamStateSpec state() { return state; }
    public StreamWindowResourceSpec resources() { return resources; }
    public StreamOutputTag lateOutput() { return lateOutput; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamGenericWindowSpec)) return false;
        StreamGenericWindowSpec that = (StreamGenericWindowSpec) other;
        return functionKind == that.functionKind
                && assigner.equals(that.assigner)
                && trigger.equals(that.trigger)
                && accumulationMode == that.accumulationMode
                && allowedLatenessMillis == that.allowedLatenessMillis
                && function.equals(that.function)
                && outputSchema.equals(that.outputSchema)
                && outputStableKeyFieldIds.equals(
                that.outputStableKeyFieldIds)
                && state.equals(that.state)
                && resources.equals(that.resources)
                && Objects.equals(lateOutput, that.lateOutput);
    }

    @Override public int hashCode() {
        return Objects.hash(functionKind, assigner, trigger, accumulationMode,
                allowedLatenessMillis, function, outputSchema,
                outputStableKeyFieldIds, state, resources, lateOutput);
    }
}
