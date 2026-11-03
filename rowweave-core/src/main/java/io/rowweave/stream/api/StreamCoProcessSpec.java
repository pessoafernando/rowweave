package io.rowweave.stream.api;

import io.rowweave.portable.FunctionDeploymentReference;

import java.util.List;
import java.util.Collections;
import java.util.Objects;

/** Complete inert declaration for a symmetric bounded keyed co-process. */
public final class StreamCoProcessSpec {
    private final FunctionDeploymentReference function;
    private final StreamSchema outputSchema;
    private final List<String> outputStableKeyFieldIds;
    private final List<StreamManagedStateSpec> managedStates;
    private final List<StreamTimerSpec> timerFamilies;
    private final StreamEmissionLimit leftEmissionLimit;
    private final StreamEmissionLimit rightEmissionLimit;
    private final StreamEmissionLimit timerEmissionLimit;
    private final StreamBroadcastStateSpec broadcastState;
    private final List<StreamOutputTag> outputTags;

    private StreamCoProcessSpec(FunctionDeploymentReference function,
            StreamSchema outputSchema, List<String> outputStableKeyFieldIds,
            List<StreamManagedStateSpec> managedStates,
            List<StreamTimerSpec> timerFamilies,
            StreamEmissionLimit leftEmissionLimit,
            StreamEmissionLimit rightEmissionLimit,
            StreamEmissionLimit timerEmissionLimit,
            StreamBroadcastStateSpec broadcastState,
            List<StreamOutputTag> outputTags) {
        StreamProcessSpec common = StreamProcessSpec.keyed(function,
                outputSchema, outputStableKeyFieldIds, managedStates,
                timerFamilies, leftEmissionLimit, timerEmissionLimit)
                .withOutputTags(outputTags);
        if (rightEmissionLimit == null) {
            throw new IllegalArgumentException(
                    "right input emission limit is required");
        }
        this.function = common.function();
        this.outputSchema = common.outputSchema();
        this.outputStableKeyFieldIds = common.outputStableKeyFieldIds();
        this.managedStates = common.managedStates();
        this.timerFamilies = common.timerFamilies();
        this.leftEmissionLimit = common.elementEmissionLimit();
        this.rightEmissionLimit = rightEmissionLimit;
        this.timerEmissionLimit = common.timerEmissionLimit();
        this.broadcastState = broadcastState;
        this.outputTags = common.outputTags();
    }

    public static StreamCoProcessSpec keyed(
            FunctionDeploymentReference function, StreamSchema outputSchema,
            List<String> outputStableKeyFieldIds,
            List<StreamManagedStateSpec> managedStates,
            List<StreamTimerSpec> timerFamilies,
            StreamEmissionLimit leftEmissionLimit,
            StreamEmissionLimit rightEmissionLimit,
            StreamEmissionLimit timerEmissionLimit) {
        return new StreamCoProcessSpec(function, outputSchema,
                outputStableKeyFieldIds, managedStates, timerFamilies,
                leftEmissionLimit, rightEmissionLimit, timerEmissionLimit,
                null, Collections.emptyList());
    }

    public StreamCoProcessSpec withBroadcastRight(
            StreamBroadcastStateSpec broadcastState) {
        if (broadcastState == null) {
            throw new IllegalArgumentException(
                    "broadcast state specification is required");
        }
        return new StreamCoProcessSpec(function, outputSchema,
                outputStableKeyFieldIds, managedStates, timerFamilies,
                leftEmissionLimit, rightEmissionLimit, timerEmissionLimit,
                broadcastState, outputTags);
    }

    public StreamCoProcessSpec withOutputTags(
            List<StreamOutputTag> outputTags) {
        return new StreamCoProcessSpec(function, outputSchema,
                outputStableKeyFieldIds, managedStates, timerFamilies,
                leftEmissionLimit, rightEmissionLimit, timerEmissionLimit,
                broadcastState, outputTags);
    }

    public FunctionDeploymentReference function() { return function; }
    public StreamSchema outputSchema() { return outputSchema; }
    public List<String> outputStableKeyFieldIds() {
        return outputStableKeyFieldIds;
    }
    public List<StreamManagedStateSpec> managedStates() {
        return managedStates;
    }
    public List<StreamTimerSpec> timerFamilies() { return timerFamilies; }
    public StreamEmissionLimit leftEmissionLimit() {
        return leftEmissionLimit;
    }
    public StreamEmissionLimit rightEmissionLimit() {
        return rightEmissionLimit;
    }
    public StreamEmissionLimit timerEmissionLimit() {
        return timerEmissionLimit;
    }
    public StreamBroadcastStateSpec broadcastState() { return broadcastState; }
    public List<StreamOutputTag> outputTags() { return outputTags; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamCoProcessSpec)) return false;
        StreamCoProcessSpec that = (StreamCoProcessSpec) other;
        return function.equals(that.function)
                && outputSchema.equals(that.outputSchema)
                && outputStableKeyFieldIds.equals(
                that.outputStableKeyFieldIds)
                && managedStates.equals(that.managedStates)
                && timerFamilies.equals(that.timerFamilies)
                && leftEmissionLimit.equals(that.leftEmissionLimit)
                && rightEmissionLimit.equals(that.rightEmissionLimit)
                && timerEmissionLimit.equals(that.timerEmissionLimit)
                && Objects.equals(broadcastState, that.broadcastState)
                && outputTags.equals(that.outputTags);
    }

    @Override public int hashCode() {
        return Objects.hash(function, outputSchema, outputStableKeyFieldIds,
                managedStates, timerFamilies, leftEmissionLimit,
                rightEmissionLimit, timerEmissionLimit, broadcastState,
                outputTags);
    }
}
