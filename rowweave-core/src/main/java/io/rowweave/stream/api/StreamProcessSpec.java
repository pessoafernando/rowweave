package io.rowweave.stream.api;

import io.rowweave.portable.FunctionDeploymentReference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Complete inert declaration for a bounded keyed process operator. */
public final class StreamProcessSpec {
    public static final int MAX_STATES = 64;
    public static final int MAX_TIMER_FAMILIES = 64;
    public static final int MAX_OUTPUT_TAGS = 32;

    private final FunctionDeploymentReference function;
    private final StreamSchema outputSchema;
    private final List<String> outputStableKeyFieldIds;
    private final List<StreamManagedStateSpec> managedStates;
    private final List<StreamTimerSpec> timerFamilies;
    private final StreamEmissionLimit elementEmissionLimit;
    private final StreamEmissionLimit timerEmissionLimit;
    private final List<StreamOutputTag> outputTags;

    private StreamProcessSpec(FunctionDeploymentReference function,
            StreamSchema outputSchema, List<String> outputStableKeyFieldIds,
            List<StreamManagedStateSpec> managedStates,
            List<StreamTimerSpec> timerFamilies,
            StreamEmissionLimit elementEmissionLimit,
            StreamEmissionLimit timerEmissionLimit,
            List<StreamOutputTag> outputTags) {
        if (function == null || outputSchema == null
                || outputStableKeyFieldIds == null || managedStates == null
                || timerFamilies == null || elementEmissionLimit == null
                || timerEmissionLimit == null
                || outputTags == null
                || managedStates.size() > MAX_STATES
                || timerFamilies.size() > MAX_TIMER_FAMILIES) {
            throw new IllegalArgumentException(
                    "keyed process declaration is incomplete");
        }
        this.outputStableKeyFieldIds = stableKeys(
                outputSchema, outputStableKeyFieldIds);
        this.managedStates = uniqueStates(managedStates);
        this.timerFamilies = uniqueTimers(timerFamilies);
        this.function = function;
        this.outputSchema = outputSchema;
        this.elementEmissionLimit = elementEmissionLimit;
        this.timerEmissionLimit = timerEmissionLimit;
        this.outputTags = uniqueTags(outputTags);
    }

    public static StreamProcessSpec keyed(
            FunctionDeploymentReference function, StreamSchema outputSchema,
            List<String> outputStableKeyFieldIds,
            List<StreamManagedStateSpec> managedStates,
            List<StreamTimerSpec> timerFamilies,
            StreamEmissionLimit elementEmissionLimit,
            StreamEmissionLimit timerEmissionLimit) {
        return new StreamProcessSpec(function, outputSchema,
                outputStableKeyFieldIds, managedStates, timerFamilies,
                elementEmissionLimit, timerEmissionLimit,
                Collections.emptyList());
    }

    public StreamProcessSpec withOutputTags(List<StreamOutputTag> outputTags) {
        return new StreamProcessSpec(function, outputSchema,
                outputStableKeyFieldIds, managedStates, timerFamilies,
                elementEmissionLimit, timerEmissionLimit, outputTags);
    }

    private static List<String> stableKeys(StreamSchema schema,
            List<String> values) {
        Set<String> unique = new HashSet<>();
        List<String> copy = new ArrayList<>(values.size());
        for (String value : values) {
            boolean found = false;
            for (StreamField field : schema.fields()) {
                if (field.fieldId().equals(value) && !field.nullable()) {
                    found = true;
                    break;
                }
            }
            if (!found || !unique.add(value)) {
                throw new IllegalArgumentException(
                        "process output keys must be unique non-null fields");
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<StreamManagedStateSpec> uniqueStates(
            List<StreamManagedStateSpec> values) {
        Set<String> names = new HashSet<>();
        List<StreamManagedStateSpec> copy = new ArrayList<>(values.size());
        for (StreamManagedStateSpec value : values) {
            if (value == null || !names.add(value.state().stateName())) {
                throw new IllegalArgumentException(
                        "managed state names must be unique");
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<StreamTimerSpec> uniqueTimers(
            List<StreamTimerSpec> values) {
        Set<String> names = new HashSet<>();
        List<StreamTimerSpec> copy = new ArrayList<>(values.size());
        for (StreamTimerSpec value : values) {
            if (value == null || !names.add(value.timerFamilyId())) {
                throw new IllegalArgumentException(
                        "timer family IDs must be unique");
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
    }

    private static List<StreamOutputTag> uniqueTags(
            List<StreamOutputTag> values) {
        if (values.size() > MAX_OUTPUT_TAGS) {
            throw new IllegalArgumentException("too many output tags");
        }
        Set<String> names = new HashSet<>();
        List<StreamOutputTag> copy = new ArrayList<>(values.size());
        for (StreamOutputTag value : values) {
            if (value == null || !names.add(value.tagId())) {
                throw new IllegalArgumentException(
                        "output tag IDs must be unique");
            }
            copy.add(value);
        }
        return Collections.unmodifiableList(copy);
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
    public StreamEmissionLimit elementEmissionLimit() {
        return elementEmissionLimit;
    }
    public StreamEmissionLimit timerEmissionLimit() {
        return timerEmissionLimit;
    }
    public List<StreamOutputTag> outputTags() { return outputTags; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamProcessSpec)) return false;
        StreamProcessSpec that = (StreamProcessSpec) other;
        return function.equals(that.function)
                && outputSchema.equals(that.outputSchema)
                && outputStableKeyFieldIds.equals(
                that.outputStableKeyFieldIds)
                && managedStates.equals(that.managedStates)
                && timerFamilies.equals(that.timerFamilies)
                && elementEmissionLimit.equals(that.elementEmissionLimit)
                && timerEmissionLimit.equals(that.timerEmissionLimit)
                && outputTags.equals(that.outputTags);
    }

    @Override public int hashCode() {
        return Objects.hash(function, outputSchema, outputStableKeyFieldIds,
                managedStates, timerFamilies, elementEmissionLimit,
                timerEmissionLimit, outputTags);
    }
}
