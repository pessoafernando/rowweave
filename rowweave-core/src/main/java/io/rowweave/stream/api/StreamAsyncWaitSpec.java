package io.rowweave.stream.api;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/** Complete inert declaration for one supervised asynchronous lookup. */
public final class StreamAsyncWaitSpec {
    private final FunctionDeploymentReference function;
    private final StreamSchema outputSchema;
    private final List<String> outputStableKeyFieldIds;
    private final StreamAsyncOrdering ordering;
    private final StreamAsyncReplaySpec replay;
    private final StreamAsyncResourceSpec resources;

    private StreamAsyncWaitSpec(FunctionDeploymentReference function,
            StreamSchema outputSchema, List<String> outputStableKeyFieldIds,
            StreamAsyncOrdering ordering, StreamAsyncReplaySpec replay,
            StreamAsyncResourceSpec resources) {
        if (function == null || outputSchema == null
                || outputStableKeyFieldIds == null || ordering == null
                || replay == null || resources == null
                || function.functionAbi() != FunctionAbi.STREAM_ASYNC_V1
                || resources.resultEmissionLimit().maximumOutputsPerInput()
                > function.resourceProfile().maximumOutputRowsPerInput()) {
            throw new IllegalArgumentException("invalid async wait declaration");
        }
        this.outputStableKeyFieldIds = stableKeys(
                outputSchema, outputStableKeyFieldIds);
        this.function = function;
        this.outputSchema = outputSchema;
        this.ordering = ordering;
        this.replay = replay;
        this.resources = resources;
    }

    public static StreamAsyncWaitSpec of(FunctionDeploymentReference function,
            StreamSchema outputSchema, List<String> outputStableKeyFieldIds,
            StreamAsyncOrdering ordering, StreamAsyncReplaySpec replay,
            StreamAsyncResourceSpec resources) {
        return new StreamAsyncWaitSpec(function, outputSchema,
                outputStableKeyFieldIds, ordering, replay, resources);
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
                        "async output keys must be unique non-null fields");
            }
            result.add(value);
        }
        return Collections.unmodifiableList(result);
    }

    public FunctionDeploymentReference function() { return function; }
    public StreamSchema outputSchema() { return outputSchema; }
    public List<String> outputStableKeyFieldIds() {
        return outputStableKeyFieldIds;
    }
    public StreamAsyncOrdering ordering() { return ordering; }
    public StreamAsyncReplaySpec replay() { return replay; }
    public StreamAsyncResourceSpec resources() { return resources; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamAsyncWaitSpec)) return false;
        StreamAsyncWaitSpec that = (StreamAsyncWaitSpec) other;
        return function.equals(that.function)
                && outputSchema.equals(that.outputSchema)
                && outputStableKeyFieldIds.equals(
                that.outputStableKeyFieldIds)
                && ordering == that.ordering && replay.equals(that.replay)
                && resources.equals(that.resources);
    }

    @Override public int hashCode() {
        return Objects.hash(function, outputSchema, outputStableKeyFieldIds,
                ordering, replay, resources);
    }
}
