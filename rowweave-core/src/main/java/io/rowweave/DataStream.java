package io.rowweave;

import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamCoProcessSpec;
import io.rowweave.stream.api.StreamEventTimeSpec;
import io.rowweave.stream.api.StreamGenericWindowSpec;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTemporalJoinSpec;
import io.rowweave.stream.api.StreamWindowSpec;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable public Stream DAG node. Data rows are never materialized here. */
public final class DataStream {
    enum Kind { SOURCE, MAP, FILTER, CALC, FLAT_MAP, UNION, ASSIGN_EVENT_TIME, KEY_BY, KEYED_AGGREGATE, KEYED_PROCESS, KEYED_CO_PROCESS, TAGGED_VIEW, GENERIC_WINDOW, ASYNC_WAIT, EVENT_TIME_WINDOW, TEMPORAL_JOIN }

    private final StreamSession session;
    private final Kind kind;
    private final DataStream input;
    private final StreamSourceSpec sourceSpec;
    private final FunctionDeploymentReference function;
    private final StreamSchema schema;
    private final String operatorUid;
    private final List<String> keyFields;
    private final StreamStateSpec stateSpec;
    private final DataStream secondInput;
    private final StreamTemporalJoinSpec temporalJoinSpec;
    private final StreamStateSpec secondStateSpec;
    private final StreamWindowSpec windowSpec;
    private final StreamEmissionLimit emissionLimit;
    private final Object atomicSpec;

    static DataStream source(StreamSession session, StreamSourceSpec source) {
        return new DataStream(session, Kind.SOURCE, null, source, null,
                source.schema(), null, source.stableKeyFieldIds(), null,
                null, null, null, null, null);
    }

    static DataStream taggedView(DataStream process, StreamOutputTag tag) {
        return new DataStream(process.session, Kind.TAGGED_VIEW, process,
                null, null, tag.schema(), null, tag.stableKeyFieldIds(),
                null, null, null, null, null, null, tag);
    }

    private DataStream(StreamSession session, Kind kind, DataStream input,
            StreamSourceSpec sourceSpec, FunctionDeploymentReference function,
            StreamSchema schema, String operatorUid, List<String> keyFields) {
        this(session, kind, input, sourceSpec, function, schema, operatorUid,
                keyFields, null, null, null, null, null);
    }

    private DataStream(StreamSession session, Kind kind, DataStream input,
            StreamSourceSpec sourceSpec, FunctionDeploymentReference function,
            StreamSchema schema, String operatorUid, List<String> keyFields,
            StreamStateSpec stateSpec, DataStream secondInput,
            StreamTemporalJoinSpec temporalJoinSpec,
            StreamStateSpec secondStateSpec, StreamWindowSpec windowSpec) {
        this(session, kind, input, sourceSpec, function, schema, operatorUid,
                keyFields, stateSpec, secondInput, temporalJoinSpec,
                secondStateSpec, windowSpec, null);
    }

    private DataStream(StreamSession session, Kind kind, DataStream input,
            StreamSourceSpec sourceSpec, FunctionDeploymentReference function,
            StreamSchema schema, String operatorUid, List<String> keyFields,
            StreamStateSpec stateSpec, DataStream secondInput,
            StreamTemporalJoinSpec temporalJoinSpec,
            StreamStateSpec secondStateSpec, StreamWindowSpec windowSpec,
            StreamEmissionLimit emissionLimit) {
        this(session, kind, input, sourceSpec, function, schema, operatorUid,
                keyFields, stateSpec, secondInput, temporalJoinSpec,
                secondStateSpec, windowSpec, emissionLimit, null);
    }

    private DataStream(StreamSession session, Kind kind, DataStream input,
            StreamSourceSpec sourceSpec, FunctionDeploymentReference function,
            StreamSchema schema, String operatorUid, List<String> keyFields,
            StreamStateSpec stateSpec, DataStream secondInput,
            StreamTemporalJoinSpec temporalJoinSpec,
            StreamStateSpec secondStateSpec, StreamWindowSpec windowSpec,
            StreamEmissionLimit emissionLimit, Object atomicSpec) {
        this.session = session;
        this.kind = kind;
        this.input = input;
        this.sourceSpec = sourceSpec;
        this.function = function;
        this.schema = schema;
        this.operatorUid = operatorUid;
        this.keyFields = Collections.unmodifiableList(new ArrayList<>(keyFields));
        this.stateSpec = stateSpec;
        this.secondInput = secondInput;
        this.temporalJoinSpec = temporalJoinSpec;
        this.secondStateSpec = secondStateSpec;
        this.windowSpec = windowSpec;
        this.emissionLimit = emissionLimit;
        this.atomicSpec = atomicSpec;
    }

    public DataStream uid(String uid) {
        if (uid == null || uid.isEmpty()) {
            throw new IllegalArgumentException("operator UID is required");
        }
        return new DataStream(session, kind, input, sourceSpec, function,
                schema, uid, keyFields, stateSpec, secondInput,
                temporalJoinSpec, secondStateSpec, windowSpec, emissionLimit,
                atomicSpec);
    }

    public DataStream map(FunctionDeploymentReference mapper,
                           StreamSchema outputSchema) {
        return map(mapper, outputSchema, new String[0]);
    }

    /** Declares the stable result identity required by RETRACT transforms. */
    public DataStream map(FunctionDeploymentReference mapper,
            StreamSchema outputSchema, String... outputStableKeyFieldIds) {
        if (mapper == null || outputSchema == null) {
            throw new IllegalArgumentException("mapper and output schema are required");
        }
        List<String> keys = stableKeys(outputStableKeyFieldIds);
        DataStream mapped = new DataStream(session, Kind.MAP, this, null,
                mapper, outputSchema, null, keys);
        session.registerNode(mapped);
        return mapped;
    }

    public DataStream filter(FunctionDeploymentReference predicate) {
        if (predicate == null) throw new IllegalArgumentException("predicate is required");
        DataStream filtered = new DataStream(session, Kind.FILTER, this, null,
                predicate, schema, null, keyFields);
        session.registerNode(filtered);
        return filtered;
    }

    public DataStream calc(StreamCalcSpec specification,
            StreamSchema outputSchema) {
        if (specification == null || outputSchema == null) {
            throw new IllegalArgumentException(
                    "CALC specification and output schema are required");
        }
        DataStream result = new DataStream(session, Kind.CALC, this, null,
                null, outputSchema, null, Collections.<String>emptyList(),
                null, null, null, null, null, null, specification);
        session.registerNode(result);
        return result;
    }

    public DataStream assignEventTime(StreamEventTimeSpec specification) {
        if (specification == null) {
            throw new IllegalArgumentException(
                    "event-time specification is required");
        }
        DataStream result = new DataStream(session,
                Kind.ASSIGN_EVENT_TIME, this, null, null, schema, null,
                keyFields, null, null, null, null, null, null,
                specification);
        session.registerNode(result);
        return result;
    }

    public DataStream keyedProcess(StreamProcessSpec specification) {
        if (specification == null) {
            throw new IllegalArgumentException(
                    "keyed process specification is required");
        }
        DataStream result = new DataStream(session, Kind.KEYED_PROCESS,
                this, null, specification.function(),
                specification.outputSchema(), null,
                specification.outputStableKeyFieldIds(), null, null, null,
                null, null, null, specification);
        session.registerNode(result);
        return result;
    }

    public StreamProcessResult keyedProcessWithOutputs(
            StreamProcessSpec specification, String operatorUid) {
        if (specification == null || specification.outputTags().isEmpty()
                || operatorUid == null || operatorUid.isEmpty()) {
            throw new IllegalArgumentException(
                    "tagged process specification and UID are required");
        }
        DataStream main = keyedProcess(specification).uid(operatorUid);
        return new StreamProcessResult(main, specification.outputTags());
    }

    public DataStream keyedCoProcess(DataStream right,
            StreamCoProcessSpec specification) {
        if (right == null || right.session != session) {
            throw new IllegalArgumentException(
                    "co-process inputs must belong to the same session");
        }
        if (specification == null) {
            throw new IllegalArgumentException(
                    "co-process specification is required");
        }
        DataStream result = new DataStream(session, Kind.KEYED_CO_PROCESS,
                this, null, specification.function(),
                specification.outputSchema(), null,
                specification.outputStableKeyFieldIds(), null, right, null,
                null, null, null, specification);
        session.registerNode(result);
        return result;
    }

    public StreamProcessResult keyedCoProcessWithOutputs(DataStream right,
            StreamCoProcessSpec specification, String operatorUid) {
        if (specification == null || specification.outputTags().isEmpty()
                || operatorUid == null || operatorUid.isEmpty()) {
            throw new IllegalArgumentException(
                    "tagged co-process specification and UID are required");
        }
        DataStream main = keyedCoProcess(right, specification)
                .uid(operatorUid);
        return new StreamProcessResult(main, specification.outputTags());
    }

    public DataStream genericWindow(StreamGenericWindowSpec specification) {
        if (specification == null) {
            throw new IllegalArgumentException(
                    "generic window specification is required");
        }
        DataStream result = new DataStream(session, Kind.GENERIC_WINDOW,
                this, null, specification.function(),
                specification.outputSchema(), null,
                specification.outputStableKeyFieldIds(),
                specification.state(), null, null, null, null, null,
                specification);
        session.registerNode(result);
        return result;
    }

    public StreamWindowResult genericWindowWithLateOutput(
            StreamGenericWindowSpec specification, String operatorUid) {
        if (specification == null || specification.lateOutput() == null
                || operatorUid == null || operatorUid.isEmpty()) {
            throw new IllegalArgumentException(
                    "late-output window specification and UID are required");
        }
        DataStream main = genericWindow(specification).uid(operatorUid);
        return new StreamWindowResult(main, specification.lateOutput());
    }

    public DataStream asyncWait(StreamAsyncWaitSpec specification) {
        if (specification == null) {
            throw new IllegalArgumentException(
                    "async wait specification is required");
        }
        DataStream result = new DataStream(session, Kind.ASYNC_WAIT, this,
                null, specification.function(), specification.outputSchema(),
                null, specification.outputStableKeyFieldIds(), null, null,
                null, null, null, null, specification);
        session.registerNode(result);
        return result;
    }

    public DataStream flatMap(FunctionDeploymentReference mapper,
            StreamSchema outputSchema, StreamEmissionLimit emissionLimit) {
        return flatMap(mapper, outputSchema, emissionLimit, new String[0]);
    }

    /** Declares the stable identity of every bounded flatMap result. */
    public DataStream flatMap(FunctionDeploymentReference mapper,
            StreamSchema outputSchema, StreamEmissionLimit emissionLimit,
            String... outputStableKeyFieldIds) {
        if (mapper == null || outputSchema == null || emissionLimit == null) {
            throw new IllegalArgumentException(
                    "mapper, output schema and emission limit are required");
        }
        List<String> keys = stableKeys(outputStableKeyFieldIds);
        DataStream mapped = new DataStream(session, Kind.FLAT_MAP, this, null,
                mapper, outputSchema, null, keys,
                null, null, null, null, null, emissionLimit);
        session.registerNode(mapped);
        return mapped;
    }

    private static List<String> stableKeys(String[] fieldIds) {
        if (fieldIds == null) {
            throw new IllegalArgumentException(
                    "stable result key field IDs are required");
        }
        List<String> keys = new ArrayList<>(fieldIds.length);
        Collections.addAll(keys, fieldIds);
        return keys;
    }

    public DataStream union(DataStream other) {
        if (other == null || other.session != session) {
            throw new IllegalArgumentException(
                    "union inputs must belong to the same session");
        }
        DataStream union = new DataStream(session, Kind.UNION, this, null,
                null, schema, null, keyFields, null, other, null, null,
                null, null);
        session.registerNode(union);
        return union;
    }

    public DataStream keyBy(String... stableFieldIds) {
        if (stableFieldIds == null || stableFieldIds.length == 0) {
            throw new IllegalArgumentException("keyBy requires stable field IDs");
        }
        List<String> keys = new ArrayList<>(stableFieldIds.length);
        Collections.addAll(keys, stableFieldIds);
        DataStream keyed = new DataStream(session, Kind.KEY_BY, this, null,
                null, schema, null, keys);
        session.registerNode(keyed);
        return keyed;
    }

    public DataStream keyedAggregate(FunctionDeploymentReference aggregate,
            StreamSchema outputSchema, StreamStateSpec state) {
        if (aggregate == null || outputSchema == null || state == null) {
            throw new IllegalArgumentException(
                    "aggregate, output schema and state are required");
        }
        DataStream result = new DataStream(session, Kind.KEYED_AGGREGATE,
                this, null, aggregate, outputSchema, null, keyFields, state,
                null, null, null, null);
        session.registerNode(result);
        return result;
    }

    public DataStream temporalJoin(DataStream right,
            StreamTemporalJoinSpec specification,
            FunctionDeploymentReference joinFunction,
            StreamSchema outputSchema, StreamStateSpec leftState,
            StreamStateSpec rightState) {
        if (right == null || right.session != session) {
            throw new IllegalArgumentException(
                    "temporal join inputs must belong to the same session");
        }
        if (specification == null || joinFunction == null || outputSchema == null
                || leftState == null || rightState == null) {
            throw new IllegalArgumentException("temporal join fields are required");
        }
        DataStream result = new DataStream(session, Kind.TEMPORAL_JOIN,
                this, null, joinFunction, outputSchema, null,
                specification.outputStableKeyFieldIds(), leftState, right,
                specification, rightState, null);
        session.registerNode(result);
        return result;
    }

    public DataStream eventTimeDecimalWindow(StreamWindowSpec specification,
            StreamSchema outputSchema, StreamStateSpec state) {
        if (specification == null || outputSchema == null || state == null) {
            throw new IllegalArgumentException(
                    "window specification, output schema and state are required");
        }
        DataStream result = new DataStream(session, Kind.EVENT_TIME_WINDOW,
                this, null, null, outputSchema, null, keyFields, state,
                null, null, null, specification);
        session.registerNode(result);
        return result;
    }

    public void sinkTo(String uid, StreamSinkSpec sink) {
        session.registerOutput(this, uid, sink);
    }

    public StreamSchema schema() { return schema; }
    public DataSchema getSchema() { return schema.toDataSchema(); }

    StreamSession session() { return session; }
    Kind kind() { return kind; }
    DataStream input() { return input; }
    StreamSourceSpec sourceSpec() { return sourceSpec; }
    FunctionDeploymentReference function() { return function; }
    String operatorUid() { return operatorUid; }
    List<String> keyFields() { return keyFields; }
    StreamStateSpec stateSpec() { return stateSpec; }
    DataStream secondInput() { return secondInput; }
    StreamTemporalJoinSpec temporalJoinSpec() { return temporalJoinSpec; }
    StreamStateSpec secondStateSpec() { return secondStateSpec; }
    StreamWindowSpec windowSpec() { return windowSpec; }
    StreamEmissionLimit emissionLimit() { return emissionLimit; }
    StreamCalcSpec calcSpec() { return (StreamCalcSpec) atomicSpec; }
    StreamEventTimeSpec eventTimeSpec() {
        return (StreamEventTimeSpec) atomicSpec;
    }
    StreamProcessSpec processSpec() { return (StreamProcessSpec) atomicSpec; }
    StreamCoProcessSpec coProcessSpec() {
        return (StreamCoProcessSpec) atomicSpec;
    }
    StreamGenericWindowSpec genericWindowSpec() {
        return (StreamGenericWindowSpec) atomicSpec;
    }
    StreamAsyncWaitSpec asyncWaitSpec() {
        return (StreamAsyncWaitSpec) atomicSpec;
    }
    StreamOutputTag outputTag() { return (StreamOutputTag) atomicSpec; }
}
