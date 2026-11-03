package io.rowweave.plan;

import io.rowweave.CollectionSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSinkBinding;
import io.rowweave.BatchSinkDescriptor;
import io.rowweave.Source;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.internal.BatchSourceSplitBindings;
import io.rowweave.internal.BatchSinkBindings;
import io.rowweave.internal.SourceInputSchemas;
import io.rowweave.incremental.IncrementalPlanDescriptor;
import io.rowweave.incremental.IncrementalPlanDescriptorCodec;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * One node of a plan: the serializable description of an operator. An ordinary operator is rebuilt
 * remotely from {@link #typeId}; a code lambda can only travel as an explicit Serializable userFunction
 * payload. A {@link PlanGraph} is a topologically ordered set of PlanNode values, root last, with
 * children referenced by index.
 *
 * <p>The kinds match {@link io.rowweave.DataSet}:
 * <ul>
 * <li><b>SOURCE</b>: no child, carries the {@code inputs} descriptor list and {@code outMeta}.</li>
 * <li><b>UNARY</b>: one child (childIndices[0]), typeId in {filter, select, groupBySum, distinct,
 * orderBy, top}.</li>
 * <li><b>BINARY</b>: two children (left is childIndices[0], right is childIndices[1]), typeId in
 * {union, join, coGroup}.</li>
 * </ul>
 *
 * <p>A describable operator encodes its parameters as strings (field names, expression, join type,
 * sort fields and the like), which is what makes them cross the network.
 * <p>The lambda operators filter (FilterFunction), map (MapFunction) and flatMap (FlatMapFunction)
 * travel through {@link #userFunction()}, marked -3 on the wire; all three extend Serializable.
 * <p>Every plan node is serializable: each one is either describable through typeId and params, or
 * holds a serializable userFunction, and either way it can go onto the wire.
 */
public final class PlanNode {

    // SINK is appended last so its ordinal stays stable and older serialized forms still read. A SINK is
    // a unary outlet node holding a Sink description in the __output_type__ param, and it is always the
    // end of the DAG with every stage feeding it as a stream, which removes any special case that
    // materializes the terminal result in full.
    public enum Kind { SOURCE, UNARY, BINARY, SINK }

    /** Param key naming the Sink type of a SINK node: "collecting" sends rows back to the client, and the driver side may also attach a file or print Sink. */
    public static final String OUTPUT_TYPE_PARAM = "__output_type__";
    /** Type identifier for CollectingOutput, which returns rows to the client and may ship to a worker. */
    public static final String OUTPUT_COLLECTING = "collecting";

    /** Reserved param key for the node name, which round-trips through the existing param serialization and does not change the binary protocol. */
    public static final String NAME_PARAM = "__name__";
    /** Reserved param key for the file:line call site in user code, shown on the node in DAG visualizations. */
    public static final String LOC_PARAM = "__loc__";
    /** Reserved param key for the logical operator identity, stable across compiles and publications and independent of the runtime stage or job identity. */
    public static final String OPERATOR_UID_PARAM = "__operator_uid__";
    /** Reserved param key for the canonical split descriptor frames of a SOURCE, one per Source position. */
    public static final String SOURCE_SPLIT_DESCRIPTORS_PARAM =
            "__batch_source_split_descriptors_v1__";
    /** Reserved param key for the single canonical bounded descriptor frame of the only incremental SOURCE. */
    public static final String INCREMENTAL_PLAN_DESCRIPTOR_PARAM =
            "__incremental_plan_descriptor_v1__";
    public static final int MAX_INCREMENTAL_PLAN_DESCRIPTOR_BASE64URL_CHARS =
            (IncrementalPlanDescriptorCodec.MAX_FRAME_BYTES * 8 + 5) / 6;
    /** Reserved param key for the canonical bounded capability descriptor of a SINK's Sink. */
    public static final String SINK_DESCRIPTOR_PARAM =
            "__batch_sink_descriptor_v1__";

    private final Kind kind;
    private final String typeId;
    private final DataSchema outMeta;
    private final Map<String, String> params;
    private final Map<String, List<String>> listParams;
    private final int[] childIndices;
    private final List<Source> inputs;  // SOURCE only; null otherwise. A SOURCE holds its Sources uniformly and reads them lazily, pulling nothing ahead.
    private final List<BatchSourceSplitBinding> sourceSplitBindings; // SOURCE only
    private final List<BatchSourceSplitDescriptor> sourceSplitDescriptors; // SOURCE only
    private final IncrementalPlanDescriptor incrementalPlanDescriptor; // SOURCE only, optional
    private final BatchSinkBinding batchSinkBinding; // SINK with Sink only
    private final BatchSinkDescriptor batchSinkDescriptor; // SINK with Sink only
    private final io.rowweave.Sink output;  // SINK only; null otherwise. Holds the serialized Sink instance that travels with the plan to the worker.
    // The serializable userFunction of a UNARY or BINARY code lambda (filterFn, map, flatMap,
    // reduceGroup, coGroup). It is genuinely serialized and shipped to the worker under the wire codec
    // -3 marker, which is what lets a code operator run on a cluster. null means the node is not a code
    // lambda.
    private final java.io.Serializable userFunction;

    private PlanNode(Kind kind, String typeId, DataSchema outMeta,
                     Map<String, String> params, Map<String, List<String>> listParams,
                     int[] childIndices, List<Source> inputs, io.rowweave.Sink output) {
        this(kind, typeId, outMeta, params, listParams, childIndices, inputs, output, null);
    }

    private PlanNode(Kind kind, String typeId, DataSchema outMeta,
                     Map<String, String> params, Map<String, List<String>> listParams,
                     int[] childIndices, List<Source> inputs, io.rowweave.Sink output,
                     java.io.Serializable userFunction) {
        this.kind = kind;
        this.typeId = typeId;
        this.outMeta = outMeta;
        validateIncrementalDescriptorEnvelope(kind, listParams);
        this.params = Collections.unmodifiableMap(new LinkedHashMap<>(params));
        this.childIndices = childIndices.clone();
        if (kind == Kind.SOURCE) {
            List<Source> validatedInputs = SourceInputSchemas.validateAndCopy(
                    outMeta, inputs, "PlanNode.sourceFromInputs", false);
            LinkedHashMap<String, List<String>> normalizedListParams =
                    mutableListMap(listParams);
            List<BatchSourceSplitBinding> bindings;
            if (normalizedListParams.containsKey(SOURCE_SPLIT_DESCRIPTORS_PARAM)) {
                bindings = BatchSourceSplitBindings.decodeFrozen(
                        outMeta, validatedInputs,
                        normalizedListParams.get(SOURCE_SPLIT_DESCRIPTORS_PARAM));
            } else {
                bindings = BatchSourceSplitBindings.freezeNew(outMeta, validatedInputs);
            }
            normalizedListParams.put(SOURCE_SPLIT_DESCRIPTORS_PARAM,
                    BatchSourceSplitBindings.encodeCanonical(bindings));
            this.incrementalPlanDescriptor = decodeIncrementalDescriptor(
                    normalizedListParams.get(INCREMENTAL_PLAN_DESCRIPTOR_PARAM));
            this.listParams = unmodifiableListMap(normalizedListParams);
            this.inputs = validatedInputs;
            this.sourceSplitBindings = bindings;
            List<BatchSourceSplitDescriptor> descriptors = new ArrayList<>(bindings.size());
            for (BatchSourceSplitBinding binding : bindings) {
                descriptors.add(binding.descriptor());
            }
            this.sourceSplitDescriptors = Collections.unmodifiableList(descriptors);
            this.batchSinkBinding = null;
            this.batchSinkDescriptor = null;
        } else if (kind == Kind.SINK && output != null) {
            LinkedHashMap<String, List<String>> normalizedListParams =
                    mutableListMap(listParams);
            BatchSinkBinding binding;
            if (normalizedListParams.containsKey(SINK_DESCRIPTOR_PARAM)) {
                binding = BatchSinkBindings.decodeFrozen(
                        outMeta, output,
                        normalizedListParams.get(SINK_DESCRIPTOR_PARAM));
            } else {
                binding = BatchSinkBindings.freezeNew(outMeta, output);
            }
            normalizedListParams.put(SINK_DESCRIPTOR_PARAM,
                    BatchSinkBindings.encodeCanonical(binding));
            this.listParams = unmodifiableListMap(normalizedListParams);
            this.inputs = null;
            this.sourceSplitBindings = null;
            this.sourceSplitDescriptors = null;
            this.incrementalPlanDescriptor = rejectIncrementalDescriptor(listParams);
            this.batchSinkBinding = binding;
            this.batchSinkDescriptor = binding.descriptor();
        } else {
            this.listParams = unmodifiableListMap(listParams);
            this.inputs = null;
            this.sourceSplitBindings = null;
            this.sourceSplitDescriptors = null;
            this.incrementalPlanDescriptor = rejectIncrementalDescriptor(listParams);
            this.batchSinkBinding = null;
            this.batchSinkDescriptor = null;
        }
        this.output = output;
        this.userFunction = userFunction;
    }

    /** Builds a UNARY code-lambda node holding a serializable userFunction: typeId is filterFn, map or flatMap, with no params. */
    public static PlanNode unaryFn(String typeId, DataSchema outMeta, int childIndex, java.io.Serializable userFunction) {
        return new PlanNode(Kind.UNARY, typeId, outMeta, new LinkedHashMap<>(), new LinkedHashMap<>(),
                new int[]{childIndex}, null, null, userFunction);
    }

    /**
     * Builds a UNARY node holding params and listParams together with a serializable userFunction,
     * which is reduceGroup: its keys arrive as a listParam and its body is a GroupReduceFunction.
     */
    public static PlanNode unaryFnWithParams(String typeId, DataSchema outMeta, int childIndex,
                                             Map<String, String> params, Map<String, List<String>> listParams,
                                             java.io.Serializable userFunction) {
        return new PlanNode(Kind.UNARY, typeId, outMeta, params, listParams,
                new int[]{childIndex}, null, null, userFunction);
    }

    /**
     * Builds a BINARY node holding params and listParams together with a serializable userFunction,
     * which is coGroup: leftKeys and rightKeys arrive as listParams and its body is a CoGroupFunction.
     */
    public static PlanNode binaryFn(String typeId, DataSchema outMeta, int leftIndex, int rightIndex,
                                    Map<String, String> params, Map<String, List<String>> listParams,
                                    java.io.Serializable userFunction) {
        return new PlanNode(Kind.BINARY, typeId, outMeta, params, listParams,
                new int[]{leftIndex, rightIndex}, null, null, userFunction);
    }

    /** The serializable userFunction (filterFn, map, flatMap, reduceGroup or coGroup); null means the node is not a code lambda. */
    public java.io.Serializable userFunction() {
        return userFunction;
    }

    /**
     * A SOURCE holds a list of Sources, read lazily with no rows pulled ahead. In-memory rows are wrapped
     * as one {@link CollectionSource}; a stage boundary uses the zero-Source payload of
     * {@link #placeholderSource(DataSchema)}. toPlanGraph on the driver reads no rows, and the execution
     * side iterates lazily.
     */
    public static PlanNode sourceFromInputs(DataSchema outMeta, List<Source> inputs) {
        return new PlanNode(Kind.SOURCE, "source", outMeta,
                new LinkedHashMap<>(), new LinkedHashMap<>(), new int[0], inputs, null);
    }

    /** Wire rebuild path: missing metadata is an explicit legacy downgrade, never an upgrade. */
    static PlanNode sourceFromWire(DataSchema outMeta, List<Source> inputs,
                                   Map<String, String> params,
                                   Map<String, List<String>> listParams) {
        LinkedHashMap<String, List<String>> normalized = mutableListMap(listParams);
        if (!normalized.containsKey(SOURCE_SPLIT_DESCRIPTORS_PARAM)) {
            normalized.put(SOURCE_SPLIT_DESCRIPTORS_PARAM,
                    BatchSourceSplitBindings.encodeCanonical(
                            BatchSourceSplitBindings.freezeLegacyWire(outMeta, inputs)));
        }
        return new PlanNode(Kind.SOURCE, "source", outMeta, params, normalized,
                new int[0], inputs, null);
    }

    /** A stage-boundary SOURCE bound at execution time to an external channel; its wire payload is the native inputCount=0. */
    public static PlanNode placeholderSource(DataSchema outMeta) {
        return sourceFromInputs(outMeta, Collections.emptyList());
    }

    /** Convenience: wrap rows already materialized as a single {@link CollectionSource} SOURCE, for small data and tests. It goes through the same inputs payload. */
    public static PlanNode source(DataSchema outMeta, List<DataRow> rows) {
        return sourceFromInputs(outMeta,
                java.util.Collections.singletonList(new CollectionSource(outMeta, rows)));
    }

    public static Builder unary(String typeId, DataSchema outMeta, int childIndex) {
        return new Builder(Kind.UNARY, typeId, outMeta, new int[]{childIndex});
    }

    public static Builder binary(String typeId, DataSchema outMeta, int leftIndex, int rightIndex) {
        return new Builder(Kind.BINARY, typeId, outMeta, new int[]{leftIndex, rightIndex});
    }

    /**
     * A SINK outlet node: unary, with one child, holding the Sink type, which to start with is
     * "collecting" to send rows back to the client. The DAG always ends at a SINK and every stage feeds
     * it as a stream, which removes any special case that materializes the terminal result in full. Its
     * outMeta is the child's outMeta, passed through.
     */
    public static Builder sink(DataSchema outMeta, int childIndex, String outputType) {
        Builder b = new Builder(Kind.SINK, "sink", outMeta, new int[]{childIndex});
        b.param(OUTPUT_TYPE_PARAM, outputType);
        return b;
    }

    /**
     * A SINK holds the serialized Sink instance itself, because the Sink executes in the last stage on
     * the worker: data leaves only through the Sink and is not returned to the client. It ships with the
     * PlanGraph wire codec. This supersedes the {@link #OUTPUT_TYPE_PARAM} string, which is still read
     * for compatibility.
     */
    public static Builder sink(DataSchema outMeta, int childIndex, io.rowweave.Sink output) {
        Builder b = new Builder(Kind.SINK, "sink", outMeta, new int[]{childIndex});
        b.output = output;
        return b;
    }

    /** Wire rebuild: missing metadata only bridges exact built-ins; custom capability stays legacy. */
    static PlanNode sinkFromWire(
            DataSchema outMeta, int childIndex, io.rowweave.Sink output,
            Map<String, String> params, Map<String, List<String>> listParams) {
        LinkedHashMap<String, List<String>> normalized = mutableListMap(listParams);
        if (!normalized.containsKey(SINK_DESCRIPTOR_PARAM)) {
            normalized.put(SINK_DESCRIPTOR_PARAM,
                    BatchSinkBindings.encodeCanonical(
                            BatchSinkBindings.freezeLegacyWire(outMeta, output)));
        }
        return new PlanNode(Kind.SINK, "sink", outMeta, params, normalized,
                new int[]{childIndex}, null, output);
    }

    public Kind kind() {
        return kind;
    }

    public String typeId() {
        return typeId;
    }

    /** The node name: what the user named it, or typeId#index by default, falling back to typeId when unset. Shown in DAG visualizations. */
    public String name() {
        String n = params.get(NAME_PARAM);
        return (n != null && !n.isEmpty()) ? n : typeId;
    }

    /** Returns a named copy, leaving this node unchanged. A SOURCE node can be named this way too. */
    public PlanNode withName(String name) {
        LinkedHashMap<String, String> p = new LinkedHashMap<>(params);
        p.put(NAME_PARAM, name);
        return new PlanNode(kind, typeId, outMeta, p, listParams, childIndices, inputs, output, userFunction);
    }

    /** The file:line call site in user code; null when unset. */
    public String location() {
        return params.get(LOC_PARAM);
    }

    /** Returns a copy carrying the source location, leaving this node unchanged. */
    public PlanNode withLocation(String loc) {
        if (loc == null) {
            return this;
        }
        LinkedHashMap<String, String> p = new LinkedHashMap<>(params);
        p.put(LOC_PARAM, loc);
        return new PlanNode(kind, typeId, outMeta, p, listParams, childIndices, inputs, output, userFunction);
    }

    /**
     * Returns a copy with one string param added or changed, leaving this node unchanged. kind,
     * childIndices, meta and listParams are untouched, so the node keeps its position and structure in
     * the PlanGraph and its index stays stable. An optimizer rule uses this to attach a derived
     * annotation such as {@code __fused_chain_len__}.
     */
    public PlanNode withParam(String key, String value) {
        LinkedHashMap<String, String> p = new LinkedHashMap<>(params);
        p.put(key, value);
        return new PlanNode(kind, typeId, outMeta, p, listParams, childIndices, inputs, output, userFunction);
    }

    /** Returns a copy with one list param added or changed, used to keep SOURCE extension metadata across the wire and rebuild. */
    public PlanNode withListParam(String key, List<String> value) {
        LinkedHashMap<String, List<String>> lp = new LinkedHashMap<>(listParams);
        lp.put(key, value);
        return new PlanNode(kind, typeId, outMeta, params, lp, childIndices, inputs, output, userFunction);
    }

    /** Attach the only canonical incremental descriptor to a SOURCE without changing its data bindings. */
    public PlanNode withIncrementalPlanDescriptor(IncrementalPlanDescriptor descriptor) {
        if (kind != Kind.SOURCE) {
            throw new IllegalStateException("incremental descriptor requires SOURCE node");
        }
        if (descriptor == null) {
            throw new IllegalArgumentException("incremental descriptor is required");
        }
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(
                IncrementalPlanDescriptorCodec.encode(descriptor));
        return withListParam(INCREMENTAL_PLAN_DESCRIPTOR_PARAM,
                Collections.singletonList(encoded));
    }

    /** Replaces the application-level Source splits of a SOURCE while keeping its identity and metadata intact. */
    public PlanNode withInputs(List<Source> replacementInputs) {
        if (kind != Kind.SOURCE) {
            throw new IllegalStateException("withInputs requires SOURCE node");
        }
        LinkedHashMap<String, List<String>> resetMetadata = mutableListMap(listParams);
        resetMetadata.remove(SOURCE_SPLIT_DESCRIPTORS_PARAM);
        return new PlanNode(kind, typeId, outMeta, params, resetMetadata, childIndices,
                replacementInputs, output, userFunction);
    }

    /** Replace a SOURCE with an already-frozen binding subset, preserving exact descriptor identity. */
    public PlanNode withSourceSplitBindings(List<BatchSourceSplitBinding> replacementBindings) {
        if (kind != Kind.SOURCE) {
            throw new IllegalStateException("withSourceSplitBindings requires SOURCE node");
        }
        if (replacementBindings == null) {
            throw new IllegalArgumentException("batch source bindings are required");
        }
        List<Source> replacementInputs = new ArrayList<>(replacementBindings.size());
        for (BatchSourceSplitBinding binding : replacementBindings) {
            if (binding == null) {
                throw new IllegalArgumentException("batch source binding must not be null");
            }
            replacementInputs.add(binding.input());
        }
        LinkedHashMap<String, List<String>> frozenMetadata = mutableListMap(listParams);
        frozenMetadata.put(SOURCE_SPLIT_DESCRIPTORS_PARAM,
                BatchSourceSplitBindings.encodeCanonical(replacementBindings));
        return new PlanNode(kind, typeId, outMeta, params, frozenMetadata, childIndices,
                replacementInputs, output, userFunction);
    }

    public DataSchema outMeta() {
        return outMeta;
    }

    public String param(String key) {
        return params.get(key);
    }

    public int intParam(String key) {
        return Integer.parseInt(params.get(key));
    }

    public Map<String, String> params() {
        return params;
    }

    public List<String> listParam(String key) {
        List<String> v = listParams.get(key);
        return v == null ? Collections.emptyList() : v;
    }

    public Map<String, List<String>> listParams() {
        return listParams;
    }

    public int[] childIndices() {
        return childIndices.clone();
    }

    public int childCount() {
        return childIndices.length;
    }

    public int child(int i) {
        return childIndices[i];
    }

    /** The lazy list of Sources held by a SOURCE; null for any other kind. */
    public List<Source> inputs() {
        return inputs;
    }

    /** SOURCE-only immutable Source/descriptor bindings in application Source[] order. */
    public List<BatchSourceSplitBinding> sourceSplitBindings() {
        if (kind != Kind.SOURCE) {
            throw new IllegalStateException("sourceSplitBindings requires SOURCE node");
        }
        return sourceSplitBindings;
    }

    /** SOURCE-only immutable descriptor view in application Source[] order. */
    public List<BatchSourceSplitDescriptor> sourceSplitDescriptors() {
        if (kind != Kind.SOURCE) {
            throw new IllegalStateException("sourceSplitDescriptors requires SOURCE node");
        }
        return sourceSplitDescriptors;
    }

    public boolean hasIncrementalPlanDescriptor() {
        return incrementalPlanDescriptor != null;
    }

    public IncrementalPlanDescriptor incrementalPlanDescriptor() {
        if (kind != Kind.SOURCE || incrementalPlanDescriptor == null) {
            throw new IllegalStateException("incrementalPlanDescriptor requires an incremental SOURCE");
        }
        return incrementalPlanDescriptor;
    }

    /** Called immediately before Plan serialization to reject mutable connector identity drift. */
    void validateCurrentSourceBindings() {
        if (kind == Kind.SOURCE) {
            BatchSourceSplitBindings.validateCurrent(sourceSplitBindings);
        }
    }

    /** Called before Plan serialization/admission to reject mutable Sink identity drift. */
    void validateCurrentSinkBinding() {
        if (batchSinkBinding != null) {
            BatchSinkBindings.validateCurrent(batchSinkBinding);
        }
    }

    public BatchSinkBinding batchSinkBinding() {
        if (kind != Kind.SINK || batchSinkBinding == null) {
            throw new IllegalStateException("batchSinkBinding requires a SINK Sink");
        }
        return batchSinkBinding;
    }

    public BatchSinkDescriptor batchSinkDescriptor() {
        if (kind != Kind.SINK || batchSinkDescriptor == null) {
            throw new IllegalStateException("batchSinkDescriptor requires a SINK Sink");
        }
        return batchSinkDescriptor;
    }

    /** The Sink instance held by a SINK; null unless this is a SINK, and null also for a legacy SINK that only carries the {@link #OUTPUT_TYPE_PARAM} string. */
    public io.rowweave.Sink output() {
        return output;
    }

    private static Map<String, List<String>> unmodifiableListMap(Map<String, List<String>> src) {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : src.entrySet()) {
            copy.put(e.getKey(), Collections.unmodifiableList(new ArrayList<>(e.getValue())));
        }
        return Collections.unmodifiableMap(copy);
    }

    private static IncrementalPlanDescriptor decodeIncrementalDescriptor(List<String> values) {
        if (values == null) return null;
        if (values.size() != 1 || values.get(0) == null || values.get(0).isEmpty()
                || values.get(0).indexOf('=') >= 0) {
            throw new IllegalArgumentException(
                    "incremental plan descriptor must be one base64url frame without padding");
        }
        final byte[] frame;
        try {
            frame = Base64.getUrlDecoder().decode(values.get(0));
        } catch (IllegalArgumentException malformed) {
            throw new IllegalArgumentException("incremental plan descriptor is not canonical base64url", malformed);
        }
        IncrementalPlanDescriptor descriptor = IncrementalPlanDescriptorCodec.decode(frame);
        String canonical = Base64.getUrlEncoder().withoutPadding().encodeToString(
                IncrementalPlanDescriptorCodec.encode(descriptor));
        if (!canonical.equals(values.get(0))) {
            throw new IllegalArgumentException("incremental plan descriptor is not canonical base64url");
        }
        return descriptor;
    }

    private static void validateIncrementalDescriptorEnvelope(
            Kind kind, Map<String, List<String>> listParams) {
        if (!listParams.containsKey(INCREMENTAL_PLAN_DESCRIPTOR_PARAM)) return;
        if (kind != Kind.SOURCE) {
            throw new IllegalArgumentException("incremental plan descriptor is SOURCE-only");
        }
        List<String> values = listParams.get(INCREMENTAL_PLAN_DESCRIPTOR_PARAM);
        if (values == null || values.size() != 1) {
            throw new IllegalArgumentException(
                    "incremental plan descriptor must have exactly one value");
        }
        String encoded = values.get(0);
        if (encoded == null || encoded.isEmpty()
                || encoded.length() > MAX_INCREMENTAL_PLAN_DESCRIPTOR_BASE64URL_CHARS
                || encoded.indexOf('=') >= 0) {
            throw new IllegalArgumentException(
                    "incremental plan descriptor base64url length is invalid");
        }
    }

    private static IncrementalPlanDescriptor rejectIncrementalDescriptor(
            Map<String, List<String>> listParams) {
        if (listParams.containsKey(INCREMENTAL_PLAN_DESCRIPTOR_PARAM)) {
            throw new IllegalArgumentException("incremental plan descriptor is SOURCE-only");
        }
        return null;
    }

    private static LinkedHashMap<String, List<String>> mutableListMap(
            Map<String, List<String>> src) {
        LinkedHashMap<String, List<String>> copy = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> e : src.entrySet()) {
            copy.put(e.getKey(), new ArrayList<>(e.getValue()));
        }
        return copy;
    }

    /** Immutable builder: fill parameters by chaining and produce the node at build(), since a node that exists is never modified in place. */
    public static final class Builder {
        private final Kind kind;
        private final String typeId;
        private final DataSchema outMeta;
        private final int[] childIndices;
        private final Map<String, String> params = new LinkedHashMap<>();
        private final Map<String, List<String>> listParams = new LinkedHashMap<>();
        private io.rowweave.Sink output;  // the Sink instance of a SINK, set only by the sink factory overload that accepts a Sink

        private Builder(Kind kind, String typeId, DataSchema outMeta, int[] childIndices) {
            this.kind = kind;
            this.typeId = typeId;
            this.outMeta = outMeta;
            this.childIndices = childIndices;
        }

        public Builder param(String key, String value) {
            params.put(key, value);
            return this;
        }

        public Builder param(String key, int value) {
            params.put(key, Integer.toString(value));
            return this;
        }

        public Builder listParam(String key, List<String> values) {
            listParams.put(key, new ArrayList<>(values));
            return this;
        }

        public PlanNode build() {
            return new PlanNode(kind, typeId, outMeta, params, listParams, childIndices, null, output);
        }
    }
}
