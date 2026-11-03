package io.rowweave;

import io.rowweave.ops.Distinct;
import io.rowweave.ops.FlatMap;
import io.rowweave.ops.Filter;
import io.rowweave.ops.Map;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.ReduceGroup;
import io.rowweave.ops.SchemaConversion;
import io.rowweave.ops.Select;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.ops.TopN;
import io.rowweave.ops.Union;
import io.rowweave.ops.WholeTableAgg;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.SortMergeJoin;
import io.rowweave.sched.PipelineExecutor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Skeleton of the user-facing DAG construction API: immutable nodes, chained builders forming a SOURCE plus UNARY linear chain,
 * compiled into a sequence of {@link PipelineExecutor} stages that push data to a {@link Sink} (the Sink is written by the Worker at the end of execution).
 *
 * <p>Only SOURCE plus pass-through UNARY nodes (filter, select column pruning, map, flatMap). breaker
 * (groupBy, orderBy, ...); binary (join, coGroup, union); the expression forms of filter and select; ExecutionEnvironment.commit and the cluster path.
 *
 * <p>Immutable: every builder returns a new node holding parent (the upstream) plus this node's stage. {@code executeToOutput} builds a fresh
 * PipelineExecutor each time (its stages are mutable and cannot be reused); the compile logic is extracted into {@link #compileStages} so ExecutionEnvironment.commit can reuse it.
 */
public final class DataSet {

    private enum Kind { SOURCE, UNARY, BINARY }

    private final DataSchema dataSchema;
    private final Kind kind;
    private final DataSet parent;                // null for SOURCE/BINARY (UNARY has one upstream).
    private final Iterable<DataRow> source;          // SOURCE only (must be iterable repeatedly); null when sourceInputs is non-empty.
    // One logical SOURCE holding several homogeneous Source splits (the fromSource(Source[]) shape): the
    // runtime (physical DAG) spreads them over several instances according to parallelism (single machine runLinearSourceShardedParallel, cluster placement).
    // The API layer no longer collapses them into a multi-source union. When non-empty, compilation uses this; otherwise it uses the single source Iterable.
    private final java.util.List<Source> sourceInputs; // SOURCE only; may be null.
    private final PipelineExecutor.Stage stage;   // UNARY only (the operator stage).
    private final DataSet left;                  // BINARY only (left upstream).
    private final DataSet right;                 // BINARY only (right upstream).
    private final PipelineExecutor.BinaryStage binaryStage; // BINARY only
    private final ExecutionEnvironment session;             // SOURCE only (the one built by fromSource carries a session, for output and commit); may be null.
    // Describable operator form (UNARY/BINARY: typeId plus params/listParams); null for a lambda node,
    // which is filter(fn), map or flatMap. A graph containing any lambda node makes toPlanGraph take the in-process Stage path.
    private final Descriptor descriptor;
    /** The user's explicit logical identity; null means the positional fallback is used for compatibility. */
    private final String operatorUid;

    /**
     * Describable operator form: typeId plus string params/listParams (the descriptive form), or userFunction (the code-lambda
     * forms filterFn, map, flatMap, which must be serializable). The two are mutually exclusive. toPlanGraph uses it to build a serializable PlanNode.
     */
    private static final class Descriptor {
        final String typeId;
        final java.util.Map<String, String> params;
        final java.util.Map<String, java.util.List<String>> listParams;
        final java.io.Serializable userFunction; // held by a code-lambda node; null for the descriptive form.

        Descriptor(String typeId, java.util.Map<String, String> params,
                   java.util.Map<String, java.util.List<String>> listParams) {
            this(typeId, params, listParams, null);
        }

        Descriptor(String typeId, java.util.Map<String, String> params,
                   java.util.Map<String, java.util.List<String>> listParams, java.io.Serializable userFunction) {
            this.typeId = typeId;
            this.params = params;
            this.listParams = listParams;
            this.userFunction = userFunction;
        }
    }

    private DataSet(DataSchema dataSchema, Kind kind, DataSet parent,
                     Iterable<DataRow> source, PipelineExecutor.Stage stage) {
        this(dataSchema, kind, parent, source, stage, null, null, null, null, null);
    }

    private DataSet(DataSchema dataSchema, Kind kind, DataSet parent, Iterable<DataRow> source,
                     PipelineExecutor.Stage stage, DataSet left, DataSet right,
                     PipelineExecutor.BinaryStage binaryStage, ExecutionEnvironment session) {
        this(dataSchema, kind, parent, source, stage, left, right, binaryStage, session, null);
    }

    private DataSet(DataSchema dataSchema, Kind kind, DataSet parent, Iterable<DataRow> source,
                     PipelineExecutor.Stage stage, DataSet left, DataSet right,
                     PipelineExecutor.BinaryStage binaryStage, ExecutionEnvironment session, Descriptor descriptor) {
        this(dataSchema, kind, parent, source, stage, left, right, binaryStage, session, descriptor, null);
    }

    private DataSet(DataSchema dataSchema, Kind kind, DataSet parent, Iterable<DataRow> source,
                     PipelineExecutor.Stage stage, DataSet left, DataSet right,
                     PipelineExecutor.BinaryStage binaryStage, ExecutionEnvironment session, Descriptor descriptor,
                     java.util.List<Source> sourceInputs) {
        this(dataSchema, kind, parent, source, stage, left, right, binaryStage, session, descriptor,
                sourceInputs, null);
    }

    private DataSet(DataSchema dataSchema, Kind kind, DataSet parent, Iterable<DataRow> source,
                     PipelineExecutor.Stage stage, DataSet left, DataSet right,
                     PipelineExecutor.BinaryStage binaryStage, ExecutionEnvironment session, Descriptor descriptor,
                     java.util.List<Source> sourceInputs, String operatorUid) {
        this.dataSchema = dataSchema;
        this.kind = kind;
        this.parent = parent;
        this.source = source;
        this.stage = stage;
        this.left = left;
        this.right = right;
        this.binaryStage = binaryStage;
        this.session = session;
        this.descriptor = descriptor;
        this.sourceInputs = sourceInputs;
        this.operatorUid = operatorUid;
    }

    /** Builds a describable UNARY node (typeId plus params and listParams; the stage is also kept for the single-machine or mixed lambda graph fallback). */
    private DataSet unaryDescribable(DataSchema outMeta, PipelineExecutor.Stage stage, String typeId,
                                      java.util.Map<String, String> params,
                                      java.util.Map<String, java.util.List<String>> listParams) {
        return new DataSet(outMeta, Kind.UNARY, this, null, stage, null, null, null, null,
                new Descriptor(typeId, params, listParams));
    }

    /** Builds a code-lambda describable UNARY node (typeId filterFn, map or flatMap plus a serializable userFunction). */
    private DataSet unaryFnDescribable(DataSchema outMeta, PipelineExecutor.Stage stage, String typeId,
                                        java.io.Serializable userFunction) {
        return new DataSet(outMeta, Kind.UNARY, this, null, stage, null, null, null, null,
                new Descriptor(typeId, java.util.Collections.emptyMap(), java.util.Collections.emptyMap(),
                        userFunction));
    }

    /** Builds a bounded code-function unary node with exact scalar parameters. */
    private DataSet unaryFnWithParamsDescribable(
            DataSchema outMeta, PipelineExecutor.Stage stage, String typeId,
            java.util.Map<String, String> params,
            java.io.Serializable userFunction) {
        return new DataSet(outMeta, Kind.UNARY, this, null, stage,
                null, null, null, null,
                new Descriptor(typeId, params,
                        java.util.Collections.emptyMap(), userFunction));
    }

    /** Builds a describable BINARY node (left is this, right is the argument; typeId plus params and listParams; binaryStage is kept for the mixed graph fallback). */
    private DataSet binaryDescribable(DataSchema outMeta, DataSet right, PipelineExecutor.BinaryStage binaryStage,
                                       String typeId, java.util.Map<String, String> params,
                                       java.util.Map<String, java.util.List<String>> listParams) {
        return new DataSet(outMeta, Kind.BINARY, null, null, null, this, right, binaryStage, null,
                new Descriptor(typeId, params, listParams));
    }

    private static java.util.Map<String, String> params(String... kv) {
        java.util.Map<String, String> m = new java.util.LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            if (kv[i + 1] != null) {
                m.put(kv[i], kv[i + 1]);
            }
        }
        return m;
    }

    private static java.util.Map<String, java.util.List<String>> listParams(String key, java.util.List<String> vals) {
        java.util.Map<String, java.util.List<String>> m = new java.util.LinkedHashMap<>();
        m.put(key, vals);
        return m;
    }

    /** A SOURCE node with no session (static entry point); {@code rows} must be iterable repeatedly, so the same DAG can be executed more than once. */
    public static DataSet source(DataSchema dataSchema, Iterable<DataRow> rows) {
        return new DataSet(dataSchema, Kind.SOURCE, null, rows, null);
    }

    /** A SOURCE node with a session, used by ExecutionEnvironment.fromSource, which enables output and commit. */
    static DataSet source(ExecutionEnvironment session, DataSchema dataSchema, Iterable<DataRow> rows) {
        return new DataSet(dataSchema, Kind.SOURCE, null, rows, null, null, null, null, session);
    }

    /**
     * One source with several splits (the fromSource(Source[]) shape): a single logical source holds several homogeneous Source splits,
     * and the runtime spreads them over instances by parallelism instead of the API layer collapsing them into a union. The session enables output and commit.
     */
    static DataSet source(ExecutionEnvironment session, DataSchema dataSchema, java.util.List<Source> inputs) {
        return new DataSet(dataSchema, Kind.SOURCE, null, null, null, null, null, null, session, null,
                new java.util.ArrayList<>(inputs));
    }

    public DataSchema getSchema() {
        return dataSchema;
    }

    /** Returns an immutable copy carrying a stable logical operator UID; the upstream DAG and the operator implementation are unchanged. */
    public DataSet uid(String uid) {
        return new DataSet(dataSchema, kind, parent, source, stage, left, right, binaryStage,
                session, descriptor, sourceInputs, uid);
    }

    /**
     * Registers as a sink of the session: commit executes this DAG and writes to out. Requires the DAG to originate from {@link ExecutionEnvironment#fromSource}
     * (which carries a session); a node built by the static {@link #source(DataSchema, Iterable)} has none and throws IllegalStateException.
     */
    public void sinkTo(Sink out) {
        ExecutionEnvironment s = resolveSession();
        if (s == null) {
            throw new IllegalStateException(
                    "output() requires a session-bound DataSet; use RowWeave.createEnvironment().fromSource(...)");
        }
        s.registerSink(this, out);
    }

    /** Registers a SINK carrying a stable logical UID. */
    public void sinkTo(String sinkUid, Sink out) {
        ExecutionEnvironment s = resolveSession();
        if (s == null) {
            throw new IllegalStateException(
                    "output() requires a session-bound DataSet; use RowWeave.createEnvironment().fromSource(...)");
        }
        s.registerSink(this, sinkUid, out);
    }

    /** Walks back along parent (UNARY) or left (BINARY) to the SOURCE and returns its session (may be null). */
    private ExecutionEnvironment resolveSession() {
        DataSet n = this;
        while (n != null) {
            if (n.kind == Kind.SOURCE) {
                return n.session;
            }
            n = (n.kind == Kind.BINARY) ? n.left : n.parent;
        }
        return null;
    }

    /** package-private ownership proof for atomic compound sink registration. */
    ExecutionEnvironment owningSession() {
        return resolveSession();
    }

    /** skeleton; PlanOps intentionally rejects it until QP2 installs the runtime. */
    DataSet typedQualityAcceptedSummarySkeleton() {
        PipelineExecutor.Stage unavailable = (in, out) -> {
            throw new UnsupportedOperationException(
                    "typedQualityAcceptedSummary runtime is not yet available");
        };
        return unaryDescribable(TypedQualityManifest.summarySchema(), unavailable,
                "typedQualityAcceptedSummary",
                params("summaryVersion", TypedQualityManifest.SUMMARY_VERSION),
                java.util.Collections.emptyMap());
    }

    /** skeleton; PlanOps intentionally rejects it until QP2 installs the runtime. */
    DataSet typedQualityRejectedSummarySkeleton(int maxReasonBuckets) {
        PipelineExecutor.Stage unavailable = (in, out) -> {
            throw new UnsupportedOperationException(
                    "typedQualityRejectedSummary runtime is not yet available");
        };
        return unaryDescribable(TypedQualityManifest.summarySchema(), unavailable,
                "typedQualityRejectedSummary",
                params("summaryVersion", TypedQualityManifest.SUMMARY_VERSION,
                        "maxReasonBuckets", Integer.toString(maxReasonBuckets)),
                java.util.Collections.emptyMap());
    }

    /** canonical Object quality route descriptor and local runtime. */
    DataSet objectQualityRouteSkeleton(DataSchema outMeta, String route) {
        if (!ObjectStorageQualitySource.VALID_ROUTE.equals(route)
                && !ObjectStorageQualitySource.REJECTED_ROUTE.equals(route)) {
            throw ObjectStorageQualitySource.invalid("quality route is unknown");
        }
        PipelineExecutor.Stage runtime = io.rowweave.ops.ObjectStorageQualityRoute.routeStage(
                dataSchema, outMeta, route);
        return unaryDescribable(outMeta, runtime, "objectQualityRoute",
                params("carrierVersion", ObjectStorageQualitySource.CARRIER_VERSION,
                        "route", route), java.util.Collections.emptyMap());
    }

    /** filter with a code predicate: UNARY, keeps the parent meta. Describable (userFunction is a Serializable FilterFunction). */
    public DataSet filter(FilterFunction fn) {
        Filter op = Filter.of(fn);
        return unaryFnDescribable(dataSchema, op::apply, "filterFn", fn);
    }

    /** filter with a string expression: UNARY, keeps the parent meta, evaluated by the expression engine where unknown means not passed. Describable. */
    public DataSet filter(String expression) {
        Filter op = Filter.fromExpression(dataSchema, expression);
        return unaryDescribable(dataSchema, op::apply, "filter",
                params("expr", expression), java.util.Collections.emptyMap());
    }

    /**
     * filter with an expression plus named parameters: a bare identifier resolves to a field first, and only when no such field exists is it taken from params.
     * Describable: expr goes into param and the params map (which must be serializable) into the userFunction slot, rebuilt on the worker. Empty params degrade to {@link #filter(String)}.
     */
    public DataSet filter(String expression, java.util.Map<String, Object> params) {
        if (params == null || params.isEmpty()) {
            return filter(expression);
        }
        Filter op = Filter.fromExpression(dataSchema, expression, params);
        java.util.HashMap<String, Object> serializableParams = new java.util.HashMap<>(params);
        return new DataSet(dataSchema, Kind.UNARY, this, null,
                PipelineExecutor.passThroughStream(op::apply), null, null, null, null,
                new Descriptor("filterParams", params("expr", expression),
                        java.util.Collections.emptyMap(), serializableParams));
    }

    /** select column pruning by index: UNARY, meta produced by Select.byIndex (names and types carried from the source). Describable (typeId selectByIndex). */
    public DataSet select(int... columnIndices) {
        Select op = Select.byIndex(dataSchema, columnIndices);
        java.util.List<String> idxStrs = new ArrayList<>(columnIndices.length);
        for (int c : columnIndices) {
            idxStrs.add(Integer.toString(c));
        }
        return unaryDescribable(op.getOutputSchema(), op::apply, "selectByIndex",
                params(), listParams("indices", idxStrs));
    }

    /** select expression projection: UNARY, meta produced by Select.of (alias, bare field name or exprN plus type inference). Describable. */
    public DataSet select(String... expressions) {
        Select op = Select.of(dataSchema, expressions);
        return unaryDescribable(op.getOutputSchema(), op::apply, "select",
                params(), listParams("exprs", Arrays.asList(expressions)));
    }

    /**
     * explicit bounded schema mapping. The mapping is bound to this dataset's exact input schema and is
     * encoded as primitive/string Plan parameters rather than a user function.
     */
    public DataSet convertSchema(SchemaConversion conversion) {
        if (conversion == null) {
            throw new IllegalArgumentException("SCHEMA_MAPPING_INVALID: conversion is required");
        }
        String actual = io.rowweave.commit.SchemaFingerprint.of(dataSchema);
        if (!actual.equals(conversion.inputSchemaFingerprint())) {
            throw new IllegalArgumentException("SCHEMA_MAPPING_INPUT_MISMATCH: dataset schema differs from mapping input");
        }
        java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
        lp.put("sourceIndices", conversion.sourceIndices());
        lp.put("casts", conversion.casts());
        lp.put("nullPolicies", conversion.nullPolicies());
        lp.put("defaultKinds", conversion.defaultKinds());
        lp.put("defaultValues", conversion.defaultValues());
        return unaryDescribable(conversion.getOutputSchema(), conversion::apply, "schemaConvert",
                params(), lp);
    }

    /**
     * validate already-decoded typed rows with a bounded describable rule set. QV1 binds the model and
     * Plan description; runtime execution stays explicitly fail-closed until QV2.
     */
    public DataSet validateRows(io.rowweave.ops.RowValidation validation) {
        if (validation == null) {
            throw new IllegalArgumentException("QUALITY_VALIDATION_INVALID: validation is required");
        }
        String actual = io.rowweave.commit.SchemaFingerprint.of(dataSchema);
        if (!actual.equals(validation.inputSchemaFingerprint())) {
            throw new IllegalArgumentException(
                    "QUALITY_VALIDATION_INPUT_MISMATCH: dataset schema differs from validation input");
        }
        java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
        lp.put("ruleIds", validation.ruleIds());
        lp.put("ruleKinds", validation.ruleKinds());
        lp.put("fieldIndices", validation.fieldIndices());
        lp.put("lowerBounds", validation.lowerBounds());
        lp.put("upperBounds", validation.upperBounds());
        return unaryDescribable(dataSchema, validation::apply, "rowValidate", params(), lp);
    }

    /**
     * opt in to stable source-local positions for fail-fast validation diagnostics.
     * This is intentionally legal only on an explicitly identified exact bounded SOURCE.
     */
    public DataSet validateRowsWithSourcePosition(
            io.rowweave.ops.RowValidation validation) {
        if (validation == null) {
            throw new IllegalArgumentException(
                    "QUALITY_SOURCE_POSITION_INVALID: validation is required");
        }
        if (kind != Kind.SOURCE) {
            throw new IllegalArgumentException(
                    "QUALITY_SOURCE_POSITION_INVALID: positioned validation must be called directly on SOURCE");
        }
        if (operatorUid == null || sourceInputs == null || sourceInputs.isEmpty()) {
            throw new IllegalArgumentException(
                    "QUALITY_SOURCE_POSITION_INVALID: explicit exact SOURCE is required");
        }
        String actual = io.rowweave.commit.SchemaFingerprint.of(dataSchema);
        if (!actual.equals(validation.inputSchemaFingerprint())) {
            throw new IllegalArgumentException(
                    "QUALITY_SOURCE_POSITION_INVALID: validation input schema differs from SOURCE");
        }
        java.util.List<io.rowweave.BatchSourceSplitBinding> bindings;
        try {
            bindings = io.rowweave.internal.BatchSourceSplitBindings.freezeNew(
                    dataSchema, sourceInputs);
            for (io.rowweave.BatchSourceSplitBinding binding : bindings) {
                io.rowweave.sched.stream.SourceRecordPosition.forSplit(
                        operatorUid, binding.descriptor());
            }
        } catch (RuntimeException invalid) {
            throw new IllegalArgumentException(
                    "QUALITY_SOURCE_POSITION_INVALID: SOURCE identity or replay contract is invalid");
        }
        java.util.List<io.rowweave.BatchSourceSplitDescriptor> descriptors =
                new java.util.ArrayList<>(bindings.size());
        for (io.rowweave.BatchSourceSplitBinding binding : bindings) {
            descriptors.add(binding.descriptor());
        }
        java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
        lp.put("ruleIds", validation.ruleIds());
        lp.put("ruleKinds", validation.ruleKinds());
        lp.put("fieldIndices", validation.fieldIndices());
        lp.put("lowerBounds", validation.lowerBounds());
        lp.put("upperBounds", validation.upperBounds());
        PipelineExecutor.Stage positioned = (in, out) -> validation.applyWithSourcePosition(
                in, out, operatorUid, descriptors);
        return unaryDescribable(dataSchema, positioned, "rowValidate",
                params(io.rowweave.plan.SourcePositionPlanValidator.MODE_PARAM,
                        io.rowweave.plan.SourcePositionPlanValidator.EXACT_SOURCE_V1), lp);
    }

    /**
     * classify one exact positioned SOURCE into exhaustive valid/rejected branches.
     * Callers cannot access the engine-owned carrier, and submitted plans must consume both branches.
     */
    public DataRowClassification classifyRowsWithSourcePosition(
            io.rowweave.ops.RowValidation validation) {
        if (validation == null) {
            throw classificationInvalid("validation is required");
        }
        if (kind != Kind.SOURCE) {
            throw classificationInvalid("classification must be called directly on SOURCE");
        }
        if (operatorUid == null || sourceInputs == null || sourceInputs.isEmpty()) {
            throw classificationInvalid("explicit exact SOURCE is required");
        }
        if (dataSchema.getFieldCount() + 8 > io.rowweave.ops.RowValidation.MAX_FIELDS) {
            throw classificationInvalid("carrier field count exceeds the bounded field limit");
        }
        String actual = io.rowweave.commit.SchemaFingerprint.of(dataSchema);
        if (!actual.equals(validation.inputSchemaFingerprint())) {
            throw classificationInvalid("validation input schema differs from SOURCE");
        }
        java.util.List<io.rowweave.BatchSourceSplitDescriptor> exactSplits =
                new java.util.ArrayList<>();
        try {
            java.util.List<io.rowweave.BatchSourceSplitBinding> bindings =
                    io.rowweave.internal.BatchSourceSplitBindings.freezeNew(dataSchema, sourceInputs);
            for (io.rowweave.BatchSourceSplitBinding binding : bindings) {
                io.rowweave.sched.stream.SourceRecordPosition.forSplit(
                        operatorUid, binding.descriptor());
                exactSplits.add(binding.descriptor());
            }
        } catch (RuntimeException invalid) {
            throw classificationInvalid("SOURCE identity or replay contract is invalid");
        }

        java.util.Map<String, java.util.List<String>> rules = new java.util.LinkedHashMap<>();
        rules.put("ruleIds", validation.ruleIds());
        rules.put("ruleKinds", validation.ruleKinds());
        rules.put("fieldIndices", validation.fieldIndices());
        rules.put("lowerBounds", validation.lowerBounds());
        rules.put("upperBounds", validation.upperBounds());
        io.rowweave.ops.TypedRowClassification runtime =
                io.rowweave.ops.TypedRowClassification.of(
                        dataSchema, validation, operatorUid, exactSplits);
        DataSet classifier = unaryDescribable(
                runtime.carrierSchema(), runtime.classifierStage(), "rowClassify",
                params(io.rowweave.plan.SourcePositionPlanValidator.MODE_PARAM,
                                io.rowweave.plan.SourcePositionPlanValidator.EXACT_SOURCE_V1,
                        "carrierVersion", io.rowweave.ops.TypedRowClassification.CARRIER_VERSION,
                        "routeContract", io.rowweave.ops.TypedRowClassification.ROUTE_CONTRACT), rules);
        DataSet valid = classifier.unaryDescribable(
                dataSchema, runtime.validRouteStage(), "qualityRoute",
                params("carrierVersion", io.rowweave.ops.TypedRowClassification.CARRIER_VERSION,
                        "route", io.rowweave.ops.TypedRowClassification.VALID_ROUTE),
                java.util.Collections.emptyMap());
        DataSet rejected = classifier.unaryDescribable(
                runtime.rejectedSchema(), runtime.rejectedRouteStage(), "qualityRoute",
                params("carrierVersion", io.rowweave.ops.TypedRowClassification.CARRIER_VERSION,
                        "route", io.rowweave.ops.TypedRowClassification.REJECTED_ROUTE),
                java.util.Collections.emptyMap());
        int maxReasonBuckets = Math.addExact(
                Math.addExact(validation.ruleIds().size(), dataSchema.getFieldCount()), 1);
        return new DataRowClassification(valid, rejected, maxReasonBuckets);
    }

    private static IllegalArgumentException classificationInvalid(String detail) {
        return new IllegalArgumentException("QUALITY_CLASSIFICATION_INVALID: " + detail);
    }

    /** map (one in, one out): UNARY, keeps the parent meta. Describable (userFunction is a Serializable MapFunction). */
    public DataSet map(MapFunction fn) {
        Map op = Map.of(fn);
        return unaryFnDescribable(dataSchema, op::apply, "map", fn);
    }

    /**
     * Dedicated one-to-one changelog map. Incremental execution verifies that
     * operation, key, and record-position fields remain byte-exact.
     */
    public DataSet incrementalMap(IncrementalMapFunction fn) {
        Map op = Map.of(fn);
        return unaryFnDescribable(
                dataSchema, op::apply, "incrementalMap", fn);
    }

    /** map with an explicit output meta (a mapping that changes the header, such as JoinDataSet.withFunc). Describable (map typeId, outMeta stored separately). */
    DataSet mapWithMeta(MapFunction fn, DataSchema outMeta) {
        Map op = Map.of(fn);
        return unaryFnDescribable(outMeta, op::apply, "map", fn);
    }

    /** flatMap (one in, many out): UNARY, keeps the parent meta. Describable (userFunction is a Serializable FlatMapFunction). */
    public DataSet flatMap(FlatMapFunction fn) {
        FlatMap op = FlatMap.of(fn);
        return unaryFnDescribable(dataSchema, op::apply, "flatMap", fn);
    }

    /**
     * Runs one streaming callback for every physical Batch partition. The
     * callback receives a single-pass cursor and a quota-guarded push collector.
     */
    public DataSet mapPartition(
            MapPartitionFunction fn, MapPartitionEmissionLimit limit) {
        if (fn == null || limit == null) {
            throw new IllegalArgumentException(
                    "map-partition function and limits are required");
        }
        DataSchema outMeta = fn.getResultSchema();
        if (outMeta == null) {
            throw new IllegalArgumentException(
                    "MapPartitionFunction.getResultSchema() must not be null");
        }
        io.rowweave.ops.MapPartition op =
                io.rowweave.ops.MapPartition.of(fn, outMeta, limit);
        return unaryFnWithParamsDescribable(
                outMeta, op::apply, "mapPartition",
                params("maxOutputRowsPerPartition",
                                Long.toString(limit.maxOutputRowsPerPartition()),
                        "maxOutputBytesPerPartition",
                                Long.toString(limit.maxOutputBytesPerPartition())),
                fn);
    }

    // ---- breaker nodes (through streamingBreaker; each method states its output meta) ----

    /**
     * orderBy (the sort breaker): UNARY, meta equals the input. Describable (only the sort encoding is stored).
     * Note that parallelism is an executor-level concern (PlanOps rebuilds with the executor's parallelism, and an operator's output is bit-identical
     * whatever its parallelism), so no per-node parameter is stored, which avoids a stored-but-ignored divergence. The direct executeToOutput path still uses the value passed in.
     */
    public DataSet orderBy(OrderBy op, int parallelism, boolean parallel) {
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker(
                (in, out) -> op.applyStream(in, parallelism, parallel, out));
        java.util.List<String> sort = new ArrayList<>();
        for (SortField sf : op.sortFields()) {
            sort.add(io.rowweave.plan.PlanOps.encodeSortField(sf));
        }
        return unaryDescribable(dataSchema, stage, "orderBy", params(), listParams("sort", sort));
    }

    /** top (managed bounded heap plus an external-sort breaker once the budget is exceeded): UNARY, meta equals the input. Describable (n plus sort; parallelism is executor-level). */
    public DataSet top(Top op, int parallelism) {
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker(
                (in, out) -> op.applyStream(in, parallelism, out));
        java.util.List<String> sort = new ArrayList<>();
        for (SortField sf : op.sortFields()) {
            sort.add(io.rowweave.plan.PlanOps.encodeSortField(sf));
        }
        return unaryDescribable(dataSchema, stage, "top",
                params("n", Integer.toString(op.n())), listParams("sort", sort));
    }

    /** distinct (the dedup breaker): UNARY, meta equals the input (a subset of its rows). Describable (keys; allColumns means empty keys). */
    public DataSet distinct(Distinct op) {
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker(op::dedup);
        java.util.List<String> keys = op.isAllColumns()
                ? java.util.Collections.emptyList() : op.keyFieldNames();
        return unaryDescribable(dataSchema, stage, "distinct", params(), listParams("keys", keys));
    }

    /** distinct(String...) convenience overload: deduplicates by column name, empty meaning all columns. Delegates to the existing Distinct. */
    public DataSet distinct(String... keyFields) {
        Distinct op = (keyFields == null || keyFields.length == 0)
                ? Distinct.allColumns(dataSchema) : Distinct.byColumns(dataSchema, keyFields);
        return distinct(op);
    }

    /**
     * groupBy (the grouping aggregate breaker): UNARY, meta equals op.getOutputSchema. Describable (one typeId groupBy
     * plus two parallel listParams aggKinds and aggMeasures ordered by column, with count(*) recorded as an empty string). Single and multiple aggregates share this form and both serialize.
     */
    public DataSet groupBy(ParallelGroupByAgg op) {
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker(op::aggregate);
        java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
        lp.put("keys", op.keyFieldNames());
        lp.put("aggKinds", op.aggKindNames());
        lp.put("aggMeasures", op.aggMeasureFields());
        return unaryDescribable(op.getOutputSchema(), stage, "groupBy", params(), lp);
    }

    // ---- convenience API, thin wrappers over the existing describable operators (WholeTableAgg, OrderBy, TopN, Select) ----

    /**
     * Whole-table scalar aggregate without groupBy: sum, count, max or min produces one row holding the value. Describable (typeId wholeAgg
     * with kind and field parameters, shipped to the cluster; StageAssignment forces a single-instance gather, so the global scalar is correct and nothing is scattered by hash).
     */
    private DataSet wholeAgg(WholeTableAgg.Kind kind, String field, String outName, DataType outType) {
        WholeTableAgg op = WholeTableAgg.of(dataSchema, kind, field);
        DataSchema outMeta = new DataSchema(new Field(outName, outType));
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker(op::apply);
        return unaryDescribable(outMeta, stage, "wholeAgg",
                params("kind", kind.name(), "field", field), java.util.Collections.emptyMap());
    }

    /** sum(field): whole-table sum (BigDecimal, scale 10, HALF_UP, empty or all null gives null). */
    public DataSet sum(String field) {
        return sum(field, "sum");
    }

    /** sum(field, alias): whole-table sum, output column named alias. */
    public DataSet sum(String field, String alias) {
        return wholeAgg(WholeTableAgg.Kind.SUM, field, alias, DataType.BigDecimalType);
    }

    /** count(): number of rows in the whole table (including null measures), a Long. */
    public DataSet count() {
        return wholeAgg(WholeTableAgg.Kind.COUNT_ALL, null, "count", DataType.LongType);
    }

    /** count(field): number of rows in the whole table where that field is not null, a Long. */
    public DataSet count(String field) {
        return count(field, "count");
    }

    /** count(field, alias): whole-table non-null count, output column named alias. */
    public DataSet count(String field, String alias) {
        return wholeAgg(WholeTableAgg.Kind.COUNT_FIELD, field, alias, DataType.LongType);
    }

    /** max(field): whole-table maximum (nulls ignored, empty or all null gives null), keeping the field's type. */
    public DataSet max(String field) {
        return max(field, "max");
    }

    /** max(field, alias): whole-table maximum, output column named alias. */
    public DataSet max(String field, String alias) {
        return wholeAgg(WholeTableAgg.Kind.MAX, field, alias, requireFieldType(field, "max"));
    }

    /** min(field): whole-table minimum (nulls ignored, empty or all null gives null), keeping the field's type. */
    public DataSet min(String field) {
        return min(field, "min");
    }

    /** min(field, alias): whole-table minimum, output column named alias. */
    public DataSet min(String field, String alias) {
        return wholeAgg(WholeTableAgg.Kind.MIN, field, alias, requireFieldType(field, "min"));
    }

    private DataType requireFieldType(String field, String operator) {
        int index = dataSchema.getFieldIndex(field);
        if (index < 0) {
            throw new IllegalArgumentException("unknown field for " + operator + ": " + field);
        }
        return dataSchema.getField(index).getDataType();
    }

    /** groupBySum(measure, keys...): convenience grouped sum, equal to groupBy(ParallelGroupByAgg.of(keys).sum(measure)). */
    public DataSet groupBySum(String measureField, String... keyFields) {
        return groupBy(ParallelGroupByAgg.of(dataSchema, keyFields).sum(measureField));
    }

    /** orderBy(String...): convenience all-ascending order with the DEFAULT null semantics (a numeric null counts as zero, a string null sorts first), equal to OrderBy with every field ASC. */
    public DataSet orderBy(String... fields) {
        SortField[] sf = new SortField[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int idx = dataSchema.getFieldIndex(fields[i]);
            if (idx < 0) {
                throw new IllegalArgumentException("Unknown orderBy field: " + fields[i]);
            }
            sf[i] = new SortField(idx, SortField.Direction.ASC, SortField.NullOrder.DEFAULT);
        }
        return orderBy(OrderBy.of(dataSchema, sf), 1, false);
    }

    /** top(int): convenience limit with no ordering, the first n rows in source order (TopN streams through). Describable (typeId topN). */
    public DataSet top(int n) {
        TopN op = TopN.of(dataSchema, n);
        PipelineExecutor.Stage stage = PipelineExecutor.passThroughStream(op::applyStream);
        return unaryDescribable(dataSchema, stage, "topN",
                params("n", Integer.toString(n)), java.util.Collections.emptyMap());
    }

    /** removeFields(names...): convenience column removal, a select by index over the complement. Meta is the remaining columns (names and types carried from the source). */
    public DataSet removeFields(String... fieldNames) {
        java.util.Set<String> drop = new java.util.HashSet<>(Arrays.asList(fieldNames));
        java.util.List<Integer> keep = new ArrayList<>();
        for (int i = 0; i < dataSchema.getFieldCount(); i++) {
            if (!drop.contains(dataSchema.getField(i).getName())) {
                keep.add(i);
            }
        }
        int[] idx = new int[keep.size()];
        for (int i = 0; i < idx.length; i++) {
            idx[i] = keep.get(i);
        }
        return select(idx);
    }

    // ---- fluent builder shells: graph-construction sugar that lands on the existing describable operators ----

    /** groupBy(String...): returns a {@link Grouper} to choose the aggregate by chaining (.sum, .max, .min, .count, .count(f), .reduceGroup). */
    public Grouper groupBy(String... fields) {
        return new Grouper(this, fields);
    }

    /** join(right): returns a {@link JoinDataSet} fluent builder (INNER), completed by .on(keys). */
    public JoinDataSet join(DataSet right) {
        return new JoinDataSet(this, right, io.rowweave.ops.join.JoinType.INNER);
    }

    /** leftJoin(right): LEFT OUTER fluent builder. */
    public JoinDataSet leftJoin(DataSet right) {
        return new JoinDataSet(this, right, io.rowweave.ops.join.JoinType.LEFT);
    }

    /** rightJoin(right): RIGHT OUTER fluent builder. */
    public JoinDataSet rightJoin(DataSet right) {
        return new JoinDataSet(this, right, io.rowweave.ops.join.JoinType.RIGHT);
    }

    /** fullJoin(right): FULL OUTER fluent builder. */
    public JoinDataSet fullJoin(DataSet right) {
        return new JoinDataSet(this, right, io.rowweave.ops.join.JoinType.FULL);
    }

    /** coGroup(right, fn): returns a {@link CoGroupDataSet} fluent builder, completed by .on(keys). */
    public CoGroupDataSet coGroup(DataSet right, CoGroupFunction fn) {
        return new CoGroupDataSet(this, right, fn);
    }

    /**
     * addFields (append constant columns): pass-through with output [source columns..., constant columns...]. Describable
     * through typeId=addFields with three parallel listParams addNames/addTypes/addValues; the constant text encoding round-trips bit-identically and is shipped to the cluster.
     */
    public DataSet addFields(Field[] fields, Object[] values) {
        io.rowweave.ops.AddFields op = io.rowweave.ops.AddFields.of(dataSchema, fields, values);
        PipelineExecutor.Stage stage = PipelineExecutor.passThroughStream(op::apply);
        java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
        lp.put("addNames", op.addFieldNames());
        lp.put("addTypes", op.addFieldTypes());
        lp.put("addValues", op.addFieldValues());
        return unaryDescribable(op.getOutputSchema(), stage, "addFields", params(), lp);
    }

    /**
     * partitionByHash (redistribute by key hash): rows unchanged. Describable through typeId=partitionByHash with
     * listParam keys. Locally this is an identity pass-through; on a cluster it forces a keyed shuffle, because StageAssignment.needsKeyedShuffle recognises this
     * typeId and partitions across P instances by keys, so equal keys land in the same downstream partition. The multiset is unchanged, so local and cluster output is bit-identical.
     */
    public DataSet partitionByHash(String... fields) {
        if (fields == null || fields.length == 0) {
            throw new IllegalArgumentException("partitionByHash requires at least one field");
        }
        for (String f : fields) {
            if (dataSchema.getFieldIndex(f) < 0) {
                throw new IllegalArgumentException("partitionByHash: unknown field '" + f + "'");
            }
        }
        // identity pass-through locally; on a cluster the keyed shuffle is orchestrated by StageAssignment and the executor from the typeId.
        io.rowweave.ops.Map identity = io.rowweave.ops.Map.of(in -> in);
        PipelineExecutor.Stage stage = PipelineExecutor.passThroughStream(identity::apply);
        return unaryDescribable(dataSchema, stage, "partitionByHash",
                params(), listParams("keys", Arrays.asList(fields)));
    }

    /** reduceGroup (group-reduce breaker): UNARY. Describable through keys plus the GroupReduceFunction userFunction. */
    public DataSet reduceGroup(ReduceGroup op, DataSchema outMeta) {
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker(op::reduce);
        return new DataSet(outMeta, Kind.UNARY, this, null, stage, null, null, null, null,
                new Descriptor("reduceGroup", java.util.Collections.emptyMap(),
                        listParams("keys", op.keyFieldNames()), op.function()));
    }

    /**
     * reduceGroup(GroupReduceFunction) convenience overload: reduce the whole table (no key, all rows in one group),
     * with the output schema taken from fn.getResultSchema(). Delegates to ReduceGroup with empty keys.
     */
    public DataSet reduceGroup(GroupReduceFunction fn) {
        DataSchema outMeta = fn.getResultSchema();
        if (outMeta == null) {
            throw new IllegalArgumentException("GroupReduceFunction.getResultSchema() must not be null");
        }
        return reduceGroup(ReduceGroup.of(dataSchema, new String[0], fn), outMeta);
    }

    /**
     * combineGroup (contiguous-shard map-side pre-aggregation breaker): cut the input into shards contiguous ranges in source order and call the user combine function per range.
     * Describable through typeId=combineGroup plus the shards param and the GroupCombineFunction userFunction. shards is pinned when the plan is built, so the
     * local and cluster paths use the same count; the default is 1, one range over the whole input. Bit-identical cluster output requires a single-instance upstream chain.
     */
    public DataSet combineGroup(GroupCombineFunction fn) {
        return combineGroup(fn, 1);
    }

    /** combineGroup with an explicit shard count (map-side parallel pre-aggregation; shards is a plan-time constant, identical locally and on a cluster). */
    public DataSet combineGroup(GroupCombineFunction fn, int shards) {
        DataSchema outMeta = fn.getResultSchema() != null ? fn.getResultSchema() : dataSchema;
        io.rowweave.ops.CombineGroup op = io.rowweave.ops.CombineGroup.of(dataSchema, fn);
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker((in, out) -> op.applyStream(in, shards, out));
        return new DataSet(outMeta, Kind.UNARY, this, null, stage, null, null, null, null,
                new Descriptor("combineGroup", params("shards", Integer.toString(shards)),
                        java.util.Collections.emptyMap(), fn));
    }

    /**
     * combineReduceGroup (combine pre-aggregation plus a final reduce): combine over shards as above, then hand every combine result to reduce.
     * Describable through typeId=combineReduceGroup plus shards and the GroupCombineReduceFunction. reduce runs as a single instance and must be order-insensitive.
     */
    public DataSet combineReduceGroup(GroupCombineReduceFunction fn) {
        return combineReduceGroup(fn, 1);
    }

    /** combineReduceGroup with an explicit shard count. */
    public DataSet combineReduceGroup(GroupCombineReduceFunction fn, int shards) {
        DataSchema outMeta = fn.getResultSchema();
        if (outMeta == null) {
            throw new IllegalArgumentException("GroupCombineReduceFunction.getResultSchema() must not be null");
        }
        io.rowweave.ops.CombineGroup op = io.rowweave.ops.CombineGroup.ofCombineReduce(dataSchema, fn);
        PipelineExecutor.Stage stage = PipelineExecutor.streamingBreaker((in, out) -> op.applyStream(in, shards, out));
        return new DataSet(outMeta, Kind.UNARY, this, null, stage, null, null, null, null,
                new Descriptor("combineReduceGroup", params("shards", Integer.toString(shards)),
                        java.util.Collections.emptyMap(), fn));
    }

    // ---- BINARY nodes (executed through runBinary) ----

    /**
     * join without a hint (SortMergeJoin): BINARY, left is this side and right is the argument. The plan carries no joinHint, and both the local
     * and cluster executors choose the operator under AUTO. Describable through joinType plus leftKeys and rightKeys, mapped from index to name.
     */
    public DataSet join(DataSet right, SortMergeJoin op) {
        java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
        lp.put("leftKeys", op.leftKeyFieldNames());
        lp.put("rightKeys", op.rightKeyFieldNames());
        return binaryDescribable(op.getOutputSchema(), right, op::join, "join",
                params("joinType", op.joinType().name()), lp);
    }

    /**
     * join with an explicit {@link io.rowweave.ops.join.JoinHint}: SORT_MERGE is written into the plan and forces the sort-merge path;
     * HASH selects the managed HashJoin on both the local and cluster paths. The node carries the joinHint param and the executor picks the operator from it.
     */
    public DataSet join(DataSet right, SortMergeJoin op, io.rowweave.ops.join.JoinHint hint) {
        if (hint == io.rowweave.ops.join.JoinHint.SORT_MERGE) {
            java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
            lp.put("leftKeys", op.leftKeyFieldNames());
            lp.put("rightKeys", op.rightKeyFieldNames());
            return binaryDescribable(op.getOutputSchema(), right, op::join, "join",
                    params("joinType", op.joinType().name(), "joinHint", "SORT_MERGE"), lp);
        }
        java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
        lp.put("leftKeys", op.leftKeyFieldNames());
        lp.put("rightKeys", op.rightKeyFieldNames());
        // the stage function uses HashJoin, matching the HASH branch of binaryStageFor; the node carries joinHint=HASH so it can be rebuilt after deserialization.
        io.rowweave.ops.join.HashJoin hj = io.rowweave.ops.join.HashJoin.of(
                getSchema(), right.getSchema(),
                op.leftKeyFieldNames().toArray(new String[0]),
                op.rightKeyFieldNames().toArray(new String[0]), op.joinType());
        return binaryDescribable(op.getOutputSchema(), right, hj::join, "join",
                params("joinType", op.joinType().name(), "joinHint", "HASH"), lp);
    }

    /** coGroup (CoGroup): BINARY with the output schema from fn.getResultSchema(). Describable through leftKeys/rightKeys plus the CoGroupFunction. */
    public DataSet coGroup(DataSet right, CoGroup op) {
        java.util.Map<String, java.util.List<String>> lp = new java.util.LinkedHashMap<>();
        lp.put("leftKeys", op.leftKeyFieldNames());
        lp.put("rightKeys", op.rightKeyFieldNames());
        return new DataSet(op.getResultSchema(), Kind.BINARY, null, null, null, this, right, op::coGroup, null,
                new Descriptor("coGroup", java.util.Collections.emptyMap(), lp, op.function()));
    }

    /** union (Union, binary form): BINARY with the output schema from this side, since both sides are isomorphic; the N-way applyStream adapts to two inputs. Describable with no params. */
    public DataSet union(DataSet other, Union op) {
        validateUnionSchema(dataSchema, other.dataSchema);
        PipelineExecutor.BinaryStage bs = (l, r, sink) -> op.applyStream(Arrays.asList(l, r), sink);
        return binaryDescribable(dataSchema, other, bs, "union", params(), java.util.Collections.emptyMap());
    }

    /** union(DataSet) convenience overload: both sides share one schema, and the operator is built with Union.of(this side's schema). */
    public DataSet union(DataSet other) {
        return union(other, Union.of(dataSchema));
    }

    private static void validateUnionSchema(DataSchema left, DataSchema right) {
        if (left.getFieldCount() != right.getFieldCount()) {
            throw new IllegalArgumentException("union schema mismatch: field count left="
                    + left.getFieldCount() + ", right=" + right.getFieldCount());
        }
        for (int i = 0; i < left.getFieldCount(); i++) {
            Field lf = left.getField(i);
            Field rf = right.getField(i);
            if (lf.getDataType() != rf.getDataType()) {
                throw new IllegalArgumentException("union schema mismatch at field " + i
                        + ": left=" + lf.getDataType() + ", right=" + rf.getDataType());
            }
        }
    }

    /**
     * Build this DataSet subtree into {@code nodes} in post-order, deduplicated by identity, and return the new index for this node.
     * Every operator is describable: UNARY and BINARY both produce a serializable PlanNode carrying typeId plus params and userFunction, which is the cluster path.
     * Fan-out: when a DataSet is referenced by more than one downstream node, the visited map returns the same index,
     * so the plan keeps a multi-parent DAG (diamond or wide fan-out). Whole-graph execution broadcasts after the shared node, and a shared upstream is computed once.
     */
    int buildPlanInto(List<io.rowweave.plan.PlanNode> nodes,
                      java.util.IdentityHashMap<DataSet, Integer> visited) {
        // When one DataSet is reused by N downstream nodes, visited returns the same index and PlanGraph keeps the shared identity,
        // so the whole-graph executor broadcasts after that node instead of replicating the upstream once per sink.
        Integer existing = visited.get(this);
        if (existing != null) {
            return existing;
        }
        int myIndex;
        switch (kind) {
            case SOURCE: {
                // one source with several splits (fromSource(Source[])): hold the whole List<Source> and split it by parallelism at run time, which forms the physical DAG.
                // otherwise wrap the single source lazily in IterableSource, which does not pre-read and avoids materialising the input; rows are read only when the source stage iterates.
                java.util.List<Source> srcIns = sourceInputs != null
                        ? sourceInputs
                        : java.util.Collections.singletonList(IterableSource.of(dataSchema, source));
                nodes.add(withOperatorUid(io.rowweave.plan.PlanNode.sourceFromInputs(dataSchema, srcIns)));
                myIndex = nodes.size() - 1;
                break;
            }
            case UNARY: {
                int childIdx = parent.buildPlanInto(nodes, visited);
                nodes.add(withOperatorUid(buildDescribableUnary(childIdx)));
                myIndex = nodes.size() - 1;
                break;
            }
            case BINARY: {
                int leftIdx = left.buildPlanInto(nodes, visited);
                int rightIdx = right.buildPlanInto(nodes, visited);
                nodes.add(withOperatorUid(buildDescribableBinary(leftIdx, rightIdx)));
                myIndex = nodes.size() - 1;
                break;
            }
            default:
                throw new IllegalStateException("unknown kind " + kind);
        }
        visited.put(this, myIndex);
        return myIndex;
    }

    private io.rowweave.plan.PlanNode withOperatorUid(io.rowweave.plan.PlanNode node) {
        return operatorUid == null ? node
                : node.withParam(io.rowweave.plan.PlanNode.OPERATOR_UID_PARAM, operatorUid);
    }

    /**
     * Describable UNARY to a serializable PlanNode. With a userFunction: no params gives unaryFn (filterFn/map/flatMap),
     * params gives unaryFnWithParams (reduceGroup carries keys); without a userFunction the node is typeId plus params.
     */
    private io.rowweave.plan.PlanNode buildDescribableUnary(int childIdx) {
        if (descriptor.userFunction != null) {
            if (descriptor.params.isEmpty() && descriptor.listParams.isEmpty()) {
                return io.rowweave.plan.PlanNode.unaryFn(descriptor.typeId, dataSchema, childIdx, descriptor.userFunction);
            }
            return io.rowweave.plan.PlanNode.unaryFnWithParams(descriptor.typeId, dataSchema, childIdx,
                    descriptor.params, descriptor.listParams, descriptor.userFunction);
        }
        io.rowweave.plan.PlanNode.Builder b =
                io.rowweave.plan.PlanNode.unary(descriptor.typeId, dataSchema, childIdx);
        for (java.util.Map.Entry<String, String> e : descriptor.params.entrySet()) {
            b.param(e.getKey(), e.getValue());
        }
        for (java.util.Map.Entry<String, java.util.List<String>> e : descriptor.listParams.entrySet()) {
            b.listParam(e.getKey(), e.getValue());
        }
        return b.build();
    }

    /** Describable BINARY to a serializable PlanNode; a userFunction (coGroup) becomes binaryFn, otherwise typeId plus params. */
    private io.rowweave.plan.PlanNode buildDescribableBinary(int leftIdx, int rightIdx) {
        if (descriptor.userFunction != null) {
            return io.rowweave.plan.PlanNode.binaryFn(descriptor.typeId, dataSchema, leftIdx, rightIdx,
                    descriptor.params, descriptor.listParams, descriptor.userFunction);
        }
        io.rowweave.plan.PlanNode.Builder b =
                io.rowweave.plan.PlanNode.binary(descriptor.typeId, dataSchema, leftIdx, rightIdx);
        for (java.util.Map.Entry<String, String> e : descriptor.params.entrySet()) {
            b.param(e.getKey(), e.getValue());
        }
        for (java.util.Map.Entry<String, java.util.List<String>> e : descriptor.listParams.entrySet()) {
            b.listParam(e.getKey(), e.getValue());
        }
        return b.build();
    }

    DataSchema dataSchema() {
        return dataSchema;
    }

}
