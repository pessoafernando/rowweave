package io.rowweave.plan;

import io.rowweave.DataSchema;
import io.rowweave.mem.ExecutionMemory;
import io.rowweave.mem.MemoryOwnerRequirement;
import io.rowweave.mem.OpenAddressingGroupAggBuffer;
import io.rowweave.mem.PointerSortBuffer;
import io.rowweave.ops.Distinct;
import io.rowweave.ops.Filter;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.Select;
import io.rowweave.ops.SchemaConversion;
import io.rowweave.ops.RowValidation;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.ops.WholeTableAgg;
import io.rowweave.sched.PipelineExecutor;

import java.util.List;

/**
 * Rebuilds a streaming operator from the {@link PlanNode} description, meaning typeId plus string
 * parameters, and produces a {@link PipelineExecutor.Stage} that pushes pure and returns no List.
 * Because {@link io.rowweave.DataSet} reuses the same operator classes, the two paths give byte-for-
 * byte identical results.
 *
 * <p><b>Operators expose no List outlet</b>: there is no applyShuffled, applyParallel or
 * Top.apply(List) here, because the architecture does not allow a List to carry data. An operator
 * exposes only streaming signatures: aggregate, applyStream, dedup and apply(RowStream, RowSink).
 *
 * <p>A childMeta, which is the outMeta upstream, is resolved through {@code graph} and a child index:
 * {@code node.child(0)} is an int index, not a PlanNode.
 */
public final class PlanOps {

    /** Run threshold for the orderBy spilling buffer. */
    private static final int SORT_RUN_THRESHOLD = 1024;

    private PlanOps() {
    }

    /** Every UNARY typeId this IR compiler can rebuild, reused by the submit preflight. */
    public static boolean supportsUnaryType(String typeId) {
        if (typeId == null) {
            return false;
        }
        switch (typeId) {
            case "filter": case "filterFn": case "filterParams":
            case "map": case "incrementalMap": case "flatMap":
            case "mapPartition":
            case "select": case "selectByIndex": case "addFields": case "schemaConvert": case "rowValidate":
            case "rowClassify": case "qualityRoute": case "objectQualityRoute":
            case "partitionByHash":
            case "typedQualityAcceptedSummary": case "typedQualityRejectedSummary":
            case "groupBySum": case "groupByCount": case "groupByMax": case "groupByMin": case "groupBy":
            case "distinct": case "orderBy": case "top": case "topN": case "wholeAgg":
            case "reduceGroup": case "combineGroup": case "combineReduceGroup":
                return true;
            default:
                return false;
        }
    }

    /**
     * Reject operator kinds this runtime cannot materialize before an Sink or
     * commit coordinator is opened. Parameter/schema validation remains owned by
     * the individual operator and the semantic Plan validators.
     */
    public static void validateExecutableTypes(PlanGraph graph) {
        if (graph == null) {
            throw new IllegalArgumentException("plan graph must not be null");
        }
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.UNARY && !supportsUnaryType(node.typeId())) {
                throw new IllegalArgumentException(
                        "unsupported unary typeId in IR execution: " + node.typeId());
            }
            if (node.kind() == PlanNode.Kind.BINARY
                    && !("join".equals(node.typeId())
                    || "coGroup".equals(node.typeId())
                    || "union".equals(node.typeId()))) {
                throw new IllegalArgumentException(
                        "unsupported binary typeId in IR execution: " + node.typeId());
            }
        }
    }

    /** Pass-through operators, which may run inside a push streaming stage. */
    public static boolean isPassThrough(String typeId) {
        return "filter".equals(typeId) || "filterParams".equals(typeId) || "select".equals(typeId) || "topN".equals(typeId)
                || "addFields".equals(typeId) || "schemaConvert".equals(typeId)
                || "rowValidate".equals(typeId) || "rowClassify".equals(typeId)
                || "qualityRoute".equals(typeId)
                || "objectQualityRoute".equals(typeId);
        // Note: partitionByHash is a pass-through that leaves rows unchanged, but on a cluster it must be
        // the head of a keyed stage, since needsKeyedShuffle holds. It is deliberately left out of the
        // pass-through set, because otherwise ExecutionGraphBuilder would chain it into the downstream
        // stage and it would never reshuffle.
    }

    /**
     * Whether a typeId is a breaker that draws on an ExecutionMemory quota: the groupBy family,
     * distinct, orderBy, reduceGroup, join and coGroup. A whole-table scalar aggregate and a
     * pass-through take no quota. A Top built on its own uses managed heap and may spill.
     */
    public static boolean needsMemory(String typeId) {
        switch (typeId) {
            case "groupBySum":
            case "groupByCount":
            case "groupByMax":
            case "groupByMin":
            case "groupBy":
            case "distinct":
            case "orderBy":
            case "top":
            case "reduceGroup":
            case "combineGroup":  // EqualKeyBlock caches the whole input, with spill as the backstop
            case "combineReduceGroup":
            case "join":
            case "coGroup":
            case "typedQualityRejectedSummary":
                return true;
            default:
                return false;
        }
    }

    /** Whether the graph as a whole, at any node, contains a breaker that takes a quota, which is what makes the execution entry acquire lazily. */
    public static boolean graphNeedsMemory(PlanGraph graph) {
        return countBreakers(graph) > 0;
    }

    /**
     * managed-memory owner shape is a node-local resource contract. Binary JOIN/CoGroup
     * share one owner whose two independent working sets receive separate leaf partitions; every
     * other managed breaker has one leaf.
     */
    public static MemoryOwnerRequirement memoryOwnerRequirement(String typeId) {
        if (!needsMemory(typeId)) {
            throw new IllegalArgumentException("operator does not own managed memory: " + typeId);
        }
        return "join".equals(typeId) || "coGroup".equals(typeId)
                ? MemoryOwnerRequirement.partitioned(2)
                : MemoryOwnerRequirement.ordinary();
    }

    /** Exact owner set for this portable graph, acquired atomically before execution starts. */
    public static MemoryOwnerRequirement[] memoryOwnerRequirements(PlanGraph graph) {
        java.util.ArrayList<MemoryOwnerRequirement> requirements = new java.util.ArrayList<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            String typeId = graph.node(i).typeId();
            if (needsMemory(typeId)) {
                requirements.add(memoryOwnerRequirement(typeId));
            }
        }
        return requirements.toArray(new MemoryOwnerRequirement[0]);
    }

    /**
     * How many breaker nodes in the graph take a quota, which is the denominator when that quota is
     * split per operator. Locally each breaker gets its own stage thread. The cluster keyed and binary
     * paths pass concurrency M separately, because a breaker appears once in the graph but runs M times.
     * Overestimating makes each sub-quota smaller, which degrades safely; underestimating makes
     * newOperatorMemory acquire past the pool and fail fast.
     */
    public static int countBreakers(PlanGraph graph) {
        int n = 0;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (needsMemory(graph.node(i).typeId())) {
                n++;
            }
        }
        return n;
    }

    /**
     * Rebuilds a UNARY node as a streaming stage. The childMeta is resolved through the graph, since
     * node.child(0) is an index. A pass-through becomes passThroughStream and a breaker becomes
     * streamingBreaker, which always has a spilling buffer injected and managed by the engine.
     * parallelism is used by orderBy and top, whose operators do not depend on it.
     *
     * @throws IllegalArgumentException for a typeId that is not describable, namely map and flatMap,
     *         which hold a lambda and never enter the IR
     */
    public static PipelineExecutor.Stage stageFor(PlanGraph graph, int nodeIndex, int parallelism, ExecutionMemory mem) {
        PlanNode node = graph.node(nodeIndex);
        if (!supportsUnaryType(node.typeId())) {
            throw new IllegalArgumentException(
                    "unsupported unary typeId in IR execution: " + node.typeId()
                            + " (map/flatMap hold lambdas, not describable in plan IR)");
        }
        DataSchema childMeta = graph.node(node.child(0)).outMeta();
        switch (node.typeId()) {
            case "filterFn": {
                // A code-lambda filter, which is a FilterFunction: rebuilt by deserializing the userFunction.
                Filter f = Filter.of((io.rowweave.FilterFunction) node.userFunction());
                return PipelineExecutor.passThroughStream(f::apply);
            }
            case "map": {
                io.rowweave.ops.Map m = io.rowweave.ops.Map.of((io.rowweave.MapFunction) node.userFunction());
                return PipelineExecutor.passThroughStream(m::apply);
            }
            case "incrementalMap": {
                io.rowweave.ops.Map m = io.rowweave.ops.Map.of(
                        (io.rowweave.IncrementalMapFunction) node.userFunction());
                return PipelineExecutor.passThroughStream(m::apply);
            }
            case "flatMap": {
                io.rowweave.ops.FlatMap fm =
                        io.rowweave.ops.FlatMap.of((io.rowweave.FlatMapFunction) node.userFunction());
                return PipelineExecutor.passThroughStream(fm::apply);
            }
            case "mapPartition": {
                io.rowweave.MapPartitionEmissionLimit limit =
                        new io.rowweave.MapPartitionEmissionLimit(
                                Long.parseLong(node.param(
                                        "maxOutputRowsPerPartition")),
                                Long.parseLong(node.param(
                                        "maxOutputBytesPerPartition")));
                io.rowweave.ops.MapPartition mapPartition =
                        io.rowweave.ops.MapPartition.of(
                                (io.rowweave.MapPartitionFunction)
                                        node.userFunction(),
                                node.outMeta(), limit);
                return PipelineExecutor.passThroughStream(
                        mapPartition::apply);
            }
            case "filter": {
                Filter f = Filter.fromExpression(childMeta, node.param("expr"));
                return PipelineExecutor.passThroughStream(f::apply);
            }
            case "filterParams": {
                // Convenience overload with named filter and params map: rebuilt by deserializing the userFunction slot.
                @SuppressWarnings("unchecked")
                java.util.Map<String, Object> params = (java.util.Map<String, Object>) node.userFunction();
                Filter f = Filter.fromExpression(childMeta, node.param("expr"), params);
                return PipelineExecutor.passThroughStream(f::apply);
            }
            case "select": {
                Select s = Select.of(childMeta, toArray(node.listParam("exprs")));
                return PipelineExecutor.passThroughStream(s::apply);
            }
            case "selectByIndex": {
                List<String> idxStrs = node.listParam("indices");
                int[] indices = new int[idxStrs.size()];
                for (int i = 0; i < indices.length; i++) {
                    indices[i] = Integer.parseInt(idxStrs.get(i));
                }
                Select s = Select.byIndex(childMeta, indices);
                return PipelineExecutor.passThroughStream(s::apply);
            }
            case "addFields": {
                // Append a constant column, which is pass-through: the constants are decoded back from three parallel listParams of text.
                io.rowweave.ops.AddFields af = io.rowweave.ops.AddFields.fromDescribable(childMeta,
                        node.listParam("addNames"), node.listParam("addTypes"), node.listParam("addValues"));
                return PipelineExecutor.passThroughStream(af::apply);
            }
            case "schemaConvert": {
                SchemaConversion conversion = SchemaConversion.fromDescribable(
                        childMeta, node.outMeta(),
                        node.listParam("sourceIndices"), node.listParam("casts"),
                        node.listParam("nullPolicies"), node.listParam("defaultKinds"),
                        node.listParam("defaultValues"));
                return PipelineExecutor.passThroughStream(conversion::apply);
            }
            case "rowValidate": {
                RowValidation validation = RowValidation.fromDescribable(
                        childMeta, node.outMeta(),
                        node.listParam("ruleIds"), node.listParam("ruleKinds"),
                        node.listParam("fieldIndices"), node.listParam("lowerBounds"),
                        node.listParam("upperBounds"));
                if (node.param(SourcePositionPlanValidator.MODE_PARAM) != null) {
                    SourcePositionPlanValidator.validateNode(graph, nodeIndex);
                    PlanNode source = graph.node(node.child(0));
                    return PipelineExecutor.passThroughStream((in, out) ->
                            validation.applyWithSourcePosition(
                                    in, out,
                                    source.param(PlanNode.OPERATOR_UID_PARAM),
                                    source.sourceSplitDescriptors()));
                }
                return PipelineExecutor.passThroughStream(validation::apply);
            }
            case "rowClassify": {
                io.rowweave.ops.TypedRowClassification runtime =
                        TypedRowClassificationPlanValidator.classifierRuntime(graph, nodeIndex);
                return PipelineExecutor.passThroughStream(runtime.classifierStage()::run);
            }
            case "qualityRoute": {
                return PipelineExecutor.passThroughStream(
                        TypedRowClassificationPlanValidator.routeStage(graph, nodeIndex)::run);
            }
            case "objectQualityRoute": {
                return PipelineExecutor.passThroughStream(
                        ObjectStorageQualityPlanValidator.routeStage(graph, nodeIndex)::run);
            }
            case "typedQualityAcceptedSummary": {
                requireQualitySummaryVersion(node);
                io.rowweave.ops.TypedQualitySummary summary =
                        io.rowweave.ops.TypedQualitySummary.accepted();
                return PipelineExecutor.streamingBreaker(summary::summarize);
            }
            case "typedQualityRejectedSummary": {
                requireQualitySummaryVersion(node);
                int maxReasonBuckets = node.intParam("maxReasonBuckets");
                return PipelineExecutor.managedStreamingBreaker(mem, true, opMM -> {
                    io.rowweave.ops.TypedQualitySummary summary =
                            io.rowweave.ops.TypedQualitySummary.rejected(
                                    childMeta, maxReasonBuckets);
                    summary.withBufferFactory(factory -> new OpenAddressingGroupAggBuffer(
                            childMeta,
                            new String[]{"quality_error_code", "quality_rule_id",
                                    "quality_field_index", "quality_detail"},
                            factory, opMM, mem.segmentPool(), mem.newSpillStore()));
                    return summary::summarize;
                });
            }
            case "partitionByHash": {
                // Redistribute by key. Rows are unchanged, so this is an identity pass-through; the cluster keyed shuffle is arranged by StageAssignment.
                io.rowweave.ops.Map identity = io.rowweave.ops.Map.of(in -> in);
                return PipelineExecutor.passThroughStream(identity::apply);
            }
            case "groupBySum":
            case "groupByCount":
            case "groupByMax":
            case "groupByMin": {
                String[] keys = toArray(node.listParam("keys"));
                ParallelGroupByAgg agg = configureAgg(
                        ParallelGroupByAgg.of(childMeta, keys), node.typeId(), node.param("measure"));
                // Waits for the first event before requesting its sub-budget, so a downstream breaker never holds pool quota while its dependency has produced nothing.
                return PipelineExecutor.managedStreamingBreaker(mem, false, opMM -> {
                    agg.withBufferFactory(accFactory -> new OpenAddressingGroupAggBuffer(
                            childMeta, keys, accFactory, opMM, mem.segmentPool(), mem.newSpillStore()));
                    return agg::aggregate;
                });
            }
            case "groupBy": {
                // The unified multi-aggregate groupBy: two parallel listParams, aggKinds and aggMeasures in column order, applied one configureAggByKind at a time.
                String[] keys = toArray(node.listParam("keys"));
                List<String> kinds = node.listParam("aggKinds");
                List<String> measures = node.listParam("aggMeasures");
                ParallelGroupByAgg agg = ParallelGroupByAgg.of(childMeta, keys);
                for (int i = 0; i < kinds.size(); i++) {
                    configureAggByKind(agg, kinds.get(i), measures.get(i));
                }
                return PipelineExecutor.managedStreamingBreaker(mem, false, opMM -> {
                    agg.withBufferFactory(accFactory -> new OpenAddressingGroupAggBuffer(
                            childMeta, keys, accFactory, opMM, mem.segmentPool(), mem.newSpillStore()));
                    return agg::aggregate;
                });
            }
            case "distinct": {
                List<String> keys = node.listParam("keys");
                String[] effKeys = keys.isEmpty() ? allFieldNames(childMeta) : toArray(keys);
                Distinct d = keys.isEmpty()
                        ? Distinct.allColumns(childMeta)
                        : Distinct.byColumns(childMeta, effKeys);
                return PipelineExecutor.managedStreamingBreaker(mem, false, opMM -> {
                    d.withBufferFactory(accFactory -> new OpenAddressingGroupAggBuffer(
                            childMeta, effKeys, accFactory, opMM, mem.segmentPool(), mem.newSpillStore()));
                    return d::dedup;
                });
            }
            case "orderBy": {
                OrderBy ob = OrderBy.of(childMeta, decodeSortFields(node.listParam("sort")));
                // One sub-budget for this operator. When it runs parallel, N SortBuffer instances share it,
                // but a SortBuffer never spills on behalf of another, only on its own initiative, so there
                // is no cross-thread mutation inside a sub-budget either.
                return PipelineExecutor.managedStreamingBreaker(mem, false, opMM -> {
                    // sortFields comes from the comparator's c.getSortFields(), which is augmented to
                    // include __seq, rather than from the fields captured at construction.
                    // The first tier sorts on heap; past budget three threads sort externally. The factory
                    // is installed lazily with the first event as well.
                    ob.withSortBufferFactory((m, c) -> new io.rowweave.mem.TwoTierSortBuffer(
                            m, c, opMM, c.getSortFields(), SORT_RUN_THRESHOLD, 3,
                            () -> new io.rowweave.mem.PipelinedExternalSortBuffer(
                                    m, c, c.getSortFields(), opMM, SORT_RUN_THRESHOLD, 3,
                                    mem.newSpillStore(), mem::segmentPool)));
                    return (in, out) -> ob.applyStream(in, parallelism, parallelism > 1, out);
                });
            }
            case "top": {
                Top top = Top.of(childMeta, node.intParam("n"), decodeSortFields(node.listParam("sort")));
                return PipelineExecutor.managedStreamingBreaker(mem, false, opMM -> {
                    top.withSpill(opMM, (m, c) -> new io.rowweave.mem.PipelinedExternalSortBuffer(
                            m, c, c.getSortFields(), opMM, SORT_RUN_THRESHOLD, 3,
                            mem.newSpillStore(), mem::segmentPool));
                    return (in, out) -> top.applyStream(in, parallelism, out);
                });
            }
            case "topN": {
                // A limit with no ordering, which takes the first n rows in source order, and streams as a pass-through rather than a breaker.
                io.rowweave.ops.TopN topN = io.rowweave.ops.TopN.of(childMeta, node.intParam("n"));
                return PipelineExecutor.passThroughStream(topN::applyStream);
            }
            case "wholeAgg": {
                WholeTableAgg.Kind kind = WholeTableAgg.Kind.valueOf(node.param("kind"));
                String field = node.param("field");  // null for COUNT_ALL
                WholeTableAgg wa = WholeTableAgg.of(childMeta, kind, field);
                return PipelineExecutor.streamingBreaker(wa::apply);
            }
            case "reduceGroup": {
                // A UNARY breaker: keys from a listParam and the GroupReduceFunction from the userFunction, which is serializable.
                String[] keys = toArray(node.listParam("keys"));
                io.rowweave.GroupReduceFunction fn =
                        (io.rowweave.GroupReduceFunction) node.userFunction();
                io.rowweave.ops.ReduceGroup rg = io.rowweave.ops.ReduceGroup.of(childMeta, keys, fn);
                return PipelineExecutor.managedStreamingBreaker(mem, keys.length == 0, opMM -> {
                    rg.withSpill(opMM, mem.newSpillStore(), mem.segmentPool());
                    return rg::reduce;
                });
            }
            case "combineGroup": {
                // Pre-aggregate on the map side over contiguous shards. The shard count is fixed while building the graph, the same locally and on a cluster, with spill as the backstop.
                io.rowweave.GroupCombineFunction fn =
                        (io.rowweave.GroupCombineFunction) node.userFunction();
                int shards = node.intParam("shards");
                io.rowweave.ops.CombineGroup cg = io.rowweave.ops.CombineGroup.of(childMeta, fn);
                return PipelineExecutor.managedStreamingBreaker(mem, false, opMM -> {
                    cg.withSpill(opMM, mem.newSpillStore());
                    return (in, out) -> cg.applyStream(in, shards, out);
                });
            }
            case "combineReduceGroup": {
                // combine pre-aggregates and reduce finishes the reduction: a single reduce instance, insensitive to order.
                io.rowweave.GroupCombineReduceFunction fn =
                        (io.rowweave.GroupCombineReduceFunction) node.userFunction();
                int shards = node.intParam("shards");
                io.rowweave.ops.CombineGroup cg = io.rowweave.ops.CombineGroup.ofCombineReduce(childMeta, fn);
                return PipelineExecutor.managedStreamingBreaker(mem, true, opMM -> {
                    cg.withSpill(opMM, mem.newSpillStore());
                    return (in, out) -> cg.applyStream(in, shards, out);
                });
            }
            default:
                throw new IllegalArgumentException(
                        "unsupported unary typeId in IR execution: " + node.typeId()
                                + " (map/flatMap hold lambdas, not describable in plan IR)");
        }
    }

    private static void requireQualitySummaryVersion(PlanNode node) {
        if (!io.rowweave.ops.TypedQualitySummary.SUMMARY_VERSION.equals(
                node.param("summaryVersion"))) {
            throw new IllegalArgumentException(
                    "QUALITY_SUMMARY_INVALID: unsupported summaryVersion");
        }
    }

    /**
     * Configures the aggregator for a groupBy typeId: sum, count, max or min, where measure is the field
     * aggregated. The difference between count(*) and count(field) is whether measure is null: null counts
     * every row including nulls, and non-null counts only non-null values. sum, max and min always have a
     * measure, which the describable extraction guarantees.
     */
    public static ParallelGroupByAgg configureAgg(ParallelGroupByAgg agg, String typeId, String measure) {
        switch (typeId) {
            case "groupBySum":   return agg.sum(measure);
            case "groupByCount": return measure == null ? agg.count() : agg.countField(measure);
            case "groupByMax":   return agg.max(measure);
            case "groupByMin":   return agg.min(measure);
            default: throw new IllegalArgumentException("not a groupBy aggregate typeId: " + typeId);
        }
    }

    /**
     * Adds one aggregate from an AggSpec.Kind name (SUM/COUNT/MAX/MIN) plus a measure.
     * For count(*): an empty or null measure becomes count(), otherwise countField. Aggregates are applied in
     * column order so the aggregate column order matches what downstream consumers read by position.
     */
    public static ParallelGroupByAgg configureAggByKind(ParallelGroupByAgg agg, String kind, String measure) {
        boolean noMeasure = measure == null || measure.isEmpty();
        switch (kind) {
            case "SUM":   return agg.sum(measure);
            case "COUNT": return noMeasure ? agg.count() : agg.countField(measure);
            case "MAX":   return agg.max(measure);
            case "MIN":   return agg.min(measure);
            default: throw new IllegalArgumentException("unknown agg kind: " + kind);
        }
    }

    static String[] toArray(List<String> list) {
        return list.toArray(new String[0]);
    }

    /** The whole-row key names distinct uses when keys is empty. */
    static String[] allFieldNames(DataSchema meta) {
        String[] names = new String[meta.getFieldCount()];
        for (int i = 0; i < names.length; i++) {
            names[i] = meta.getField(i).getName();
        }
        return names;
    }

    /** Encoding of a sort field: each entry is "fieldIndex:Direction:NullOrder", identical before and after serialization. */
    static SortField[] decodeSortFields(List<String> encoded) {
        SortField[] out = new SortField[encoded.size()];
        for (int i = 0; i < encoded.size(); i++) {
            String raw = encoded.get(i);
            String[] parts = raw.split(":");
            // Hardened at the deserialization boundary: a truncated or corrupt IR reports a clear error instead of an index out of bounds or a null pointer.
            if (parts.length != 3) {
                throw new IllegalArgumentException("malformed sort field (expect fieldIndex:Direction:NullOrder): " + raw);
            }
            out[i] = new SortField(Integer.parseInt(parts[0]),
                    SortField.Direction.valueOf(parts[1]),
                    SortField.NullOrder.valueOf(parts[2]));
        }
        return out;
    }

    /** Encodes sort fields, used when building the graph and in tests. */
    public static String encodeSortField(SortField sf) {
        return sf.getFieldIndex() + ":" + sf.getDirection() + ":" + sf.getNullOrder();
    }
}
