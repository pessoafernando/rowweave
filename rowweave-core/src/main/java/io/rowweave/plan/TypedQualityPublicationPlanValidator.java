package io.rowweave.plan;

import io.rowweave.DataType;
import io.rowweave.BatchSink;
import io.rowweave.Field;
import io.rowweave.ManifestFileSink;
import io.rowweave.Sink;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.TypedQualityManifest;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.ops.RowValidation;
import io.rowweave.ops.TypedQualitySummary;
import io.rowweave.ops.TypedRowClassification;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** canonical compound-publication proof before any side effect. */
public final class TypedQualityPublicationPlanValidator {

    public static final String WORKER_SINK_ROLE_PARAM =
            "__typed_quality_sink_role_v1__";
    public static final String WORKER_SINK_BINDING_PARAM =
            "__typed_quality_sink_binding_v1__";
    private static final String QUALITY_BINDING_PREFIX = "quality-gate-v1:";

    private static final String ACCEPTED_SUMMARY = "typedQualityAcceptedSummary";
    private static final String REJECTED_SUMMARY = "typedQualityRejectedSummary";
    private static final String VALID_ROLE = "typed-quality.valid";
    private static final String REJECTED_ROLE = "typed-quality.rejected";
    private static final String SUMMARY_ROLE = "typed-quality.summary";
    private static final int CANONICAL_NODE_COUNT = 11;

    private static final Set<String> ACCEPTED_PARAMS = set("summaryVersion");
    private static final Set<String> REJECTED_PARAMS =
            set("summaryVersion", "maxReasonBuckets");
    private static final Set<String> SINK_PARAMS = set(PlanNode.OPERATOR_UID_PARAM);
    private static final Set<String> SINK_LIST_PARAMS = set(PlanNode.SINK_DESCRIPTOR_PARAM);

    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));

    private TypedQualityPublicationPlanValidator() {
    }

    /** Validate the only admitted full graph containing typed-quality summary nodes. */
    public static void validateFullGraph(PlanGraph graph) {
        if (graph == null) throw invalid("plan graph is required");
        // owns the distinct 10-node Object route prefix. Its validator runs
        // before this one at every submission boundary and reuses only our shared tail.
        if (containsType(graph, "objectQualityRoute")) {
            ObjectStorageQualityPlanValidator.validateFullGraph(graph);
            return;
        }
        if (!containsSummary(graph)) {
            if (containsQualityBinding(graph)) {
                throw invalid("quality binding requires the canonical publication graph");
            }
            return;
        }
        try {
            validateCanonicalFullGraph(graph);
        } catch (IllegalArgumentException failure) {
            if (isPublicationFailure(failure)) throw failure;
            throw invalid("canonical publication proof failed", failure);
        } catch (RuntimeException failure) {
            throw invalid("canonical publication proof failed", failure);
        }
    }

    /** Validate summary operators visible in a Worker-local linear fragment. */
    public static void validateWorkerFragment(PlanGraph graph) {
        if (graph == null) throw invalid("worker fragment is required");
        try {
            for (int i = 0; i < graph.nodeCount(); i++) {
                PlanNode node = graph.node(i);
                if (ACCEPTED_SUMMARY.equals(node.typeId())) {
                    validateSummaryNode(graph, i, false, null);
                } else if (REJECTED_SUMMARY.equals(node.typeId())) {
                    validateSummaryNode(graph, i, true, null);
                }
            }
        } catch (IllegalArgumentException failure) {
            if (isPublicationFailure(failure)) throw failure;
            throw invalid("worker summary fragment is invalid", failure);
        } catch (RuntimeException failure) {
            throw invalid("worker summary fragment is invalid", failure);
        }
    }

    /** Validate the role marker, canonical tail and current Sink before opening inbound data. */
    public static void validateWorkerSinkFragment(PlanGraph graph, Sink output) {
        if (graph == null) throw invalid("worker sink fragment is required");
        try {
            Integer markedSource = null;
            for (int i = 0; i < graph.nodeCount(); i++) {
                PlanNode candidate = graph.node(i);
                if (candidate.kind() == PlanNode.Kind.SOURCE
                        && candidate.param(WORKER_SINK_ROLE_PARAM) != null) {
                    if (markedSource != null) {
                        throw invalid("worker publication sink role is repeated");
                    }
                    markedSource = i;
                }
            }
            if (markedSource == null) {
                boolean gatedManifest = output instanceof ManifestFileSink
                        && ((ManifestFileSink) output).hasQualityGateBinding();
                boolean reservedDescriptor = output instanceof BatchSink
                        && ((BatchSink) output).batchSinkDescriptor()
                        .bindingIdentity().startsWith(QUALITY_BINDING_PREFIX);
                if (gatedManifest || reservedDescriptor) {
                    throw invalid("worker quality-gate markers are missing");
                }
                return;
            }
            int sourceIndex = singleSource(graph);
            PlanNode source = graph.node(sourceIndex);
            String role = source.param(WORKER_SINK_ROLE_PARAM);
            String expectedBinding = source.param(WORKER_SINK_BINDING_PARAM);
            if (sourceIndex != markedSource || sourceIndex != 0 || source.childCount() != 0
                    || source.inputs() == null || !source.inputs().isEmpty()
                    || !source.params().keySet().equals(set(
                    WORKER_SINK_ROLE_PARAM, WORKER_SINK_BINDING_PARAM))
                    || !source.listParams().keySet().equals(
                    set(PlanNode.SOURCE_SPLIT_DESCRIPTORS_PARAM))) {
                throw invalid("worker sink boundary is noncanonical");
            }
            if (!VALID_ROLE.equals(role) && !REJECTED_ROLE.equals(role)
                    && !SUMMARY_ROLE.equals(role)) {
                throw invalid("worker sink role is unknown");
            }
            final ManifestFileSink manifestOutput;
            if (output != null && output.getClass() == ManifestFileSink.class) {
                manifestOutput = (ManifestFileSink) output;
            } else if (output != null
                    && output.getClass() == ProviderBatchSink.class) {
                try {
                    manifestOutput = ManifestFileSink.fromProviderSink(
                            (ProviderBatchSink) output);
                } catch (IllegalArgumentException invalidProviderOutput) {
                    throw invalid("worker publication provider output is inconsistent");
                }
            } else {
                throw invalid("worker publication output must be ManifestFileSink");
            }
            if (!role.equals(manifestOutput.qualityGateRole())
                    || manifestOutput.qualityGatePolicy() == null
                    || expectedBinding == null
                    || !expectedBinding.equals(manifestOutput.qualityGateBindingIdentity())
                    || !expectedBinding.equals(
                    manifestOutput.batchSinkDescriptor().bindingIdentity())) {
                throw invalid("worker publication quality-gate binding is inconsistent");
            }
            if (graph.nodeCount() != 2) {
                throw invalid("worker publication sink requires one canonical order tail");
            }
            PlanNode order = graph.node(1);
            requireUnary(order, "orderBy", 0);
            requireNoFunctionOrScalarParams(order, "worker publication order");
            String nullOrder = SUMMARY_ROLE.equals(role) ? "DEFAULT" : "NULLS_LAST";
            requireExactSort(order, order.outMeta().getFieldCount(), nullOrder);
            if (!sameMeta(source.outMeta(), order.outMeta())
                    || !sameMeta(order.outMeta(), output.getSchema())) {
                throw invalid("worker publication sink schema is inconsistent");
            }
            if (REJECTED_ROLE.equals(role) && !sameMeta(REJECTED_META, order.outMeta())) {
                throw invalid("worker rejected sink schema is noncanonical");
            }
            if (SUMMARY_ROLE.equals(role)
                    && !sameMeta(TypedQualityManifest.summarySchema(), order.outMeta())) {
                throw invalid("worker summary sink schema is noncanonical");
            }
        } catch (IllegalArgumentException failure) {
            if (isPublicationFailure(failure)) throw failure;
            throw invalid("worker publication sink fragment is invalid", failure);
        } catch (RuntimeException failure) {
            throw invalid("worker publication sink fragment is invalid", failure);
        }
    }

    /** True only for the three reserved publication sink roles. */
    public static boolean isPublicationSinkRole(String role) {
        return VALID_ROLE.equals(role) || REJECTED_ROLE.equals(role)
                || SUMMARY_ROLE.equals(role);
    }

    private static void validateCanonicalFullGraph(PlanGraph graph) {
        if (graph.nodeCount() != CANONICAL_NODE_COUNT) {
            throw invalid("publication graph must contain exactly 11 nodes");
        }
        int source = uniqueKind(graph, PlanNode.Kind.SOURCE);
        int classifier = uniqueType(graph, "rowClassify");
        TypedRowClassificationPlanValidator.validateFullGraph(graph);
        if (graph.node(classifier).child(0) != source) {
            throw invalid("publication classifier must consume its only SOURCE");
        }

        int validRoute = route(graph, TypedRowClassification.VALID_ROUTE);
        int rejectedRoute = route(graph, TypedRowClassification.REJECTED_ROUTE);
        int expectedBound;
        try {
            expectedBound = Math.addExact(Math.addExact(
                    graph.node(classifier).listParam("ruleIds").size(),
                    graph.node(source).outMeta().getFieldCount()), 1);
        } catch (ArithmeticException overflow) {
            throw invalid("reason bucket bound overflows", overflow);
        }
        validateCanonicalTail(graph, validRoute, rejectedRoute, expectedBound);
    }

    /** Shared exact three-member publication tail for typed and Object quality prefixes. */
    static void validateCanonicalTail(
            PlanGraph graph, int validRoute, int rejectedRoute,
            int expectedReasonBound) {
        int acceptedSummary = uniqueType(graph, ACCEPTED_SUMMARY);
        int rejectedSummary = uniqueType(graph, REJECTED_SUMMARY);
        int union = uniqueType(graph, "union");
        int order = uniqueType(graph, "orderBy");

        validateSummaryNode(graph, acceptedSummary, false, validRoute);
        validateSummaryNode(graph, rejectedSummary, true, rejectedRoute);
        if (!Integer.toString(expectedReasonBound).equals(
                graph.node(rejectedSummary).param("maxReasonBuckets"))) {
            throw invalid("reason bucket bound is not derived from the Plan");
        }

        PlanNode unionNode = graph.node(union);
        if (unionNode.kind() != PlanNode.Kind.BINARY
                || unionNode.childCount() != 2
                || unionNode.child(0) != acceptedSummary
                || unionNode.child(1) != rejectedSummary
                || unionNode.userFunction() != null
                || !unionNode.params().isEmpty() || !unionNode.listParams().isEmpty()
                || !sameMeta(TypedQualityManifest.summarySchema(), unionNode.outMeta())) {
            throw invalid("publication summary union is noncanonical");
        }

        PlanNode orderNode = graph.node(order);
        requireUnary(orderNode, "orderBy", union);
        requireNoFunctionOrScalarParams(orderNode, "publication summary order");
        requireExactSort(orderNode,
                TypedQualityManifest.summarySchema().getFieldCount(), "DEFAULT");
        if (!sameMeta(TypedQualityManifest.summarySchema(), orderNode.outMeta())) {
            throw invalid("publication summary order schema is noncanonical");
        }

        int validSink = sink(graph, VALID_ROLE);
        int rejectedSink = sink(graph, REJECTED_ROLE);
        int summarySink = sink(graph, SUMMARY_ROLE);
        validateSink(graph, validSink, validRoute, graph.node(validRoute).outMeta());
        validateSink(graph, rejectedSink, rejectedRoute, REJECTED_META);
        validateSink(graph, summarySink, order, TypedQualityManifest.summarySchema());
        requireManifestGroup(graph, validSink, rejectedSink, summarySink);

        List<List<Integer>> consumers = consumers(graph);
        requireConsumers(consumers, validRoute, validSink, acceptedSummary);
        requireConsumers(consumers, rejectedRoute, rejectedSink, rejectedSummary);
        requireConsumers(consumers, acceptedSummary, union);
        requireConsumers(consumers, rejectedSummary, union);
        requireConsumers(consumers, union, order);
        requireConsumers(consumers, order, summarySink);
        requireAllReachable(graph);
    }

    private static void validateSummaryNode(
            PlanGraph graph, int index, boolean rejected, Integer expectedChild) {
        PlanNode node = graph.node(index);
        String type = rejected ? REJECTED_SUMMARY : ACCEPTED_SUMMARY;
        if (node.kind() != PlanNode.Kind.UNARY || !type.equals(node.typeId())
                || node.childCount() != 1 || node.userFunction() != null
                || !node.listParams().isEmpty()) {
            throw invalid("typed-quality summary node is malformed");
        }
        if (expectedChild != null && node.child(0) != expectedChild) {
            throw invalid("typed-quality summary consumes the wrong route");
        }
        if (node.child(0) < 0 || node.child(0) >= graph.nodeCount()) {
            throw invalid("typed-quality summary child is out of range");
        }
        Set<String> expectedParams = rejected ? REJECTED_PARAMS : ACCEPTED_PARAMS;
        if (!node.params().keySet().equals(expectedParams)
                || !TypedQualitySummary.SUMMARY_VERSION.equals(node.param("summaryVersion"))
                || !sameMeta(TypedQualityManifest.summarySchema(), node.outMeta())) {
            throw invalid("typed-quality summary descriptor is noncanonical");
        }
        if (rejected) {
            int bound;
            try {
                bound = Integer.parseInt(node.param("maxReasonBuckets"));
            } catch (NumberFormatException malformed) {
                throw invalid("reason bucket bound is malformed", malformed);
            }
            if (bound <= 0 || bound > RowValidation.MAX_RULES + RowValidation.MAX_FIELDS + 1) {
                throw invalid("reason bucket bound is outside the closed limit");
            }
            if (!sameMeta(REJECTED_META, graph.node(node.child(0)).outMeta())) {
                throw invalid("rejected summary input schema is noncanonical");
            }
        }
    }

    private static void validateSink(
            PlanGraph graph, int sinkIndex, int child, DataSchema expectedMeta) {
        PlanNode sink = graph.node(sinkIndex);
        if (sink.kind() != PlanNode.Kind.SINK || !"sink".equals(sink.typeId())
                || sink.childCount() != 1 || sink.child(0) != child
                || sink.userFunction() != null
                || !sink.params().keySet().equals(SINK_PARAMS)
                || !sink.listParams().keySet().equals(SINK_LIST_PARAMS)
                || !sameMeta(expectedMeta, sink.outMeta())) {
            throw invalid("publication sink is noncanonical");
        }
        if (sink.output() == null || sink.output().getClass() != ManifestFileSink.class
                || !sameMeta(expectedMeta, sink.output().getSchema())) {
            throw invalid("publication sink Sink is noncanonical");
        }
        ManifestFileSink output = (ManifestFileSink) sink.output();
        if (!sink.param(PlanNode.OPERATOR_UID_PARAM).equals(output.qualityGateRole())
                || output.qualityGatePolicy() == null
                || !output.qualityGateBindingIdentity().equals(
                sink.batchSinkDescriptor().bindingIdentity())) {
            throw invalid("publication Sink quality-gate binding is noncanonical");
        }
        sink.validateCurrentSinkBinding();
    }

    private static void requireManifestGroup(
            PlanGraph graph, int validSink, int rejectedSink, int summarySink) {
        ManifestFileSink valid = (ManifestFileSink) graph.node(validSink).output();
        ManifestFileSink rejected = (ManifestFileSink) graph.node(rejectedSink).output();
        ManifestFileSink summary = (ManifestFileSink) graph.node(summarySink).output();
        if (!valid.domainId().equals(rejected.domainId())
                || !valid.domainId().equals(summary.domainId())) {
            throw invalid("publication members must share one Manifest domain");
        }
        if (!Arrays.equals(valid.qualityGatePolicyDigest(), rejected.qualityGatePolicyDigest())
                || !Arrays.equals(valid.qualityGatePolicyDigest(),
                summary.qualityGatePolicyDigest())
                || !Arrays.equals(valid.qualityGatePolicyBytes(),
                rejected.qualityGatePolicyBytes())
                || !Arrays.equals(valid.qualityGatePolicyBytes(),
                summary.qualityGatePolicyBytes())) {
            throw invalid("publication members must share one quality-gate policy");
        }
        Set<String> members = new HashSet<>(Arrays.asList(
                valid.memberName(), rejected.memberName(), summary.memberName()));
        if (members.size() != 3) {
            throw invalid("publication Manifest member names must be distinct");
        }
    }

    private static int route(PlanGraph graph, String route) {
        int result = -1;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (TypedRowClassificationPlanValidator.isRoute(node)
                    && route.equals(node.param("route"))) {
                if (result >= 0) throw invalid("publication route is repeated");
                result = i;
            }
        }
        if (result < 0) throw invalid("publication route is missing");
        return result;
    }

    private static boolean containsQualityBinding(PlanGraph graph) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK || node.output() == null) continue;
            if (node.output() instanceof ManifestFileSink
                    && ((ManifestFileSink) node.output()).hasQualityGateBinding()) {
                return true;
            }
            if (node.batchSinkDescriptor().bindingIdentity()
                    .startsWith(QUALITY_BINDING_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    private static int sink(PlanGraph graph, String role) {
        int result = -1;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && role.equals(node.param(PlanNode.OPERATOR_UID_PARAM))) {
                if (result >= 0) throw invalid("publication sink role is repeated");
                result = i;
            }
        }
        if (result < 0) throw invalid("publication sink role is missing: " + role);
        return result;
    }

    private static int uniqueKind(PlanGraph graph, PlanNode.Kind kind) {
        int result = -1;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == kind) {
                if (result >= 0) throw invalid("publication graph contains repeated " + kind);
                result = i;
            }
        }
        if (result < 0) throw invalid("publication graph is missing " + kind);
        return result;
    }

    private static int uniqueType(PlanGraph graph, String type) {
        int result = -1;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (type.equals(graph.node(i).typeId())) {
                if (result >= 0) throw invalid("publication operator is repeated: " + type);
                result = i;
            }
        }
        if (result < 0) throw invalid("publication operator is missing: " + type);
        return result;
    }

    private static int singleSource(PlanGraph graph) {
        return uniqueKind(graph, PlanNode.Kind.SOURCE);
    }

    private static boolean containsSummary(PlanGraph graph) {
        for (PlanNode node : graph.nodes()) {
            if (ACCEPTED_SUMMARY.equals(node.typeId())
                    || REJECTED_SUMMARY.equals(node.typeId())) return true;
        }
        return false;
    }

    private static boolean containsType(PlanGraph graph, String typeId) {
        for (PlanNode node : graph.nodes()) {
            if (typeId.equals(node.typeId())) return true;
        }
        return false;
    }

    private static void requireUnary(PlanNode node, String type, int child) {
        if (node.kind() != PlanNode.Kind.UNARY || !type.equals(node.typeId())
                || node.childCount() != 1 || node.child(0) != child) {
            throw invalid(type + " node is noncanonical");
        }
    }

    private static void requireNoFunctionOrScalarParams(PlanNode node, String label) {
        if (node.userFunction() != null || !node.params().isEmpty()) {
            throw invalid(label + " carries forbidden state");
        }
    }

    private static void requireExactSort(PlanNode node, int fieldCount, String nullOrder) {
        if (!node.listParams().keySet().equals(set("sort"))) {
            throw invalid("publication order fields are noncanonical");
        }
        List<String> expected = new ArrayList<>(fieldCount);
        for (int i = 0; i < fieldCount; i++) {
            expected.add(i + ":ASC:" + nullOrder);
        }
        if (!expected.equals(node.listParam("sort"))) {
            throw invalid("publication order specification is noncanonical");
        }
    }

    private static List<List<Integer>> consumers(PlanGraph graph) {
        List<List<Integer>> result = new ArrayList<>(graph.nodeCount());
        for (int i = 0; i < graph.nodeCount(); i++) result.add(new ArrayList<>());
        for (int i = 0; i < graph.nodeCount(); i++) {
            for (int child : graph.node(i).childIndices()) {
                if (child < 0 || child >= graph.nodeCount()) {
                    throw invalid("publication graph child is out of range");
                }
                result.get(child).add(i);
            }
        }
        return result;
    }

    private static void requireConsumers(
            List<List<Integer>> consumers, int producer, int... expected) {
        List<Integer> actual = new ArrayList<>(consumers.get(producer));
        List<Integer> wanted = new ArrayList<>();
        for (int value : expected) wanted.add(value);
        Collections.sort(actual);
        Collections.sort(wanted);
        if (!actual.equals(wanted)) {
            throw invalid("publication operator has noncanonical consumers");
        }
    }

    private static void requireAllReachable(PlanGraph graph) {
        boolean[] reachable = new boolean[graph.nodeCount()];
        ArrayDeque<Integer> pending = new ArrayDeque<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SINK) pending.add(i);
        }
        while (!pending.isEmpty()) {
            int index = pending.removeFirst();
            if (reachable[index]) continue;
            reachable[index] = true;
            for (int child : graph.node(index).childIndices()) pending.addLast(child);
        }
        for (boolean value : reachable) {
            if (!value) throw invalid("publication graph contains an unreachable node");
        }
    }

    private static boolean sameMeta(DataSchema left, DataSchema right) {
        return left != null && right != null
                && SchemaFingerprint.of(left).equals(SchemaFingerprint.of(right));
    }

    private static boolean isPublicationFailure(RuntimeException failure) {
        return failure.getMessage() != null
                && failure.getMessage().startsWith("QUALITY_PUBLICATION_INVALID:");
    }

    private static Set<String> set(String... values) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("QUALITY_PUBLICATION_INVALID: " + detail);
    }

    private static IllegalArgumentException invalid(String detail, Throwable cause) {
        return new IllegalArgumentException("QUALITY_PUBLICATION_INVALID: " + detail, cause);
    }
}
