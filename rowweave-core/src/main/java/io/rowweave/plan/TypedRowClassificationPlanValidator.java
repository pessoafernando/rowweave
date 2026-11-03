package io.rowweave.plan;

import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.ops.RowValidation;
import io.rowweave.ops.TypedRowClassification;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** full-graph dual-route proof and fragment-local Worker admission. */
public final class TypedRowClassificationPlanValidator {

    private static final Set<String> CLASSIFIER_PARAMS = immutableSet(
            SourcePositionPlanValidator.MODE_PARAM, "carrierVersion", "routeContract");
    private static final Set<String> CLASSIFIER_LIST_PARAMS = immutableSet(
            "ruleIds", "ruleKinds", "fieldIndices", "lowerBounds", "upperBounds");
    private static final Set<String> ROUTE_PARAMS = immutableSet("carrierVersion", "route");

    private TypedRowClassificationPlanValidator() {
    }

    /** Complete submitted graph: local node contracts plus exactly one reachable route of each kind. */
    public static void validateFullGraph(PlanGraph graph) {
        validateWorkerFragment(graph);
        List<List<Integer>> consumers = consumers(graph);
        boolean[] reachable = reachableFromSinks(graph);
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (isClassifier(node)) {
                if (!reachable[i]) {
                    throw invalid("classifier is not reachable from a SINK");
                }
                List<Integer> direct = consumers.get(i);
                if (direct.size() != 2) {
                    throw invalid("classifier must have exactly two direct route consumers");
                }
                int valid = 0;
                int rejected = 0;
                for (int consumer : direct) {
                    PlanNode route = graph.node(consumer);
                    if (!isRoute(route) || !reachable[consumer]) {
                        throw invalid("classifier direct consumers must be reachable quality routes");
                    }
                    if (TypedRowClassification.VALID_ROUTE.equals(route.param("route"))) {
                        valid++;
                    } else if (TypedRowClassification.REJECTED_ROUTE.equals(route.param("route"))) {
                        rejected++;
                    }
                }
                if (valid != 1 || rejected != 1) {
                    throw invalid("classifier requires one VALID and one REJECTED direct route");
                }
            } else if (isRoute(node)) {
                if (node.childCount() != 1 || !isClassifier(graph.node(node.child(0)))) {
                    throw invalid("quality route must be directly adjacent to classifier");
                }
            }
        }
    }

    /** Worker can only re-prove contracts visible in its canonical fragment. */
    public static void validateWorkerFragment(PlanGraph graph) {
        if (graph == null) throw invalid("plan graph is required");
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (isClassifier(node)) {
                classifierRuntime(graph, i);
            } else if (isRoute(node)) {
                validateRouteNode(graph, i);
            }
        }
    }

    /** Canonical rebuild shared by admission and PlanOps. */
    public static TypedRowClassification classifierRuntime(
            PlanGraph graph, int classifierIndex) {
        PlanNode node = graph.node(classifierIndex);
        if (!isClassifier(node) || node.kind() != PlanNode.Kind.UNARY
                || node.childCount() != 1 || node.userFunction() != null) {
            throw invalid("classifier node is malformed");
        }
        requireExactKeys(node.params().keySet(), CLASSIFIER_PARAMS,
                "classifier scalar parameters");
        requireExactKeys(node.listParams().keySet(), CLASSIFIER_LIST_PARAMS,
                "classifier list parameters");
        if (!SourcePositionPlanValidator.EXACT_SOURCE_V1.equals(
                node.param(SourcePositionPlanValidator.MODE_PARAM))) {
            throw invalid("source position mode is unsupported");
        }
        if (!TypedRowClassification.CARRIER_VERSION.equals(node.param("carrierVersion"))) {
            throw invalid("carrier version is unsupported");
        }
        if (!TypedRowClassification.ROUTE_CONTRACT.equals(node.param("routeContract"))) {
            throw invalid("route contract is unsupported");
        }
        PlanNode source = graph.node(node.child(0));
        if (source.kind() != PlanNode.Kind.SOURCE) {
            throw invalid("classifier must be directly adjacent to SOURCE");
        }
        String uid = source.param(PlanNode.OPERATOR_UID_PARAM);
        if (uid == null || source.sourceSplitDescriptors().isEmpty()) {
            throw invalid("classifier requires an explicit exact SOURCE");
        }
        try {
            source.validateCurrentSourceBindings();
            DataSchema inputMeta = source.outMeta();
            RowValidation validation = RowValidation.fromDescribable(
                    inputMeta, inputMeta,
                    node.listParam("ruleIds"), node.listParam("ruleKinds"),
                    node.listParam("fieldIndices"), node.listParam("lowerBounds"),
                    node.listParam("upperBounds"));
            TypedRowClassification runtime = TypedRowClassification.of(
                    inputMeta, validation, uid, source.sourceSplitDescriptors());
            if (!sameMeta(runtime.carrierSchema(), node.outMeta())) {
                throw invalid("classifier carrier schema is invalid");
            }
            return runtime;
        } catch (IllegalArgumentException failure) {
            if (isClassificationFailure(failure)) throw failure;
            throw invalid("classifier descriptor is invalid");
        } catch (RuntimeException failure) {
            throw invalid("classifier descriptor is invalid");
        }
    }

    public static io.rowweave.sched.PipelineExecutor.Stage routeStage(
            PlanGraph graph, int routeIndex) {
        validateRouteNode(graph, routeIndex);
        PlanNode node = graph.node(routeIndex);
        return TypedRowClassification.routeStage(
                graph.node(node.child(0)).outMeta(), node.outMeta(), node.param("route"));
    }

    private static void validateRouteNode(PlanGraph graph, int routeIndex) {
        PlanNode node = graph.node(routeIndex);
        if (!isRoute(node) || node.kind() != PlanNode.Kind.UNARY
                || node.childCount() != 1 || node.userFunction() != null) {
            throw invalid("quality route node is malformed");
        }
        requireExactKeys(node.params().keySet(), ROUTE_PARAMS,
                "quality route scalar parameters");
        if (!node.listParams().isEmpty()) {
            throw invalid("quality route list parameters must be empty");
        }
        if (!TypedRowClassification.CARRIER_VERSION.equals(node.param("carrierVersion"))) {
            throw invalid("quality route carrier version is unsupported");
        }
        try {
            TypedRowClassification.routeStage(
                    graph.node(node.child(0)).outMeta(), node.outMeta(), node.param("route"));
        } catch (RuntimeException failure) {
            throw invalid("quality route descriptor or schema is invalid");
        }
    }

    public static boolean isClassifier(PlanNode node) {
        return node != null && "rowClassify".equals(node.typeId());
    }

    public static boolean isRoute(PlanNode node) {
        return node != null && "qualityRoute".equals(node.typeId());
    }

    private static List<List<Integer>> consumers(PlanGraph graph) {
        List<List<Integer>> result = new ArrayList<>(graph.nodeCount());
        for (int i = 0; i < graph.nodeCount(); i++) result.add(new ArrayList<>());
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            for (int child : node.childIndices()) result.get(child).add(i);
        }
        return result;
    }

    private static boolean[] reachableFromSinks(PlanGraph graph) {
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
        return reachable;
    }

    private static boolean sameMeta(DataSchema left, DataSchema right) {
        return SchemaFingerprint.of(left).equals(SchemaFingerprint.of(right));
    }

    private static void requireExactKeys(
            Set<String> actual, Set<String> expected, String field) {
        if (!actual.equals(expected)) throw invalid(field + " are noncanonical");
    }

    private static boolean isClassificationFailure(RuntimeException failure) {
        return failure.getMessage() != null
                && failure.getMessage().startsWith("QUALITY_CLASSIFICATION_INVALID:");
    }

    private static Set<String> immutableSet(String... values) {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(values)));
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("QUALITY_CLASSIFICATION_INVALID: " + detail);
    }

}
