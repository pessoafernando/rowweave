package io.rowweave.plan;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.Source;
import io.rowweave.ObjectStorageQualitySource;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.ops.ObjectStorageQualityRoute;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** fail-closed proof for the canonical Object row-quality graph. */
public final class ObjectStorageQualityPlanValidator {

    private static final int CANONICAL_NODE_COUNT = 10;
    private static final int MAX_REASON_BUCKETS = 4;
    private static final Set<String> SOURCE_PARAMS = set(PlanNode.OPERATOR_UID_PARAM);
    private static final Set<String> SOURCE_LIST_PARAMS =
            set(PlanNode.SOURCE_SPLIT_DESCRIPTORS_PARAM);
    private static final Set<String> ROUTE_PARAMS = set("carrierVersion", "route");

    private ObjectStorageQualityPlanValidator() {
    }

    /** Validate the only admitted full graph containing an Object quality carrier or route. */
    public static void validateFullGraph(PlanGraph graph) {
        if (graph == null) throw invalid("plan graph is required");
        if (!containsObjectQuality(graph)) return;
        try {
            validateCanonicalFullGraph(graph);
        } catch (IllegalArgumentException failure) {
            if (isPublicationFailure(failure)) throw failure;
            throw invalid("canonical Object quality proof failed", failure);
        } catch (RuntimeException failure) {
            throw invalid("canonical Object quality proof failed", failure);
        }
    }

    /** Re-prove any Object routes and available exact source bindings in a Worker fragment. */
    public static void validateWorkerFragment(PlanGraph graph) {
        if (graph == null) throw invalid("worker fragment is required");
        try {
            boolean hasRoute = false;
            for (int i = 0; i < graph.nodeCount(); i++) {
                if (isRoute(graph.node(i))) {
                    hasRoute = true;
                    validateRouteNode(graph, i);
                }
            }
            if (!hasRoute) return;
            int sourceIndex = singleSource(graph);
            PlanNode source = graph.node(sourceIndex);
            if (source.inputs() != null && !source.inputs().isEmpty()) {
                validateSource(source);
            } else if (source.param(PlanNode.OPERATOR_UID_PARAM) == null) {
                throw invalid("Object quality Worker source identity is missing");
            }
        } catch (IllegalArgumentException failure) {
            if (isPublicationFailure(failure)) throw failure;
            throw invalid("worker Object quality fragment is invalid", failure);
        } catch (RuntimeException failure) {
            throw invalid("worker Object quality fragment is invalid", failure);
        }
    }

    /** Canonical rebuild shared by admission and PlanOps. */
    public static io.rowweave.sched.PipelineExecutor.Stage routeStage(
            PlanGraph graph, int routeIndex) {
        validateRouteNode(graph, routeIndex);
        PlanNode route = graph.node(routeIndex);
        return ObjectStorageQualityRoute.routeStage(
                graph.node(route.child(0)).outMeta(), route.outMeta(),
                route.param("route"));
    }

    public static boolean isRoute(PlanNode node) {
        return node != null && "objectQualityRoute".equals(node.typeId());
    }

    private static void validateCanonicalFullGraph(PlanGraph graph) {
        if (graph.nodeCount() != CANONICAL_NODE_COUNT) {
            throw invalid("Object quality publication graph must contain exactly 10 nodes");
        }
        int sourceIndex = singleSource(graph);
        PlanNode source = graph.node(sourceIndex);
        validateSource(source);

        int validRoute = route(graph, ObjectStorageQualityRoute.VALID_ROUTE);
        int rejectedRoute = route(graph, ObjectStorageQualityRoute.REJECTED_ROUTE);
        validateRouteNode(graph, validRoute);
        validateRouteNode(graph, rejectedRoute);
        if (graph.node(validRoute).child(0) != sourceIndex
                || graph.node(rejectedRoute).child(0) != sourceIndex) {
            throw invalid("Object quality routes must directly consume their only SOURCE");
        }

        List<List<Integer>> consumers = consumers(graph);
        requireConsumers(consumers, sourceIndex, validRoute, rejectedRoute);
        TypedQualityPublicationPlanValidator.validateCanonicalTail(
                graph, validRoute, rejectedRoute, MAX_REASON_BUCKETS);
    }

    private static void validateSource(PlanNode source) {
        if (source.kind() != PlanNode.Kind.SOURCE || !"source".equals(source.typeId())
                || source.childCount() != 0 || source.userFunction() != null
                || !source.params().keySet().equals(SOURCE_PARAMS)
                || !source.listParams().keySet().equals(SOURCE_LIST_PARAMS)
                || source.inputs() == null || source.inputs().isEmpty()) {
            throw invalid("Object quality SOURCE is noncanonical");
        }
        String sourceUid = source.param(PlanNode.OPERATOR_UID_PARAM);
        source.validateCurrentSourceBindings();

        DataSchema originalMeta = null;
        String connectorType = null;
        Integer connectorVersion = null;
        ObjectStorageReadRequest group = null;
        Set<String> splitIds = new HashSet<>();
        for (Source input : source.inputs()) {
            if (input == null || input.getClass() != ObjectStorageQualitySource.class) {
                throw invalid("Object quality SOURCE accepts only engine-owned wrappers");
            }
            ObjectStorageQualitySource wrapper = (ObjectStorageQualitySource) input;
            if (!sourceUid.equals(wrapper.sourceOperatorUid())
                    || !sameMeta(source.outMeta(), wrapper.getSchema())) {
                throw invalid("Object quality wrapper identity or carrier schema changed");
            }
            ObjectStorageSnapshotSource original = wrapper.originalSource();
            BatchSourceSplitDescriptor descriptor = original.batchSourceSplit();
            ObjectStorageReadRequest request = original.readRequest();
            if (originalMeta == null) {
                originalMeta = original.getSchema();
                connectorType = descriptor.connectorType();
                connectorVersion = descriptor.connectorVersion();
                group = request;
            } else if (!sameMeta(originalMeta, original.getSchema())
                    || !connectorType.equals(descriptor.connectorType())
                    || connectorVersion.intValue() != descriptor.connectorVersion()
                    || !group.endpointRef().equals(request.endpointRef())
                    || !group.credentialRef().equals(request.credentialRef())
                    || !group.namespace().equals(request.namespace())) {
                throw invalid("Object quality SOURCE group is incoherent");
            }
            if (!splitIds.add(descriptor.splitId())) {
                throw invalid("Object quality SOURCE split identity is duplicated");
            }
        }
        // This also re-proves the UID syntax and exact-replay capability without opening input.
        io.rowweave.sched.stream.SourceRecordPosition.forSplit(
                sourceUid, source.sourceSplitDescriptors().get(0));
    }

    private static void validateRouteNode(PlanGraph graph, int routeIndex) {
        PlanNode node = graph.node(routeIndex);
        if (!isRoute(node) || node.kind() != PlanNode.Kind.UNARY
                || node.childCount() != 1 || node.userFunction() != null
                || !node.params().keySet().equals(ROUTE_PARAMS)
                || !node.listParams().isEmpty()) {
            throw invalid("Object quality route node is malformed");
        }
        if (!ObjectStorageQualityRoute.CARRIER_VERSION.equals(
                node.param("carrierVersion"))) {
            throw invalid("Object quality carrier version is unsupported");
        }
        try {
            ObjectStorageQualityRoute.routeStage(
                    graph.node(node.child(0)).outMeta(), node.outMeta(),
                    node.param("route"));
        } catch (RuntimeException failure) {
            throw invalid("Object quality route descriptor or schema is invalid", failure);
        }
    }

    private static int route(PlanGraph graph, String route) {
        int result = -1;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (isRoute(node) && route.equals(node.param("route"))) {
                if (result >= 0) throw invalid("Object quality route is repeated");
                result = i;
            }
        }
        if (result < 0) throw invalid("Object quality route is missing");
        return result;
    }

    private static int singleSource(PlanGraph graph) {
        int result = -1;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == PlanNode.Kind.SOURCE) {
                if (result >= 0) throw invalid("Object quality graph has repeated SOURCE nodes");
                result = i;
            }
        }
        if (result < 0) throw invalid("Object quality graph is missing its SOURCE");
        return result;
    }

    private static boolean containsObjectQuality(PlanGraph graph) {
        for (PlanNode node : graph.nodes()) {
            if (isRoute(node)) return true;
            if (node.kind() == PlanNode.Kind.SOURCE && node.inputs() != null) {
                for (Source input : node.inputs()) {
                    if (input != null && input.getClass() == ObjectStorageQualitySource.class) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static List<List<Integer>> consumers(PlanGraph graph) {
        List<List<Integer>> result = new ArrayList<>(graph.nodeCount());
        for (int i = 0; i < graph.nodeCount(); i++) result.add(new ArrayList<>());
        for (int i = 0; i < graph.nodeCount(); i++) {
            for (int child : graph.node(i).childIndices()) {
                if (child < 0 || child >= graph.nodeCount()) {
                    throw invalid("Object quality graph child is out of range");
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
            throw invalid("Object quality operator has noncanonical consumers");
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
