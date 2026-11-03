package io.rowweave.portable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/** Immutable bounded batch DAG validated without loading application classes. */
public final class PortableBatchPlan {
    public static final int CURRENT_VERSION = 1;
    public static final int MAX_PARALLELISM = 1024;
    public static final int MAX_NODES = 4096;
    public static final int MAX_ROOT_SINKS = 64;
    public static final int MAX_CAPABILITIES = 256;
    public static final String BATCH_CAPABILITY = "rowweave.runtime.batch.v1";
    public static final String MAP_PARTITION_CAPABILITY =
            "rowweave.runtime.batch.map-partition.v1";

    private final int planVersion;
    private final int desiredParallelism;
    private final List<String> rootSinkOperatorIds;
    private final List<PortablePlanNode> nodes;
    private final List<String> requiredCapabilities;

    public PortableBatchPlan(
            int planVersion, int desiredParallelism,
            List<String> rootSinkOperatorIds,
            List<PortablePlanNode> nodes,
            List<String> requiredCapabilities) {
        if (planVersion != CURRENT_VERSION) {
            throw PortablePlanText.failure("UNSUPPORTED_PLAN_VERSION", "plan.version",
                    "portable plan version is not supported");
        }
        if (desiredParallelism < 1 || desiredParallelism > MAX_PARALLELISM) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "plan.parallelism",
                    "desired parallelism is outside its limit");
        }
        this.rootSinkOperatorIds = roots(rootSinkOperatorIds);
        this.nodes = validateNodes(nodes, this.rootSinkOperatorIds);
        this.requiredCapabilities = capabilities(requiredCapabilities);
        requireOperatorCapabilities(this.nodes, this.requiredCapabilities);
        this.planVersion = planVersion;
        this.desiredParallelism = desiredParallelism;
    }

    private static List<String> roots(List<String> values) {
        if (values == null || values.isEmpty()
                || values.size() > MAX_ROOT_SINKS) {
            throw PortablePlanText.failure("INVALID_ROOT_SET", "plan.rootSinks",
                    "portable plan requires a bounded non-empty root sink set");
        }
        TreeSet<String> sorted = new TreeSet<>();
        for (String value : values) {
            String root = PortablePlanText.require(value, "plan.rootSinks",
                    PortablePlanNode.MAX_OPERATOR_ID_BYTES);
            if (!sorted.add(root)) {
                throw PortablePlanText.failure("INVALID_ROOT_SET", "plan.rootSinks",
                        "root sink IDs must be unique");
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(sorted));
    }

    private static List<PortablePlanNode> validateNodes(
            List<PortablePlanNode> values, List<String> roots) {
        if (values == null || values.isEmpty() || values.size() > MAX_NODES) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "plan.nodes",
                    "node count is outside its limit");
        }
        List<PortablePlanNode> copied = new ArrayList<>(values.size());
        Map<String, PortablePlanNode> seen = new HashMap<>();
        Set<String> sinks = new HashSet<>();
        for (PortablePlanNode node : values) {
            if (node == null) {
                throw PortablePlanText.failure("INVALID_TOPOLOGY", "plan.nodes",
                        "plan node is required");
            }
            if (seen.containsKey(node.operatorId())) {
                throw PortablePlanText.failure("DUPLICATE_OPERATOR_ID",
                        "plan.nodes", "operator IDs must be unique");
            }
            for (int input = 0; input < node.inputOperatorIds().size(); input++) {
                PortablePlanNode upstream = seen.get(node.inputOperatorIds().get(input));
                if (upstream == null) {
                    throw PortablePlanText.failure("INVALID_TOPOLOGY", "plan.nodes",
                            "operator inputs must reference earlier nodes");
                }
                if (!upstream.outputSchema().fingerprint().equals(
                        node.inputSchemaFingerprints().get(input))) {
                    throw PortablePlanText.failure("SCHEMA_MISMATCH", "plan.nodes",
                            "input schema fingerprint does not match its upstream node");
                }
            }
            seen.put(node.operatorId(), node);
            if (node.opcode() == PortableOperatorCode.SINK) {
                sinks.add(node.operatorId());
            }
            copied.add(node);
        }
        if (!sinks.equals(new HashSet<>(roots))) {
            throw PortablePlanText.failure("INVALID_ROOT_SET", "plan.rootSinks",
                    "root sink set must equal the plan sink set");
        }
        requireReachable(seen, roots);
        return Collections.unmodifiableList(copied);
    }

    private static void requireReachable(
            Map<String, PortablePlanNode> nodes, List<String> roots) {
        Set<String> reachable = new HashSet<>();
        ArrayDeque<String> pending = new ArrayDeque<>(roots);
        while (!pending.isEmpty()) {
            String id = pending.removeFirst();
            if (!reachable.add(id)) continue;
            PortablePlanNode node = nodes.get(id);
            if (node == null) {
                throw PortablePlanText.failure("INVALID_ROOT_SET", "plan.rootSinks",
                        "root sink is not present in the plan");
            }
            pending.addAll(node.inputOperatorIds());
        }
        if (reachable.size() != nodes.size()) {
            throw PortablePlanText.failure("INVALID_TOPOLOGY", "plan.nodes",
                    "every node must be reachable from a root sink");
        }
    }

    private static List<String> capabilities(List<String> values) {
        if (values == null || values.isEmpty()
                || values.size() > MAX_CAPABILITIES) {
            throw PortablePlanText.failure("LIMIT_EXCEEDED", "plan.capabilities",
                    "required capability count is outside its limit");
        }
        TreeSet<String> sorted = new TreeSet<>();
        for (String value : values) {
            String capability = PortablePlanText.require(
                    value, "plan.capabilities", 256);
            if (!sorted.add(capability)) {
                throw PortablePlanText.failure("DUPLICATE_CAPABILITY",
                        "plan.capabilities", "required capabilities must be unique");
            }
        }
        if (!sorted.contains(BATCH_CAPABILITY)) {
            throw PortablePlanText.failure("MISSING_CAPABILITY", "plan.capabilities",
                    "portable batch runtime capability is required");
        }
        return Collections.unmodifiableList(new ArrayList<>(sorted));
    }

    private static void requireOperatorCapabilities(
            List<PortablePlanNode> nodes, List<String> capabilities) {
        for (PortablePlanNode node : nodes) {
            if (node.opcode()
                    == PortableOperatorCode.FUNCTION_MAP_PARTITION
                    && !capabilities.contains(MAP_PARTITION_CAPABILITY)) {
                throw PortablePlanText.failure("MISSING_CAPABILITY",
                        "plan.capabilities",
                        "map-partition requires its runtime capability");
            }
        }
    }

    public int planVersion() { return planVersion; }
    public int desiredParallelism() { return desiredParallelism; }
    public List<String> rootSinkOperatorIds() { return rootSinkOperatorIds; }
    public List<PortablePlanNode> nodes() { return nodes; }
    public List<String> requiredCapabilities() { return requiredCapabilities; }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof PortableBatchPlan)) return false;
        PortableBatchPlan that = (PortableBatchPlan) other;
        return planVersion == that.planVersion
                && desiredParallelism == that.desiredParallelism
                && rootSinkOperatorIds.equals(that.rootSinkOperatorIds)
                && nodes.equals(that.nodes)
                && requiredCapabilities.equals(that.requiredCapabilities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(planVersion, desiredParallelism,
                rootSinkOperatorIds, nodes, requiredCapabilities);
    }
}
