package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamNodeExecutionDescriptor;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamSourceBinding;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Immutable node/edge-local normalization of one admitted portable job. */
final class StreamExecutionGraph {
    private final PortableStreamJob job;
    private final List<String> nodeUids;
    private final Map<String, Node> nodes;

    private StreamExecutionGraph(PortableStreamJob job,
            List<String> nodeUids, Map<String, Node> nodes) {
        this.job = job;
        this.nodeUids = nodeUids;
        this.nodes = nodes;
    }

    static StreamExecutionGraph from(PortableStreamJob job) {
        return normalize(job, null, Collections.emptyMap());
    }

    static StreamExecutionGraph forParallelism(
            PortableStreamJob job, int targetParallelism) {
        if (targetParallelism < 1) {
            throw new IllegalArgumentException(
                    "Stream target parallelism must be positive");
        }
        return normalize(job, targetParallelism, Collections.emptyMap());
    }

    static StreamExecutionGraph forParallelism(
            PortableStreamJob job, int targetParallelism,
            Map<String, Integer> nodeMinimumParallelism) {
        if (targetParallelism < 1 || nodeMinimumParallelism == null) {
            throw new IllegalArgumentException(
                    "Stream target parallelism contract is incomplete");
        }
        return normalize(job, targetParallelism, nodeMinimumParallelism);
    }

    private static StreamExecutionGraph normalize(
            PortableStreamJob job, Integer parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism) {
        if (job == null) {
            throw new IllegalArgumentException("Stream job is required");
        }
        PortableStreamPlan plan = job.plan();
        Map<String, List<StreamEdge>> inputs = new LinkedHashMap<>();
        Map<String, List<StreamEdge>> outputs = new LinkedHashMap<>();
        for (PortableStreamNode node : plan.nodes()) {
            StreamOperatorDefinitionRegistry.definition(node.opcode());
            inputs.put(node.operatorUid(), new ArrayList<StreamEdge>());
            outputs.put(node.operatorUid(), new ArrayList<StreamEdge>());
        }
        for (StreamEdge edge : plan.edges()) {
            List<StreamEdge> targetInputs = inputs.get(edge.targetOperatorUid());
            List<StreamEdge> sourceOutputs = outputs.get(edge.sourceOperatorUid());
            if (targetInputs == null || sourceOutputs == null) {
                throw new IllegalArgumentException(
                        "Stream edge references an unknown node");
            }
            targetInputs.add(edge);
            sourceOutputs.add(edge);
        }
        Comparator<StreamEdge> inputOrder = Comparator
                .comparingInt(StreamEdge::targetInputIndex)
                .thenComparing(StreamEdge::logicalChannelId);
        Comparator<StreamEdge> outputOrder = Comparator
                .comparing(StreamEdge::targetOperatorUid)
                .thenComparingInt(StreamEdge::targetInputIndex)
                .thenComparing(StreamEdge::logicalChannelId);
        LinkedHashMap<String, Node> normalized = new LinkedHashMap<>();
        List<String> order = new ArrayList<>(plan.nodes().size());
        for (PortableStreamNode node : plan.nodes()) {
            List<StreamEdge> nodeInputs = inputs.get(node.operatorUid());
            List<StreamEdge> nodeOutputs = outputs.get(node.operatorUid());
            nodeInputs.sort(inputOrder);
            nodeOutputs.sort(outputOrder);
            List<StreamEdge> immutableInputs = Collections.unmodifiableList(
                    new ArrayList<>(nodeInputs));
            List<StreamEdge> immutableOutputs = Collections.unmodifiableList(
                    new ArrayList<>(nodeOutputs));
            StreamNodeExecutionDescriptor execution =
                    node.executionDescriptor();
            int maximumParallelism = execution == null
                    ? plan.maxParallelism()
                    : execution.maximumParallelism();
            int parallelism = parallelismOverride == null
                    ? execution == null
                    ? legacyParallelism(plan, node)
                    : execution.parallelism()
                    : parallelismOverride;
            if (parallelismOverride != null
                    && node.opcode() == StreamOperatorCode.SOURCE) {
                StreamSourceBinding source =
                        (StreamSourceBinding) node.binding();
                if (!source.dynamicDiscovery()) {
                    parallelism = Math.max(
                            parallelism, source.fixedSplits().size());
                }
            }
            Integer minimum = nodeMinimumParallelism.get(
                    node.operatorUid());
            if (minimum != null) {
                if (minimum < 1) {
                    throw new IllegalArgumentException(
                            "Stream node minimum parallelism is invalid");
                }
                parallelism = Math.max(parallelism, minimum);
            }
            if (parallelism > maximumParallelism) {
                throw new IllegalArgumentException(
                        "Stream target parallelism exceeds node maximumParallelism");
            }
            Node value = new Node(node,
                    plan.outputChangelogMode(node.operatorUid()),
                    execution == null ? plan.timeMode()
                            : execution.timeMode(),
                    parallelism, maximumParallelism, immutableInputs,
                    immutableOutputs, execution == null
                            ? resources(node, immutableInputs,
                            immutableOutputs, job.resourceProfile())
                            : resources(execution, immutableInputs,
                            immutableOutputs));
            normalized.put(node.operatorUid(), value);
            order.add(node.operatorUid());
        }
        StreamExecutionGraph graph = new StreamExecutionGraph(job,
                Collections.unmodifiableList(order),
                Collections.unmodifiableMap(normalized));
        for (Node node : graph.nodes()) {
            StreamNodeResourceValidator.validate(node);
        }
        return graph;
    }

    private static int legacyParallelism(PortableStreamPlan plan,
            PortableStreamNode node) {
        if (node.opcode() == StreamOperatorCode.SOURCE) {
            return ((StreamSourceBinding) node.binding())
                    .fixedSplits().size();
        }
        return plan.parallelism();
    }

    PortableStreamJob portableJob() { return job; }
    List<String> nodeUids() { return nodeUids; }

    Node node(String operatorUid) {
        Node node = nodes.get(operatorUid);
        if (node == null) {
            throw new IllegalArgumentException(
                    "unknown Stream operator UID: " + operatorUid);
        }
        return node;
    }

    List<Node> nodes() {
        return Collections.unmodifiableList(new ArrayList<>(nodes.values()));
    }

    private static ResourceRequest resources(PortableStreamNode node,
            List<StreamEdge> inputs, List<StreamEdge> outputs,
            StreamResourceProfile jobResources) {
        int maximumFrameBytes = 1;
        long residentBytes = 0L;
        for (StreamEdge edge : inputs) {
            maximumFrameBytes = Math.max(maximumFrameBytes,
                    edge.maximumFrameBytes());
        }
        for (StreamEdge edge : outputs) {
            maximumFrameBytes = Math.max(maximumFrameBytes,
                    edge.maximumFrameBytes());
        }
        if (!node.stateDescriptors().isEmpty()
                || isManagedStateOperator(node.opcode())) {
            residentBytes = add(residentBytes,
                    jobResources.managedStateBytes());
        }
        int threads = 1;
        if (node.opcode() == StreamOperatorCode.SOURCE
                && node.scalarParameters().containsKey(
                "source.time.idleTimeoutMillis")) {
            threads = Math.addExact(threads, 1);
        }
        if (node.binding() instanceof StreamAsyncBinding) {
            StreamAsyncBinding async = (StreamAsyncBinding) node.binding();
            residentBytes = add(residentBytes,
                    async.resources().maximumPendingBytes());
            threads = Math.addExact(threads,
                    Math.addExact(async.resources().maximumConcurrency(), 1));
        }
        residentBytes = Math.max(residentBytes, maximumFrameBytes);
        return new ResourceRequest(maximumFrameBytes, residentBytes, threads,
                jobResources.timerCount(),
                jobResources.windowCount(), jobResources.checkpointBytes(),
                jobResources.spillDiskBytes());
    }

    private static ResourceRequest resources(
            StreamNodeExecutionDescriptor execution,
            List<StreamEdge> inputs, List<StreamEdge> outputs) {
        int maximumFrameBytes = 1;
        for (StreamEdge edge : inputs) {
            maximumFrameBytes = Math.max(maximumFrameBytes,
                    edge.maximumFrameBytes());
        }
        for (StreamEdge edge : outputs) {
            maximumFrameBytes = Math.max(maximumFrameBytes,
                    edge.maximumFrameBytes());
        }
        return new ResourceRequest(maximumFrameBytes,
                execution.maximumResidentBytes(), execution.maximumThreads(),
                execution.maximumTimerCount(),
                execution.maximumWindowCount(),
                execution.maximumCheckpointBytes(),
                execution.maximumSpillDiskBytes());
    }

    private static boolean isManagedStateOperator(StreamOperatorCode opcode) {
        return opcode == StreamOperatorCode.KEYED_AGGREGATE
                || opcode == StreamOperatorCode.EVENT_TIME_WINDOW
                || opcode == StreamOperatorCode.TEMPORAL_JOIN
                || opcode == StreamOperatorCode.KEYED_PROCESS
                || opcode == StreamOperatorCode.KEYED_CO_PROCESS
                || opcode == StreamOperatorCode.GENERIC_WINDOW;
    }

    private static long add(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "Stream node resource request overflow", overflow);
        }
    }

    static final class Node {
        private final PortableStreamNode portableNode;
        private final PortableStreamPlan.ChangelogMode changelogMode;
        private final PortableStreamPlan.TimeMode timeMode;
        private final int parallelism;
        private final int maximumParallelism;
        private final List<StreamEdge> inputEdges;
        private final List<StreamEdge> outputEdges;
        private final ResourceRequest resourceRequest;

        private Node(PortableStreamNode portableNode,
                PortableStreamPlan.ChangelogMode changelogMode,
                PortableStreamPlan.TimeMode timeMode, int parallelism,
                int maximumParallelism, List<StreamEdge> inputEdges,
                List<StreamEdge> outputEdges,
                ResourceRequest resourceRequest) {
            this.portableNode = portableNode;
            this.changelogMode = changelogMode;
            this.timeMode = timeMode;
            this.parallelism = parallelism;
            this.maximumParallelism = maximumParallelism;
            this.inputEdges = inputEdges;
            this.outputEdges = outputEdges;
            this.resourceRequest = resourceRequest;
        }

        PortableStreamNode portableNode() { return portableNode; }
        PortableStreamPlan.ChangelogMode changelogMode() {
            return changelogMode;
        }
        PortableStreamPlan.TimeMode timeMode() { return timeMode; }
        int parallelism() { return parallelism; }
        int maximumParallelism() { return maximumParallelism; }
        List<StreamEdge> inputEdges() { return inputEdges; }
        List<StreamEdge> outputEdges() { return outputEdges; }
        ResourceRequest resourceRequest() { return resourceRequest; }
    }

    static final class ResourceRequest {
        private final int maximumFrameBytes;
        private final long maximumResidentBytes;
        private final int threadCount;
        private final int timerCount;
        private final int windowCount;
        private final long checkpointBytes;
        private final long spillDiskBytes;

        private ResourceRequest(int maximumFrameBytes,
                long maximumResidentBytes, int threadCount, int timerCount,
                int windowCount, long checkpointBytes,
                long spillDiskBytes) {
            this.maximumFrameBytes = maximumFrameBytes;
            this.maximumResidentBytes = maximumResidentBytes;
            this.threadCount = threadCount;
            this.timerCount = timerCount;
            this.windowCount = windowCount;
            this.checkpointBytes = checkpointBytes;
            this.spillDiskBytes = spillDiskBytes;
        }

        int maximumFrameBytes() { return maximumFrameBytes; }
        long maximumResidentBytes() { return maximumResidentBytes; }
        int threadCount() { return threadCount; }
        int timerCount() { return timerCount; }
        int windowCount() { return windowCount; }
        long checkpointBytes() { return checkpointBytes; }
        long spillDiskBytes() { return spillDiskBytes; }
    }
}
