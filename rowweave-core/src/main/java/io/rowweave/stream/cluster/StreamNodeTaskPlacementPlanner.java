package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamFunctionBinding;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamWindowBinding;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;

/** Expands the immutable node graph into canonical physical task requests. */
final class StreamNodeTaskPlacementPlanner {
    // A conservative charge for request/identity/capability and physical
    // task/lane indexes. The coordinator may use at most one eighth of its
    // heap for placement metadata; even a large heap has a finite ceiling.
    private static final long TASK_METADATA_BYTES = 8_192L;
    private static final long LANE_METADATA_BYTES = 4_096L;
    private static final long MAX_PLACEMENT_METADATA_BYTES =
            256L * 1024L * 1024L;

    private StreamNodeTaskPlacementPlanner() { }

    static List<StreamTaskPlacementRequest> requests(
            StreamExecutionGraph graph, int controlProtocolVersion,
            int dataProtocolVersion, Collection<String> capabilities) {
        if (graph == null || controlProtocolVersion <= 0
                || dataProtocolVersion <= 0 || capabilities == null) {
            throw new IllegalArgumentException(
                    "Stream graph and placement protocol are required");
        }
        preflight(graph);
        List<String> required =
                StreamWorkerCapabilityProfile.capabilities(capabilities);
        List<StreamTaskPlacementRequest> result = new ArrayList<>();
        for (StreamExecutionGraph.Node node : graph.nodes()) {
            StreamExecutionGraph.ResourceRequest resource =
                    node.resourceRequest();
            List<String> nodeCapabilities = nodeCapabilities(required, node);
            long checkpointBytes = checkpointLeaseBytes(
                    node.portableNode(), resource.checkpointBytes());
            PhysicalTaskResources physical = physicalResources(
                    graph, node, resource);
            for (int subtask = 0; subtask < node.parallelism(); subtask++) {
                result.add(new StreamTaskPlacementRequest(
                        new StreamTaskIdentity(
                                node.portableNode().operatorUid(), subtask),
                        controlProtocolVersion, dataProtocolVersion,
                        resource.maximumFrameBytes(),
                        physical.residentBytes,
                        physical.threads, physical.channels,
                        physical.fileDescriptors,
                        resource.timerCount(), resource.windowCount(),
                        checkpointBytes, resource.spillDiskBytes(),
                        nodeCapabilities));
            }
        }
        return Collections.unmodifiableList(result);
    }

    /** Rejects a whole physical graph before allocating any subtask request. */
    static long preflight(StreamExecutionGraph graph) {
        long budget = Math.min(MAX_PLACEMENT_METADATA_BYTES,
                Runtime.getRuntime().maxMemory() / 8L);
        return preflight(graph, budget);
    }

    static long preflight(StreamExecutionGraph graph,
            long metadataBudgetBytes) {
        if (graph == null) {
            throw new IllegalArgumentException("Stream graph is required");
        }
        if (metadataBudgetBytes < 1L) {
            throw new IllegalArgumentException(
                    "Stream placement metadata budget must be positive");
        }
        long taskCount = 0L;
        long fileDescriptors = 0L;
        long ceiling = graph.portableJob().resourceProfile()
                .fileDescriptorCount();
        for (StreamExecutionGraph.Node node : graph.nodes()) {
            PhysicalTaskResources physical;
            try {
                physical = physicalResources(graph, node,
                        node.resourceRequest());
            } catch (ArithmeticException overflow) {
                throw new IllegalArgumentException(
                        "physical Stream task resources overflow", overflow);
            }
            if (physical.channels > 4_096 || physical.threads > 65_536
                    || physical.residentBytes <= 0L) {
                throw new IllegalArgumentException(
                        "physical Stream task resources exceed placement limits");
            }
            checkpointLeaseBytes(node.portableNode(),
                    node.resourceRequest().checkpointBytes());
            taskCount = addTasks(taskCount, node.parallelism());
            fileDescriptors = addFileDescriptors(fileDescriptors,
                    (long) physical.fileDescriptors * node.parallelism());
            if (fileDescriptors > ceiling) {
                throw new IllegalArgumentException(
                        "physical Stream file descriptor resources exceed the job ceiling");
            }
            long metadata = Math.addExact(
                    Math.multiplyExact(taskCount, TASK_METADATA_BYTES),
                    Math.multiplyExact(fileDescriptors,
                            LANE_METADATA_BYTES));
            if (metadata > metadataBudgetBytes) {
                throw new StreamClusterProtocolException(
                        StreamClusterProtocolException.Code.LIMIT_EXCEEDED,
                        "physical Stream placement metadata exceeds the coordinator budget");
            }
        }
        return taskCount;
    }

    /** Necessary node-local and whole-job Worker bounds before expansion. */
    static void preflightCapacity(StreamExecutionGraph graph,
            Collection<StreamWorkerCapabilityProfile> profiles,
            int controlProtocolVersion, int dataProtocolVersion,
            Collection<String> capabilities,
            BiPredicate<StreamTaskPlacementRequest,
                    StreamWorkerCapabilityProfile> eligibility) {
        long tasks = preflight(graph);
        if (profiles == null || profiles.isEmpty()
                || controlProtocolVersion <= 0 || dataProtocolVersion <= 0
                || capabilities == null || eligibility == null) {
            throw new IllegalArgumentException(
                    "Stream worker placement inputs are required");
        }
        List<String> required =
                StreamWorkerCapabilityProfile.capabilities(capabilities);
        long slots = 0L;
        Set<String> workerIds = new HashSet<>();
        for (StreamWorkerCapabilityProfile profile : profiles) {
            if (profile == null) {
                throw new IllegalArgumentException(
                        "Stream worker profile must not be null");
            }
            if (!workerIds.add(profile.workerId())) {
                throw new StreamClusterProtocolException(
                        StreamClusterProtocolException.Code
                                .AMBIGUOUS_WORKER_INCARNATION,
                        "multiple incarnations exist for one Stream worker ID");
            }
            slots += Math.min((long) profile.taskSlots(), tasks - slots);
        }
        if (tasks > slots) {
            insufficientCapacity();
        }
        for (StreamExecutionGraph.Node node : graph.nodes()) {
            StreamExecutionGraph.ResourceRequest resource =
                    node.resourceRequest();
            PhysicalTaskResources physical = physicalResources(
                    graph, node, resource);
            StreamTaskPlacementRequest sample =
                    new StreamTaskPlacementRequest(
                            new StreamTaskIdentity(
                                    node.portableNode().operatorUid(), 0),
                            controlProtocolVersion, dataProtocolVersion,
                            resource.maximumFrameBytes(),
                            physical.residentBytes, physical.threads,
                            physical.channels, physical.fileDescriptors,
                            resource.timerCount(), resource.windowCount(),
                            checkpointLeaseBytes(node.portableNode(),
                                    resource.checkpointBytes()),
                            resource.spillDiskBytes(),
                            nodeCapabilities(required, node));
            long available = 0L;
            for (StreamWorkerCapabilityProfile profile : profiles) {
                if (eligibility.test(sample, profile)) {
                    available += Math.min(
                            (long) eligibleCapacity(profile, sample),
                            node.parallelism() - available);
                }
            }
            if (available < node.parallelism()) insufficientCapacity();
        }
    }

    private static int eligibleCapacity(StreamWorkerCapabilityProfile profile,
            StreamTaskPlacementRequest request) {
        if (profile.controlProtocolVersion()
                < request.controlProtocolVersion()
                || profile.dataProtocolVersion()
                < request.dataProtocolVersion()
                || profile.maximumFrameBytes() < request.maximumFrameBytes()
                || !profile.capabilities().containsAll(
                        request.requiredCapabilities())) {
            return 0;
        }
        long capacity = profile.taskSlots();
        capacity = Math.min(capacity,
                profile.channelCapacity() / request.channels());
        capacity = Math.min(capacity,
                profile.maximumFileDescriptorCount()
                        / request.fileDescriptorCount());
        capacity = Math.min(capacity,
                profile.maximumResidentBytes() / request.residentBytes());
        capacity = Math.min(capacity,
                profile.maximumThreads() / request.threads());
        if (request.timerCount() > 0) {
            capacity = Math.min(capacity,
                    profile.maximumTimerCount() / request.timerCount());
        }
        if (request.windowCount() > 0) {
            capacity = Math.min(capacity,
                    profile.maximumWindowCount() / request.windowCount());
        }
        if (request.checkpointBytes() > 0L) {
            capacity = Math.min(capacity,
                    profile.maximumCheckpointBytes()
                            / request.checkpointBytes());
        }
        if (request.spillDiskBytes() > 0L) {
            capacity = Math.min(capacity,
                    profile.maximumSpillDiskBytes()
                            / request.spillDiskBytes());
        }
        return (int) capacity;
    }

    private static void insufficientCapacity() {
        throw new StreamClusterProtocolException(
                StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                "eligible Stream worker capacity is insufficient");
    }

    private static List<String> nodeCapabilities(List<String> required,
            StreamExecutionGraph.Node node) {
        List<String> nodeCapabilities = new ArrayList<>(required);
        boolean signedConnector = nodeCapabilities.remove(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
        String connectorSet = null;
        if (signedConnector) {
            for (String capability : required) {
                if (capability.startsWith(
                        PlanArtifact.DEPLOYMENT_FEATURE_PREFIX)) {
                    connectorSet = capability;
                    break;
                }
            }
            if (connectorSet == null) {
                throw new IllegalArgumentException(
                        "signed connector placement has no deployment set");
            }
        }
        nodeCapabilities.removeIf(value ->
                value.startsWith(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX));
        if (signedConnector && connectorEndpoint(node.portableNode())) {
            nodeCapabilities.add(
                    PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
            nodeCapabilities.add(connectorSet);
        }
        FunctionDeploymentReference function = function(node.portableNode());
        if (function != null) {
            nodeCapabilities.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                    + function.deploymentSetId());
        }
        return nodeCapabilities;
    }

    private static boolean connectorEndpoint(PortableStreamNode node) {
        return node.binding() instanceof StreamSourceBinding
                || node.binding() instanceof StreamSinkBinding;
    }

    static int fileDescriptorCount(StreamExecutionGraph graph,
            StreamExecutionGraph.Node node) {
        return physicalResources(graph, node,
                node.resourceRequest()).fileDescriptors;
    }

    private static long checkpointLeaseBytes(PortableStreamNode node,
            long maximumPayloadBytes) {
        int payloads = 1;
        if (node.binding() instanceof StreamProcessBinding) {
            StreamProcessBinding process = (StreamProcessBinding) node.binding();
            payloads = process.managedStates().size()
                    + process.timerFamilies().size();
            if (process.broadcastState() != null) {
                payloads = Math.addExact(payloads, 2);
            }
            if (process.functionKind()
                    == StreamProcessBinding.FunctionKind.KEYED_CO_PROCESS) {
                payloads = Math.addExact(payloads, 1);
            }
            payloads = Math.max(1, payloads);
        }
        try {
            return Math.multiplyExact(maximumPayloadBytes, (long) payloads);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "physical Stream checkpoint resources overflow", overflow);
        }
    }

    private static long addFileDescriptors(long current, long count) {
        try {
            return Math.addExact(current, count);
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "physical Stream file descriptor resources overflow",
                    overflow);
        }
    }

    private static long addTasks(long current, int count) {
        try {
            long total = Math.addExact(current, (long) count);
            if (total > Integer.MAX_VALUE - 8L) {
                throw new IllegalArgumentException(
                        "physical Stream task count exceeds placement limits");
            }
            return total;
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "physical Stream task count overflows", overflow);
        }
    }

    private static PhysicalTaskResources physicalResources(
            StreamExecutionGraph graph, StreamExecutionGraph.Node node,
            StreamExecutionGraph.ResourceRequest resource) {
        int inbound = 0;
        int outbound = 0;
        long creditBytes = 0L;
        for (io.rowweave.stream.contract.StreamEdge edge
                : node.inputEdges()) {
            StreamExecutionGraph.Node source = graph.node(
                    edge.sourceOperatorUid());
            int lanes = pointwise(edge, source, node)
                    ? 1 : source.parallelism();
            inbound = Math.addExact(inbound, lanes);
            creditBytes = addCredits(
                    creditBytes, edge.initialCreditBytes(), lanes);
        }
        for (io.rowweave.stream.contract.StreamEdge edge
                : node.outputEdges()) {
            StreamExecutionGraph.Node target = graph.node(
                    edge.targetOperatorUid());
            int lanes = pointwise(edge, node, target)
                    ? 1 : target.parallelism();
            outbound = Math.addExact(outbound, lanes);
            creditBytes = addCredits(
                    creditBytes, edge.initialCreditBytes(), lanes);
        }
        int channels = Math.max(1, Math.addExact(inbound, outbound));
        int fileDescriptors = Math.max(1, Math.addExact(outbound,
                Math.multiplyExact(inbound, 2)));
        int threads = Math.addExact(resource.threadCount(),
                Math.addExact(1, outbound));
        long laneIndexBytes = Math.addExact(TASK_METADATA_BYTES,
                Math.multiplyExact((long) channels, LANE_METADATA_BYTES));
        long resident = Math.addExact(Math.addExact(
                resource.maximumResidentBytes(), creditBytes),
                laneIndexBytes);
        return new PhysicalTaskResources(
                resident, threads, channels, fileDescriptors);
    }

    private static boolean pointwise(
            io.rowweave.stream.contract.StreamEdge edge,
            StreamExecutionGraph.Node source,
            StreamExecutionGraph.Node target) {
        return edge.partitioning()
                == io.rowweave.stream.contract.StreamEdge.Partitioning.FORWARD
                && source.parallelism() == target.parallelism();
    }

    private static long addCredits(
            long current, long creditBytes, int lanes) {
        try {
            return Math.addExact(current,
                    Math.multiplyExact(creditBytes, (long) lanes));
        } catch (ArithmeticException overflow) {
            throw new IllegalArgumentException(
                    "physical Stream lane resources overflow", overflow);
        }
    }

    private static final class PhysicalTaskResources {
        final long residentBytes;
        final int threads;
        final int channels;
        final int fileDescriptors;

        private PhysicalTaskResources(
                long residentBytes, int threads, int channels,
                int fileDescriptors) {
            this.residentBytes = residentBytes;
            this.threads = threads;
            this.channels = channels;
            this.fileDescriptors = fileDescriptors;
        }
    }

    private static FunctionDeploymentReference function(
            PortableStreamNode node) {
        if (node.binding() instanceof StreamFunctionBinding) {
            return ((StreamFunctionBinding) node.binding())
                    .deploymentReference();
        }
        if (node.binding() instanceof StreamProcessBinding) {
            return ((StreamProcessBinding) node.binding())
                    .deploymentReference();
        }
        if (node.binding() instanceof StreamWindowBinding) {
            return ((StreamWindowBinding) node.binding())
                    .deploymentReference();
        }
        if (node.binding() instanceof StreamAsyncBinding) {
            return ((StreamAsyncBinding) node.binding())
                    .deploymentReference();
        }
        return null;
    }

    static StreamWorkerPlacement place(StreamExecutionGraph graph,
            Collection<StreamWorkerCapabilityProfile> profiles,
            int controlProtocolVersion, int dataProtocolVersion,
            Collection<String> capabilities,
            BiPredicate<StreamTaskPlacementRequest,
                    StreamWorkerCapabilityProfile> eligibility) {
        preflightCapacity(graph, profiles, controlProtocolVersion,
                dataProtocolVersion, capabilities, eligibility);
        return StreamWorkerCapabilityPool.placeTasks(profiles,
                requests(graph, controlProtocolVersion, dataProtocolVersion,
                        capabilities), eligibility);
    }
}
