package io.rowweave.cluster;

import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.sched.net.Frame;
import io.rowweave.sched.net.ShuffleEndpoint;

import java.util.List;

/**
 * conservative per-assignment Java thread cost, excluding the
 * stage owner and progress timer that are charged once per Worker slot.
 */
final class StageThreadCostModel {

    private StageThreadCostModel() {
    }

    static Cost estimate(StageCommand command, DataPlaneConfig dataPlaneConfig) {
        if (command == null || dataPlaneConfig == null) {
            throw new IllegalArgumentException(
                    "stage command and data-plane config are required");
        }
        int pipeline = pipelineThreads(command);
        int helpers = operatorHelperThreads(command);
        int network = networkThreads(command, dataPlaneConfig);
        int blocking = blockingReplayThreads(command);
        return new Cost(pipeline, helpers, network, blocking);
    }

    static Cost requireWithin(StageCommand command,
                              DataPlaneConfig dataPlaneConfig,
                              int maximumThreads) {
        if (maximumThreads < 1) {
            throw new ProcessCapacityException(
                    ProcessCapacityException.Code.STAGE_THREADS,
                    "maximum stage threads must be positive");
        }
        Cost cost = estimate(command, dataPlaneConfig);
        if (cost.totalThreads() > maximumThreads) {
            throw new ProcessCapacityException(
                    ProcessCapacityException.Code.STAGE_THREADS,
                    "stage thread cost " + cost.totalThreads()
                            + " exceeds configured maximum " + maximumThreads
                            + " for role " + command.role());
        }
        return cost;
    }

    private static int pipelineThreads(StageCommand command) {
        switch (command.role()) {
            case SOURCE_STAGE:
            case KEYED_OP:
                return linearPipeline(command.fragment());
            case SINK:
                return linearPipeline(command.sinkFragment());
            case WHOLE_GRAPH:
                PlanGraph graph = requiredGraph(command.fragment());
                // One task per logical node is the normal bound. sinkCount + 1
                // covers tee/terminal tasks in shared-output execution shapes.
                return exactAdd(graph.nodeCount(), exactAdd(graph.sinkCount(), 1));
            default:
                return 0;
        }
    }

    private static int linearPipeline(PlanGraph graph) {
        return exactAdd(requiredGraph(graph).nodeCount(), 1);
    }

    private static int operatorHelperThreads(StageCommand command) {
        switch (command.role()) {
            case SOURCE_STAGE:
            case KEYED_OP:
            case WHOLE_GRAPH:
                return graphHelperThreads(command.fragment());
            case SINK:
                return graphHelperThreads(command.sinkFragment());
            case JOIN_CONSUMER:
                // Two concurrent input drains plus two pipelined sort workers
                // per side cover SMJ, CoGroup and HASH/AUTO fallback.
                return 6;
            default:
                return 0;
        }
    }

    private static int graphHelperThreads(PlanGraph graph) {
        PlanGraph checked = requiredGraph(graph);
        int threads = 0;
        for (PlanNode node : checked.nodes()) {
            String type = node.typeId();
            if ("orderBy".equals(type)) {
                // sort + spill pipeline plus one partition-sort worker at P=1.
                threads = exactAdd(threads, 3);
            } else if ("top".equals(type)) {
                threads = exactAdd(threads, 2);
            } else if (node.kind() == PlanNode.Kind.BINARY
                    && ("join".equals(type) || "coGroup".equals(type))) {
                threads = exactAdd(threads, 6);
            }
        }
        return threads;
    }

    private static int networkThreads(StageCommand command,
                                      DataPlaneConfig dataPlaneConfig) {
        switch (command.role()) {
            case CONSUMER:
                return 3; // SocketChannelSource boss + I/O + handler.
            case PRODUCER:
                return 1; // One legacy SocketChannelSink event loop.
            case ROUTER:
                return endpointThreads(command.downstream(), dataPlaneConfig);
            case SOURCE_STAGE:
                return outboundThreads(command, dataPlaneConfig);
            case KEYED_OP:
            case SINK:
                return exactAdd(inboundThreads(
                                command.inboundConnections(), command.localInbound(),
                                command.inboundTargetId(), dataPlaneConfig),
                        command.role() == StageCommand.Role.SINK
                                ? 0 : outboundThreads(command, dataPlaneConfig));
            case JOIN_CONSUMER:
                int left = inboundThreads(
                        command.inboundConnections(), command.localInbound(),
                        command.inboundTargetId(), dataPlaneConfig);
                int right = inboundThreads(
                        command.rightInboundConnections(), command.localInboundRight(),
                        command.inboundTargetIdRight(), dataPlaneConfig);
                return exactAdd(exactAdd(left, right),
                        outboundThreads(command, dataPlaneConfig));
            case WHOLE_GRAPH:
                return 0;
            default:
                throw new IllegalArgumentException(
                        "unknown stage role " + command.role());
        }
    }

    private static int inboundThreads(int totalConnections, int localConnections,
                                      java.util.UUID muxTarget,
                                      DataPlaneConfig dataPlaneConfig) {
        int socketConnections;
        try {
            socketConnections = Math.subtractExact(totalConnections, localConnections);
        } catch (ArithmeticException overflow) {
            throw threadArithmeticFailure(overflow);
        }
        if (socketConnections <= 0) return 0;
        if (dataPlaneConfig.muxEnabled() && muxTarget != null
                && !Frame.NO_TARGET.equals(muxTarget)) {
            return 0;
        }
        return exactAdd(2, socketConnections);
    }

    private static int outboundThreads(StageCommand command,
                                       DataPlaneConfig dataPlaneConfig) {
        List<StageCommand.BroadcastGroup> groups = command.broadcastGroups();
        if (groups != null && groups.size() > 1) {
            int threads = 0;
            for (StageCommand.BroadcastGroup group : groups) {
                threads = exactAdd(threads,
                        targetThreads(group.targets, dataPlaneConfig));
            }
            return threads;
        }
        List<ShuffleTarget> targets = command.downTargets();
        if (targets != null) return targetThreads(targets, dataPlaneConfig);
        List<ShuffleEndpoint> downstream = command.downstream();
        if (downstream != null) {
            return endpointThreads(downstream, dataPlaneConfig);
        }
        return endpointThread(command.sinkEndpoint(), dataPlaneConfig);
    }

    private static int targetThreads(List<ShuffleTarget> targets,
                                     DataPlaneConfig dataPlaneConfig) {
        if (targets == null) return 0;
        int threads = 0;
        for (ShuffleTarget target : targets) {
            if (target != null && !target.isLocal()) {
                threads = exactAdd(threads,
                        endpointThread(target.endpoint(), dataPlaneConfig));
            }
        }
        return threads;
    }

    private static int endpointThreads(List<ShuffleEndpoint> endpoints,
                                       DataPlaneConfig dataPlaneConfig) {
        if (endpoints == null) return 0;
        int threads = 0;
        for (ShuffleEndpoint endpoint : endpoints) {
            threads = exactAdd(threads,
                    endpointThread(endpoint, dataPlaneConfig));
        }
        return threads;
    }

    private static int endpointThread(ShuffleEndpoint endpoint,
                                      DataPlaneConfig dataPlaneConfig) {
        if (endpoint == null) return 0;
        return dataPlaneConfig.muxEnabled() && endpoint.dataPort() >= 0
                && !Frame.NO_TARGET.equals(endpoint.targetId()) ? 0 : 1;
    }

    private static int blockingReplayThreads(StageCommand command) {
        List<StageCommand.BroadcastGroup> groups = command.broadcastGroups();
        if (groups == null || groups.size() <= 1) return 0;
        for (StageCommand.BroadcastGroup group : groups) {
            if (group.blocking) return groups.size();
        }
        return 0;
    }

    private static PlanGraph requiredGraph(PlanGraph graph) {
        if (graph == null) {
            throw new IllegalArgumentException(
                    "stage role requires a materialized plan graph");
        }
        return graph;
    }

    private static int exactAdd(int left, int right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw threadArithmeticFailure(overflow);
        }
    }

    private static ProcessCapacityException threadArithmeticFailure(
            ArithmeticException overflow) {
        return new ProcessCapacityException(
                ProcessCapacityException.Code.STAGE_THREADS,
                "stage thread cost arithmetic overflow", overflow);
    }

    static final class Cost {
        private final int pipelineThreads;
        private final int operatorHelperThreads;
        private final int networkThreads;
        private final int blockingReplayThreads;
        private final int totalThreads;

        private Cost(int pipelineThreads, int operatorHelperThreads,
                     int networkThreads, int blockingReplayThreads) {
            this.pipelineThreads = pipelineThreads;
            this.operatorHelperThreads = operatorHelperThreads;
            this.networkThreads = networkThreads;
            this.blockingReplayThreads = blockingReplayThreads;
            this.totalThreads = exactAdd(exactAdd(pipelineThreads,
                    operatorHelperThreads), exactAdd(networkThreads,
                    blockingReplayThreads));
        }

        int pipelineThreads() { return pipelineThreads; }
        int operatorHelperThreads() { return operatorHelperThreads; }
        int networkThreads() { return networkThreads; }
        int blockingReplayThreads() { return blockingReplayThreads; }
        int totalThreads() { return totalThreads; }
    }
}
