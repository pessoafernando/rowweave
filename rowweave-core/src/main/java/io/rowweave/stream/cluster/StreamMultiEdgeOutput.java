package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.StreamChannelMetrics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Broadcasts one node result across multiple logical outgoing edges. */
final class StreamMultiEdgeOutput implements StreamChannelOutput {
    private final List<StreamChannelOutput> routes;

    StreamMultiEdgeOutput(StreamExecutionGraph graph,
            StreamExecutionGraph.Node node, int subtaskIndex,
            List<StreamTaskLaneSpec> specs,
            List<? extends StreamChannelOutput> senders) {
        this(graph, node, subtaskIndex, null, specs, senders);
    }

    StreamMultiEdgeOutput(StreamExecutionGraph graph,
            StreamExecutionGraph.Node node, int subtaskIndex,
            String outputTagId,
            List<StreamTaskLaneSpec> specs,
            List<? extends StreamChannelOutput> senders) {
        if (graph == null || node == null || specs == null || senders == null
                || specs.size() != senders.size()) {
            throw new IllegalArgumentException(
                    "multi-edge Stream output is incomplete");
        }
        List<StreamEdge> portEdges = new ArrayList<>();
        for (StreamEdge edge : node.outputEdges()) {
            if (Objects.equals(outputTagId, edge.sourceOutputTagId())) {
                portEdges.add(edge);
            }
        }
        if (portEdges.isEmpty()) {
            throw new IllegalArgumentException(
                    "Stream output port has no logical edge");
        }
        Map<String, List<StreamChannelOutput>> grouped = new LinkedHashMap<>();
        for (int index = 0; index < specs.size(); index++) {
            StreamEdge bound = StreamPhysicalLaneBinding.output(
                    graph, node, subtaskIndex, specs.get(index).laneId());
            if (!portEdges.contains(bound)) {
                throw new IllegalArgumentException(
                        "multi-edge Stream output lane uses another port");
            }
            String logical = bound.logicalChannelId();
            grouped.computeIfAbsent(logical, ignored -> new ArrayList<>())
                    .add(senders.get(index));
        }
        List<StreamChannelOutput> result = new ArrayList<>();
        for (StreamEdge edge : portEdges) {
            List<StreamChannelOutput> group = grouped.get(
                    edge.logicalChannelId());
            if (group == null || group.isEmpty()) {
                throw new IllegalArgumentException(
                        "multi-edge Stream output misses a logical edge");
            }
            result.add(route(graph, edge, group));
        }
        routes = Collections.unmodifiableList(result);
    }

    private static StreamChannelOutput route(StreamExecutionGraph graph,
            StreamEdge edge, List<StreamChannelOutput> senders) {
        switch (edge.partitioning()) {
            case FORWARD:
                return new StreamParallelForwardOutput(senders);
            case BROADCAST:
                return new StreamParallelBroadcastOutput(senders);
            case HASH:
                StreamExecutionGraph.Node targetNode = graph.node(
                        edge.targetOperatorUid());
                List<String> partitionKeyFields =
                        StreamEdgePartitioning.keyFields(graph, edge);
                if (partitionKeyFields.isEmpty()) {
                    throw new IllegalArgumentException(
                            "HASH edge has no stable partition key");
                }
                return new StreamParallelKeyedOutput(targetNode,
                        StreamEdgePartitioning.outputSchema(graph, edge),
                        partitionKeyFields, senders,
                        StreamEdgePartitioning.outputChangelogMode(
                                graph, edge),
                        edge.maximumFrameBytes());
            default:
                throw new IllegalArgumentException(
                        "unknown Stream output partitioning");
        }
    }

    @Override public StreamChannelIdentity channelIdentity() {
        return routes.get(0).channelIdentity();
    }

    @Override public void send(StreamEnvelope envelope) {
        for (StreamChannelOutput route : routes) {
            route.send(StreamParallelKeyedOutput.rebind(envelope,
                    route.channelIdentity(), envelope.sequence()));
        }
    }

    @Override public void close() {
        for (StreamChannelOutput route : routes) route.close();
    }

    @Override public void cancel() {
        for (StreamChannelOutput route : routes) route.cancel();
    }

    @Override public void fail(Throwable cause) {
        for (StreamChannelOutput route : routes) route.fail(cause);
    }

    @Override public StreamChannelMetrics metrics() {
        return StreamChannelMetrics.aggregateOutputs(routes);
    }
}
