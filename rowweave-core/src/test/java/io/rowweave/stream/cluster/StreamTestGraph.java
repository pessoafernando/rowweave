package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamOperatorCode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Test-only node/edge view; contains no whole-job shape admission. */
final class StreamTestGraph {
    final StreamProductExecutionGraph product;
    final PlanArtifact artifact;
    final PortableStreamPlan plan;
    final StreamExecutionGraph executionGraph;
    final List<StreamProductExecutionGraph.SourceRoute> sources;
    final List<StreamProductExecutionGraph.SinkRoute> sinks;
    final String commandDigest;
    final PortableStreamNode source;
    final PortableStreamNode keyBy;
    final PortableStreamNode genericWindow;
    final PortableStreamNode aggregate;
    final PortableStreamNode process;
    final PortableStreamNode coProcess;
    final PortableStreamNode temporalJoin;
    final PortableStreamNode asyncWait;
    final List<PortableStreamNode> unaryNodes;

    private StreamTestGraph(StreamProductExecutionGraph product) {
        this.product = product;
        this.artifact = product.artifact;
        this.plan = product.plan;
        this.executionGraph = product.executionGraph;
        this.sources = product.sources;
        this.sinks = product.sinks;
        this.commandDigest = product.commandDigest;
        this.source = first(StreamOperatorCode.SOURCE);
        this.keyBy = first(StreamOperatorCode.KEY_BY);
        this.genericWindow = first(StreamOperatorCode.GENERIC_WINDOW);
        this.aggregate = first(StreamOperatorCode.KEYED_AGGREGATE);
        this.process = first(StreamOperatorCode.KEYED_PROCESS);
        this.coProcess = first(StreamOperatorCode.KEYED_CO_PROCESS);
        this.temporalJoin = first(StreamOperatorCode.TEMPORAL_JOIN);
        this.asyncWait = first(StreamOperatorCode.ASYNC_WAIT);
        List<PortableStreamNode> unary = new ArrayList<>();
        for (StreamExecutionGraph.Node node : executionGraph.nodes()) {
            switch (node.portableNode().opcode()) {
                case MAP:
                case FILTER:
                case CALC:
                case FLAT_MAP:
                case ASSIGN_EVENT_TIME:
                case ASYNC_WAIT:
                    unary.add(node.portableNode());
                    break;
                default:
                    break;
            }
        }
        this.unaryNodes = Collections.unmodifiableList(unary);
    }

    static StreamTestGraph from(PlanArtifact artifact) {
        return new StreamTestGraph(StreamProductExecutionGraph.from(artifact));
    }

    PortableStreamNode onlyNode(StreamOperatorCode opcode) {
        List<PortableStreamNode> nodes = product.nodes(opcode);
        if (nodes.size() != 1) {
            throw new AssertionError("expected one " + opcode
                    + " node but found " + nodes.size());
        }
        return nodes.get(0);
    }

    StreamProductExecutionGraph.SinkRoute onlySink() {
        if (sinks.size() != 1) {
            throw new AssertionError("expected exactly one test Sink");
        }
        return sinks.get(0);
    }

    int maximumEvents() { return product.maximumEvents(); }
    int maximumFrameBytes() { return product.maximumFrameBytes(); }

    StreamEdge sourceInputEdge() {
        PortableStreamNode keyed = onlyNode(StreamOperatorCode.KEY_BY);
        return executionGraph.node(keyed.operatorUid()).inputEdges().get(0);
    }

    StreamEdge sourceInputEdge(StreamProductExecutionGraph.SourceRoute route) {
        for (StreamExecutionGraph.Node node : executionGraph.nodes()) {
            if (node.portableNode().opcode() == StreamOperatorCode.KEY_BY
                    && node.portableNode().inputOperatorUids().equals(
                    Collections.singletonList(route.node.operatorUid()))) {
                return node.inputEdges().get(0);
            }
        }
        throw new AssertionError("source has no keyed boundary edge: "
                + route.node.operatorUid());
    }

    List<StreamProductExecutionGraph.SourceRoute> inputSources(
            PortableStreamNode operator) {
        List<StreamProductExecutionGraph.SourceRoute> result =
                new ArrayList<>();
        for (String inputUid : operator.inputOperatorUids()) {
            result.add(upstreamSource(inputUid));
        }
        return Collections.unmodifiableList(result);
    }

    private StreamProductExecutionGraph.SourceRoute upstreamSource(
            String operatorUid) {
        StreamExecutionGraph.Node node = executionGraph.node(operatorUid);
        if (node.portableNode().opcode() == StreamOperatorCode.SOURCE) {
            for (StreamProductExecutionGraph.SourceRoute route : sources) {
                if (route.node.operatorUid().equals(operatorUid)) return route;
            }
        }
        if (node.inputEdges().size() != 1) {
            throw new AssertionError("input branch has no unique source: "
                    + operatorUid);
        }
        return upstreamSource(node.inputEdges().get(0).sourceOperatorUid());
    }

    private PortableStreamNode first(StreamOperatorCode opcode) {
        List<PortableStreamNode> nodes = product.nodes(opcode);
        return nodes.isEmpty() ? null : nodes.get(0);
    }
}
