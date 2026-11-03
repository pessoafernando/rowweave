package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamEdge;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable node-local inputs, semantics and resource lease for one task. */
final class StreamNodeTaskContext {
    private final String artifactId;
    private final PortableStreamNode node;
    private final List<Source> inputs;
    private final List<Sink> outputs;
    private final StreamResourceProfile resources;
    private final PortableStreamPlan.ChangelogMode changelogMode;
    private final PortableStreamPlan.TimeMode timeMode;
    private final int parallelism;
    private final int maximumParallelism;
    private final int maximumFrameBytes;

    private StreamNodeTaskContext(String artifactId,
            PortableStreamNode node, List<Source> inputs,
            List<Sink> outputs,
            StreamResourceProfile resources,
            PortableStreamPlan.ChangelogMode changelogMode,
            PortableStreamPlan.TimeMode timeMode, int parallelism,
            int maximumParallelism, int maximumFrameBytes) {
        this.artifactId = artifactId;
        this.node = node;
        this.inputs = inputs;
        this.outputs = outputs;
        this.resources = resources;
        this.changelogMode = changelogMode;
        this.timeMode = timeMode;
        this.parallelism = parallelism;
        this.maximumParallelism = maximumParallelism;
        this.maximumFrameBytes = maximumFrameBytes;
    }

    static StreamNodeTaskContext from(String artifactId,
            StreamExecutionGraph graph, String operatorUid) {
        if (artifactId == null || artifactId.isEmpty() || graph == null) {
            throw new IllegalArgumentException(
                    "Stream node task identity is incomplete");
        }
        StreamExecutionGraph.Node graphNode = graph.node(operatorUid);
        List<Source> inputs = new ArrayList<>(graphNode.inputEdges().size());
        List<Sink> outputs = new ArrayList<>(
                graphNode.outputEdges().size());
        int maximumFrameBytes = 1;
        for (StreamEdge edge : graphNode.inputEdges()) {
            inputs.add(new Source(edge, graph.node(
                    edge.sourceOperatorUid()).portableNode(),
                    StreamEdgePartitioning.outputSchema(graph, edge),
                    StreamEdgePartitioning.outputStableKeyFields(graph, edge),
                    StreamEdgePartitioning.outputChangelogMode(graph, edge)));
            maximumFrameBytes = Math.max(maximumFrameBytes,
                    edge.maximumFrameBytes());
        }
        for (StreamEdge edge : graphNode.outputEdges()) {
            outputs.add(new Sink(edge, graph.node(
                    edge.targetOperatorUid()).portableNode()));
            maximumFrameBytes = Math.max(maximumFrameBytes,
                    edge.maximumFrameBytes());
        }
        StreamExecutionGraph.ResourceRequest lease =
                graphNode.resourceRequest();
        StreamResourceProfile jobResources =
                graph.portableJob().resourceProfile();
        StreamResourceProfile runtimeResources = new StreamResourceProfile(
                jobResources.mailboxEvents(), lease.maximumFrameBytes(),
                Math.max(lease.maximumFrameBytes(),
                        jobResources.channelCreditBytes()),
                lease.maximumResidentBytes(), lease.timerCount(),
                lease.windowCount(), lease.checkpointBytes(),
                lease.threadCount(),
                StreamNodeTaskPlacementPlanner.fileDescriptorCount(
                        graph, graphNode),
                lease.spillDiskBytes());
        return new StreamNodeTaskContext(artifactId,
                graphNode.portableNode(), Collections.unmodifiableList(inputs),
                Collections.unmodifiableList(outputs),
                runtimeResources,
                graphNode.changelogMode(), graphNode.timeMode(),
                graphNode.parallelism(), graphNode.maximumParallelism(),
                maximumFrameBytes);
    }

    String artifactId() { return artifactId; }
    PortableStreamNode node() { return node; }
    List<Source> inputs() { return inputs; }
    List<Sink> outputs() { return outputs; }
    StreamResourceProfile resources() { return resources; }
    PortableStreamPlan.ChangelogMode changelogMode() { return changelogMode; }
    PortableStreamPlan.TimeMode timeMode() { return timeMode; }
    int parallelism() { return parallelism; }
    int maximumParallelism() { return maximumParallelism; }
    int maximumFrameBytes() { return maximumFrameBytes; }

    static final class Source {
        private final StreamEdge edge;
        private final PortableStreamNode producer;
        private final PortableRowSchema schema;
        private final List<String> stableKeyFields;
        private final PortableStreamPlan.ChangelogMode changelogMode;

        private Source(StreamEdge edge, PortableStreamNode producer,
                PortableRowSchema schema, List<String> stableKeyFields,
                PortableStreamPlan.ChangelogMode changelogMode) {
            this.edge = edge;
            this.producer = producer;
            this.schema = schema;
            this.stableKeyFields = Collections.unmodifiableList(
                    new ArrayList<>(stableKeyFields));
            this.changelogMode = changelogMode;
        }

        StreamEdge edge() { return edge; }
        PortableStreamNode producer() { return producer; }
        PortableRowSchema schema() { return schema; }
        List<String> stableKeyFields() { return stableKeyFields; }
        PortableStreamPlan.ChangelogMode changelogMode() {
            return changelogMode;
        }
    }

    static final class Sink {
        private final StreamEdge edge;
        private final PortableStreamNode consumer;

        private Sink(StreamEdge edge, PortableStreamNode consumer) {
            this.edge = edge;
            this.consumer = consumer;
        }

        StreamEdge edge() { return edge; }
        PortableStreamNode consumer() { return consumer; }
    }
}
