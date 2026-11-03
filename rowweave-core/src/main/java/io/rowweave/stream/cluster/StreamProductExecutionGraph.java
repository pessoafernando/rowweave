package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.PortableStreamPlanCodec;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamProcessBinding;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamWindowBinding;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Topology-neutral Worker projection of one validated portable Stream DAG.
 *
 * <p>This class deliberately exposes nodes, edges and endpoint bindings only.
 * It must never classify a whole job into an admitted operator shape.</p>
 */
final class StreamProductExecutionGraph {
    final PlanArtifact artifact;
    final PortableStreamJob job;
    final PortableStreamPlan plan;
    final StreamExecutionGraph executionGraph;
    final List<SourceRoute> sources;
    final List<SinkRoute> sinks;
    final String commandDigest;
    final int parallelismOverride;
    final Map<String, Integer> nodeMinimumParallelism;

    private StreamProductExecutionGraph(
            PlanArtifact artifact, PortableStreamJob job,
            int parallelismOverride) {
        this(artifact, job, parallelismOverride, Collections.emptyMap());
    }

    private StreamProductExecutionGraph(
            PlanArtifact artifact, PortableStreamJob job,
            int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism) {
        this.artifact = artifact;
        this.job = job;
        this.plan = job.plan();
        this.parallelismOverride = parallelismOverride;
        this.nodeMinimumParallelism = Collections.unmodifiableMap(
                new java.util.LinkedHashMap<>(nodeMinimumParallelism));
        this.executionGraph = parallelismOverride == 0
                ? StreamExecutionGraph.from(job)
                : StreamExecutionGraph.forParallelism(
                        job, parallelismOverride, nodeMinimumParallelism);
        List<SourceRoute> sourceRoutes = new ArrayList<>();
        List<SinkRoute> sinkRoutes = new ArrayList<>();
        for (StreamExecutionGraph.Node graphNode : executionGraph.nodes()) {
            PortableStreamNode node = graphNode.portableNode();
            if (node.opcode() == StreamOperatorCode.SOURCE) {
                sourceRoutes.add(new SourceRoute(node,
                        (StreamSourceBinding) node.binding()));
            } else if (node.opcode() == StreamOperatorCode.SINK) {
                sinkRoutes.add(new SinkRoute(node,
                        (StreamSinkBinding) node.binding(),
                        graphNode.inputEdges().get(0)));
            }
        }
        this.sources = Collections.unmodifiableList(sourceRoutes);
        this.sinks = Collections.unmodifiableList(sinkRoutes);
        this.commandDigest = sha256Hex(PortableStreamPlanCodec.encode(plan));
    }

    static StreamProductExecutionGraph decode(byte[] artifactFrame) {
        PlanArtifact artifact = PlanArtifactCodec.decode(artifactFrame);
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        return new StreamProductExecutionGraph(artifact,
                PortableStreamJobArtifact.decodeValidated(artifact), 0);
    }

    static StreamProductExecutionGraph decode(
            byte[] artifactFrame, int parallelismOverride) {
        PlanArtifact artifact = PlanArtifactCodec.decode(artifactFrame);
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        return new StreamProductExecutionGraph(artifact,
                PortableStreamJobArtifact.decodeValidated(artifact),
                parallelismOverride);
    }

    static StreamProductExecutionGraph decode(
            byte[] artifactFrame, int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism) {
        PlanArtifact artifact = PlanArtifactCodec.decode(artifactFrame);
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        return new StreamProductExecutionGraph(artifact,
                PortableStreamJobArtifact.decodeValidated(artifact),
                parallelismOverride, nodeMinimumParallelism);
    }

    static StreamProductExecutionGraph from(PlanArtifact artifact) {
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        return new StreamProductExecutionGraph(artifact,
                PortableStreamJobArtifact.decodeValidated(artifact), 0);
    }

    static StreamProductExecutionGraph from(
            PlanArtifact artifact, int parallelismOverride) {
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        return new StreamProductExecutionGraph(artifact,
                PortableStreamJobArtifact.decodeValidated(artifact),
                parallelismOverride);
    }

    static StreamProductExecutionGraph from(
            PlanArtifact artifact, int parallelismOverride,
            Map<String, Integer> nodeMinimumParallelism) {
        PlanArtifactValidator.validateForStreamProductExecution(artifact);
        return new StreamProductExecutionGraph(artifact,
                PortableStreamJobArtifact.decodeValidated(artifact),
                parallelismOverride, nodeMinimumParallelism);
    }

    int maximumEvents() {
        return job.resourceProfile().mailboxEvents();
    }

    int maximumFrameBytes() {
        int maximum = 0;
        for (StreamEdge edge : plan.edges()) {
            maximum = Math.max(maximum, edge.maximumFrameBytes());
        }
        return maximum;
    }

    List<String> clusterCapabilities() {
        List<String> capabilities = new ArrayList<>(
                plan.requiredCapabilities());
        addCapability(capabilities,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY);
        addCapability(capabilities,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        if (controlProtocolVersion() >= 2) {
            addCapability(capabilities,
                    StreamWorkerCapabilityProfile
                            .CHECKPOINT_TRANSFER_CAPABILITY);
        }
        return Collections.unmodifiableList(capabilities);
    }

    int controlProtocolVersion() {
        return plan.planVersion() == PortableStreamPlan.FIXED_SOURCE_VERSION
                && !plan.requiredCapabilities().contains(
                        PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY)
                ? 1 : 2;
    }

    List<String> executionCapabilities() {
        List<String> capabilities = new ArrayList<>(clusterCapabilities());
        if (artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1)) {
            addCapability(capabilities,
                    PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
            addCapability(capabilities,
                    PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                            + PlanArtifactDeploymentBinding.deploymentSetId(
                            artifact));
        }
        for (PortableStreamNode node : plan.nodes()) {
            FunctionDeploymentReference reference = function(node);
            if (reference != null) {
                addCapability(capabilities,
                        PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                                + reference.deploymentSetId());
            }
        }
        return Collections.unmodifiableList(capabilities);
    }

    List<PortableStreamNode> nodes(StreamOperatorCode opcode) {
        List<PortableStreamNode> selected = new ArrayList<>();
        for (StreamExecutionGraph.Node node : executionGraph.nodes()) {
            if (node.portableNode().opcode() == opcode) {
                selected.add(node.portableNode());
            }
        }
        return Collections.unmodifiableList(selected);
    }

    boolean isResidentCdc(SourceRoute source) {
        return source != null && "RESIDENT_CDC_SNAPSHOT_LOG".equals(
                source.node.scalarParameters().get("source.mode"));
    }

    List<SourceRoute> residentCdcSources() {
        List<SourceRoute> selected = new ArrayList<>();
        for (SourceRoute source : sources) {
            if (isResidentCdc(source)) selected.add(source);
        }
        return Collections.unmodifiableList(selected);
    }

    StreamEdge firstOutputEdge(SourceRoute source) {
        List<StreamEdge> outputs = executionGraph.node(
                source.node.operatorUid()).outputEdges();
        if (outputs.isEmpty()) {
            throw new IllegalStateException("Stream source has no output edge");
        }
        return outputs.get(0);
    }

    long sourceIdleTimeoutMillis(SourceRoute source) {
        String value = source.node.scalarParameters().get(
                "source.time.idleTimeoutMillis");
        return value == null ? 0L : Long.parseLong(value);
    }

    SinkRoute sink(String operatorUid) {
        for (SinkRoute sink : sinks) {
            if (sink.node.operatorUid().equals(operatorUid)) return sink;
        }
        throw new IllegalArgumentException(
                "Stream Sink operator is not in the admitted Artifact");
    }

    boolean appendRecords(SinkRoute sink) {
        return StreamEdgePartitioning.outputChangelogMode(
                executionGraph, sink.edge)
                == PortableStreamPlan.ChangelogMode.APPEND;
    }

    private static FunctionDeploymentReference function(
            PortableStreamNode node) {
        if (node.binding() instanceof
                io.rowweave.stream.contract.StreamFunctionBinding) {
            return ((io.rowweave.stream.contract.StreamFunctionBinding)
                    node.binding()).deploymentReference();
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

    private static void addCapability(
            List<String> capabilities, String capability) {
        if (!capabilities.contains(capability)) capabilities.add(capability);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder text = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                text.append(String.format("%02x", current & 0xff));
            }
            return text.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    static final class SourceRoute {
        final PortableStreamNode node;
        final StreamSourceBinding binding;

        SourceRoute(PortableStreamNode node, StreamSourceBinding binding) {
            this.node = node;
            this.binding = binding;
        }
    }

    static final class SinkRoute {
        final PortableStreamNode node;
        final StreamSinkBinding binding;
        final StreamEdge edge;

        SinkRoute(PortableStreamNode node, StreamSinkBinding binding,
                StreamEdge edge) {
            this.node = node;
            this.binding = binding;
            this.edge = edge;
        }
    }
}
