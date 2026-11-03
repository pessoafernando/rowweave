package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamChannelState;
import io.rowweave.stream.contract.StreamChannelStateCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalStreamChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Legal overlapping logical IDs must never change physical edge ownership. */
class StreamPhysicalLaneBindingTest {
    @Test
    void literalQualifiedLookingIdDoesNotBindToAnotherEdge(
            @TempDir Path root) throws Exception {
        StreamSession session = RowWeave.createStreamSession("lane-marker-alias");
        DataStream source = session.fromSource(source("shared"))
                .uid("shared");
        source.union(source).uid("union").sinkTo("sink", sink());
        StreamTestGraph product = renamed(session.toPlanArtifact(),
                "x", "@1:x");
        StreamExecutionGraph graph = product.executionGraph;
        StreamExecutionGraph.Node node = graph.node("union");
        assertEquals(2, node.inputEdges().size());
        for (StreamEdge edge : node.inputEdges()) {
            assertEquals(edge, StreamPhysicalLaneBinding.input(graph, node, 0,
                    spec(edge, 0, 0).laneId()));
        }
        List<StreamWorkerPlacement.Assignment> tasks = new ArrayList<>();
        for (StreamExecutionGraph.Node one : graph.nodes()) {
            for (int subtask = 0; subtask < one.parallelism(); subtask++) {
                tasks.add(new StreamWorkerPlacement.Assignment(tasks.size(),
                        one.portableNode().operatorUid(), subtask,
                        "worker-" + tasks.size(), "incarnation"));
            }
        }
        StreamWorkerPlacement placement = new StreamWorkerPlacement(tasks);
        StreamPhysicalExecutionPlan.Lane oldLane = null;
        for (StreamPhysicalExecutionPlan.Lane lane
                : StreamPhysicalExecutionPlan.from(graph, placement).lanes()) {
            if (lane.logicalEdge().logicalChannelId().equals("@1:x")) {
                oldLane = lane;
                break;
            }
        }
        assertTrue(oldLane != null);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 256 * 1024L);
        CheckpointIdentity cut = checkpoint(product, "attempt-1", 3L, 1L);
        StreamChannelIdentity identity = new StreamChannelIdentity(
                "run", "attempt-1", 3L, 1L,
                oldLane.physicalChannelId(), 1L);
        StreamChannelState state = StreamChannelState.capture(cut, identity,
                0L, 1L, Collections.emptyList(), product.maximumEvents(),
                node.resourceRequest().checkpointBytes(),
                oldLane.logicalEdge().maximumFrameBytes());
        Path staged = root.resolve("marker-alias.state");
        Files.write(staged, StreamChannelStateCodec.encode(state));
        FileCheckpointPayloadStore.StoredPayload payload = store.store(staged);
        StreamCheckpointMember member = StreamCheckpointMember.channelState(
                "union", 0, oldLane.physicalChannelId(), 1L,
                payload.digest(), payload.contentBytes());
        List<StreamCheckpointMember> migrated =
                StreamLiveChannelStateRescaler.rescale(product.product,
                        placement, product.product, placement,
                        checkpoint(product, "attempt-2", 4L, 2L), 2L, 2L,
                        Collections.singletonList(member), store);
        assertTrue(migrated.stream().anyMatch(next ->
                next.subjectId().equals("@1:x#shared#0->union#0")));
    }

    @Test
    void physicalIdsRemainUniqueWhenUidAndLogicalIdBoundariesAlias(
            @TempDir Path root) throws Exception {
        StreamSession session = RowWeave.createStreamSession(
                "lane-boundary-collision");
        DataStream left = session.fromSource(source("left"))
                .uid("y#z");
        DataStream right = session.fromSource(source("right"))
                .uid("z");
        left.union(right).uid("union").sinkTo("sink", sink());
        StreamTestGraph product = renamed(session.toPlanArtifact());
        StreamExecutionGraph graph = product.executionGraph;
        List<StreamWorkerPlacement.Assignment> tasks = new ArrayList<>();
        for (StreamExecutionGraph.Node node : graph.nodes()) {
            for (int subtask = 0; subtask < node.parallelism(); subtask++) {
                tasks.add(new StreamWorkerPlacement.Assignment(tasks.size(),
                        node.portableNode().operatorUid(), subtask,
                        "worker-" + tasks.size(), "incarnation"));
            }
        }
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(graph,
                        new StreamWorkerPlacement(tasks));
        List<StreamPhysicalExecutionPlan.Lane> inputs = new ArrayList<>();
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (lane.target().operatorUid().equals("union")) {
                inputs.add(lane);
            }
        }
        assertEquals(2, inputs.size());
        assertNotEquals(inputs.get(0).physicalChannelId(),
                inputs.get(1).physicalChannelId());
        for (StreamPhysicalExecutionPlan.Lane lane : inputs) {
            assertEquals(lane.logicalEdge(), StreamPhysicalLaneBinding.input(
                    graph, graph.node("union"), 0,
                    lane.physicalChannelId()));
            assertEquals(lane.logicalEdge(), StreamPhysicalLaneBinding.output(
                    graph, graph.node(lane.source().operatorUid()), 0,
                    lane.physicalChannelId()));
            assertEquals(0, StreamPhysicalLaneBinding.sourceSubtask(
                    lane.logicalEdge(), lane.physicalChannelId()));
        }
        // A Worker may retain one independently decoded graph per task.
        // Its lane cache must charge only lanes incident to that task, not
        // every lane in the graph once for each task.
        StreamExecutionGraph.Node firstSource = graph.node("y#z");
        assertEquals(1, StreamPhysicalLaneBinding.taskIndex(
                graph, firstSource, 0).size());
        assertEquals(3, StreamPhysicalLaneBinding.taskIndex(
                graph, graph.node("union"), 0).size());
        assertEquals(1, StreamPhysicalLaneBinding.taskIndex(
                graph, graph.node("sink"), 0).size());
        assertTrue(StreamPhysicalLaneBinding.release(graph,
                new StreamTaskIdentity("y#z", 0)));
        assertFalse(StreamPhysicalLaneBinding.release(graph,
                new StreamTaskIdentity("y#z", 0)));
        assertEquals(1, StreamPhysicalLaneBinding.taskIndex(
                graph, firstSource, 0).size());
        LocalStreamChannel first = channel("collision-first");
        LocalStreamChannel second = channel("collision-second");
        LocalStreamChannel result = channel("collision-result");
        List<StreamTaskLaneSpec> specs = new ArrayList<>();
        for (StreamPhysicalExecutionPlan.Lane lane : inputs) {
            specs.add(StreamTaskLaneSpec.input(lane.physicalChannelId(),
                    lane.logicalEdge().maximumFrameBytes(),
                    lane.logicalEdge().initialCreditBytes()));
        }
        StreamOperatorTask task = StreamRemoteOperatorTaskRunner.start(
                graph, product.artifact.artifactId(),
                graph.node("union").portableNode(),
                Arrays.asList(first, second), specs,
                Collections.singletonList(new StreamTaskOutput(null, result)),
                ignored -> { throw new AssertionError("no function"); },
                0, new FileCheckpointPayloadStore(root.resolve("payloads"),
                        4_096L), Collections.emptyList(), false);
        try {
            first.send(StreamEnvelope.endOfInput(first.channelIdentity(), 1L));
            second.send(StreamEnvelope.endOfInput(second.channelIdentity(), 1L));
            assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                    result.poll(2_000L).kind());
        } finally {
            task.cancel();
            task.awaitCompletion(2_000L);
        }

        StreamPhysicalExecutionPlan.Lane restoredLane = inputs.get(0);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("channel-state"), 256 * 1024L);
        CheckpointIdentity oldCut = checkpoint(product, "attempt-1", 3L, 1L);
        StreamChannelIdentity oldIdentity = new StreamChannelIdentity(
                "run", "attempt-1", 3L, 1L,
                restoredLane.physicalChannelId(), 1L);
        StreamChannelState state = StreamChannelState.capture(oldCut,
                oldIdentity, 0L, 1L, Collections.emptyList(),
                product.maximumEvents(), graph.node("union")
                        .resourceRequest().checkpointBytes(),
                restoredLane.logicalEdge().maximumFrameBytes());
        Path staged = root.resolve("old-channel.state");
        Files.write(staged, StreamChannelStateCodec.encode(state));
        FileCheckpointPayloadStore.StoredPayload payload = store.store(staged);
        StreamCheckpointMember oldMember = StreamCheckpointMember.channelState(
                "union", 0, restoredLane.physicalChannelId(), 1L,
                payload.digest(), payload.contentBytes());
        List<StreamCheckpointMember> migrated =
                StreamLiveChannelStateRescaler.rescale(product.product,
                        new StreamWorkerPlacement(tasks), product.product,
                        new StreamWorkerPlacement(tasks),
                        checkpoint(product, "attempt-2", 4L, 2L),
                        2L, 2L, Collections.singletonList(oldMember), store);
        assertTrue(migrated.stream().anyMatch(member ->
                        member.subjectId().equals(
                                restoredLane.physicalChannelId())),
                "qualified lane ID must survive channel-state migration");
    }

    @Test
    void remoteUnionBindsOverlappingInputIdsToTheirExactEdges(
            @TempDir Path root) throws Exception {
        StreamSession session = RowWeave.createStreamSession("lane-prefix-union");
        DataStream left = session.fromSource(source("left")).uid("left");
        DataStream right = session.fromSource(source("right")).uid("right");
        left.union(right).uid("union").sinkTo("sink", sink());
        StreamTestGraph graph = renamed(session.toPlanArtifact());
        List<StreamEdge> edges = graph.executionGraph.node("union")
                .inputEdges();
        LocalStreamChannel first = channel("first");
        LocalStreamChannel second = channel("second");
        LocalStreamChannel result = channel("result");
        StreamOperatorTask task = StreamRemoteOperatorTaskRunner.start(
                graph.executionGraph, graph.artifact.artifactId(),
                graph.executionGraph.node("union").portableNode(),
                Arrays.asList(first, second), Arrays.asList(
                        spec(edges.get(0), 0, 0), spec(edges.get(1), 0, 0)),
                Collections.singletonList(new StreamTaskOutput(null, result)),
                ignored -> { throw new AssertionError("no function"); },
                0, new FileCheckpointPayloadStore(root.resolve("payloads"),
                        4_096L), Collections.emptyList(), false);
        try {
            first.send(StreamEnvelope.endOfInput(first.channelIdentity(), 1L));
            second.send(StreamEnvelope.endOfInput(second.channelIdentity(), 1L));
            assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                    result.poll(2_000L).kind());
        } finally {
            task.cancel();
            task.awaitCompletion(2_000L);
        }
    }

    @Test
    void fanoutBindsOverlappingOutputIdsToTheirExactEdges()
            throws Exception {
        StreamSession session = RowWeave.createStreamSession("lane-prefix-fanout");
        DataStream source = session.fromSource(source("source")).uid("source");
        source.sinkTo("sink-a", sink());
        source.sinkTo("sink-b", sink());
        StreamTestGraph graph = renamed(session.toPlanArtifact());
        StreamExecutionGraph.Node node = graph.executionGraph.node("source");
        List<StreamEdge> edges = node.outputEdges();
        LocalStreamChannel first = channel("first");
        LocalStreamChannel second = channel("second");
        StreamMultiEdgeOutput output = new StreamMultiEdgeOutput(
                graph.executionGraph, node, 0, Arrays.asList(
                        spec(edges.get(0), 0, 0), spec(edges.get(1), 0, 0)),
                Arrays.asList(first, second));
        output.send(StreamEnvelope.endOfInput(output.channelIdentity(), 1L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                first.poll(2_000L).kind());
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                second.poll(2_000L).kind());
    }

    private static StreamTestGraph renamed(PlanArtifact artifact) {
        return renamed(artifact, "x", "x#y");
    }

    private static StreamTestGraph renamed(PlanArtifact artifact,
            String first, String second) {
        PortableStreamJob original = PortableStreamJobArtifact
                .decodeValidated(artifact);
        PortableStreamPlan plan = original.plan();
        List<StreamEdge> edges = new ArrayList<>();
        int renamed = 0;
        for (StreamEdge edge : plan.edges()) {
            String id = edge.logicalChannelId();
            if ((edge.targetOperatorUid().equals("union")
                    || edge.sourceOperatorUid().equals("source"))
                    && renamed < 2) {
                id = renamed++ == 0 ? first : second;
            }
            edges.add(new StreamEdge(edge.sourceOperatorUid(),
                    edge.targetOperatorUid(), edge.targetInputIndex(), id,
                    edge.partitioning(), edge.maximumFrameBytes(),
                    edge.initialCreditBytes(), edge.sourceOutputTagId()));
        }
        assertEquals(2, renamed);
        PortableStreamPlan changed = new PortableStreamPlan(
                plan.planVersion(), plan.deliveryGuarantee(),
                plan.parallelism(), plan.maxParallelism(),
                plan.changelogMode(), plan.timeMode(),
                plan.rootSinkOperatorUids(), plan.nodes(), edges,
                plan.requiredCapabilities());
        PortableStreamJob job = new PortableStreamJob(changed,
                original.checkpointConfig(), original.restartPolicy(),
                original.resourceProfile());
        return StreamTestGraph.from(PortableStreamJobArtifact.wrap(
                "lane-prefix", job));
    }

    private static StreamTaskLaneSpec spec(StreamEdge edge,
            int sourceIndex, int targetIndex) {
        String id = edge.logicalChannelId() + '#'
                + new StreamTaskIdentity(edge.sourceOperatorUid(), sourceIndex)
                + "->"
                + new StreamTaskIdentity(edge.targetOperatorUid(), targetIndex);
        return StreamTaskLaneSpec.input(id, edge.maximumFrameBytes(),
                edge.initialCreditBytes());
    }

    private static CheckpointIdentity checkpoint(StreamTestGraph graph,
            String attempt, long fence, long sequence) {
        return new CheckpointIdentity(graph.artifact.definitionId(),
                graph.artifact.artifactId(), "run", attempt, fence, 1L,
                "coordinator", sequence, graph.commandDigest,
                graph.artifact.artifactId(), 2, 1);
    }

    private static LocalStreamChannel channel(String id) {
        return new LocalStreamChannel(
                new io.rowweave.stream.contract.StreamChannelIdentity(
                        "run", "attempt", 1L, 1L, id, 1L),
                16, 4_096, 65_536L);
    }

    private static StreamSourceSpec source(String id) {
        return StreamSourceSpec.exactOrdered(new StreamSchema(
                        Collections.singletonList(new StreamField(
                                "id", "id", "id", DataType.LongType,
                                false))),
                "source-" + id, digest("source-" + id), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split-" + id), "sequence-v1",
                        new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce("sink", digest("sink"),
                new byte[] {3});
    }

    private static String digest(String text) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                    .digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder();
            for (byte one : bytes) {
                result.append(String.format("%02x", one & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
