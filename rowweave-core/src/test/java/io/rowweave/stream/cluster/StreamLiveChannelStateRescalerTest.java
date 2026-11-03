package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamChannelState;
import io.rowweave.stream.contract.StreamChannelStateCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.StreamChannelInput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

/** in-flight unaligned frames follow the target physical plan. */
class StreamLiveChannelStateRescalerTest {

    @Test
    void realUnalignedCaptureRescalesAndRestoresDivergentLaneWatermarks(
            @TempDir Path root) throws Exception {
        StreamProductExecutionGraph graph = StreamProductExecutionGraph.from(
                StreamAtomicCompositionProductTest.session(null)
                        .toPlanArtifact(), 3);
        StreamWorkerPlacement placement = placement(graph.executionGraph);
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(
                        graph.executionGraph, placement);
        List<StreamPhysicalExecutionPlan.Lane> oldLanes = new ArrayList<>();
        String edgeId = null;
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (lane.partitioning() != StreamEdge.Partitioning.HASH) continue;
            if (edgeId == null) edgeId = lane.logicalEdge().logicalChannelId();
            if (edgeId.equals(lane.logicalEdge().logicalChannelId())
                    && lane.target().subtaskIndex() == 0) oldLanes.add(lane);
        }
        assertEquals(3, oldLanes.size());
        List<EmptyInput> oldTransport = new ArrayList<>();
        for (StreamPhysicalExecutionPlan.Lane lane : oldLanes) {
            oldTransport.add(new EmptyInput(new StreamChannelIdentity(
                    "run", "attempt-1", 3L, 1L,
                    lane.physicalChannelId(), 1L)));
        }
        CheckpointIdentity sourceCut = checkpoint(
                graph, "attempt-1", 3L, 1L);
        oldTransport.get(0).add(StreamEnvelope.watermark(
                oldTransport.get(0).identity, 1L, 100L));
        oldTransport.get(0).add(StreamEnvelope.checkpointBarrier(
                oldTransport.get(0).identity, 2L, sourceCut));
        oldTransport.get(1).add(StreamEnvelope.watermark(
                oldTransport.get(1).identity, 1L, 0L));
        oldTransport.get(1).add(StreamEnvelope.checkpointBarrier(
                oldTransport.get(1).identity, 2L, sourceCut));
        oldTransport.get(2).add(StreamEnvelope.checkpointBarrier(
                oldTransport.get(2).identity, 1L, sourceCut));
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 256 * 1024L);
        StreamExecutionGraph.Node node = graph.executionGraph.node(
                oldLanes.get(0).target().operatorUid());
        long laneBytes = node.resourceRequest().checkpointBytes();
        StreamUnalignedTaskGate sourceGate = new StreamUnalignedTaskGate(
                node.portableNode().operatorUid(), 0,
                oldTransport, store, Collections.emptyList(), false,
                graph.maximumEvents(), laneBytes,
                laneBytes * oldTransport.size(), 128 * 1024);
        List<StreamChannelInput> inputs = sourceGate.inputs();
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                cut = sourceGate.checkpoint(sourceCut);
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                inputs.get(2).poll(0L).kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                inputs.get(0).poll(0L).kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                inputs.get(1).poll(0L).kind());
        assertEquals(100L, inputs.get(0).poll(0L).timestamp());
        assertEquals(0L, inputs.get(1).poll(0L).timestamp());
        assertEquals(null, inputs.get(0).poll(0L));
        assertEquals(null, inputs.get(1).poll(0L));
        List<StreamCheckpointMember> captured =
                cut.get(2L, TimeUnit.SECONDS);

        List<StreamCheckpointMember> migrated =
                StreamLiveChannelStateRescaler.rescale(graph, placement,
                        checkpoint(graph, "attempt-2", 4L, 2L),
                        2L, 2L, captured, store);
        List<StreamChannelInput> newTransport = new ArrayList<>();
        for (StreamPhysicalExecutionPlan.Lane lane : oldLanes) {
            newTransport.add(new EmptyInput(new StreamChannelIdentity(
                    "run", "attempt-2", 4L, 2L,
                    lane.physicalChannelId(), 2L)));
        }
        StreamUnalignedTaskGate targetGate = new StreamUnalignedTaskGate(
                node.portableNode().operatorUid(), 0,
                newTransport, store, migrated, true,
                graph.maximumEvents(), laneBytes,
                laneBytes * newTransport.size(), 128 * 1024);
        StreamInputGateChannel logical = new StreamInputGateChannel(
                targetGate.inputs());
        assertEquals(null, logical.poll(0L),
                "the unadvanced third lane holds the merged frontier");
    }

    @Test
    void distinctSourceLaneWatermarksDoNotRegressOrAdvanceSlowLane(
            @TempDir Path root) throws Exception {
        StreamProductExecutionGraph target = StreamProductExecutionGraph.from(
                StreamAtomicCompositionProductTest.session(null)
                        .toPlanArtifact(), 3);
        StreamWorkerPlacement placement = placement(target.executionGraph);
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(
                        target.executionGraph, placement);
        List<StreamPhysicalExecutionPlan.Lane> oldLanes = new ArrayList<>();
        String edgeId = null;
        int targetSubtask = -1;
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (lane.partitioning() != StreamEdge.Partitioning.HASH) continue;
            if (edgeId == null) {
                edgeId = lane.logicalEdge().logicalChannelId();
                targetSubtask = lane.target().subtaskIndex();
            }
            if (edgeId.equals(lane.logicalEdge().logicalChannelId())
                    && targetSubtask == lane.target().subtaskIndex()) {
                oldLanes.add(lane);
            }
        }
        assertTrue(oldLanes.size() >= 3);
        CheckpointIdentity sourceCheckpoint = checkpoint(
                target, "attempt-1", 3L, 1L);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 256 * 1024L);
        List<StreamCheckpointMember> oldMembers = new ArrayList<>();
        long[] watermarks = {100L, 0L};
        for (int index = 0; index < oldLanes.size(); index++) {
            StreamPhysicalExecutionPlan.Lane lane = oldLanes.get(index);
            StreamChannelIdentity channel = new StreamChannelIdentity(
                    "run", "attempt-1", 3L, 1L,
                    lane.physicalChannelId(), 1L);
            List<StreamEnvelope> captured = index < watermarks.length
                    ? Collections.singletonList(StreamEnvelope.watermark(
                    channel, 1L, watermarks[index]))
                    : Collections.emptyList();
            StreamChannelState state = StreamChannelState.capture(
                    sourceCheckpoint, channel, 0L,
                    captured.size() + 1L, captured,
                    target.maximumEvents(), target.executionGraph.node(
                    lane.target().operatorUid()).resourceRequest()
                    .checkpointBytes(), 128 * 1024);
            Path staged = root.resolve("source-lane-" + index + ".state");
            Files.write(staged, StreamChannelStateCodec.encode(state));
            FileCheckpointPayloadStore.StoredPayload stored = store.store(staged);
            oldMembers.add(StreamCheckpointMember.channelState(
                    lane.target().operatorUid(), lane.target().subtaskIndex(),
                    lane.physicalChannelId(), 1L,
                    stored.digest(), stored.contentBytes()));
        }
        List<StreamCheckpointMember> migrated =
                StreamLiveChannelStateRescaler.rescale(
                        target, placement,
                        checkpoint(target, "attempt-2", 4L, 2L),
                        2L, 2L, oldMembers, store);
        boolean sawHigh = false;
        boolean sawLow = false;
        for (StreamCheckpointMember member : migrated) {
            if (!member.subjectId().startsWith(edgeId + '#')
                    || member.subtaskIndex() != targetSubtask) continue;
            Path restored = root.resolve(member.memberId() + ".state");
            store.restore(new FileCheckpointPayloadStore.StoredPayload(
                    member.contentDigest(), member.contentBytes()), restored);
            StreamChannelState state = StreamChannelStateCodec.decode(
                    Files.readAllBytes(restored), target.maximumEvents(),
                    target.executionGraph.node(member.operatorUid())
                    .resourceRequest().checkpointBytes(), 128 * 1024);
            long previous = Long.MIN_VALUE;
            for (StreamEnvelope frame : state.envelopes()) {
                if (frame.kind() != StreamEnvelope.Kind.WATERMARK) continue;
                assertTrue(frame.timestamp() >= previous,
                        "a target physical lane must not regress");
                previous = frame.timestamp();
                sawHigh |= frame.timestamp() == 100L;
                sawLow |= frame.timestamp() == 0L;
            }
        }
        assertTrue(sawHigh);
        assertTrue(sawLow);
        assertFalse(migrated.isEmpty());

        List<StreamChannelInput> transport = new ArrayList<>();
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (edgeId.equals(lane.logicalEdge().logicalChannelId())
                    && lane.target().subtaskIndex() == targetSubtask) {
                transport.add(new EmptyInput(new StreamChannelIdentity(
                        "run", "attempt-2", 4L, 2L,
                        lane.physicalChannelId(), 2L)));
            }
        }
        StreamExecutionGraph.Node targetNode = target.executionGraph.node(
                oldLanes.get(0).target().operatorUid());
        StreamUnalignedTaskGate restored = new StreamUnalignedTaskGate(
                targetNode.portableNode().operatorUid(), targetSubtask,
                transport, store, migrated, true, target.maximumEvents(),
                targetNode.resourceRequest().checkpointBytes(),
                targetNode.resourceRequest().checkpointBytes()
                        * transport.size(), 128 * 1024);
        StreamInputGateChannel logical = new StreamInputGateChannel(
                restored.inputs());
        int delivered = 0;
        StreamEnvelope next;
        while ((next = logical.poll(0L)) != null) {
            assertTrue(next.kind() != StreamEnvelope.Kind.WATERMARK
                    || next.timestamp() <= 0L);
            delivered++;
        }
        assertEquals(0, delivered,
                "the third old lane has not advanced its watermark");
    }

    @Test
    void downscaleMergesDifferentOldLaneWatermarksConservatively(
            @TempDir Path root) throws Exception {
        io.rowweave.artifact.PlanArtifact artifact =
                StreamAtomicCompositionProductTest.session(null)
                        .toPlanArtifact();
        StreamProductExecutionGraph source =
                StreamProductExecutionGraph.from(artifact, 3);
        StreamProductExecutionGraph target =
                StreamProductExecutionGraph.from(artifact, 1);
        StreamWorkerPlacement sourcePlacement =
                placement(source.executionGraph);
        StreamWorkerPlacement targetPlacement =
                placement(target.executionGraph);
        List<StreamPhysicalExecutionPlan.Lane> oldLanes =
                new ArrayList<>();
        String edgeId = null;
        for (StreamPhysicalExecutionPlan.Lane lane
                : StreamPhysicalExecutionPlan.from(
                source.executionGraph, sourcePlacement).lanes()) {
            if (lane.partitioning() != StreamEdge.Partitioning.HASH) continue;
            if (edgeId == null) edgeId = lane.logicalEdge().logicalChannelId();
            if (edgeId.equals(lane.logicalEdge().logicalChannelId())
                    && lane.target().subtaskIndex() == 0) oldLanes.add(lane);
        }
        assertEquals(3, oldLanes.size());
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 256 * 1024L);
        List<StreamCheckpointMember> oldMembers = new ArrayList<>();
        long[] watermarks = {100L, 0L, 50L};
        for (int index = 0; index < oldLanes.size(); index++) {
            StreamPhysicalExecutionPlan.Lane lane = oldLanes.get(index);
            StreamChannelIdentity channel = new StreamChannelIdentity(
                    "run", "attempt-1", 3L, 1L,
                    lane.physicalChannelId(), 1L);
            List<StreamEnvelope> captured = new ArrayList<>();
            captured.add(StreamEnvelope.watermark(
                    channel, 1L, watermarks[index]));
            captured.add(StreamEnvelope.watermarkStatus(channel, 2L,
                    StreamEnvelope.WatermarkStatus.IDLE,
                    index == 2 ? 5L : 1L));
            if (index == 0) captured.add(StreamEnvelope.watermarkStatus(
                    channel, 3L,
                    StreamEnvelope.WatermarkStatus.ACTIVE, 2L));
            StreamChannelState state = StreamChannelState.capture(
                    checkpoint(source, "attempt-1", 3L, 1L),
                    channel, 0L, captured.size() + 1L, captured,
                    source.maximumEvents(), source.executionGraph.node(
                    lane.target().operatorUid()).resourceRequest()
                    .checkpointBytes(), 128 * 1024);
            Path staged = root.resolve("old-" + index + ".state");
            Files.write(staged, StreamChannelStateCodec.encode(state));
            FileCheckpointPayloadStore.StoredPayload stored = store.store(staged);
            oldMembers.add(StreamCheckpointMember.channelState(
                    lane.target().operatorUid(), lane.target().subtaskIndex(),
                    lane.physicalChannelId(), 1L,
                    stored.digest(), stored.contentBytes()));
        }
        List<StreamCheckpointMember> migrated =
                StreamLiveChannelStateRescaler.rescale(source,
                        sourcePlacement, target, targetPlacement,
                        checkpoint(target, "attempt-2", 4L, 2L),
                        2L, 2L, oldMembers, store);
        boolean sawMinimum = false;
        long previousWatermark = Long.MIN_VALUE;
        StreamEnvelope.WatermarkStatus finalStatus = null;
        for (StreamCheckpointMember member : migrated) {
            if (!member.subjectId().startsWith(edgeId + '#')) continue;
            Path restored = root.resolve(member.memberId() + ".state");
            store.restore(new FileCheckpointPayloadStore.StoredPayload(
                    member.contentDigest(), member.contentBytes()), restored);
            StreamChannelState state = StreamChannelStateCodec.decode(
                    Files.readAllBytes(restored), target.maximumEvents(),
                    target.executionGraph.node(member.operatorUid())
                    .resourceRequest().checkpointBytes(), 128 * 1024);
            for (StreamEnvelope frame : state.envelopes()) {
                if (frame.kind() != StreamEnvelope.Kind.WATERMARK) continue;
                assertTrue(frame.timestamp() >= previousWatermark,
                        "downscale must preserve a monotone merged frontier");
                if (previousWatermark == Long.MIN_VALUE) {
                    assertEquals(0L, frame.timestamp(),
                            "downscale cannot initially outrun a slow old lane");
                    sawMinimum = true;
                }
                previousWatermark = frame.timestamp();
            }
            for (StreamEnvelope frame : state.envelopes()) {
                if (frame.kind()
                        == StreamEnvelope.Kind.WATERMARK_STATUS) {
                    finalStatus = frame.watermarkStatus();
                }
            }
        }
        assertTrue(sawMinimum);
        assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE, finalStatus,
                "a lower-generation ACTIVE must not leave the merged lane idle");
    }

    @Test
    void timeControlsStayOnMappedSourceLanesAndAfterInflightRecords(
            @TempDir Path root) throws Exception {
        StreamProductExecutionGraph target = StreamProductExecutionGraph.from(
                StreamAtomicCompositionProductTest.session(null)
                        .toPlanArtifact(), 3);
        StreamWorkerPlacement placement = placement(target.executionGraph);
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(
                        target.executionGraph, placement);
        StreamPhysicalExecutionPlan.Lane sourceLane = null;
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (lane.partitioning() == StreamEdge.Partitioning.HASH) {
                sourceLane = lane;
                break;
            }
        }
        if (sourceLane == null) throw new AssertionError("HASH lane missing");
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(
                        StreamEdgePartitioning.outputSchema(
                                target.executionGraph,
                                sourceLane.logicalEdge())));
        DataRow row = new DataRow(1);
        row.set(0, 7L);
        byte[] record = serializer.serialize(row);
        DataRow lateRow = new DataRow(1);
        lateRow.set(0, 8L);
        byte[] lateRecord = serializer.serialize(lateRow);
        CheckpointIdentity sourceCheckpoint = checkpoint(
                target, "attempt-1", 3L, 1L);
        StreamChannelIdentity sourceChannel = new StreamChannelIdentity(
                "run", "attempt-1", 3L, 1L,
                sourceLane.physicalChannelId(), 1L);
        List<StreamEnvelope> envelopes = java.util.Arrays.asList(
                StreamEnvelope.record(sourceChannel, 1L, record),
                StreamEnvelope.watermark(sourceChannel, 2L, 200L),
                StreamEnvelope.record(sourceChannel, 3L, lateRecord),
                StreamEnvelope.watermarkStatus(sourceChannel, 4L,
                        StreamEnvelope.WatermarkStatus.IDLE, 7L),
                StreamEnvelope.asyncCompletion(
                        sourceChannel, 5L, "snapshot-upload-1"));
        StreamChannelState sourceState = StreamChannelState.capture(
                sourceCheckpoint, sourceChannel, 0L, 6L,
                envelopes, target.maximumEvents(),
                target.executionGraph.node(
                        sourceLane.target().operatorUid())
                        .resourceRequest().checkpointBytes(),
                128 * 1024);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 256 * 1024L);
        Path staged = root.resolve("source-controls.state");
        Files.write(staged, StreamChannelStateCodec.encode(sourceState));
        FileCheckpointPayloadStore.StoredPayload stored = store.store(staged);
        StreamCheckpointMember member = StreamCheckpointMember.channelState(
                sourceLane.target().operatorUid(),
                sourceLane.target().subtaskIndex(),
                sourceLane.physicalChannelId(), 1L,
                stored.digest(), stored.contentBytes());

        List<StreamCheckpointMember> migrated =
                StreamLiveChannelStateRescaler.rescale(
                        target, placement,
                        checkpoint(target, "attempt-2", 4L, 2L),
                        2L, 2L, Collections.singletonList(member), store);

        int edgeLanes = 0;
        int records = 0;
        for (StreamCheckpointMember migratedMember : migrated) {
            if (!migratedMember.subjectId().startsWith(
                    sourceLane.logicalEdge().logicalChannelId() + '#')) {
                continue;
            }
            edgeLanes++;
            Path restored = root.resolve(
                    "control-target-" + edgeLanes + ".state");
            store.restore(new FileCheckpointPayloadStore.StoredPayload(
                    migratedMember.contentDigest(),
                    migratedMember.contentBytes()), restored);
            StreamExecutionGraph.Node node = target.executionGraph.node(
                    migratedMember.operatorUid());
            StreamChannelState state = StreamChannelStateCodec.decode(
                    Files.readAllBytes(restored), target.maximumEvents(),
                    node.resourceRequest().checkpointBytes(),
                    128 * 1024);
            List<StreamEnvelope> replay = state.envelopes();
            String mappedSource = sourceLane.logicalEdge()
                    .logicalChannelId() + '#' + sourceLane.source() + "->";
            if (!migratedMember.subjectId().startsWith(mappedSource)) {
                assertTrue(replay.isEmpty());
                continue;
            }
            assertTrue(replay.size() >= 2);
            int watermarkIndex = -1;
            int statusIndex = -1;
            int beforeIndex = -1;
            int afterIndex = -1;
            int completionIndex = -1;
            for (int index = 0; index < replay.size(); index++) {
                StreamEnvelope envelope = replay.get(index);
                if (envelope.kind() == StreamEnvelope.Kind.WATERMARK) {
                    watermarkIndex = index;
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.WATERMARK_STATUS) {
                    statusIndex = index;
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.RECORD) {
                    records++;
                    if (java.util.Arrays.equals(
                            record, envelope.recordFrame())) {
                        beforeIndex = index;
                    } else if (java.util.Arrays.equals(
                            lateRecord, envelope.recordFrame())) {
                        afterIndex = index;
                    }
                } else if (envelope.kind()
                        == StreamEnvelope.Kind.ASYNC_COMPLETION) {
                    completionIndex = index;
                }
            }
            assertTrue(watermarkIndex >= 0);
            assertTrue(statusIndex > watermarkIndex);
            assertTrue(completionIndex > statusIndex);
            if (beforeIndex >= 0) assertTrue(beforeIndex < watermarkIndex);
            if (afterIndex >= 0) {
                assertTrue(afterIndex > watermarkIndex);
                assertTrue(afterIndex < statusIndex);
            }
            assertEquals(200L, replay.get(watermarkIndex).timestamp());
            assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                    replay.get(statusIndex).watermarkStatus());
        }
        assertTrue(edgeLanes > 1);
        assertEquals(2, records);
    }

    @Test
    void hashRecordMovesToOneTargetLaneWithNewGenerationIdentity(
            @TempDir Path root) throws Exception {
        StreamProductExecutionGraph target = StreamProductExecutionGraph.from(
                StreamAtomicCompositionProductTest.session(null)
                        .toPlanArtifact(), 3);
        StreamWorkerPlacement placement = placement(target.executionGraph);
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(
                        target.executionGraph, placement);
        StreamPhysicalExecutionPlan.Lane sourceLane = null;
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (lane.partitioning() == StreamEdge.Partitioning.HASH) {
                sourceLane = lane;
                break;
            }
        }
        if (sourceLane == null) throw new AssertionError("HASH lane missing");
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(
                        StreamEdgePartitioning.outputSchema(
                                target.executionGraph,
                                sourceLane.logicalEdge())));
        DataRow row = new DataRow(1);
        row.set(0, 7L);
        byte[] record = serializer.serialize(row);
        CheckpointIdentity sourceCheckpoint = checkpoint(
                target, "attempt-1", 3L, 1L);
        StreamChannelIdentity sourceChannel = new StreamChannelIdentity(
                "run", "attempt-1", 3L, 1L,
                sourceLane.physicalChannelId(), 1L);
        StreamEnvelope envelope = StreamEnvelope.record(
                sourceChannel, 1L, record);
        StreamChannelState sourceState = StreamChannelState.capture(
                sourceCheckpoint, sourceChannel, 0L, 2L,
                Collections.singletonList(envelope), target.maximumEvents(),
                target.executionGraph.node(
                        sourceLane.target().operatorUid())
                        .resourceRequest().checkpointBytes(),
                128 * 1024);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 256 * 1024L);
        Path staged = root.resolve("source-channel.state");
        Files.write(staged, StreamChannelStateCodec.encode(sourceState));
        FileCheckpointPayloadStore.StoredPayload stored = store.store(staged);
        StreamCheckpointMember member = StreamCheckpointMember.channelState(
                sourceLane.target().operatorUid(),
                sourceLane.target().subtaskIndex(),
                sourceLane.physicalChannelId(), 1L,
                stored.digest(), stored.contentBytes());
        CheckpointIdentity targetCheckpoint = checkpoint(
                target, "attempt-2", 4L, 2L);

        List<StreamCheckpointMember> migrated =
                StreamLiveChannelStateRescaler.rescale(
                        target, placement, targetCheckpoint,
                        2L, 2L, Collections.singletonList(member), store);

        int records = 0;
        for (StreamCheckpointMember migratedMember : migrated) {
            Path restored = root.resolve(
                    "target-" + migratedMember.memberId() + ".state");
            store.restore(new FileCheckpointPayloadStore.StoredPayload(
                    migratedMember.contentDigest(),
                    migratedMember.contentBytes()), restored);
            StreamExecutionGraph.Node node = target.executionGraph.node(
                    migratedMember.operatorUid());
            StreamChannelState state = StreamChannelStateCodec.decode(
                    Files.readAllBytes(restored), target.maximumEvents(),
                    node.resourceRequest().checkpointBytes(),
                    (int) Math.min(Integer.MAX_VALUE, Math.min(
                            node.resourceRequest().checkpointBytes(),
                            (long) node.resourceRequest().maximumFrameBytes()
                                    + 64L * 1024L)));
            assertEquals("attempt-2",
                    state.channelIdentity().attemptId());
            assertEquals(2L,
                    state.channelIdentity().assignmentGeneration());
            assertEquals(2L, state.channelIdentity().channelEpoch());
            if (!state.envelopes().isEmpty()) {
                records += state.envelopes().size();
                assertArrayEquals(record,
                        state.envelopes().get(0).recordFrame());
                assertEquals(1L, state.envelopes().get(0).sequence());
            }
        }
        assertEquals(1, records);
        assertEquals(physical.lanes().size(), migrated.size());
    }

    @Test
    void forwardRescaleUsesTheOnlyPointwiseTargetLane(
            @TempDir Path root) throws Exception {
        StreamProductExecutionGraph target = StreamProductExecutionGraph.from(
                StreamAtomicCompositionProductTest.session(null)
                        .toPlanArtifact(), 2);
        StreamWorkerPlacement placement = placement(target.executionGraph);
        StreamPhysicalExecutionPlan.Lane pointwise = null;
        for (StreamPhysicalExecutionPlan.Lane lane
                : StreamPhysicalExecutionPlan.from(
                target.executionGraph, placement).lanes()) {
            if (lane.partitioning() == StreamEdge.Partitioning.FORWARD
                    && lane.source().subtaskIndex() == 1
                    && lane.target().subtaskIndex() == 1) {
                pointwise = lane;
                break;
            }
        }
        assertTrue(pointwise != null);
        String oldChannelId = pointwise.logicalEdge().logicalChannelId()
                + '#' + pointwise.source() + "->"
                + pointwise.target().operatorUid() + "#0";
        StreamChannelIdentity oldIdentity = new StreamChannelIdentity(
                "run", "attempt-1", 3L, 1L, oldChannelId, 1L);
        byte[] record = null;
        for (int value = 0; value < 256; value++) {
            byte[] candidate = new byte[] {(byte) value};
            int hash = 31 * oldChannelId.hashCode()
                    + java.util.Arrays.hashCode(
                    io.rowweave.stream.contract.StreamEnvelopeCodec.encode(
                    StreamEnvelope.record(oldIdentity, 1L, candidate)));
            if (Math.floorMod(hash, 2) == 0) {
                record = candidate;
                break;
            }
        }
        assertTrue(record != null);
        StreamChannelState oldState = StreamChannelState.capture(
                checkpoint(target, "attempt-1", 3L, 1L), oldIdentity,
                0L, 2L, Collections.singletonList(
                StreamEnvelope.record(oldIdentity, 1L, record)),
                target.maximumEvents(), target.executionGraph.node(
                pointwise.target().operatorUid()).resourceRequest()
                .checkpointBytes(), 128 * 1024);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 256 * 1024L);
        Path staged = root.resolve("old-forward.state");
        Files.write(staged, StreamChannelStateCodec.encode(oldState));
        FileCheckpointPayloadStore.StoredPayload payload = store.store(staged);
        StreamCheckpointMember member = StreamCheckpointMember.channelState(
                pointwise.target().operatorUid(), 0, oldChannelId, 1L,
                payload.digest(), payload.contentBytes());

        List<StreamCheckpointMember> migrated =
                StreamLiveChannelStateRescaler.rescale(target, placement,
                        checkpoint(target, "attempt-2", 4L, 2L),
                        2L, 2L, Collections.singletonList(member), store);
        int seen = 0;
        for (StreamCheckpointMember next : migrated) {
            if (!next.subjectId().startsWith(
                    pointwise.logicalEdge().logicalChannelId() + '#')) continue;
            Path restored = root.resolve("new-forward-" + seen + ".state");
            store.restore(new FileCheckpointPayloadStore.StoredPayload(
                    next.contentDigest(), next.contentBytes()), restored);
            StreamChannelState state = StreamChannelStateCodec.decode(
                    Files.readAllBytes(restored), target.maximumEvents(),
                    target.executionGraph.node(next.operatorUid())
                    .resourceRequest().checkpointBytes(), 128 * 1024);
            seen += state.envelopes().size();
            if (!state.envelopes().isEmpty()) {
                assertEquals(pointwise.physicalChannelId(), next.subjectId());
            }
        }
        assertEquals(1, seen);
    }

    private static StreamWorkerPlacement placement(
            StreamExecutionGraph graph) {
        List<StreamWorkerPlacement.Assignment> assignments =
                new ArrayList<>();
        int ordinal = 0;
        for (StreamExecutionGraph.Node node : graph.nodes()) {
            for (int subtask = 0; subtask < node.parallelism(); subtask++) {
                assignments.add(new StreamWorkerPlacement.Assignment(
                        ordinal++, node.portableNode().operatorUid(), subtask,
                        "worker-" + ordinal, "incarnation-1"));
            }
        }
        return new StreamWorkerPlacement(assignments);
    }

    private static CheckpointIdentity checkpoint(
            StreamProductExecutionGraph graph, String attempt,
            long fence, long sequence) {
        return new CheckpointIdentity(
                graph.artifact.definitionId(), graph.artifact.artifactId(),
                "run", attempt, fence, 1L, "coordinator", sequence,
                graph.commandDigest, graph.artifact.artifactId(), 2, 1);
    }

    private static final class EmptyInput implements StreamChannelInput {
        private final StreamChannelIdentity identity;
        private final ArrayDeque<StreamEnvelope> pending =
                new ArrayDeque<>();

        private EmptyInput(StreamChannelIdentity identity) {
            this.identity = identity;
        }

        @Override public StreamChannelIdentity channelIdentity() {
            return identity;
        }

        private void add(StreamEnvelope envelope) {
            pending.addLast(envelope);
        }

        @Override public StreamEnvelope receive() { return pending.pollFirst(); }

        @Override public StreamEnvelope poll(long timeoutMillis) {
            return pending.pollFirst();
        }

        @Override public void cancel() { }

        @Override public void fail(Throwable cause) { }
    }
}
