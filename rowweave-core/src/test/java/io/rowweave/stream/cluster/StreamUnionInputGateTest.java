package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** UNION is a bounded multi-input control/data gate. */
class StreamUnionInputGateTest {

    @Test
    void remoteUnionSnapshotsEndAlignedWatermarkBeforeBarrier(
            @TempDir Path root) throws Exception {
        StreamSession session = RowWeave.createStreamSession("union-end-cut");
        DataStream leftStream = session.fromSource(source("left"))
                .uid("left");
        DataStream rightStream = session.fromSource(source("right"))
                .uid("right");
        leftStream.union(rightStream).uid("union")
                .sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(session.toPlanArtifact());
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 4_096L);
        LocalStreamChannel left = channel("end-cut-left");
        LocalStreamChannel right = channel("end-cut-right");
        LocalStreamChannel output = channel("end-cut-output");
        StreamUnionOperatorTask initial = new StreamUnionOperatorTask(
                plan.onlyNode(StreamOperatorCode.UNION),
                Arrays.asList(left, right), output, store,
                Collections.emptyList(), 0, 0, 0);
        CheckpointIdentity cut = checkpoint(plan);
        initial.start();
        List<StreamCheckpointMember> members;
        try {
            right.send(StreamEnvelope.watermark(
                    right.channelIdentity(), 1L, 0L));
            left.send(StreamEnvelope.watermark(
                    left.channelIdentity(), 1L, 100L));
            assertEquals(0L, output.poll(2_000L).timestamp());
            CompletableFuture<List<StreamCheckpointMember>> pending =
                    initial.checkpoint(cut);
            left.send(StreamEnvelope.checkpointBarrier(
                    left.channelIdentity(), 2L, cut));
            right.send(StreamEnvelope.endOfInput(
                    right.channelIdentity(), 2L));
            StreamEnvelope advanced = output.poll(2_000L);
            assertEquals(StreamEnvelope.Kind.WATERMARK, advanced.kind());
            assertEquals(100L, advanced.timestamp());
            assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                    output.poll(2_000L).kind());
            members = pending.get(2L, TimeUnit.SECONDS);
            assertEquals(1, members.size());
        } finally {
            initial.cancel();
            initial.awaitCompletion(2_000L);
        }

        LocalStreamChannel restoredLeft = channel("end-restore-left");
        LocalStreamChannel restoredRight = channel("end-restore-right");
        LocalStreamChannel restoredOutput = channel("end-restore-output");
        StreamUnionOperatorTask restored = new StreamUnionOperatorTask(
                plan.onlyNode(StreamOperatorCode.UNION),
                Arrays.asList(restoredLeft, restoredRight), restoredOutput,
                store, members, 0, 0, 0);
        restored.start();
        try {
            restoredLeft.send(StreamEnvelope.watermark(
                    restoredLeft.channelIdentity(), 1L, 100L));
            assertNull(restoredOutput.poll(0L));
            restoredLeft.send(StreamEnvelope.watermark(
                    restoredLeft.channelIdentity(), 2L, 101L));
            assertEquals(101L, restoredOutput.poll(2_000L).timestamp());
            restoredLeft.send(StreamEnvelope.endOfInput(
                    restoredLeft.channelIdentity(), 3L));
            assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                    restoredOutput.poll(2_000L).kind());
        } finally {
            restored.cancel();
            restored.awaitCompletion(2_000L);
        }
    }

    @Test
    void remoteUnionRestoreRejectsLegacyCutWithoutGateState(
            @TempDir Path root) {
        StreamSession session = RowWeave.createStreamSession("union-legacy-cut");
        DataStream leftStream = session.fromSource(source("left"))
                .uid("left");
        DataStream rightStream = session.fromSource(source("right"))
                .uid("right");
        leftStream.union(rightStream).uid("union")
                .sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(session.toPlanArtifact());
        List<io.rowweave.stream.contract.StreamEdge> edges =
                plan.executionGraph.node("union").inputEdges();
        LocalStreamChannel left = channel("legacy-left");
        LocalStreamChannel right = channel("legacy-right");
        LocalStreamChannel output = channel("legacy-out");
        AtomicReference<StreamOperatorTask> opened = new AtomicReference<>();
        try {
            assertThrows(IllegalArgumentException.class, () -> opened.set(
                    StreamRemoteOperatorTaskRunner.start(
                            plan.executionGraph,
                            plan.artifact.artifactId(),
                            plan.onlyNode(StreamOperatorCode.UNION),
                            Arrays.asList(left, right),
                            Arrays.asList(StreamTaskLaneSpec.input(
                                    physical(edges.get(0), 0),
                                    4_096, 65_536L),
                                    StreamTaskLaneSpec.input(
                                    physical(edges.get(1), 0),
                                    4_096, 65_536L)),
                            Collections.singletonList(
                                    new StreamTaskOutput(null, output)),
                            ignored -> { throw new AssertionError(
                                    "no function"); },
                            0, new FileCheckpointPayloadStore(
                                    root.resolve("payloads"), 4_096L),
                            Collections.emptyList(), true)));
        } finally {
            if (opened.get() != null) opened.get().cancel();
        }
    }

    @Test
    void remoteUnionRecoveryKeepsLogicalGateStableWhenPhysicalFanInChanges(
            @TempDir Path root) throws Exception {
        StreamSession session = RowWeave.createStreamSession("union-lane-rescale");
        DataStream leftStream = session.fromSource(source("left"))
                .uid("left");
        DataStream rightStream = session.fromSource(source("right"))
                .uid("right");
        leftStream.union(rightStream).uid("union")
                .sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(session.toPlanArtifact());
        List<io.rowweave.stream.contract.StreamEdge> edges =
                plan.executionGraph.node("union").inputEdges();
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 4_096L);
        LocalStreamChannel left = channel("old-left");
        LocalStreamChannel right = channel("old-right");
        LocalStreamChannel output = channel("old-out");
        StreamOperatorTask initial = StreamRemoteOperatorTaskRunner.start(
                plan.executionGraph, plan.artifact.artifactId(),
                plan.onlyNode(StreamOperatorCode.UNION),
                Arrays.asList(left, right),
                Arrays.asList(StreamTaskLaneSpec.input(
                        physical(edges.get(0), 0), 4_096,
                        65_536L), StreamTaskLaneSpec.input(
                        physical(edges.get(1), 0), 4_096,
                        65_536L)),
                Collections.singletonList(new StreamTaskOutput(null, output)),
                ignored -> { throw new AssertionError("no function"); },
                0, store, Collections.emptyList(), false);
        CheckpointIdentity cut = checkpoint(plan);
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 1L));
        right.send(StreamEnvelope.watermark(right.channelIdentity(),
                1L, 50L));
        assertEquals(50L, output.poll(2_000L).timestamp());
        CompletableFuture<List<StreamCheckpointMember>> pending =
                initial.checkpoint(cut);
        right.send(StreamEnvelope.checkpointBarrier(
                right.channelIdentity(), 2L, cut));
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                output.poll(2_000L).kind());
        List<StreamCheckpointMember> members = pending.get(
                2L, TimeUnit.SECONDS);
        initial.cancel();
        initial.awaitCompletion(2_000L);

        LocalStreamChannel leftA = channel("new-left-a");
        LocalStreamChannel leftB = channel("new-left-b");
        LocalStreamChannel rightA = channel("new-right-a");
        LocalStreamChannel rightB = channel("new-right-b");
        LocalStreamChannel recoveredOutput = channel("new-out");
        java.util.Map<String, Integer> sourceMinimums =
                new java.util.LinkedHashMap<>();
        sourceMinimums.put("left", 2);
        sourceMinimums.put("right", 2);
        StreamExecutionGraph widerInputs =
                StreamExecutionGraph.forParallelism(plan.product.job, 1,
                        sourceMinimums);
        StreamOperatorTask restored = StreamRemoteOperatorTaskRunner.start(
                widerInputs, plan.artifact.artifactId(),
                plan.onlyNode(StreamOperatorCode.UNION),
                Arrays.asList(leftA, leftB, rightA, rightB),
                Arrays.asList(StreamTaskLaneSpec.input(
                        physical(edges.get(0), 0), 4_096,
                        65_536L), StreamTaskLaneSpec.input(
                        physical(edges.get(0), 1), 4_096,
                        65_536L), StreamTaskLaneSpec.input(
                        physical(edges.get(1), 0), 4_096,
                        65_536L), StreamTaskLaneSpec.input(
                        physical(edges.get(1), 1), 4_096,
                        65_536L)), Collections.singletonList(
                        new StreamTaskOutput(null, recoveredOutput)),
                ignored -> { throw new AssertionError("no function"); },
                0, store, members, true);
        try {
            rightA.send(StreamEnvelope.watermark(
                    rightA.channelIdentity(), 1L, 150L));
            rightB.send(StreamEnvelope.watermark(
                    rightB.channelIdentity(), 1L, 150L));
            StreamEnvelope advanced = recoveredOutput.poll(2_000L);
            assertEquals(StreamEnvelope.Kind.WATERMARK, advanced.kind());
            assertEquals(150L, advanced.timestamp());
        } finally {
            restored.cancel();
            restored.awaitCompletion(2_000L);
        }
    }

    @Test
    void remoteUnionRestoresEndedLaneAndMergedFrontier(
            @TempDir Path root) throws Exception {
        StreamSession session = RowWeave.createStreamSession("remote-union-cut");
        DataStream leftStream = session.fromSource(source("left"))
                .uid("left");
        DataStream rightStream = session.fromSource(source("right"))
                .uid("right");
        leftStream.union(rightStream).uid("union")
                .sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(session.toPlanArtifact());
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 4_096L);
        LocalStreamChannel left = channel("cut-left");
        LocalStreamChannel right = channel("cut-right");
        LocalStreamChannel output = channel("cut-output");
        StreamUnionOperatorTask initial = new StreamUnionOperatorTask(
                plan.onlyNode(StreamOperatorCode.UNION),
                Arrays.asList(left, right), output, store,
                Collections.emptyList(), 0, 0, 0);
        CheckpointIdentity cut = checkpoint(plan);
        initial.start();
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 1L));
        right.send(StreamEnvelope.watermark(right.channelIdentity(),
                1L, 50L));
        assertEquals(50L, output.poll(2_000L).timestamp());
        CompletableFuture<List<StreamCheckpointMember>> pending =
                initial.checkpoint(cut);
        right.send(StreamEnvelope.checkpointBarrier(
                right.channelIdentity(), 2L, cut));
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                output.poll(2_000L).kind());
        List<StreamCheckpointMember> members = pending.get(
                2L, TimeUnit.SECONDS);
        assertEquals(1, members.size());
        initial.cancel();
        initial.awaitCompletion(2_000L);

        LocalStreamChannel recoveredLeft = channel("restore-left");
        LocalStreamChannel recoveredRight = channel("restore-right");
        LocalStreamChannel recoveredOutput = channel("restore-output");
        StreamUnionOperatorTask restored = new StreamUnionOperatorTask(
                plan.onlyNode(StreamOperatorCode.UNION),
                Arrays.asList(recoveredLeft, recoveredRight),
                recoveredOutput, store, members, 0, 0, 0);
        restored.start();
        try {
            recoveredRight.send(StreamEnvelope.watermark(
                    recoveredRight.channelIdentity(), 1L, 150L));
            StreamEnvelope advanced = recoveredOutput.poll(2_000L);
            assertEquals(StreamEnvelope.Kind.WATERMARK, advanced.kind());
            assertEquals(150L, advanced.timestamp(),
                    "the ended lane must not hold back restored time");
            recoveredRight.send(StreamEnvelope.endOfInput(
                    recoveredRight.channelIdentity(), 2L));
            assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                    recoveredOutput.poll(2_000L).kind());
        } finally {
            restored.cancel();
        }
    }

    @Test
    void remoteUnionMergesWatermarksAndIdleStatusBeforeDownstream()
            throws Exception {
        StreamSession session = RowWeave.createStreamSession("remote-union-time");
        DataStream leftStream = session.fromSource(source("left"))
                .uid("left");
        DataStream rightStream = session.fromSource(source("right"))
                .uid("right");
        leftStream.union(rightStream).uid("union")
                .sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(session.toPlanArtifact());
        LocalStreamChannel left = channel("remote-time-left");
        LocalStreamChannel right = channel("remote-time-right");
        LocalStreamChannel output = channel("remote-time-output");
        StreamUnionOperatorTask task = new StreamUnionOperatorTask(
                plan.onlyNode(StreamOperatorCode.UNION),
                Arrays.asList(left, right), output);
        left.send(StreamEnvelope.watermark(left.channelIdentity(), 1L,
                100L));
        right.send(StreamEnvelope.record(right.channelIdentity(), 1L,
                1L, new byte[] {1}));
        right.send(StreamEnvelope.watermark(right.channelIdentity(), 2L,
                50L));
        left.send(StreamEnvelope.watermarkStatus(left.channelIdentity(),
                2L, StreamEnvelope.WatermarkStatus.IDLE, 1L));
        right.send(StreamEnvelope.watermarkStatus(right.channelIdentity(),
                3L, StreamEnvelope.WatermarkStatus.IDLE, 1L));
        task.start();
        try {
            assertEquals(StreamEnvelope.Kind.RECORD,
                    output.poll(2_000L).kind(),
                    "one fast input must not advance the merged frontier");
            StreamEnvelope watermark = output.poll(2_000L);
            assertEquals(StreamEnvelope.Kind.WATERMARK,
                    watermark.kind());
            assertEquals(50L, watermark.timestamp());
            StreamEnvelope idle = output.poll(2_000L);
            assertEquals(StreamEnvelope.Kind.WATERMARK_STATUS, idle.kind());
            assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                    idle.watermarkStatus());
            assertEquals(1L, idle.watermarkStatusGeneration());
            assertNull(output.poll(100L),
                    "two physical IDLE frames produce one logical status");
        } finally {
            task.cancel();
        }
    }

    @Test
    void unionPreservesInputOrderMergesWatermarksAndEndsAfterBothInputs()
            throws Exception {
        StreamSession session = RowWeave.createStreamSession("union-runtime");
        DataStream left = session.fromSource(source("left")).uid("left");
        DataStream right = session.fromSource(source("right")).uid("right");
        left.union(right).uid("union").sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(
                session.toPlanArtifact());
        RecordingOutput output = new RecordingOutput();
        StreamStatelessDagOutput graph = new StreamStatelessDagOutput(
                plan.executionGraph, Collections.singletonMap("sink", output),
                ignored -> { throw new AssertionError("no function expected"); });
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(plan.source.outputSchema()));
        StreamChannelIdentity leftIdentity = identity("left-edge");
        StreamChannelIdentity rightIdentity = identity("right-edge");
        try {
            sendFrom(graph, "left", record(leftIdentity, 1L, serializer, 1L));
            sendFrom(graph, "right", record(rightIdentity, 1L, serializer, 2L));
            sendFrom(graph, "left", StreamEnvelope.watermark(
                    leftIdentity, 2L, 100L));
            sendFrom(graph, "right", StreamEnvelope.watermark(
                    rightIdentity, 2L, 50L));
            sendFrom(graph, "left", StreamEnvelope.watermark(
                    leftIdentity, 3L, 200L));
            sendFrom(graph, "right", StreamEnvelope.watermarkStatus(
                    rightIdentity, 3L,
                    StreamEnvelope.WatermarkStatus.IDLE, 1L));
            sendFrom(graph, "right", StreamEnvelope.watermarkStatus(
                    rightIdentity, 4L,
                    StreamEnvelope.WatermarkStatus.ACTIVE, 2L));
            sendFrom(graph, "right", StreamEnvelope.watermark(
                    rightIdentity, 5L, 250L));
            sendFrom(graph, "left", StreamEnvelope.endOfInput(
                    leftIdentity, 4L));
            sendFrom(graph, "right", StreamEnvelope.endOfInput(
                    rightIdentity, 6L));
            graph.start();
            output.awaitEvents(8, 2_000L);

            assertEquals(Arrays.asList(
                            StreamEnvelope.Kind.RECORD,
                            StreamEnvelope.Kind.RECORD,
                            StreamEnvelope.Kind.WATERMARK,
                            StreamEnvelope.Kind.WATERMARK,
                            StreamEnvelope.Kind.WATERMARK_STATUS,
                            StreamEnvelope.Kind.WATERMARK_STATUS,
                            StreamEnvelope.Kind.WATERMARK,
                            StreamEnvelope.Kind.END_OF_INPUT),
                    kinds(output.events));
            assertEquals(1L, serializer.deserialize(
                    output.events.get(0).recordFrame()).getLong(0));
            assertEquals(2L, serializer.deserialize(
                    output.events.get(1).recordFrame()).getLong(0));
            assertEquals(50L, output.events.get(2).timestamp());
            assertEquals(200L, output.events.get(3).timestamp());
            assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                    output.events.get(4).watermarkStatus());
            assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                    output.events.get(5).watermarkStatus());
            assertEquals(250L, output.events.get(6).timestamp());
        } finally {
            graph.close();
        }
    }

    @Test
    void activeInputsArePolledRoundRobinWithoutChangingPerInputOrder()
            throws Exception {
        Fixture fixture = fixture(null, Collections.emptyList());
        RowSerializer serializer = fixture.serializer;
        sendFrom(fixture.graph, "left", record(
                fixture.left, 1L, serializer, 1L));
        sendFrom(fixture.graph, "left", record(
                fixture.left, 2L, serializer, 2L));
        sendFrom(fixture.graph, "left", record(
                fixture.left, 3L, serializer, 3L));
        sendFrom(fixture.graph, "right", record(
                fixture.right, 1L, serializer, 10L));
        sendFrom(fixture.graph, "right", record(
                fixture.right, 2L, serializer, 20L));

        fixture.graph.start();
        try {
            fixture.output.awaitEvents(5, 2_000L);
            assertEquals(Arrays.asList(1L, 10L, 2L, 20L, 3L),
                    values(fixture.output.events, serializer));
        } finally {
            fixture.graph.close();
        }
    }

    @Test
    void equalRecordsFromBothInputsAreBothEmitted() throws Exception {
        Fixture fixture = fixture(null, Collections.emptyList());
        sendFrom(fixture.graph, "left", record(
                fixture.left, 1L, fixture.serializer, 7L));
        sendFrom(fixture.graph, "right", record(
                fixture.right, 1L, fixture.serializer, 7L));
        fixture.graph.start();
        try {
            fixture.output.awaitEvents(2, 2_000L);
            assertEquals(Arrays.asList(7L, 7L), values(
                    fixture.output.events, fixture.serializer));
        } finally {
            fixture.graph.close();
        }
    }

    @Test
    void unioningOneStreamWithItselfEmitsEachRecordTwice()
            throws Exception {
        StreamSession session = RowWeave.createStreamSession("self-union");
        DataStream source = session.fromSource(source("shared")).uid("shared");
        source.union(source).uid("union").sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(
                session.toPlanArtifact());
        RecordingOutput output = new RecordingOutput();
        StreamStatelessDagOutput graph = new StreamStatelessDagOutput(
                plan.executionGraph, Collections.singletonMap("sink", output),
                ignored -> { throw new AssertionError("no function expected"); });
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(plan.source.outputSchema()));
        graph.start();
        try {
            sendFrom(graph, "shared", record(
                    output.identity, 1L, serializer, 9L));
            output.awaitEvents(2, 2_000L);
            assertEquals(Arrays.asList(9L, 9L),
                    values(output.events, serializer));
        } finally {
            graph.close();
        }
    }

    @Test
    void perInputMailboxIsBoundedAndBackpressuresItsProducer()
            throws Exception {
        Fixture fixture = fixture(null, Collections.emptyList(), 1);
        sendFrom(fixture.graph, "left", record(
                fixture.left, 1L, fixture.serializer, 1L));
        AtomicReference<Throwable> producerFailure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                sendFrom(fixture.graph, "left", record(
                        fixture.left, 2L, fixture.serializer, 2L));
            } catch (Throwable failure) {
                producerFailure.set(failure);
            }
        }, "union-backpressure-producer");
        producer.start();
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(1L);
        while (producer.getState() != Thread.State.WAITING
                && System.nanoTime() < deadline) Thread.yield();
        assertTrue(producer.isAlive(),
                "second record must wait for bounded mailbox credit");

        fixture.graph.start();
        try {
            producer.join(2_000L);
            assertFalse(producer.isAlive());
            assertNull(producerFailure.get());
            fixture.output.awaitEvents(2, 2_000L);
            assertEquals(Arrays.asList(1L, 2L), values(
                    fixture.output.events, fixture.serializer));
        } finally {
            fixture.graph.close();
        }
    }

    @Test
    void alignedBarrierBlocksPostBarrierDataAndCheckpointsGateState(
            @TempDir Path root) throws Exception {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 4_096L);
        Fixture fixture = fixture(store, Collections.emptyList());
        CheckpointIdentity checkpoint = checkpoint(fixture.plan);
        sendFrom(fixture.graph, "left", StreamEnvelope.checkpointBarrier(
                fixture.left, 1L, checkpoint));
        sendFrom(fixture.graph, "left", record(
                fixture.left, 2L, fixture.serializer, 2L));
        sendFrom(fixture.graph, "right", record(
                fixture.right, 1L, fixture.serializer, 10L));
        sendFrom(fixture.graph, "right", StreamEnvelope.checkpointBarrier(
                fixture.right, 2L, checkpoint));

        fixture.graph.start();
        try {
            fixture.output.awaitEvents(3, 2_000L);
            assertEquals(Arrays.asList(
                            StreamEnvelope.Kind.RECORD,
                            StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                            StreamEnvelope.Kind.RECORD),
                    kinds(fixture.output.events));
            assertEquals(Arrays.asList(10L, 2L), values(
                    fixture.output.events, fixture.serializer));
            List<StreamCheckpointMember> members =
                    fixture.graph.checkpointMembers(checkpoint);
            assertEquals(1, members.size());
            assertEquals(StreamCheckpointMember.Kind.STATE,
                    members.get(0).kind());
            assertEquals("union", members.get(0).operatorUid());
        } finally {
            fixture.graph.close();
        }
    }

    @Test
    void endedInputLeavesFutureBarrierAlignment(@TempDir Path root)
            throws Exception {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 4_096L);
        Fixture fixture = fixture(store, Collections.emptyList());
        CheckpointIdentity checkpoint = checkpoint(fixture.plan);
        fixture.graph.start();
        try {
            sendFrom(fixture.graph, "left", StreamEnvelope.endOfInput(
                    fixture.left, 1L));
            sendFrom(fixture.graph, "right", StreamEnvelope.checkpointBarrier(
                    fixture.right, 1L, checkpoint));
            fixture.output.awaitKind(
                    StreamEnvelope.Kind.CHECKPOINT_BARRIER, 2_000L);
            sendFrom(fixture.graph, "right", StreamEnvelope.endOfInput(
                    fixture.right, 2L));
            fixture.output.awaitKind(
                    StreamEnvelope.Kind.END_OF_INPUT, 2_000L);
        } finally {
            fixture.graph.close();
        }
    }

    @Test
    void remoteUnionExcludesEndedLaneFromBarrierAndPropagatesFinalEnd()
            throws Exception {
        StreamSession session = RowWeave.createStreamSession("remote-union-end");
        DataStream leftStream = session.fromSource(source("left"))
                .uid("left");
        DataStream rightStream = session.fromSource(source("right"))
                .uid("right");
        leftStream.union(rightStream).uid("union")
                .sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(session.toPlanArtifact());
        LocalStreamChannel left = channel("remote-left");
        LocalStreamChannel right = channel("remote-right");
        LocalStreamChannel output = channel("remote-output");
        StreamUnionOperatorTask task = new StreamUnionOperatorTask(
                plan.onlyNode(StreamOperatorCode.UNION),
                Arrays.asList(left, right), output);
        CheckpointIdentity checkpoint = checkpoint(plan);

        CompletableFuture<List<StreamCheckpointMember>> cut =
                task.checkpoint(checkpoint);
        assertFalse(cut.isDone(),
                "UNION checkpoint must wait for aligned input barriers");
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 1L));
        right.send(StreamEnvelope.checkpointBarrier(
                right.channelIdentity(), 1L, checkpoint));
        right.send(StreamEnvelope.endOfInput(right.channelIdentity(), 2L));
        task.start();
        try {
            StreamEnvelope barrier = output.poll(2_000L);
            assertNotNull(barrier,
                    "ended lanes must not block later checkpoint alignment");
            assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                    barrier.kind());
            assertEquals(checkpoint, barrier.checkpointIdentity());
            assertTrue(cut.get(2L, TimeUnit.SECONDS).isEmpty());
            StreamEnvelope end = output.poll(2_000L);
            assertNotNull(end,
                    "UNION must propagate logical end before closing transport");
            assertEquals(StreamEnvelope.Kind.END_OF_INPUT, end.kind());
        } finally {
            task.cancel();
        }
    }

    @Test
    void restorePreservesPerInputWatermarkAndStatusGeneration(
            @TempDir Path root) throws Exception {
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 4_096L);
        Fixture before = fixture(store, Collections.emptyList());
        CheckpointIdentity checkpoint = checkpoint(before.plan);
        before.graph.start();
        List<StreamCheckpointMember> members;
        try {
            sendFrom(before.graph, "left", StreamEnvelope.watermark(
                    before.left, 1L, 100L));
            sendFrom(before.graph, "right", StreamEnvelope.watermark(
                    before.right, 1L, 50L));
            sendFrom(before.graph, "left", StreamEnvelope.watermarkStatus(
                    before.left, 2L,
                    StreamEnvelope.WatermarkStatus.IDLE, 1L));
            sendFrom(before.graph, "right", StreamEnvelope.watermarkStatus(
                    before.right, 2L,
                    StreamEnvelope.WatermarkStatus.IDLE, 1L));
            sendFrom(before.graph, "left", StreamEnvelope.checkpointBarrier(
                    before.left, 3L, checkpoint));
            sendFrom(before.graph, "right", StreamEnvelope.checkpointBarrier(
                    before.right, 3L, checkpoint));
            before.output.awaitKind(
                    StreamEnvelope.Kind.CHECKPOINT_BARRIER, 2_000L);
            members = before.graph.checkpointMembers(checkpoint);
        } finally {
            before.graph.close();
        }

        Fixture after = fixture(store, members);
        after.graph.start();
        try {
            sendFrom(after.graph, "right", StreamEnvelope.watermarkStatus(
                    after.right, 1L,
                    StreamEnvelope.WatermarkStatus.ACTIVE, 2L));
            after.output.awaitKind(
                    StreamEnvelope.Kind.WATERMARK_STATUS, 2_000L);
            sendFrom(after.graph, "right", StreamEnvelope.watermark(
                    after.right, 2L, 150L));
            after.output.awaitKind(StreamEnvelope.Kind.WATERMARK, 2_000L);
            assertEquals(2, after.output.events.size());
            assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                    after.output.events.get(0).watermarkStatus());
            assertEquals(150L, after.output.events.get(1).timestamp());
        } finally {
            after.graph.close();
        }
    }

    @Test
    void mismatchedAndReplayedBarriersPoisonTheGate(@TempDir Path root)
            throws Exception {
        FileCheckpointPayloadStore mismatchStore =
                new FileCheckpointPayloadStore(
                        root.resolve("mismatch"), 4_096L);
        Fixture mismatch = fixture(mismatchStore, Collections.emptyList());
        mismatch.graph.start();
        try {
            sendFrom(mismatch.graph, "left", StreamEnvelope.checkpointBarrier(
                    mismatch.left, 1L, checkpoint(mismatch.plan, 1L)));
            sendFrom(mismatch.graph, "right", StreamEnvelope.checkpointBarrier(
                    mismatch.right, 1L, checkpoint(mismatch.plan, 2L)));
            mismatch.output.awaitFailure(2_000L);
            assertTrue(mismatch.output.failure.getMessage().contains(
                    "barriers differ"));
        } finally {
            mismatch.graph.cancel();
        }

        FileCheckpointPayloadStore replayStore =
                new FileCheckpointPayloadStore(
                        root.resolve("replay"), 4_096L);
        Fixture replay = fixture(replayStore, Collections.emptyList());
        CheckpointIdentity checkpoint = checkpoint(replay.plan, 1L);
        replay.graph.start();
        try {
            sendFrom(replay.graph, "left", StreamEnvelope.checkpointBarrier(
                    replay.left, 1L, checkpoint));
            sendFrom(replay.graph, "right", StreamEnvelope.checkpointBarrier(
                    replay.right, 1L, checkpoint));
            replay.output.awaitKind(
                    StreamEnvelope.Kind.CHECKPOINT_BARRIER, 2_000L);
            sendFrom(replay.graph, "left", StreamEnvelope.checkpointBarrier(
                    replay.left, 2L, checkpoint));
            replay.output.awaitFailure(2_000L);
            assertTrue(replay.output.failure.getMessage().contains(
                    "replayed"));
        } finally {
            replay.graph.cancel();
        }
    }

    @Test
    void idleInputDataAndInputErrorPoisonTheAttemptFirstCause()
            throws Exception {
        Fixture idle = fixture(null, Collections.emptyList());
        idle.graph.start();
        try {
            sendFrom(idle.graph, "left", StreamEnvelope.watermarkStatus(
                    idle.left, 1L, StreamEnvelope.WatermarkStatus.IDLE, 1L));
            sendFrom(idle.graph, "left", record(
                    idle.left, 2L, idle.serializer, 1L));
            idle.output.awaitFailure(2_000L);
            assertTrue(idle.output.failure.getMessage().contains("IDLE"));
        } finally {
            idle.graph.cancel();
        }

        Fixture failed = fixture(null, Collections.emptyList());
        failed.graph.start();
        try {
            sendFrom(failed.graph, "right", StreamEnvelope.error(
                    failed.right, 1L, "RIGHT_FAILED"));
            failed.output.awaitFailure(2_000L);
            assertTrue(failed.output.failure.getMessage().contains(
                    "RIGHT_FAILED"));
            assertThrows(IllegalStateException.class, () -> sendFrom(
                    failed.graph, "left", StreamEnvelope.error(
                            failed.left, 1L, "LEFT_FAILED")));
        } finally {
            failed.graph.cancel();
        }
    }

    private static Fixture fixture(FileCheckpointPayloadStore store,
            List<StreamCheckpointMember> restoreMembers) {
        return fixture(store, restoreMembers, 4_096);
    }

    private static Fixture fixture(FileCheckpointPayloadStore store,
            List<StreamCheckpointMember> restoreMembers, int mailboxEvents) {
        StreamSession session = RowWeave.createStreamSession("union-runtime");
        session.setResourceProfile(new StreamResourceProfile(
                mailboxEvents, 1_024, 4_096, 0L, 0, 0,
                store == null ? 0L : 4_096L, 5, 16, 0L));
        DataStream left = session.fromSource(source("left")).uid("left");
        DataStream right = session.fromSource(source("right")).uid("right");
        left.union(right).uid("union").sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(
                session.toPlanArtifact());
        RecordingOutput output = new RecordingOutput();
        StreamStatelessDagOutput graph = new StreamStatelessDagOutput(
                plan.executionGraph, Collections.singletonMap("sink", output),
                ignored -> { throw new AssertionError("no function expected"); },
                store, restoreMembers);
        return new Fixture(plan, graph, output,
                new RowSerializer(PortablePlanCompiler.toDataSchema(
                        plan.source.outputSchema())));
    }

    private static void sendFrom(StreamStatelessDagOutput graph,
            String sourceUid, StreamEnvelope envelope) throws Exception {
        Method method = StreamStatelessDagOutput.class.getDeclaredMethod(
                "sendFrom", String.class, StreamEnvelope.class);
        method.setAccessible(true);
        try {
            method.invoke(graph, sourceUid, envelope);
        } catch (InvocationTargetException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof Exception) throw (Exception) cause;
            if (cause instanceof Error) throw (Error) cause;
            throw failure;
        }
    }

    private static List<StreamEnvelope.Kind> kinds(
            List<StreamEnvelope> events) {
        List<StreamEnvelope.Kind> result = new ArrayList<>();
        for (StreamEnvelope event : events) result.add(event.kind());
        return result;
    }

    private static List<Long> values(List<StreamEnvelope> events,
            RowSerializer serializer) {
        List<Long> result = new ArrayList<>();
        for (StreamEnvelope event : events) {
            if (event.kind() == StreamEnvelope.Kind.RECORD) {
                result.add(serializer.deserialize(
                        event.recordFrame()).getLong(0));
            }
        }
        return result;
    }

    private static CheckpointIdentity checkpoint(
            StreamTestGraph plan) {
        return checkpoint(plan, 1L);
    }

    private static CheckpointIdentity checkpoint(
            StreamTestGraph plan, long sequence) {
        return new CheckpointIdentity(
                plan.artifact.definitionId(), plan.artifact.artifactId(),
                "run", "attempt", 1L, 1L, "coordinator", sequence,
                plan.commandDigest, plan.artifact.artifactId(), 1, 1);
    }

    private static StreamEnvelope record(StreamChannelIdentity identity,
            long sequence, RowSerializer serializer, long value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return StreamEnvelope.record(identity, sequence,
                serializer.serialize(row));
    }

    private static StreamChannelIdentity identity(String channel) {
        return new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, channel, 1L);
    }

    private static LocalStreamChannel channel(String channel) {
        return new LocalStreamChannel(identity(channel), 16, 4_096,
                65_536L);
    }

    private static String physical(
            io.rowweave.stream.contract.StreamEdge edge,
            int sourceSubtask) {
        return edge.logicalChannelId() + '#'
                + edge.sourceOperatorUid() + '#' + sourceSubtask
                + "->" + edge.targetOperatorUid() + "#0";
    }

    private static StreamSourceSpec source(String name) {
        return StreamSourceSpec.exactOrdered(schema(), "source-" + name,
                digest("source-" + name), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split-" + name), "sequence-v1",
                        new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink", digest("sink"), new byte[] {3});
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(new StreamField(
                "id", "id", "id", DataType.LongType, false)));
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) {
                result.append(String.format("%02x", one & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static final class RecordingOutput implements StreamChannelOutput {
        final StreamChannelIdentity identity = identity("sink-edge");
        final List<StreamEnvelope> events = new ArrayList<>();
        Throwable failure;
        @Override public StreamChannelIdentity channelIdentity() {
            return identity;
        }
        @Override public synchronized void send(StreamEnvelope envelope) {
            events.add(envelope);
            notifyAll();
        }
        @Override public void close() { }
        @Override public void cancel() { }
        @Override public synchronized void fail(Throwable cause) {
            if (failure == null) failure = cause;
            notifyAll();
        }

        synchronized void awaitEvents(int count, long timeoutMillis)
                throws InterruptedException {
            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    timeoutMillis);
            while (events.size() < count && failure == null
                    && System.nanoTime() < deadline) wait(10L);
            assertEquals(count, events.size(),
                    "timed out waiting for UNION output events");
        }

        synchronized void awaitKind(StreamEnvelope.Kind kind,
                long timeoutMillis) throws InterruptedException {
            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    timeoutMillis);
            while (!contains(kind) && failure == null
                    && System.nanoTime() < deadline) wait(10L);
            assertTrue(contains(kind), "timed out waiting for " + kind);
        }

        synchronized void awaitFailure(long timeoutMillis)
                throws InterruptedException {
            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    timeoutMillis);
            while (failure == null && System.nanoTime() < deadline) wait(10L);
            assertNotNull(failure, "timed out waiting for UNION failure");
        }

        private boolean contains(StreamEnvelope.Kind kind) {
            for (StreamEnvelope event : events) {
                if (event.kind() == kind) return true;
            }
            return false;
        }
    }

    private static final class Fixture {
        final StreamTestGraph plan;
        final StreamStatelessDagOutput graph;
        final RecordingOutput output;
        final RowSerializer serializer;
        final StreamChannelIdentity left = identity("left-edge");
        final StreamChannelIdentity right = identity("right-edge");

        Fixture(StreamTestGraph plan,
                StreamStatelessDagOutput graph, RecordingOutput output,
                RowSerializer serializer) {
            this.plan = plan;
            this.graph = graph;
            this.output = output;
            this.serializer = serializer;
        }
    }
}
