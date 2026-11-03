package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.api.StreamEventTimeSpec;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** assigned event time is executed inside the generic graph. */
class StreamAtomicEventTimeTaskTest {

    @Test
    void assignmentStampsRecordsAndEmitsOnlyAdvancingBoundedWatermarks() {
        StreamSession session = RowWeave.createStreamSession("assigned-runtime");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.fromSource(source()).uid("source")
                .assignEventTime(StreamEventTimeSpec.boundedOutOfOrderness(
                        StreamExpression.input("event-time",
                                PortableDataType.TIMESTAMP, false),
                        100L, 30_000L,
                        StreamEventTimeSpec.InvalidTimestampPolicy.FAIL))
                .uid("assign-time").sinkTo("sink", sink());

        StreamTestGraph plan = StreamTestGraph.from(
                session.toPlanArtifact());
        RecordingOutput output = new RecordingOutput();
        StreamStatelessDagOutput graph = new StreamStatelessDagOutput(
                plan.executionGraph, Collections.singletonMap("sink", output),
                ignored -> { throw new AssertionError("no function expected"); });
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(plan.source.outputSchema()));

        graph.send(record(output.identity, 1L, serializer, 1_000L));
        graph.send(record(output.identity, 2L, serializer, 900L));

        assertEquals(3, output.events.size());
        assertEquals(StreamEnvelope.Kind.RECORD, output.events.get(0).kind());
        assertTrue(output.events.get(0).hasEventTimestamp());
        assertEquals(1_000L, output.events.get(0).timestamp());
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.events.get(1).kind());
        assertEquals(900L, output.events.get(1).timestamp());
        assertEquals(StreamEnvelope.Kind.RECORD, output.events.get(2).kind());
        assertEquals(900L, output.events.get(2).timestamp());
    }

    @Test
    void invalidNullableTimestampCanBeDroppedWithoutAdvancingWatermark() {
        StreamSession session = RowWeave.createStreamSession("drop-time");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        StreamSchema nullable = schema(true);
        session.fromSource(source(nullable)).uid("source")
                .assignEventTime(StreamEventTimeSpec.monotonic(
                        StreamExpression.input("event-time",
                                PortableDataType.TIMESTAMP, true),
                        30_000L,
                        StreamEventTimeSpec.InvalidTimestampPolicy.DROP))
                .uid("assign-time").sinkTo("sink", sink());
        StreamTestGraph plan = StreamTestGraph.from(
                session.toPlanArtifact());
        RecordingOutput output = new RecordingOutput();
        StreamStatelessDagOutput graph = new StreamStatelessDagOutput(
                plan.executionGraph, Collections.singletonMap("sink", output),
                ignored -> { throw new AssertionError("no function expected"); });
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(plan.source.outputSchema()));
        DataRow row = new DataRow(1);

        graph.send(StreamEnvelope.record(output.identity, 1L,
                serializer.serialize(row)));

        assertTrue(output.events.isEmpty());
        assertFalse(output.closed);
    }

    @Test
    void assignmentOwnsIdleActiveTransitions(@TempDir Path root)
            throws Exception {
        StreamTestGraph plan = plan(20L);
        RecordingOutput output = new RecordingOutput();
        StreamStatelessDagOutput graph = new StreamStatelessDagOutput(
                plan.executionGraph, Collections.singletonMap("sink", output),
                ignored -> { throw new AssertionError("no function expected"); });
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(plan.source.outputSchema()));
        graph.start();
        try {
            output.awaitKind(StreamEnvelope.Kind.WATERMARK_STATUS, 2_000L);
            graph.send(record(output.identity, 1L, serializer, 1_000L));

            assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                    output.events.get(0).watermarkStatus());
            assertEquals(1L,
                    output.events.get(0).watermarkStatusGeneration());
            assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                    output.events.get(1).watermarkStatus());
            assertEquals(2L,
                    output.events.get(1).watermarkStatusGeneration());
            assertEquals(StreamEnvelope.Kind.RECORD,
                    output.events.get(2).kind());
            assertEquals(StreamEnvelope.Kind.WATERMARK,
                    output.events.get(3).kind());
        } finally {
            graph.close();
        }
    }

    @Test
    void checkpointRestorePreservesGeneratorWatermarkAndIdleGeneration(
            @TempDir Path root) {
        StreamTestGraph plan = plan(30_000L);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("payloads"), 4_096L);
        RecordingOutput before = new RecordingOutput();
        StreamStatelessDagOutput first = new StreamStatelessDagOutput(
                plan.executionGraph, Collections.singletonMap("sink", before),
                ignored -> { throw new AssertionError("no function expected"); },
                store, Collections.emptyList());
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(plan.source.outputSchema()));
        CheckpointIdentity checkpoint = checkpoint(plan);
        first.send(record(before.identity, 1L, serializer, 1_000L));
        first.send(StreamEnvelope.checkpointBarrier(
                before.identity, 2L, checkpoint));
        List<StreamCheckpointMember> members =
                first.checkpointMembers(checkpoint);
        assertEquals(1, members.size());
        assertEquals(StreamCheckpointMember.Kind.STATE,
                members.get(0).kind());
        first.close();

        RecordingOutput after = new RecordingOutput();
        StreamStatelessDagOutput restored = new StreamStatelessDagOutput(
                plan.executionGraph, Collections.singletonMap("sink", after),
                ignored -> { throw new AssertionError("no function expected"); },
                store, members);
        restored.send(record(after.identity, 1L, serializer, 950L));
        restored.send(record(after.identity, 2L, serializer, 1_100L));

        assertEquals(3, after.events.size());
        assertEquals(StreamEnvelope.Kind.RECORD, after.events.get(0).kind());
        assertEquals(StreamEnvelope.Kind.RECORD, after.events.get(1).kind());
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                after.events.get(2).kind());
        assertEquals(1_000L, after.events.get(2).timestamp());
        restored.close();
    }

    @Test
    void remoteInlineTaskSnapshotsAtBarrierAndRestoresBeforeProcessing(
            @TempDir Path root) throws Exception {
        StreamTestGraph plan = plan(30_000L, 2);
        assertEquals(2, plan.executionGraph.node("assign-time").parallelism());
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                root.resolve("remote-payloads"), 4_096L);
        RowSerializer serializer = new RowSerializer(
                PortablePlanCompiler.toDataSchema(plan.source.outputSchema()));
        CheckpointIdentity checkpoint = checkpoint(plan);
        LocalStreamChannel input = channel("remote-time-input");
        LocalStreamChannel output = channel("remote-time-output");
        StreamOperatorTask original = remoteTask(
                plan, input, output, store, Collections.emptyList(), false, 1);
        try {
            CompletableFuture<List<StreamCheckpointMember>> cut =
                    original.checkpoint(checkpoint);
            assertFalse(cut.isDone(),
                    "inline checkpoint must wait for its data-plane barrier");
            input.send(record(input.channelIdentity(), 1L,
                    serializer, 1_000L));
            input.send(StreamEnvelope.checkpointBarrier(
                    input.channelIdentity(), 2L, checkpoint));
            List<StreamCheckpointMember> members = cut.get(
                    2L, TimeUnit.SECONDS);
            assertEquals(1, members.size());
            assertEquals("assign-time", members.get(0).operatorUid());
            assertEquals(1, members.get(0).subtaskIndex());
            assertEquals(io.rowweave.stream.runtime
                            .OfflineKeyedStateRescaler.keyGroupStart(1, 2,
                            plan.executionGraph.node("assign-time")
                                    .maximumParallelism()),
                    members.get(0).keyGroupStart());

            LocalStreamChannel restoredInput = channel(
                    "restored-time-input");
            LocalStreamChannel restoredOutput = channel(
                    "restored-time-output");
            StreamOperatorTask restored = remoteTask(plan, restoredInput,
                    restoredOutput, store, members, true, 1);
            try {
                restoredInput.send(record(restoredInput.channelIdentity(),
                        1L, serializer, 950L));
                restoredInput.send(StreamEnvelope.endOfInput(
                        restoredInput.channelIdentity(), 2L));
                assertEquals(StreamEnvelope.Kind.RECORD,
                        restoredOutput.poll(2_000L).kind());
                assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                        restoredOutput.poll(2_000L).kind(),
                        "restored watermark must not regress and re-emit");
            } finally {
                restored.cancel();
            }
        } finally {
            original.cancel();
        }
    }

    private static StreamOperatorTask remoteTask(
            StreamTestGraph plan, LocalStreamChannel input,
            LocalStreamChannel output, FileCheckpointPayloadStore store,
            List<StreamCheckpointMember> restoreMembers, boolean restore,
            int subtaskIndex) {
        io.rowweave.stream.contract.PortableStreamNode node =
                plan.onlyNode(StreamOperatorCode.ASSIGN_EVENT_TIME);
        StreamEdge edge = plan.executionGraph.node(node.operatorUid())
                .inputEdges().get(0);
        int sourceSubtask = plan.executionGraph.node(
                edge.sourceOperatorUid()).parallelism()
                == plan.executionGraph.node(edge.targetOperatorUid())
                .parallelism() ? subtaskIndex : 0;
        return StreamRemoteOperatorTaskRunner.start(plan.executionGraph,
                plan.artifact.artifactId(), node,
                Collections.singletonList(input),
                Collections.singletonList(StreamTaskLaneSpec.input(
                        edge.logicalChannelId() + '#'
                                + edge.sourceOperatorUid() + '#'
                                + sourceSubtask + "->"
                                + edge.targetOperatorUid() + '#'
                                + subtaskIndex, 4_096, 65_536L)),
                Collections.singletonList(new StreamTaskOutput(null, output)),
                ignored -> { throw new AssertionError("no function expected"); },
                subtaskIndex, store, restoreMembers, restore);
    }

    private static StreamTestGraph plan(long idleTimeoutMillis) {
        return plan(idleTimeoutMillis, 1);
    }

    private static StreamTestGraph plan(
            long idleTimeoutMillis, int parallelism) {
        StreamSession session = RowWeave.createStreamSession("assigned-state");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setParallelism(parallelism);
        session.fromSource(source()).uid("source")
                .assignEventTime(StreamEventTimeSpec.boundedOutOfOrderness(
                        StreamExpression.input("event-time",
                                PortableDataType.TIMESTAMP, false),
                        100L, idleTimeoutMillis,
                        StreamEventTimeSpec.InvalidTimestampPolicy.FAIL))
                .uid("assign-time").sinkTo("sink", sink());
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    private static CheckpointIdentity checkpoint(
            StreamTestGraph plan) {
        return new CheckpointIdentity(
                plan.artifact.definitionId(), plan.artifact.artifactId(),
                "run", "attempt", 1L, 1L, "coordinator", 1L,
                plan.commandDigest, plan.artifact.artifactId(), 1, 1);
    }

    private static StreamEnvelope record(StreamChannelIdentity identity,
            long sequence, RowSerializer serializer, long timestamp) {
        DataRow row = new DataRow(1);
        row.set(0, new Timestamp(timestamp));
        return StreamEnvelope.record(identity, sequence,
                serializer.serialize(row));
    }

    private static LocalStreamChannel channel(String channel) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, channel, 1L),
                16, 4_096, 65_536L);
    }

    private static StreamSourceSpec source() { return source(schema(false)); }

    private static StreamSourceSpec source(StreamSchema schema) {
        return StreamSourceSpec.exactOrdered(schema, "time-source",
                digest("time-source"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("time-split"), "time-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "time-sink", digest("time-sink"), new byte[] {3});
    }

    private static StreamSchema schema(boolean nullable) {
        return new StreamSchema(Collections.singletonList(new StreamField(
                "event-time", "eventTime", "eventTime",
                DataType.TimestampType, nullable)));
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
        final StreamChannelIdentity identity = new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, "time-edge", 1L);
        final List<StreamEnvelope> events = new ArrayList<>();
        boolean closed;
        @Override public StreamChannelIdentity channelIdentity() {
            return identity;
        }
        @Override public synchronized void send(StreamEnvelope envelope) {
            events.add(envelope);
            notifyAll();
        }
        @Override public void close() { closed = true; }
        @Override public void cancel() { }
        @Override public void fail(Throwable cause) { }

        synchronized void awaitKind(StreamEnvelope.Kind kind,
                long timeoutMillis) throws InterruptedException {
            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    timeoutMillis);
            while (!contains(kind) && System.nanoTime() < deadline) {
                wait(10L);
            }
            assertTrue(contains(kind), "timed out waiting for " + kind);
        }

        private boolean contains(StreamEnvelope.Kind kind) {
            for (StreamEnvelope event : events) {
                if (event.kind() == kind) return true;
            }
            return false;
        }
    }
}
