package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTemporalJoinSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.StreamTemporalJoinException;
import io.rowweave.stream.runtime.StreamTimeException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** temporal fan-in time and bounded-state product semantics. */
class StreamTemporalJoinOperatorWatermarkStatusTest {

    @Test
    void endedLeftDoesNotHoldRightFrontierOrAccumulateRightState()
            throws Exception {
        StreamTestGraph plan = plan(8_704L);
        java.util.List<StreamProductExecutionGraph.SourceRoute> sources =
                plan.inputSources(plan.temporalJoin);
        LocalStreamChannel left = channel(plan,
                plan.sourceInputEdge(sources.get(0)).logicalChannelId());
        LocalStreamChannel right = channel(plan,
                plan.sourceInputEdge(sources.get(1)).logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamTemporalJoinOperatorTask task = new StreamTemporalJoinOperatorTask(
                "one-ended-frontier", temporalContext(plan), left, right,
                Collections.singletonList(new StreamTaskOutput(null, output)),
                ignored -> new StreamProductTemporalJoinWorkerProcessFixture
                        .PairSum(), null);
        task.start();
        try {
            left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 1L));
            for (int index = 1; index <= 5; index++) {
                long timestamp = index * 100L;
                right.send(record(right, index * 2L - 1L, "A", "1",
                        timestamp));
                right.send(StreamEnvelope.watermark(right.channelIdentity(),
                        index * 2L, timestamp));
                assertWatermark(output.poll(2_000L), timestamp);
                assertEquals(null, task.failure());
            }
        } finally {
            task.cancel();
            task.awaitCompletion(2_000L);
        }
    }

    @Test
    void endedLeftRetainsRightInsertUntilItsRetraction()
            throws Exception {
        StreamTestGraph plan = retractPlan();
        java.util.List<StreamProductExecutionGraph.SourceRoute> sources =
                plan.inputSources(plan.temporalJoin);
        LocalStreamChannel left = channel(plan,
                plan.sourceInputEdge(sources.get(0)).logicalChannelId());
        LocalStreamChannel right = channel(plan,
                plan.sourceInputEdge(sources.get(1)).logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamTemporalJoinOperatorTask task = new StreamTemporalJoinOperatorTask(
                "one-ended-retract", temporalContext(plan), left, right,
                Collections.singletonList(new StreamTaskOutput(null, output)),
                ignored -> new StreamProductTemporalJoinWorkerProcessFixture
                        .PairSum(), null);
        task.start();
        try {
            left.send(changeRecord(left, 1L, "A", "1", 100L,
                    StreamChangeRecord.Kind.INSERT));
            left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 2L));
            right.send(changeRecord(right, 1L, "A", "2", 100L,
                    StreamChangeRecord.Kind.INSERT));
            assertEquals(StreamChangeRecord.Kind.INSERT,
                    change(output.poll(2_000L)).kind());
            StreamTaskResourceUsage usage = task.resourceUsage();
            assertTrue(usage.complete());
            assertTrue(usage.managedStateBytes() > 0L);
            right.send(changeRecord(right, 2L, "A", "2", 100L,
                    StreamChangeRecord.Kind.DELETE));
            assertEquals(StreamChangeRecord.Kind.DELETE,
                    change(output.poll(2_000L)).kind());
            right.send(StreamEnvelope.watermark(right.channelIdentity(),
                    3L, 101L));
            assertWatermark(output.poll(2_000L), 101L);
            assertEquals(null, task.failure());
        } finally {
            task.cancel();
            task.awaitCompletion(2_000L);
        }
    }

    @Test
    void oneEndedInputStillAlignsAndTerminalJoinReusesSnapshot(
            @TempDir Path temp) throws Exception {
        StreamTestGraph plan = plan(64 * 1024L);
        java.util.List<StreamProductExecutionGraph.SourceRoute> sources =
                plan.inputSources(plan.temporalJoin);
        LocalStreamChannel left = channel(plan,
                plan.sourceInputEdge(sources.get(0)).logicalChannelId());
        LocalStreamChannel right = channel(plan,
                plan.sourceInputEdge(sources.get(1)).logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamTemporalJoinOperatorTask task =
                new StreamTemporalJoinOperatorTask("terminal-checkpoint",
                        temporalContext(plan), left, right,
                        Collections.singletonList(
                                new StreamTaskOutput(null, output)),
                        ignored -> new StreamProductTemporalJoinWorkerProcessFixture
                                .PairSum(), new FileCheckpointPayloadStore(
                                temp.resolve("payloads"), 1_048_576L));
        task.start();
        left.send(record(left, 1L, "A", "1", 100L));
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 2L));

        CheckpointIdentity first = checkpoint(plan, 1L);
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                frozen = task.triggerCheckpoint(first);
        right.send(StreamEnvelope.checkpointBarrier(
                right.channelIdentity(), 1L, first));
        assertEquals(2, frozen.get(5L, TimeUnit.SECONDS).size());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                output.poll(5_000L).kind());

        right.send(record(right, 2L, "A", "2", 100L));
        assertEquals("3.0000000000", amount(output.poll(5_000L)));

        right.send(StreamEnvelope.endOfInput(right.channelIdentity(), 3L));
        assertWatermark(output.poll(5_000L), Long.MAX_VALUE);
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        task.awaitCompletion(5_000L);
        assertEquals(null, task.failure());
        assertEquals(2, task.triggerCheckpoint(checkpoint(plan, 2L))
                .get(5L, TimeUnit.SECONDS).size());
    }

    @Test
    void boundedInputsEndWithoutFailingTemporalJoin() throws Exception {
        StreamTestGraph plan = plan(64 * 1024L);
        java.util.List<StreamProductExecutionGraph.SourceRoute> sources =
                plan.inputSources(plan.temporalJoin);
        LocalStreamChannel left = channel(plan,
                plan.sourceInputEdge(sources.get(0)).logicalChannelId());
        LocalStreamChannel right = channel(plan,
                plan.sourceInputEdge(sources.get(1)).logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamTemporalJoinOperatorTask task =
                new StreamTemporalJoinOperatorTask(
                        "bounded-terminal", temporalContext(plan),
                        left, right, Collections.singletonList(
                        new StreamTaskOutput(null, output)),
                        ignored -> new StreamProductTemporalJoinWorkerProcessFixture
                                .PairSum(), null);
        task.start();
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 1L));
        right.send(StreamEnvelope.endOfInput(right.channelIdentity(), 1L));
        assertWatermark(output.poll(2_000L), Long.MAX_VALUE);
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(2_000L).kind());
        task.awaitCompletion(2_000L);
        assertEquals(null, task.failure());
    }

    @Test
    void retractInputDeletesRetainedMatchAndEmitsCompensation()
            throws Exception {
        StreamTestGraph plan = retractPlan();
        java.util.List<StreamProductExecutionGraph.SourceRoute> sources =
                plan.inputSources(plan.temporalJoin);
        LocalStreamChannel left = channel(plan,
                plan.sourceInputEdge(sources.get(0)).logicalChannelId());
        LocalStreamChannel right = channel(plan,
                plan.sourceInputEdge(sources.get(1)).logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamTemporalJoinOperatorTask task =
                new StreamTemporalJoinOperatorTask(
                        "retract-compensation", temporalContext(plan),
                        left, right, Collections.singletonList(
                        new StreamTaskOutput(null, output)),
                        ignored -> new StreamProductTemporalJoinWorkerProcessFixture
                                .PairSum(), null);
        task.start();
        try {
            left.send(changeRecord(left, 1L, "A", "1", 100L,
                    StreamChangeRecord.Kind.INSERT));
            right.send(changeRecord(right, 1L, "A", "2", 100L,
                    StreamChangeRecord.Kind.INSERT));
            StreamChangeRecord inserted = change(output.receive());
            assertEquals(StreamChangeRecord.Kind.INSERT, inserted.kind());
            assertEquals("3.0000000000", amount(inserted.afterImageFrame()));

            right.send(changeRecord(right, 2L, "A", "2", 100L,
                    StreamChangeRecord.Kind.DELETE));
            StreamChangeRecord deleted = change(output.receive());
            assertEquals(StreamChangeRecord.Kind.DELETE, deleted.kind());
            assertEquals("3.0000000000", amount(deleted.beforeImageFrame()));
        } finally {
            task.cancel();
            task.awaitCompletion(2_000L);
        }
        assertEquals(null, task.failure());
    }

    @Test
    void idleInputIsExcludedAndStaleReactivationFailsClosed()
            throws Exception {
        StreamTestGraph plan = plan(64 * 1024L);
        java.util.List<StreamProductExecutionGraph.SourceRoute> sources =
                plan.inputSources(plan.temporalJoin);
        LocalStreamChannel left = channel(plan,
                plan.sourceInputEdge(sources.get(0)).logicalChannelId());
        LocalStreamChannel right = channel(plan,
                plan.sourceInputEdge(sources.get(1)).logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamTemporalJoinOperatorTask task =
                new StreamTemporalJoinOperatorTask(
                        "watermark-status", temporalContext(plan),
                        left, right, Collections.singletonList(
                                new StreamTaskOutput(null, output)),
                        ignored -> new StreamProductTemporalJoinWorkerProcessFixture
                                .PairSum(), null);
        task.start();
        try {
            left.send(StreamEnvelope.watermark(
                    left.channelIdentity(), 1L, 100L));
            right.send(StreamEnvelope.watermark(
                    right.channelIdentity(), 1L, 100L));
            assertWatermark(output.poll(2_000L), 100L);

            left.send(StreamEnvelope.watermarkStatus(
                    left.channelIdentity(), 2L,
                    StreamEnvelope.WatermarkStatus.IDLE, 1L));
            right.send(StreamEnvelope.watermark(
                    right.channelIdentity(), 2L, 200L));
            assertWatermark(output.poll(2_000L), 200L);

            left.send(StreamEnvelope.watermarkStatus(
                    left.channelIdentity(), 3L,
                    StreamEnvelope.WatermarkStatus.ACTIVE, 2L));
            task.awaitCompletion(2_000L);
            assertNotNull(task.failure());
            assertTrue(task.failure() instanceof StreamTimeException);
            assertEquals(StreamTimeException.Code.ACTIVATION_WATERMARK,
                    ((StreamTimeException) task.failure()).code());
        } finally {
            task.cancel();
            task.awaitCompletion(2_000L);
        }
    }

    @Test
    void oppositeWatermarksRecycleTightStateAndLateInputFailsClosed()
            throws Exception {
        StreamTestGraph plan = plan(8_704L);
        java.util.List<StreamProductExecutionGraph.SourceRoute> sources =
                plan.inputSources(plan.temporalJoin);
        LocalStreamChannel left = channel(plan,
                plan.sourceInputEdge(sources.get(0)).logicalChannelId());
        LocalStreamChannel right = channel(plan,
                plan.sourceInputEdge(sources.get(1)).logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamTemporalJoinOperatorTask task =
                new StreamTemporalJoinOperatorTask(
                        "state-cleanup", temporalContext(plan),
                        left, right, Collections.singletonList(
                                new StreamTaskOutput(null, output)),
                        ignored -> new StreamProductTemporalJoinWorkerProcessFixture
                        .PairSum(), null);
        task.start();
        try {
            left.send(record(left, 1L, "A", "1", 100L));
            right.send(record(right, 1L, "A", "2", 100L));
            assertEquals("3.0000000000", amount(output.receive()));

            right.send(StreamEnvelope.watermark(
                    right.channelIdentity(), 2L, 120L));
            left.send(StreamEnvelope.watermark(
                    left.channelIdentity(), 2L, 110L));
            assertWatermark(output.receive(), 110L);

            left.send(record(left, 3L, "B", "3", 200L));
            right.send(record(right, 3L, "B", "4", 200L));
            assertEquals("7.0000000000", amount(output.receive()));

            left.send(record(left, 4L, "late", "1", 100L));
            task.awaitCompletion(2_000L);
            assertTrue(task.failure() instanceof StreamTemporalJoinException);
            assertEquals(StreamTemporalJoinException.Code.LATE_RECORD,
                    ((StreamTemporalJoinException) task.failure()).code());
        } finally {
            task.cancel();
            task.awaitCompletion(2_000L);
        }
    }

    private static StreamNodeTaskContext temporalContext(
            StreamTestGraph plan) {
        return StreamNodeTaskContext.from(plan.artifact.artifactId(),
                plan.executionGraph, plan.temporalJoin.operatorUid());
    }

    private static CheckpointIdentity checkpoint(StreamTestGraph plan,
            long sequence) {
        return new CheckpointIdentity(plan.artifact.definitionId(),
                plan.artifact.artifactId(), "run", "attempt", 1L, 1L,
                "coordinator", sequence, plan.commandDigest,
                plan.artifact.artifactId(), 1, 1);
    }

    private static void assertWatermark(
            StreamEnvelope envelope, long expected) {
        assertNotNull(envelope);
        assertEquals(StreamEnvelope.Kind.WATERMARK, envelope.kind());
        assertEquals(expected, envelope.timestamp());
    }

    private static LocalStreamChannel channel(
            StreamTestGraph plan, String logicalChannelId) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, logicalChannelId, 1L),
                plan.maximumEvents(), plan.maximumFrameBytes(), 8_192L);
    }

    private static StreamEnvelope record(LocalStreamChannel channel,
            long sequence, String groupKey, String amount, long timestamp) {
        DataRow row = new DataRow(2);
        row.set(0, groupKey);
        row.set(1, new BigDecimal(amount));
        return StreamEnvelope.record(channel.channelIdentity(), sequence,
                timestamp, new RowSerializer(
                        StreamProductTemporalJoinWorkerProcessFixture.dataSchema())
                        .serialize(row));
    }

    private static StreamEnvelope changeRecord(LocalStreamChannel channel,
            long sequence, String groupKey, String amount, long timestamp,
            StreamChangeRecord.Kind kind) {
        DataRow row = new DataRow(2);
        row.set(0, groupKey);
        row.set(1, new BigDecimal(amount));
        RowSerializer serializer = new RowSerializer(
                StreamProductTemporalJoinWorkerProcessFixture.dataSchema());
        byte[] rowFrame = serializer.serialize(row);
        DataRow keyRow = new DataRow(1);
        keyRow.set(0, groupKey);
        RowSerializer keySerializer = new RowSerializer(
                io.rowweave.portable.PortablePlanCompiler.toDataSchema(
                        new io.rowweave.portable.PortableRowSchema(
                                Collections.singletonList(
                                        schema().toPortableSchema()
                                                .field(0)))));
        StreamChangeRecord change = kind == StreamChangeRecord.Kind.INSERT
                ? StreamChangeRecord.insert(rowFrame)
                : StreamChangeRecord.delete(
                keySerializer.serialize(keyRow), rowFrame);
        return StreamEnvelope.record(channel.channelIdentity(), sequence,
                timestamp, StreamChangeRecordCodec.encode(change));
    }

    private static StreamChangeRecord change(StreamEnvelope envelope) {
        assertEquals(StreamEnvelope.Kind.RECORD, envelope.kind());
        return StreamChangeRecordCodec.decode(
                envelope.recordFrame(), 4_096, 4_096, 4_096);
    }

    private static String amount(byte[] rowFrame) {
        return new RowSerializer(
                StreamProductTemporalJoinWorkerProcessFixture.dataSchema())
                .deserialize(rowFrame).getBigDecimal(1).toPlainString();
    }

    private static String amount(StreamEnvelope envelope) {
        assertEquals(StreamEnvelope.Kind.RECORD, envelope.kind());
        StreamUpsertRecord upsert = StreamUpsertRecordCodec.decode(
                envelope.recordFrame(), 4_096, 4_096);
        DataRow row = new RowSerializer(
                StreamProductTemporalJoinWorkerProcessFixture.dataSchema())
                .deserialize(upsert.afterImageFrame());
        return row.getBigDecimal(1).toPlainString();
    }

    private static StreamTestGraph plan(long managedStateBytes) {
        StreamSession session = RowWeave.createStreamSession(
                "temporal-watermark-status", "temporal-watermark-status");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, managedStateBytes, 0, 0,
                64 * 1024L, 8, 32, 1_048_576L));
        DataStream left = session.fromSource(source("left"))
                .uid("left-source").keyBy("adaptor").uid("left-key");
        DataStream right = session.fromSource(source("right"))
                .uid("right-source").keyBy("adaptor").uid("right-key");
        left.temporalJoin(right,
                        new StreamTemporalJoinSpec(
                                "adaptor", "adaptor", -10L, 20L,
                                Collections.singletonList("adaptor")),
                        StreamProductTemporalJoinWorkerProcessFixture.function(),
                        schema(), state("left-state"), state("right-state"))
                .uid("temporal-join")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        StreamProductTemporalJoinWorkerProcessFixture.SINK,
                        StreamProductTemporalJoinWorkerProcessFixture
                                .SINK_CONTRACT, new byte[] {3}));
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    private static StreamTestGraph retractPlan() {
        StreamSession session = RowWeave.createStreamSession(
                "temporal-retract", "temporal-retract");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 64 * 1024L, 0, 0,
                64 * 1024L, 8, 32, 1_048_576L));
        DataStream left = session.fromSource(retractSource("left"))
                .uid("left-source").keyBy("adaptor").uid("left-key");
        DataStream right = session.fromSource(retractSource("right"))
                .uid("right-source").keyBy("adaptor").uid("right-key");
        left.temporalJoin(right,
                        new StreamTemporalJoinSpec(
                                "adaptor", "adaptor", -10L, 20L,
                                Collections.singletonList("adaptor")),
                        StreamProductTemporalJoinWorkerProcessFixture.function(),
                        schema(), state("left-state"), state("right-state"))
                .uid("temporal-join")
                .sinkTo("sink", StreamSinkSpec.retractEagerAtLeastOnce(
                        StreamProductTemporalJoinWorkerProcessFixture.SINK,
                        StreamProductTemporalJoinWorkerProcessFixture
                                .SINK_CONTRACT, new byte[] {3}));
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    private static StreamSourceSpec source(String side) {
        String provider = "left".equals(side)
                ? StreamProductTemporalJoinWorkerProcessFixture.LEFT_SOURCE
                : StreamProductTemporalJoinWorkerProcessFixture.RIGHT_SOURCE;
        String contract = "left".equals(side)
                ? StreamProductTemporalJoinWorkerProcessFixture.LEFT_CONTRACT
                : StreamProductTemporalJoinWorkerProcessFixture.RIGHT_CONTRACT;
        return StreamSourceSpec.exactOrderedEventTime(
                schema(), provider, contract, new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        StreamProductTemporalJoinWorkerProcessFixture
                                .digest(side + "-status-split"),
                        "sequence-v1", new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(30_000L));
    }

    private static StreamSourceSpec retractSource(String side) {
        String provider = "left".equals(side)
                ? StreamProductTemporalJoinWorkerProcessFixture.LEFT_SOURCE
                : StreamProductTemporalJoinWorkerProcessFixture.RIGHT_SOURCE;
        String contract = "left".equals(side)
                ? StreamProductTemporalJoinWorkerProcessFixture.LEFT_CONTRACT
                : StreamProductTemporalJoinWorkerProcessFixture.RIGHT_CONTRACT;
        return StreamSourceSpec.retractExactOrderedEventTime(
                schema(), Collections.singletonList("adaptor"),
                provider, contract, new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        StreamProductTemporalJoinWorkerProcessFixture
                                .digest(side + "-retract-split"),
                        "sequence-v2", new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(30_000L));
    }

    private static StreamStateSpec state(String name) {
        return new StreamStateSpec(name, StreamStateSpec.Kind.KEYED_MAP,
                "temporal-record-v1", 1,
                StreamStateSpec.TtlMode.EVENT_TIME, 30L);
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, false)));
    }
}
