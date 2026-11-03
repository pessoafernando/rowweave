package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
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
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.api.StreamWindowSpec;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalStreamChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** A terminal physical window remains a checkpoint participant. */
class StreamWindowOperatorTerminalTest {
    @Test
    void endWithoutFinalWatermarkFiresPendingEventWindow(
            @TempDir Path temp) throws Exception {
        pendingWindowAtEnd(temp, false);
    }

    @Test
    void idleEndWithoutFinalWatermarkFiresPendingEventWindow(
            @TempDir Path temp) throws Exception {
        pendingWindowAtEnd(temp, true);
    }

    private void pendingWindowAtEnd(Path temp, boolean idle)
            throws Exception {
        StreamSession session = RowWeave.createStreamSession("terminal-window");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 16_384L, 32, 32,
                65_536L, 5, 32, 1_048_576L));
        StreamSchema inputSchema = new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
        StreamSchema outputSchema = new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("window-start", "windowStart", "windowStart",
                        DataType.LongType, false),
                new StreamField("window-end", "windowEnd", "windowEnd",
                        DataType.LongType, false),
                new StreamField("total", "total", "total",
                        DataType.BigDecimalType, false)));
        session.fromSource(StreamSourceSpec.exactOrderedEventTime(
                        inputSchema, "source-v1", digest("source"),
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(digest("split"),
                                "sequence-v1", new byte[] {2})),
                        StreamWatermarkPolicy.providerEvents(1_000L)))
                .uid("source").keyBy("adaptor").uid("key")
                .eventTimeDecimalWindow(StreamWindowSpec.tumblingDecimal(
                        "event-time", "amount", 100L, "window-start",
                        "window-end", "total"), outputSchema,
                        StreamStateSpec.decimalWindow("window-state"))
                .uid("window").sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink"), new byte[] {3}));
        StreamTestGraph plan = StreamTestGraph.from(session.toPlanArtifact());
        LocalStreamChannel input = channel(plan, plan.executionGraph
                .node("window").inputEdges().get(0).logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamWindowOperatorTask task = new StreamWindowOperatorTask(
                "terminal-window", StreamNodeTaskContext.from(
                plan.artifact.artifactId(), plan.executionGraph,
                plan.onlyNode(StreamOperatorCode.EVENT_TIME_WINDOW)
                        .operatorUid()), input,
                Collections.singletonList(new StreamTaskOutput(null, output)),
                new FileCheckpointPayloadStore(temp.resolve("payloads"),
                        1_048_576L), null);
        task.start();
        DataRow row = new DataRow(3);
        row.set(0, "A");
        row.set(1, new Timestamp(1L));
        row.set(2, BigDecimal.ONE);
        byte[] frame = new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.keyBy.outputSchema()))
                .serialize(row);
        input.send(StreamEnvelope.record(input.channelIdentity(), 1L,
                1L, frame));
        if (idle) {
            input.send(StreamEnvelope.watermarkStatus(
                    input.channelIdentity(), 2L,
                    StreamEnvelope.WatermarkStatus.IDLE, 1L));
            assertEquals(StreamEnvelope.Kind.WATERMARK_STATUS,
                    output.poll(5_000L).kind());
        }
        input.send(StreamEnvelope.endOfInput(input.channelIdentity(),
                idle ? 3L : 2L));
        StreamEnvelope result = output.poll(5_000L);
        assertEquals(StreamEnvelope.Kind.RECORD, result.kind());
        DataRow emitted = new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.onlyNode(StreamOperatorCode.EVENT_TIME_WINDOW)
                        .outputSchema())).deserialize(result.recordFrame());
        assertEquals(new BigDecimal("1.0000000000"),
                emitted.getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void completedDecimalWindowAcceptsNextCheckpoint(@TempDir Path temp)
            throws Exception {
        StreamSession session = RowWeave.createStreamSession("terminal-window");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 16_384L, 32, 32,
                65_536L, 5, 32, 1_048_576L));
        StreamSchema inputSchema = new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("event-time", "eventTime", "eventTime",
                        DataType.TimestampType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
        StreamSchema outputSchema = new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("window-start", "windowStart", "windowStart",
                        DataType.LongType, false),
                new StreamField("window-end", "windowEnd", "windowEnd",
                        DataType.LongType, false),
                new StreamField("total", "total", "total",
                        DataType.BigDecimalType, false)));
        session.fromSource(StreamSourceSpec.exactOrderedEventTime(
                        inputSchema, "source-v1", digest("source"),
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(digest("split"),
                                "sequence-v1", new byte[] {2})),
                        StreamWatermarkPolicy.providerEvents(1_000L)))
                .uid("source").keyBy("adaptor").uid("key")
                .eventTimeDecimalWindow(StreamWindowSpec.tumblingDecimal(
                        "event-time", "amount", 100L, "window-start",
                        "window-end", "total"), outputSchema,
                        StreamStateSpec.decimalWindow("window-state"))
                .uid("window").sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink"), new byte[] {3}));
        StreamTestGraph plan = StreamTestGraph.from(session.toPlanArtifact());
        String inputId = plan.executionGraph.node("window")
                .inputEdges().get(0).logicalChannelId();
        String outputId = plan.onlySink().edge.logicalChannelId();
        LocalStreamChannel input = channel(plan, inputId);
        LocalStreamChannel output = channel(plan, outputId);
        StreamWindowOperatorTask task = new StreamWindowOperatorTask(
                "terminal-window", StreamNodeTaskContext.from(
                plan.artifact.artifactId(), plan.executionGraph,
                plan.onlyNode(StreamOperatorCode.EVENT_TIME_WINDOW)
                        .operatorUid()), input,
                Collections.singletonList(new StreamTaskOutput(null, output)),
                new FileCheckpointPayloadStore(temp.resolve("payloads"),
                        1_048_576L), null);
        task.start();
        input.send(StreamEnvelope.endOfInput(input.channelIdentity(), 1L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        input.close();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
        CheckpointIdentity checkpoint = new CheckpointIdentity(
                plan.artifact.definitionId(), plan.artifact.artifactId(),
                "run", "attempt", 1L, 1L, "coordinator", 1L,
                plan.commandDigest, plan.artifact.artifactId(), 1, 1);
        StreamCheckpointMember member = task.triggerCheckpoint(checkpoint)
                .get(5L, TimeUnit.SECONDS);
        assertEquals(StreamCheckpointMember.Kind.STATE, member.kind());
        CheckpointIdentity next = new CheckpointIdentity(
                plan.artifact.definitionId(), plan.artifact.artifactId(),
                "run", "attempt", 1L, 1L, "coordinator", 2L,
                plan.commandDigest, plan.artifact.artifactId(), 1, 1);
        assertEquals(member.memberId(), task.triggerCheckpoint(next)
                .get(5L, TimeUnit.SECONDS).memberId());
    }

    private static LocalStreamChannel channel(StreamTestGraph plan,
            String channelId) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, channelId, 1L),
                plan.maximumEvents(), plan.maximumFrameBytes(), 8_192L);
    }

    private static String digest(String value) {
        return StreamGenericWindowProductAdmissionTest.digest(value);
    }
}
