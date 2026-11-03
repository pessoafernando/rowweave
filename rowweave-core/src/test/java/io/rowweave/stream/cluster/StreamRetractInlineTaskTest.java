package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalStreamChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** generic inline tasks preserve canonical RETRACT rows. */
class StreamRetractInlineTaskTest {

    @Test
    void calcAppliesTheSamePredicateAndProjectionToBeforeAndAfterImages(
            @TempDir Path root) throws Exception {
        StreamTestGraph graph = graph();
        io.rowweave.stream.contract.PortableStreamNode calc =
                graph.onlyNode(StreamOperatorCode.CALC);
        StreamEdge edge = graph.executionGraph.node(calc.operatorUid())
                .inputEdges().get(0);
        LocalStreamChannel input = channel("retract-input");
        LocalStreamChannel output = channel("retract-output");
        StreamOperatorTask task = StreamRemoteOperatorTaskRunner.start(
                graph.executionGraph, graph.artifact.artifactId(), calc,
                Collections.singletonList(input),
                Collections.singletonList(StreamTaskLaneSpec.input(
                        edge.logicalChannelId() + '#'
                                + edge.sourceOperatorUid() + "#0->"
                                + edge.targetOperatorUid() + "#0",
                        4_096, 65_536L)),
                Collections.singletonList(new StreamTaskOutput(null, output)),
                ignored -> { throw new AssertionError("no function expected"); },
                0, new FileCheckpointPayloadStore(
                        root.resolve("payloads"), 65_536L),
                Collections.emptyList(), false);
        try {
            RowSerializer rows = new RowSerializer(
                    PortablePlanCompiler.toDataSchema(graph.source.outputSchema()));
            RowSerializer keys = keySerializer(graph.source.outputSchema());
            DataRow positive = row(7L, 5L);
            DataRow negative = row(7L, -1L);
            DataRow positiveAgain = row(7L, 9L);
            byte[] key = keys.serializeProjected(positive, new int[] {0});

            input.send(record(input, 1L, StreamChangeRecord.insert(
                    rows.serialize(positive))));
            input.send(record(input, 2L, StreamChangeRecord.updateBefore(
                    key, rows.serialize(positive))));
            input.send(record(input, 3L, StreamChangeRecord.updateAfter(
                    key, rows.serialize(negative))));
            input.send(record(input, 4L, StreamChangeRecord.updateBefore(
                    key, rows.serialize(negative))));
            input.send(record(input, 5L, StreamChangeRecord.updateAfter(
                    key, rows.serialize(positiveAgain))));
            input.send(record(input, 6L, StreamChangeRecord.delete(
                    key, rows.serialize(positiveAgain))));
            input.send(StreamEnvelope.endOfInput(
                    input.channelIdentity(), 7L));

            List<StreamChangeRecord.Kind> kinds = Arrays.asList(
                    StreamChangeRecord.Kind.INSERT,
                    StreamChangeRecord.Kind.UPDATE_BEFORE,
                    StreamChangeRecord.Kind.UPDATE_AFTER,
                    StreamChangeRecord.Kind.DELETE);
            long[] expectedAmounts = new long[] {10L, 10L, 18L, 18L};
            for (int index = 0; index < kinds.size(); index++) {
                StreamEnvelope envelope = output.poll(2_000L);
                assertEquals(StreamEnvelope.Kind.RECORD, envelope.kind());
                StreamChangeRecord change = StreamChangeRecordCodec.decode(
                        envelope.recordFrame(), 4_096, 4_096, 4_096);
                assertEquals(kinds.get(index), change.kind());
                if (change.hasStableKey()) {
                    assertArrayEquals(key, change.stableKeyFrame());
                }
                byte[] image = change.hasBeforeImage()
                        ? change.beforeImageFrame() : change.afterImageFrame();
                DataRow result = rows.deserialize(image);
                assertEquals(7L, result.get(0));
                assertEquals(expectedAmounts[index], result.get(1));
            }
            assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                    output.poll(2_000L).kind());
            task.awaitCompletion(TimeUnit.SECONDS.toMillis(2L));
        } finally {
            task.cancel();
        }
    }

    private static StreamTestGraph graph() {
        StreamExpression id = StreamExpression.input(
                "id", PortableDataType.LONG, false);
        StreamExpression amount = StreamExpression.input(
                "amount", PortableDataType.LONG, false);
        StreamCalcSpec calc = StreamCalcSpec.of(
                StreamExpression.call(StreamExpression.Function.GREATER_THAN,
                        amount, StreamExpression.longLiteral(0L)),
                Arrays.asList(
                        StreamCalcSpec.projection("id", id),
                        StreamCalcSpec.projection("amount",
                                StreamExpression.call(
                                        StreamExpression.Function.MULTIPLY,
                                        amount,
                                        StreamExpression.longLiteral(2L)))));
        StreamSession session = RowWeave.createStreamSession("retract-inline");
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.setParallelism(1);
        session.setMaxParallelism(1);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 65_536, 0L, 0, 0,
                65_536L, 4, 16, 0L));
        session.fromSource(StreamSourceSpec.retractExactOrdered(
                schema(), Collections.singletonList("id"), "source-v2",
                digest("source"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split"), "position-v1", new byte[] {2}))))
                .uid("source")
                .calc(calc, schema()).uid("calc")
                .sinkTo("sink", StreamSinkSpec.retractEagerAtLeastOnce(
                        "sink-v2", digest("sink"), new byte[] {3}));
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.LongType, false)));
    }

    private static RowSerializer keySerializer(PortableRowSchema schema) {
        return new RowSerializer(PortablePlanCompiler.toDataSchema(
                new PortableRowSchema(Collections.singletonList(
                        schema.field(0)))));
    }

    private static DataRow row(long id, long amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, amount);
        return row;
    }

    private static StreamEnvelope record(LocalStreamChannel input,
            long sequence, StreamChangeRecord change) {
        return StreamEnvelope.record(input.channelIdentity(), sequence,
                StreamChangeRecordCodec.encode(change));
    }

    private static LocalStreamChannel channel(String id) {
        return new LocalStreamChannel(
                new io.rowweave.stream.contract.StreamChannelIdentity(
                        "run", "attempt", 1L, 1L, id, 1L),
                16, 4_096, 65_536L);
    }

    private static String digest(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder text = new StringBuilder(64);
            for (byte current : digest) {
                text.append(Character.forDigit((current >>> 4) & 0xf, 16));
                text.append(Character.forDigit(current & 0xf, 16));
            }
            return text.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
