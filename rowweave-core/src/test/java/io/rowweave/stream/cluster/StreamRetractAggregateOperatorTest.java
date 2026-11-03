package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.RetractableStreamAggregateFunction;
import io.rowweave.DataRow;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalStreamChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** keyed aggregate produces exact RETRACT compensation. */
class StreamRetractAggregateOperatorTest {

    @Test
    void reversibleAccumulatorEmitsBeforeAfterAndFinalDelete(
            @TempDir Path root) throws Exception {
        StreamTestGraph plan = plan();
        LocalStreamChannel input = channel(plan, "aggregate-input");
        LocalStreamChannel output = channel(plan, "aggregate-output");
        StreamKeyedAggregateOperatorTask task =
                new StreamKeyedAggregateOperatorTask(
                        "retract-aggregate",
                        StreamNodeTaskContext.from(
                                plan.artifact.artifactId(),
                                plan.executionGraph,
                                plan.aggregate.operatorUid()),
                        input, Collections.singletonList(
                        new StreamTaskOutput(null, output)),
                        ignored -> new SumAggregate(),
                        new FileCheckpointPayloadStore(
                                root.resolve("payloads"), 65_536L),
                        null);
        RowSerializer rows = new RowSerializer(
                PortablePlanCompiler.toDataSchema(
                        plan.source.outputSchema()));
        RowSerializer keys = new RowSerializer(
                PortablePlanCompiler.toDataSchema(
                        new io.rowweave.portable.PortableRowSchema(
                                Collections.singletonList(
                                        plan.source.outputSchema().field(0)))));
        DataRow ten = row(7L, 10L);
        DataRow five = row(7L, 5L);
        byte[] key = keys.serializeProjected(ten, new int[] {0});
        task.start();
        try {
            input.send(change(input, 1L,
                    StreamChangeRecord.insert(rows.serialize(ten))));
            input.send(change(input, 2L,
                    StreamChangeRecord.insert(rows.serialize(five))));
            input.send(change(input, 3L,
                    StreamChangeRecord.delete(key, rows.serialize(five))));
            input.send(change(input, 4L,
                    StreamChangeRecord.delete(key, rows.serialize(ten))));

            List<StreamChangeRecord.Kind> expectedKinds = Arrays.asList(
                    StreamChangeRecord.Kind.INSERT,
                    StreamChangeRecord.Kind.UPDATE_BEFORE,
                    StreamChangeRecord.Kind.UPDATE_AFTER,
                    StreamChangeRecord.Kind.UPDATE_BEFORE,
                    StreamChangeRecord.Kind.UPDATE_AFTER,
                    StreamChangeRecord.Kind.DELETE);
            long[] expectedAmounts = new long[] {10L, 10L, 15L, 15L, 10L,
                    10L};
            for (int index = 0; index < expectedKinds.size(); index++) {
                StreamChangeRecord change = StreamChangeRecordCodec.decode(
                        output.receive().recordFrame(), 4_096, 4_096, 4_096);
                assertEquals(expectedKinds.get(index), change.kind());
                byte[] image = change.hasBeforeImage()
                        ? change.beforeImageFrame() : change.afterImageFrame();
                assertEquals(expectedAmounts[index],
                        rows.deserialize(image).get(1));
            }
        } finally {
            task.cancel();
            task.awaitCompletion(5_000L);
        }
        assertNull(task.failure());
    }

    private static StreamTestGraph plan() {
        StreamSession session = RowWeave.createStreamSession(
                "retract-aggregate", "retract-aggregate");
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 8_192, 8_192L, 0, 0,
                65_536L, 5, 32, 1_048_576L));
        session.fromSource(StreamSourceSpec.retractExactOrdered(
                        schema(), Collections.singletonList("id"),
                        "source-v2", digest("source"), new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("split"), "position-v1",
                                new byte[] {2}))))
                .uid("source")
                .keyBy("id").uid("key")
                .keyedAggregate(function(), schema(), new StreamStateSpec(
                        "sum", StreamStateSpec.Kind.KEYED_AGGREGATING,
                        "sum-v2", 2,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L))
                .uid("aggregate")
                .sinkTo("sink", StreamSinkSpec.retractEagerAtLeastOnce(
                        "sink-v2", digest("sink"), new byte[] {3}));
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    private static StreamEnvelope change(LocalStreamChannel channel,
            long sequence, StreamChangeRecord record) {
        return StreamEnvelope.record(channel.channelIdentity(), sequence,
                StreamChangeRecordCodec.encode(record));
    }

    private static LocalStreamChannel channel(
            StreamTestGraph plan, String id) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, id, 1L),
                plan.maximumEvents(), plan.maximumFrameBytes(), 8_192L);
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.LongType, false)));
    }

    private static FunctionDeploymentReference function() {
        byte[] configuration = new byte[] {4};
        return new FunctionDeploymentReference(digest("deployment"),
                "stream-functions", digest("jar"), "example.SumAggregate",
                FunctionAbi.STREAM_RETRACT_AGGREGATE_V2,
                "stream-function-config-v2", digest(configuration),
                configuration, new FunctionResourceProfile(1_024L, 0L, 8));
    }

    private static DataRow row(long id, long amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, amount);
        return row;
    }

    private static final class SumAggregate
            implements RetractableStreamAggregateFunction {
        @Override public byte[] createAccumulator() {
            return ByteBuffer.allocate(24).putLong(0L)
                    .putLong(0L).putLong(0L).array();
        }
        @Override public byte[] add(DataRow value, byte[] current) {
            ByteBuffer input = ByteBuffer.wrap(current);
            long ignoredId = input.getLong();
            long sum = input.getLong();
            long count = input.getLong();
            return ByteBuffer.allocate(24).putLong(value.getLong(0))
                    .putLong(Math.addExact(sum, value.getLong(1)))
                    .putLong(Math.addExact(count, 1L)).array();
        }
        @Override public byte[] retract(DataRow value, byte[] current) {
            ByteBuffer input = ByteBuffer.wrap(current);
            long id = input.getLong();
            long sum = input.getLong();
            long count = input.getLong();
            if (id != value.getLong(0) || count < 1L) {
                throw new IllegalArgumentException("invalid retract input");
            }
            return ByteBuffer.allocate(24).putLong(id)
                    .putLong(Math.subtractExact(sum, value.getLong(1)))
                    .putLong(count - 1L).array();
        }
        @Override public boolean isEmpty(byte[] accumulator) {
            return ByteBuffer.wrap(accumulator).getLong(16) == 0L;
        }
        @Override public DataRow getResult(byte[] accumulator) {
            ByteBuffer input = ByteBuffer.wrap(accumulator);
            DataRow row = new DataRow(2);
            row.set(0, input.getLong());
            row.set(1, input.getLong());
            return row;
        }
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
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
