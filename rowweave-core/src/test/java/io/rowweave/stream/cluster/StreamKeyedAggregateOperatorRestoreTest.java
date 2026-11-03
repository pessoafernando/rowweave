package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
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
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalStreamChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** aggregate replacement restores the exact managed state cut. */
class StreamKeyedAggregateOperatorRestoreTest {

    @Test
    void terminalAggregateAcceptsCheckpointAfterEnd(@TempDir Path temporary)
            throws Exception {
        StreamTestGraph plan = plan();
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("payloads"), 32_768L);
        Channels channels = channels(plan, "attempt-1");
        StreamKeyedAggregateOperatorTask task = task(plan, channels,
                store, null, "terminal-aggregate");
        task.start();
        channels.input.send(record(channels.input, 1L, "A",
                BigDecimal.ONE));
        assertEquals("1.0000000000", amount(channels.output.receive()));
        channels.input.send(StreamEnvelope.endOfInput(
                channels.input.channelIdentity(), 2L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                channels.output.receive().kind());
        channels.input.close();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
        StreamCheckpointMember member = task.triggerCheckpoint(
                checkpoint(plan, "attempt-1")).get(5L, TimeUnit.SECONDS);
        assertEquals(StreamCheckpointMember.Kind.STATE, member.kind());
        assertEquals(member.memberId(), task.triggerCheckpoint(
                checkpoint(plan, "attempt-1", 2L))
                .get(5L, TimeUnit.SECONDS).memberId());
    }

    @Test
    void freshTaskContinuesFromCheckpointedAccumulator(@TempDir Path temporary)
            throws Exception {
        StreamTestGraph plan = plan();
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("payloads"), 32_768L);
        Channels first = channels(plan, "attempt-1");
        StreamKeyedAggregateOperatorTask initial = task(
                plan, first, store, null, "initial");
        initial.start();
        first.input.send(record(first.input, 1L, "A",
                new BigDecimal("1.00000000006")));
        assertEquals("1.0000000001", amount(first.output.receive()));

        CheckpointIdentity checkpoint = checkpoint(plan, "attempt-1");
        java.util.concurrent.CompletableFuture<StreamCheckpointMember> frozen =
                initial.triggerCheckpoint(checkpoint);
        first.input.send(StreamEnvelope.checkpointBarrier(
                first.input.channelIdentity(), 2L, checkpoint));
        StreamCheckpointMember member = frozen.get(5L, TimeUnit.SECONDS);
        assertEquals(StreamCheckpointMember.Kind.STATE, member.kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                first.output.receive().kind());
        initial.cancel();
        initial.awaitCompletion(5_000L);
        assertNull(initial.failure());

        Channels replacement = channels(plan, "attempt-2");
        StreamKeyedAggregateOperatorTask restored = task(
                plan, replacement, store, member, "restored");
        restored.start();
        replacement.input.send(record(replacement.input, 1L, "A",
                new BigDecimal("2.00000000005")));
        assertEquals("3.0000000001", amount(replacement.output.receive()));
        restored.cancel();
        restored.awaitCompletion(5_000L);
        assertNull(restored.failure());
    }

    private static StreamKeyedAggregateOperatorTask task(
            StreamTestGraph plan, Channels channels,
            FileCheckpointPayloadStore store, StreamCheckpointMember restore,
            String name) {
        return new StreamKeyedAggregateOperatorTask(name,
                StreamNodeTaskContext.from(plan.artifact.artifactId(),
                        plan.executionGraph,
                        plan.aggregate.operatorUid()),
                channels.input, Collections.singletonList(
                new StreamTaskOutput(null, channels.output)),
                ignored -> new StreamProductAggregateWorkerProcessFixture
                        .DecimalReadoutAggregate(),
                store, restore);
    }

    private static Channels channels(
            StreamTestGraph plan, String attempt) {
        return new Channels(channel(plan, attempt,
                plan.sourceInputEdge().logicalChannelId()),
                channel(plan, attempt,
                        plan.onlySink().edge.logicalChannelId()));
    }

    private static LocalStreamChannel channel(
            StreamTestGraph plan, String attempt, String channelId) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", attempt, 1L, 1L, channelId, 1L),
                plan.maximumEvents(), plan.maximumFrameBytes(), 8_192L);
    }

    private static StreamEnvelope record(LocalStreamChannel channel,
            long sequence, String groupKey, BigDecimal amount) {
        DataRow row = new DataRow(2);
        row.set(0, groupKey);
        row.set(1, amount);
        return StreamEnvelope.record(channel.channelIdentity(), sequence,
                new RowSerializer(StreamProductAggregateWorkerProcessFixture
                        .dataSchema()).serialize(row));
    }

    private static String amount(StreamEnvelope envelope) {
        assertEquals(StreamEnvelope.Kind.RECORD, envelope.kind());
        StreamUpsertRecord upsert = StreamUpsertRecordCodec.decode(
                envelope.recordFrame(), 4_096, 4_096);
        DataRow row = new RowSerializer(
                StreamProductAggregateWorkerProcessFixture.dataSchema())
                .deserialize(upsert.afterImageFrame());
        return row.getBigDecimal(1).toPlainString();
    }

    private static CheckpointIdentity checkpoint(
            StreamTestGraph plan, String attempt) {
        return checkpoint(plan, attempt, 1L);
    }

    private static CheckpointIdentity checkpoint(
            StreamTestGraph plan, String attempt, long sequence) {
        return new CheckpointIdentity(plan.artifact.definitionId(),
                plan.artifact.artifactId(), "run", attempt, 1L, 1L,
                "coordinator", sequence, plan.commandDigest,
                plan.artifact.artifactId(), 1, 1);
    }

    private static StreamTestGraph plan() {
        StreamSession session = RowWeave.createStreamSession(
                "aggregate-restore", "aggregate-restore");
        session.setChangelogMode(StreamChangelogMode.UPSERT);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 8_192L, 0, 0,
                32_768L, 5, 32, 1_048_576L));
        StreamSchema schema = new StreamSchema(Arrays.asList(
                new StreamField("adaptor", "adaptor", "adaptor",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, false)));
        session.fromSource(StreamSourceSpec.exactOrdered(schema,
                        StreamProductAggregateWorkerProcessFixture
                                .SOURCE_PROVIDER,
                        StreamProductAggregateWorkerProcessFixture
                                .SOURCE_CONTRACT,
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(
                                StreamProductAggregateWorkerProcessFixture
                                        .digest("restore-split"),
                                "sequence-v1", new byte[] {2}))))
                .uid("source")
                .keyBy("adaptor").uid("adaptor-key")
                .keyedAggregate(
                        StreamProductAggregateWorkerProcessFixture.function(),
                        schema, new StreamStateSpec("readout",
                                StreamStateSpec.Kind.KEYED_AGGREGATING,
                                "aggregate-decimal-state-v1", 1,
                                StreamStateSpec.TtlMode.PROCESSING_TIME,
                                60_000L))
                .uid("readout-aggregate")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        StreamProductAggregateWorkerProcessFixture.SINK_PROVIDER,
                        StreamProductAggregateWorkerProcessFixture.SINK_CONTRACT,
                        new byte[] {3}));
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    private static final class Channels {
        final LocalStreamChannel input;
        final LocalStreamChannel output;

        private Channels(LocalStreamChannel input,
                LocalStreamChannel output) {
            this.input = input;
            this.output = output;
        }
    }
}
