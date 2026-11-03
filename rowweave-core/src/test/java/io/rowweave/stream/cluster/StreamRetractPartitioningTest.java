package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamSession;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamChangelogMode;
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
import io.rowweave.stream.runtime.LocalStreamChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** RETRACT routing uses the canonical stable key. */
class StreamRetractPartitioningTest {

    @Test
    void hashRouteKeepsInsertAndKeyOnlyDeleteOnTheSameSubtask()
            throws Exception {
        StreamTestGraph plan = graph();
        StreamExecutionGraph.Node source = plan.executionGraph.node("source");
        StreamEdge edge = source.outputEdges().get(0);
        LocalStreamChannel first = channel("partition-0");
        LocalStreamChannel second = channel("partition-1");
        StreamMultiEdgeOutput router = new StreamMultiEdgeOutput(
                plan.executionGraph, source, 0,
                Arrays.asList(lane(edge, 0), lane(edge, 1)),
                Arrays.asList(first, second));
        RowSerializer rows = new RowSerializer(
                PortablePlanCompiler.toDataSchema(source.portableNode()
                        .outputSchema()));
        RowSerializer keys = keySerializer(
                source.portableNode().outputSchema());
        DataRow row = new DataRow(2);
        row.set(0, 41L);
        row.set(1, 99L);
        byte[] key = keys.serializeProjected(row, new int[] {0});

        router.send(StreamEnvelope.record(router.channelIdentity(), 1L,
                StreamChangeRecordCodec.encode(
                        StreamChangeRecord.insert(rows.serialize(row)))));
        router.send(StreamEnvelope.record(router.channelIdentity(), 2L,
                StreamChangeRecordCodec.encode(
                        StreamChangeRecord.keyOnlyDelete(key))));

        StreamEnvelope firstEvent = first.poll(100L);
        LocalStreamChannel selected = firstEvent == null ? second : first;
        LocalStreamChannel other = selected == first ? second : first;
        StreamEnvelope insert = firstEvent == null
                ? second.poll(100L) : firstEvent;
        assertEquals(StreamChangeRecord.Kind.INSERT,
                decode(insert).kind());
        StreamChangeRecord delete = decode(selected.poll(100L));
        assertEquals(StreamChangeRecord.Kind.DELETE, delete.kind());
        assertArrayEquals(key, delete.stableKeyFrame());
        assertNull(other.poll(20L));
    }

    private static StreamChangeRecord decode(StreamEnvelope envelope) {
        assertEquals(StreamEnvelope.Kind.RECORD, envelope.kind());
        return StreamChangeRecordCodec.decode(
                envelope.recordFrame(), 4_096, 4_096, 4_096);
    }

    private static StreamTaskLaneSpec lane(StreamEdge edge, int partition) {
        return StreamTaskLaneSpec.input(edge.logicalChannelId() + '#'
                        + edge.sourceOperatorUid() + "#0->"
                        + edge.targetOperatorUid() + '#'
                        + partition,
                edge.maximumFrameBytes(), edge.initialCreditBytes());
    }

    private static StreamTestGraph graph() {
        StreamSession session = RowWeave.createStreamSession("retract-route");
        session.setChangelogMode(StreamChangelogMode.RETRACT);
        session.setParallelism(2);
        session.setMaxParallelism(4);
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 65_536, 0L, 0, 0,
                65_536L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.retractExactOrdered(
                schema(), Collections.singletonList("id"), "source-v2",
                digest("source"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split"), "position-v1", new byte[] {2}))))
                .uid("source")
                .keyBy("id").uid("key")
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
