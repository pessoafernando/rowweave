package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamCollector;
import io.rowweave.StreamKeyedCoProcessFunction;
import io.rowweave.StreamProcessContext;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelOutput;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** both keyed inputs execute in one state-owning mailbox. */
class StreamKeyedCoProcessOperatorTaskTest {

    @Test
    void distinctInputSchemasShareManagedStateForTheCanonicalKey()
            throws Exception {
        StreamTestGraph plan = StreamTestGraph.from(
                StreamProductKeyedCoProcessPlanAdmissionTest.session()
                        .toPlanArtifact());
        PortableStreamNode leftKey = node(plan, "left-key");
        PortableStreamNode rightKey = node(plan, "right-key");
        RowSerializer leftSerializer = serializer(leftKey);
        RowSerializer rightSerializer = serializer(rightKey);
        LocalStreamChannel left = channel("left-input");
        LocalStreamChannel right = channel("right-input");
        RecordingOutput output = new RecordingOutput();
        Map<String, StreamChannelOutput> outputs = new LinkedHashMap<>();
        outputs.put("sink", output);
        StreamKeyedCoProcessFunction function =
                new StreamKeyedCoProcessFunction() {
                    @Override public void processLeft(DataRow value,
                            StreamProcessContext context,
                            StreamCollector collector) {
                        context.mapState("shared").put(bytes("left"),
                                bytes(value.getString(1)));
                    }

                    @Override public void processRight(DataRow value,
                            StreamProcessContext context,
                            StreamCollector collector) {
                        DataRow result = new DataRow(2);
                        result.set(0, value.getLong(0));
                        result.set(1, text(context.mapState("shared")
                                .get(bytes("left"))) + ':'
                                + value.getString(1));
                        collector.collect(result);
                    }
                };
        StreamKeyedProcessOperatorTask task =
                new StreamKeyedProcessOperatorTask("co-process",
                        context(plan),
                        Arrays.asList(left, right), outputs,
                        ignored -> function, null, Collections.emptyList(),
                        0, 0, plan.plan.maxParallelism() - 1);

        task.start();
        left.send(StreamEnvelope.record(left.channelIdentity(), 1L,
                leftSerializer.serialize(row(7L, "L"))));
        right.send(StreamEnvelope.record(right.channelIdentity(), 1L,
                rightSerializer.serialize(row(7L, "R"))));
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 2L));
        right.send(StreamEnvelope.endOfInput(right.channelIdentity(), 2L));
        task.awaitCompletion(2_000L);

        assertNull(task.failure());
        assertEquals(2, output.events.size());
        DataRow result = serializer(plan.coProcess).deserialize(
                output.events.get(0).recordFrame());
        assertEquals(7L, result.getLong(0));
        assertEquals("L:R", result.getString(1));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.events.get(1).kind());
    }

    private static StreamNodeTaskContext context(
            StreamTestGraph plan) {
        return StreamNodeTaskContext.from(plan.artifact.artifactId(),
                plan.executionGraph, plan.coProcess.operatorUid());
    }

    private static DataRow row(long key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return value == null ? "missing"
                : new String(value, StandardCharsets.UTF_8);
    }

    private static RowSerializer serializer(PortableStreamNode node) {
        return new RowSerializer(PortablePlanCompiler.toDataSchema(
                node.outputSchema()));
    }

    private static PortableStreamNode node(StreamTestGraph plan,
            String uid) {
        for (PortableStreamNode node : plan.plan.nodes()) {
            if (node.operatorUid().equals(uid)) return node;
        }
        throw new AssertionError("missing node " + uid);
    }

    private static LocalStreamChannel channel(String channelId) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, channelId, 1L),
                16, 4_096, 65_536L);
    }

    private static final class RecordingOutput
            implements StreamChannelOutput {
        final StreamChannelIdentity identity = new StreamChannelIdentity(
                "run", "attempt", 1L, 1L, "output", 1L);
        final List<StreamEnvelope> events = new ArrayList<>();

        @Override public StreamChannelIdentity channelIdentity() {
            return identity;
        }

        @Override public synchronized void send(StreamEnvelope envelope) {
            events.add(envelope);
        }

        @Override public void cancel() { }

        @Override public void fail(Throwable cause) { }

        @Override public void close() { }
    }
}
