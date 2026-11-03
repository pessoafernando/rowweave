package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamBroadcastProcessContext;
import io.rowweave.StreamCollector;
import io.rowweave.StreamKeyedBroadcastProcessContext;
import io.rowweave.StreamKeyedBroadcastProcessFunction;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelOutput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** immutable broadcast snapshot gates keyed callbacks. */
class StreamKeyedBroadcastProcessOperatorTaskTest {

    @Test
    void staticBroadcastSideCompletesBeforeQueuedKeyedRecordsRun()
            throws Exception {
        StreamTestGraph plan = StreamTestGraph.from(
                StreamProductKeyedCoProcessPlanAdmissionTest
                        .broadcastSession().toPlanArtifact());
        RowSerializer leftSerializer = serializer(node(plan, "left-key"));
        RowSerializer rightSerializer = serializer(node(plan, "right"));
        LocalStreamChannel left = channel("left-input");
        LocalStreamChannel right = channel("right-input");
        RecordingOutput output = new RecordingOutput();
        Map<String, StreamChannelOutput> outputs = new LinkedHashMap<>();
        outputs.put(outputUid(plan), output);
        StreamKeyedBroadcastProcessFunction function = function();
        StreamKeyedProcessOperatorTask task =
                new StreamKeyedProcessOperatorTask("broadcast",
                        context(plan),
                        Arrays.asList(left, right), outputs,
                        ignored -> function, null, Collections.emptyList(),
                        0, 0, plan.plan.maxParallelism() - 1);

        left.send(StreamEnvelope.record(left.channelIdentity(), 1L,
                leftSerializer.serialize(row(7L, "gallery"))));
        right.send(StreamEnvelope.record(right.channelIdentity(), 1L,
                rightSerializer.serialize(row(7L, "approved"))));
        right.send(StreamEnvelope.endOfInput(right.channelIdentity(), 2L));
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 2L));
        task.start();
        task.awaitCompletion(2_000L);

        assertNull(task.failure());
        DataRow result = serializer(plan.coProcess).deserialize(
                output.events.get(0).recordFrame());
        assertEquals("approved:gallery", result.getString(1));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.events.get(1).kind());
    }

    @Test
    void checkpointRestoresBroadcastAndCompletedInputGate(
            @TempDir Path temporary) throws Exception {
        StreamTestGraph plan = StreamTestGraph.from(
                StreamProductKeyedCoProcessPlanAdmissionTest
                        .broadcastSession().toPlanArtifact());
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("payloads"), 1_048_576L);
        LocalStreamChannel left = channel("left-before");
        LocalStreamChannel right = channel("right-before");
        RecordingOutput beforeOutput = new RecordingOutput();
        StreamKeyedProcessOperatorTask before = task(plan, left, right,
                beforeOutput, store, Collections.emptyList());
        RowSerializer rightSerializer = serializer(node(plan, "right"));

        right.send(StreamEnvelope.record(right.channelIdentity(), 1L,
                rightSerializer.serialize(row(7L, "approved"))));
        right.send(StreamEnvelope.endOfInput(right.channelIdentity(), 2L));
        before.start();
        CheckpointIdentity checkpoint = checkpoint(plan);
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                checkpointMembers = before.triggerCheckpoint(checkpoint);
        left.send(StreamEnvelope.checkpointBarrier(
                left.channelIdentity(), 1L, checkpoint));
        List<StreamCheckpointMember> members = checkpointMembers.get(
                2L, TimeUnit.SECONDS);
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 2L));
        before.awaitCompletion(2_000L);

        assertNull(before.failure());
        assertEquals(4, members.size());
        assertTrue(members.stream().anyMatch(member ->
                "__rowweave_gate__/inputs".equals(member.subjectId())));

        LocalStreamChannel restoredLeft = channel("left-after");
        LocalStreamChannel restoredRight = channel("right-after");
        RecordingOutput afterOutput = new RecordingOutput();
        StreamKeyedProcessOperatorTask replacement = task(
                plan, restoredLeft, restoredRight, afterOutput,
                store, members);
        RowSerializer leftSerializer = serializer(node(plan, "left-key"));
        restoredLeft.send(StreamEnvelope.record(
                restoredLeft.channelIdentity(), 1L,
                leftSerializer.serialize(row(7L, "gallery"))));
        restoredLeft.send(StreamEnvelope.endOfInput(
                restoredLeft.channelIdentity(), 2L));
        replacement.start();
        replacement.awaitCompletion(2_000L);

        assertNull(replacement.failure());
        DataRow result = serializer(plan.coProcess).deserialize(
                afterOutput.events.get(0).recordFrame());
        assertEquals("approved:gallery", result.getString(1));
    }

    @Test
    void parallelBroadcastReplicasHaveOneLogicalSnapshotDigest(
            @TempDir Path temporary) throws Exception {
        StreamTestGraph plan = StreamTestGraph.from(
                StreamProductKeyedCoProcessPlanAdmissionTest
                        .broadcastSession().toPlanArtifact());
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("replica-payloads"), 1_048_576L);

        List<StreamCheckpointMember> first = checkpointReplica(
                plan, store, 0, 0, 63, "first", false);
        List<StreamCheckpointMember> second = checkpointReplica(
                plan, store, 1, 64, 127, "second", true);

        assertEquals(broadcastMember(first).contentDigest(),
                broadcastMember(second).contentDigest());
        assertEquals(broadcastMember(first).contentBytes(),
                broadcastMember(second).contentBytes());
    }

    @Test
    void replayableChangelogUpdatesAreVisibleAtMailboxOrder()
            throws Exception {
        StreamTestGraph plan = StreamTestGraph.from(
                StreamProductKeyedCoProcessPlanAdmissionTest
                        .changelogBroadcastSession().toPlanArtifact());
        LocalStreamChannel left = channel("left-changelog");
        LocalStreamChannel right = channel("right-changelog");
        RecordingOutput output = new RecordingOutput();
        Map<String, StreamChannelOutput> outputs = new LinkedHashMap<>();
        outputs.put(outputUid(plan), output);
        Semaphore applied = new Semaphore(0);
        StreamKeyedBroadcastProcessFunction function =
                new StreamKeyedBroadcastProcessFunction() {
                    private final StreamKeyedBroadcastProcessFunction delegate =
                            function();

                    @Override public void processKeyed(DataRow value,
                            StreamKeyedBroadcastProcessContext context,
                            StreamCollector collector) {
                        delegate.processKeyed(value, context, collector);
                    }

                    @Override public void processBroadcast(DataRow value,
                            StreamBroadcastProcessContext context,
                            StreamCollector collector) {
                        delegate.processBroadcast(value, context, collector);
                        applied.release();
                    }
                };
        StreamKeyedProcessOperatorTask task =
                new StreamKeyedProcessOperatorTask("changelog",
                        context(plan),
                        Arrays.asList(left, right), outputs,
                        ignored -> function, null, Collections.emptyList(),
                        0, 0, plan.plan.maxParallelism() - 1);
        RowSerializer leftSerializer = serializer(node(plan, "left-key"));
        RowSerializer rightSerializer = serializer(node(plan, "right"));

        task.start();
        right.send(StreamEnvelope.record(right.channelIdentity(), 1L,
                rightSerializer.serialize(row(7L, "approved"))));
        assertTrue(applied.tryAcquire(2L, TimeUnit.SECONDS));
        left.send(StreamEnvelope.record(left.channelIdentity(), 1L,
                leftSerializer.serialize(row(7L, "gallery"))));
        output.awaitEvents(1, 2_000L);
        right.send(StreamEnvelope.record(right.channelIdentity(), 2L,
                rightSerializer.serialize(row(7L, "denied"))));
        assertTrue(applied.tryAcquire(2L, TimeUnit.SECONDS));
        left.send(StreamEnvelope.record(left.channelIdentity(), 2L,
                leftSerializer.serialize(row(7L, "refund"))));
        right.send(StreamEnvelope.endOfInput(right.channelIdentity(), 3L));
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 3L));
        task.awaitCompletion(2_000L);

        assertNull(task.failure());
        assertEquals("approved:gallery", serializer(plan.coProcess)
                .deserialize(output.events.get(0).recordFrame())
                .getString(1));
        assertEquals("denied:refund", serializer(plan.coProcess)
                .deserialize(output.events.get(1).recordFrame())
                .getString(1));
    }

    private static List<StreamCheckpointMember> checkpointReplica(
            StreamTestGraph plan,
            FileCheckpointPayloadStore store, int subtask,
            int keyGroupStart, int keyGroupEnd, String suffix,
            boolean reverse)
            throws Exception {
        LocalStreamChannel left = channel("left-" + suffix);
        LocalStreamChannel right = channel("right-" + suffix);
        RecordingOutput output = new RecordingOutput();
        Map<String, StreamChannelOutput> outputs = new LinkedHashMap<>();
        outputs.put(outputUid(plan), output);
        StreamKeyedProcessOperatorTask task =
                new StreamKeyedProcessOperatorTask(suffix, context(plan),
                        Arrays.asList(left, right), outputs,
                        ignored -> function(), store,
                        Collections.emptyList(), subtask,
                        keyGroupStart, keyGroupEnd);
        RowSerializer rightSerializer = serializer(node(plan, "right"));
        DataRow first = reverse ? row(8L, "review")
                : row(7L, "approved");
        DataRow second = reverse ? row(7L, "approved")
                : row(8L, "review");
        right.send(StreamEnvelope.record(right.channelIdentity(), 1L,
                rightSerializer.serialize(first)));
        right.send(StreamEnvelope.record(right.channelIdentity(), 2L,
                rightSerializer.serialize(second)));
        right.send(StreamEnvelope.endOfInput(right.channelIdentity(), 3L));
        task.start();
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                checkpoint = task.triggerCheckpoint(checkpoint(plan));
        left.send(StreamEnvelope.checkpointBarrier(
                left.channelIdentity(), 1L, checkpoint(plan)));
        List<StreamCheckpointMember> members = checkpoint.get(
                2L, TimeUnit.SECONDS);
        left.send(StreamEnvelope.endOfInput(left.channelIdentity(), 2L));
        task.awaitCompletion(2_000L);
        assertNull(task.failure());
        return members;
    }

    private static StreamCheckpointMember broadcastMember(
            List<StreamCheckpointMember> members) {
        return members.stream().filter(member ->
                "__rowweave_broadcast_digest__/rules".equals(
                        member.subjectId()))
                .findFirst().orElseThrow(AssertionError::new);
    }

    private static StreamKeyedProcessOperatorTask task(
            StreamTestGraph plan, LocalStreamChannel left,
            LocalStreamChannel right, RecordingOutput output,
            FileCheckpointPayloadStore store,
            List<StreamCheckpointMember> restore) {
        Map<String, StreamChannelOutput> outputs = new LinkedHashMap<>();
        outputs.put(outputUid(plan), output);
        return new StreamKeyedProcessOperatorTask("broadcast", context(plan),
                Arrays.asList(left, right), outputs, ignored -> function(),
                store, restore, 0, 0, plan.plan.maxParallelism() - 1);
    }

    private static StreamNodeTaskContext context(
            StreamTestGraph plan) {
        return StreamNodeTaskContext.from(plan.artifact.artifactId(),
                plan.executionGraph, plan.coProcess.operatorUid());
    }

    private static String outputUid(StreamTestGraph plan) {
        return context(plan).outputs().get(0).consumer().operatorUid();
    }

    private static StreamKeyedBroadcastProcessFunction function() {
        return new StreamKeyedBroadcastProcessFunction() {
            @Override public void processKeyed(DataRow value,
                    StreamKeyedBroadcastProcessContext context,
                    StreamCollector collector) {
                DataRow result = new DataRow(2);
                result.set(0, value.getLong(0));
                byte[] rule = context.broadcastState("rules")
                        .get(longBytes(value.getLong(0)));
                result.set(1, rule == null ? "missing"
                        : text(rule) + ':' + value.getString(1));
                collector.collect(result);
            }

            @Override public void processBroadcast(DataRow value,
                    StreamBroadcastProcessContext context,
                    StreamCollector collector) {
                context.broadcastState("rules").put(
                        longBytes(value.getLong(0)),
                        bytes(value.getString(1)));
            }
        };
    }

    private static CheckpointIdentity checkpoint(
            StreamTestGraph plan) {
        return new CheckpointIdentity(plan.artifact.definitionId(),
                plan.artifact.artifactId(), "run", "attempt", 1L, 1L,
                "coordinator", 1L, plan.commandDigest,
                plan.artifact.artifactId(), 1, 1);
    }

    private static DataRow row(long key, String value) {
        DataRow row = new DataRow(2);
        row.set(0, key);
        row.set(1, value);
        return row;
    }

    private static byte[] longBytes(long value) {
        return ByteBuffer.allocate(Long.BYTES).putLong(value).array();
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
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
            notifyAll();
        }

        synchronized void awaitEvents(int count, long timeoutMillis)
                throws InterruptedException {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            while (events.size() < count) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) throw new AssertionError(
                        "timed out waiting for output events");
                wait(Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remaining)));
            }
        }

        @Override public void cancel() { }

        @Override public void fail(Throwable cause) { }

        @Override public void close() { }
    }
}
