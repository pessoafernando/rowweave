package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.StreamCollector;
import io.rowweave.StreamAggregatingState;
import io.rowweave.StreamKeyedProcessFunction;
import io.rowweave.StreamListState;
import io.rowweave.StreamMapState;
import io.rowweave.StreamProcessContext;
import io.rowweave.StreamProcessException;
import io.rowweave.StreamProcessResult;
import io.rowweave.StreamSession;
import io.rowweave.StreamStateCursor;
import io.rowweave.StreamTimerContext;
import io.rowweave.StreamValueState;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamTimerSpec;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** keyed process owns one ordered state/output cut. */
class StreamKeyedProcessOperatorTaskTest {

    @Test
    void terminalEventTimerFiresBeforeEndWithoutProviderWatermark(
            @TempDir Path temporary) throws Exception {
        TimerFunction function = new TimerFunction();
        StreamTestGraph plan = plan(function, 1, false,
                Collections.singletonList(valueState()),
                Collections.singletonList(eventTimer(1)), true);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("payloads"), 262_144L);
        Channels channels = channels(plan, "terminal-event-timer");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                store, Collections.emptyList(), function);
        task.start();
        channels.input.send(record(channels.input, 1L, 17L, 9L, 1L));
        channels.input.send(StreamEnvelope.endOfInput(
                channels.input.channelIdentity(), 2L));
        assertRow(channels.main.receive(), 17L, 9L, 100L);
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                channels.main.receive().kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
        StreamTaskResourceUsage usage = task.resourceUsage();
        assertTrue(usage.complete());
        assertTrue(usage.managedStateBytes() > 0L);
        assertEquals(1, usage.timerHighWaterCount());
        List<StreamCheckpointMember> first = task.triggerCheckpoint(
                checkpoint(plan, "terminal-event-timer"))
                .get(5L, TimeUnit.SECONDS);
        assertEquals(first.get(0).memberId(), task.triggerCheckpoint(
                checkpoint(plan, "terminal-event-timer", 2L))
                .get(5L, TimeUnit.SECONDS).get(0).memberId());
    }

    @Test
    void terminalKeyedProcessAcceptsCheckpointAfterEnd(
            @TempDir Path temporary) throws Exception {
        StreamTestGraph plan = plan(new RunningTotal(), 1, true);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("payloads"), 65_536L);
        Channels channels = channels(plan, "attempt-1");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                store, Collections.emptyList(), new RunningTotal());
        task.start();
        channels.input.send(record(channels.input, 1L, 17L, 2L, 100L));
        assertRow(channels.main.receive(), 17L, 2L, 100L);
        channels.input.send(StreamEnvelope.endOfInput(
                channels.input.channelIdentity(), 2L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                channels.main.receive().kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
        List<StreamCheckpointMember> members = task.triggerCheckpoint(
                checkpoint(plan, "attempt-1")).get(5L, TimeUnit.SECONDS);
        assertEquals(1, members.size());
        assertEquals(members.get(0).memberId(), task.triggerCheckpoint(
                checkpoint(plan, "attempt-1", 2L))
                .get(5L, TimeUnit.SECONDS).get(0).memberId());
    }
    private static final StreamOutputTag REJECTS = StreamOutputTag.of(
            "rejects", schema(), StreamChangelogMode.APPEND,
            Collections.emptyList(), StreamEmissionLimit.of(1, 4_096L),
            StreamEmissionLimit.of(1, 4_096L),
            StreamEmissionLimit.of(1, 4_096L));

    @Test
    void valueStateMainTagAndCheckpointRestoreShareOneMailboxCut(
            @TempDir Path temporary) throws Exception {
        StreamTestGraph plan = plan(
                new RunningTotal(), 1, true);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("payloads"), 65_536L);
        Channels first = channels(plan, "attempt-1");
        StreamKeyedProcessOperatorTask initial = task(
                plan, first, store, Collections.emptyList(),
                new RunningTotal());
        initial.start();

        first.input.send(record(first.input, 1L, 17L, 2L, 100L));
        assertRow(first.main.receive(), 17L, 2L, 100L);
        first.input.send(record(first.input, 2L, 17L, -1L, 101L));
        assertRow(first.tag.receive(), 17L, -1L, 101L);
        first.input.send(record(first.input, 3L, 17L, 3L, 102L));
        assertRow(first.main.receive(), 17L, 5L, 102L);

        CheckpointIdentity checkpoint = checkpoint(plan, "attempt-1");
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                frozen = initial.triggerCheckpoint(checkpoint);
        first.input.send(StreamEnvelope.checkpointBarrier(
                first.input.channelIdentity(), 4L, checkpoint));
        List<StreamCheckpointMember> members = frozen.get(
                5L, TimeUnit.SECONDS);
        assertEquals(1, members.size());
        assertEquals("latest", members.get(0).subjectId());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                first.main.receive().kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                first.tag.receive().kind());
        initial.cancel();
        initial.awaitCompletion(5_000L);
        assertNull(initial.failure());

        Channels second = channels(plan, "attempt-2");
        StreamKeyedProcessOperatorTask restored = task(
                plan, second, store, members, new RunningTotal());
        restored.start();
        second.input.send(record(second.input, 1L, 17L, 4L, 103L));
        assertRow(second.main.receive(), 17L, 9L, 103L);
        restored.cancel();
        restored.awaitCompletion(5_000L);
        assertNull(restored.failure());
    }

    @Test
    void undeclaredStateAndOutputQuotaFailWithStableCodes(
            @TempDir Path temporary) throws Exception {
        assertFailure(temporary.resolve("undeclared"), new UndeclaredState(),
                StreamProcessException.Code.UNDECLARED_STATE);
        assertFailure(temporary.resolve("quota"), new ExcessOutput(),
                StreamProcessException.Code.OUTPUT_QUOTA_EXCEEDED);
    }

    @Test
    void allManagedStateKindsStreamThroughCursorsAndRestoreTogether(
            @TempDir Path temporary) throws Exception {
        List<StreamManagedStateSpec> states = allStates(3);
        StreamTestGraph plan = plan(
                new AllStateFunction(), 1, false, states);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("payloads"), 262_144L);
        Channels first = channels(plan, "all-state-1");
        StreamKeyedProcessOperatorTask initial = task(
                plan, first, store, Collections.emptyList(),
                new AllStateFunction());
        initial.start();
        first.input.send(record(first.input, 1L, 17L, 2L, 100L));
        assertRow(first.main.receive(), 17L, 25L, 100L);
        first.input.send(record(first.input, 2L, 17L, 3L, 101L));
        assertRow(first.main.receive(), 17L, 62L, 101L);

        CheckpointIdentity checkpoint = checkpoint(plan, "all-state-1");
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                frozen = initial.triggerCheckpoint(checkpoint);
        first.input.send(StreamEnvelope.checkpointBarrier(
                first.input.channelIdentity(), 3L, checkpoint));
        List<StreamCheckpointMember> members = frozen.get(
                5L, TimeUnit.SECONDS);
        assertEquals(4, members.size());
        initial.cancel();
        initial.awaitCompletion(5_000L);
        assertNull(initial.failure());

        Channels second = channels(plan, "all-state-2");
        StreamKeyedProcessOperatorTask restored = task(
                plan, second, store, members, new AllStateFunction());
        restored.start();
        second.input.send(record(second.input, 1L, 17L, 4L, 102L));
        assertRow(second.main.receive(), 17L, 111L, 102L);
        restored.cancel();
        restored.awaitCompletion(5_000L);
        assertNull(restored.failure());
    }

    @Test
    void mapEntriesPerKeyQuotaFailsBeforeUnboundedGrowth(
            @TempDir Path temporary) throws Exception {
        StreamKeyedProcessFunction function = new MapQuotaFunction();
        StreamTestGraph plan = plan(function, 1, false,
                allStates(1));
        Channels channels = channels(plan, "map-quota");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                new FileCheckpointPayloadStore(
                        temporary.resolve("payloads"), 262_144L),
                Collections.emptyList(), function);
        task.start();
        channels.input.send(record(channels.input, 1L, 1L, 2L, 10L));
        task.awaitCompletion(5_000L);
        assertNotNull(task.failure());
        assertEquals(StreamProcessException.Code.STATE_QUOTA_EXCEEDED,
                ((StreamProcessException) task.failure()).code());
    }

    @Test
    void eventTimerIsIdempotentCheckpointedAndFiresAfterRestore(
            @TempDir Path temporary) throws Exception {
        TimerFunction function = new TimerFunction();
        StreamTestGraph plan = plan(function, 1, false,
                Collections.singletonList(valueState()),
                Collections.singletonList(eventTimer(1)), true);
        FileCheckpointPayloadStore store = new FileCheckpointPayloadStore(
                temporary.resolve("payloads"), 262_144L);
        Channels first = channels(plan, "timer-1");
        StreamKeyedProcessOperatorTask initial = task(
                plan, first, store, Collections.emptyList(), function);
        initial.start();
        first.input.send(record(first.input, 1L, 17L, 9L, 50L));

        CheckpointIdentity checkpoint = checkpoint(plan, "timer-1");
        java.util.concurrent.CompletableFuture<List<StreamCheckpointMember>>
                frozen = initial.triggerCheckpoint(checkpoint);
        first.input.send(StreamEnvelope.checkpointBarrier(
                first.input.channelIdentity(), 2L, checkpoint));
        List<StreamCheckpointMember> members = frozen.get(
                5L, TimeUnit.SECONDS);
        assertEquals(2, members.size());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                first.main.receive().kind());
        initial.cancel();
        initial.awaitCompletion(5_000L);
        assertNull(initial.failure());

        Channels second = channels(plan, "timer-2");
        StreamKeyedProcessOperatorTask restored = task(
                plan, second, store, members, new TimerFunction());
        restored.start();
        second.input.send(StreamEnvelope.watermark(
                second.input.channelIdentity(), 1L, 99L));
        assertEquals(99L, second.main.receive().timestamp());
        second.input.send(StreamEnvelope.watermark(
                second.input.channelIdentity(), 2L, 100L));
        assertRow(second.main.receive(), 17L, 9L, 100L);
        assertEquals(100L, second.main.receive().timestamp());
        restored.cancel();
        restored.awaitCompletion(5_000L);
        assertNull(restored.failure());
    }

    @Test
    void timerQuotaFailsBeforeSecondDistinctRegistration(
            @TempDir Path temporary) throws Exception {
        StreamKeyedProcessFunction function = new TimerQuotaFunction();
        StreamTestGraph plan = plan(function, 1, false,
                Collections.singletonList(valueState()),
                Collections.singletonList(eventTimer(1)), true);
        Channels channels = channels(plan, "timer-quota");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                new FileCheckpointPayloadStore(
                        temporary.resolve("payloads"), 262_144L),
                Collections.emptyList(), function);
        task.start();
        channels.input.send(record(channels.input, 1L, 1L, 2L, 10L));
        task.awaitCompletion(5_000L);
        assertNotNull(task.failure());
        assertEquals(StreamProcessException.Code.TIMER_QUOTA_EXCEEDED,
                ((StreamProcessException) task.failure()).code());
    }

    @Test
    void timerLeaseIsPerSubtaskAndIsNotMultipliedByParallelism() {
        StreamTestGraph graph = plan(new TimerQuotaFunction(), 1, false,
                Collections.singletonList(valueState()),
                Collections.singletonList(processingTimer()), false,
                1_000, 2);

        StreamExecutionGraph.Node process = graph.executionGraph
                .node("process");
        assertEquals(2, process.parallelism());
        assertEquals(1_000, process.resourceRequest().timerCount());
    }

    @Test
    void processingTimerFiresWithoutAnotherInputOnMailboxThread(
            @TempDir Path temporary) throws Exception {
        ProcessingTimerFunction function = new ProcessingTimerFunction();
        StreamTestGraph plan = plan(function, 1, false,
                Collections.singletonList(valueState()),
                Collections.singletonList(processingTimer()), false);
        Channels channels = channels(plan, "processing-timer");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                new FileCheckpointPayloadStore(
                        temporary.resolve("payloads"), 262_144L),
                Collections.emptyList(), function);
        task.start();
        channels.input.send(record(channels.input, 1L, 23L, 5L, 10L));
        StreamEnvelope fired = channels.main.poll(2_000L);
        assertNotNull(fired);
        assertTrue(!fired.hasEventTimestamp());
        DataRow result = serializer().deserialize(fired.recordFrame());
        assertEquals(23L, result.getLong(0));
        assertEquals(5L, result.getLong(1));
        task.cancel();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
        assertEquals(1, function.callbacks);
        assertEquals(function.processThread, function.timerThread);
    }

    @Test
    void eventTimeStateExpiresAtDeclaredTtlBoundary(
            @TempDir Path temporary) throws Exception {
        EventTtlFunction function = new EventTtlFunction();
        StreamTestGraph plan = plan(function, 1, false,
                Collections.singletonList(eventTtlState()),
                Collections.emptyList(), true);
        Channels channels = channels(plan, "event-ttl");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                new FileCheckpointPayloadStore(
                        temporary.resolve("payloads"), 262_144L),
                Collections.emptyList(), function);
        task.start();
        channels.input.send(record(channels.input, 1L, 17L, 9L, 100L));
        assertRow(channels.main.receive(), 17L, 9L, 100L);
        channels.input.send(StreamEnvelope.watermark(
                channels.input.channelIdentity(), 2L, 109L));
        assertEquals(109L, channels.main.receive().timestamp());
        channels.input.send(record(channels.input, 3L, 17L, 0L, 109L));
        assertRow(channels.main.receive(), 17L, 9L, 109L);
        channels.input.send(StreamEnvelope.watermark(
                channels.input.channelIdentity(), 4L, 110L));
        assertEquals(110L, channels.main.receive().timestamp());
        channels.input.send(record(channels.input, 5L, 17L, 0L, 110L));
        assertRow(channels.main.receive(), 17L, 0L, 110L);
        task.cancel();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void dueTimerCannotReenterTheCurrentFiringBoundary(
            @TempDir Path temporary) throws Exception {
        ReentrantTimerFunction function = new ReentrantTimerFunction();
        StreamTestGraph plan = plan(function, 1, false,
                Collections.singletonList(valueState()),
                Collections.singletonList(eventTimer(2)), true);
        Channels channels = channels(plan, "timer-reentry");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                new FileCheckpointPayloadStore(
                        temporary.resolve("payloads"), 262_144L),
                Collections.emptyList(), function);
        task.start();
        channels.input.send(record(channels.input, 1L, 17L, 1L, 50L));
        channels.input.send(StreamEnvelope.watermark(
                channels.input.channelIdentity(), 2L, 100L));
        task.awaitCompletion(5_000L);
        assertNotNull(task.failure());
        assertEquals(StreamProcessException.Code.TIMER_REENTRANCE_REJECTED,
                ((StreamProcessException) task.failure()).code());
        assertEquals(1, function.callbacks);
    }

    @Test
    void timersFireByTimestampKeyFamilyAndNamespaceAfterDeletion(
            @TempDir Path temporary) throws Exception {
        OrderedTimerFunction function = new OrderedTimerFunction();
        StreamTestGraph plan = plan(function, 1, false,
                Collections.singletonList(valueState()),
                Arrays.asList(
                        StreamTimerSpec.eventTime(
                                "alpha", 4, 1_000L, 1_048_576L),
                        StreamTimerSpec.eventTime(
                                "zeta", 4, 1_000L, 1_048_576L)), true);
        Channels channels = channels(plan, "timer-order");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                new FileCheckpointPayloadStore(
                        temporary.resolve("payloads"), 262_144L),
                Collections.emptyList(), function);
        task.start();
        channels.input.send(record(channels.input, 1L, 17L, 1L, 50L));
        channels.input.send(StreamEnvelope.watermark(
                channels.input.channelIdentity(), 2L, 100L));
        assertRow(channels.main.receive(), 17L, 11L, 100L);
        assertRow(channels.main.receive(), 17L, 12L, 100L);
        assertRow(channels.main.receive(), 17L, 20L, 100L);
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                channels.main.receive().kind());
        task.cancel();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void timerDeclarationMustFitJobResourceBeforeWorkerOpen() {
        assertThrows(UnsupportedOperationException.class,
                () -> plan(new TimerFunction(), 1, false,
                        Collections.singletonList(valueState()),
                        Collections.singletonList(eventTimer(1)),
                        true, 999));
    }

    @Test
    void internalTimerStateIdentityIsReservedAtCompilation() {
        assertThrows(RuntimeException.class,
                () -> plan(new TimerFunction(), 1, false,
                        Collections.singletonList(managed(
                                "__rowweave_timer__/expiry",
                                StreamStateSpec.Kind.KEYED_VALUE, 1)),
                        Collections.singletonList(eventTimer(1)), true));
    }

    private static void assertFailure(Path temporary,
            StreamKeyedProcessFunction function,
            StreamProcessException.Code code) throws Exception {
        StreamTestGraph plan = plan(function, 1, false);
        Channels channels = channels(plan, "attempt-failure");
        StreamKeyedProcessOperatorTask task = task(plan, channels,
                new FileCheckpointPayloadStore(
                        temporary.resolve("payloads"), 65_536L),
                Collections.emptyList(), function);
        task.start();
        channels.input.send(record(
                channels.input, 1L, 1L, 2L, 10L));
        task.awaitCompletion(5_000L);
        assertNotNull(task.failure());
        assertEquals(code, ((StreamProcessException) task.failure()).code());
    }

    private static StreamKeyedProcessOperatorTask task(
            StreamTestGraph plan, Channels channels,
            FileCheckpointPayloadStore store,
            List<StreamCheckpointMember> restore,
            StreamKeyedProcessFunction function) {
        Map<String, io.rowweave.stream.runtime.StreamChannelOutput> outputs =
                new LinkedHashMap<>();
        outputs.put("main-sink", channels.main);
        if (channels.tag != null) outputs.put("tag-sink", channels.tag);
        return new StreamKeyedProcessOperatorTask("test", context(plan),
                channels.input, outputs, ignored -> function, store, restore,
                0, 0, plan.plan.maxParallelism() - 1);
    }

    private static StreamNodeTaskContext context(
            StreamTestGraph plan) {
        return StreamNodeTaskContext.from(plan.artifact.artifactId(),
                plan.executionGraph, plan.process.operatorUid());
    }

    private static Channels channels(
            StreamTestGraph plan, String attempt) {
        LocalStreamChannel input = channel(plan, attempt,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel main = null;
        LocalStreamChannel tag = null;
        for (StreamProductExecutionGraph.SinkRoute sink : plan.sinks) {
            LocalStreamChannel output = channel(plan, attempt,
                    sink.edge.logicalChannelId());
            if (sink.edge.sourceOutputTagId() == null) main = output;
            else tag = output;
        }
        return new Channels(input, main, tag);
    }

    private static LocalStreamChannel channel(
            StreamTestGraph plan, String attempt,
            String channelId) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", attempt, 1L, 1L, channelId, 1L),
                plan.maximumEvents(), plan.maximumFrameBytes(), 8_192L);
    }

    private static StreamEnvelope record(LocalStreamChannel channel,
            long sequence, long id, long value, long timestamp) {
        DataRow row = row(id, value);
        return StreamEnvelope.record(channel.channelIdentity(), sequence,
                timestamp, serializer().serialize(row));
    }

    private static void assertRow(StreamEnvelope envelope,
            long id, long value, long timestamp) {
        assertEquals(StreamEnvelope.Kind.RECORD, envelope.kind());
        assertEquals(timestamp, envelope.timestamp());
        DataRow row = serializer().deserialize(envelope.recordFrame());
        assertEquals(id, row.getLong(0));
        assertEquals(value, row.getLong(1));
    }

    private static RowSerializer serializer() {
        return new RowSerializer(PortablePlanCompiler.toDataSchema(
                schema().toPortableSchema()));
    }

    private static DataRow row(long id, long value) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, value);
        return row;
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

    private static StreamTestGraph plan(
            StreamKeyedProcessFunction function, int maximumOutputs,
            boolean withTag) {
        return plan(function, maximumOutputs, withTag,
                Collections.singletonList(valueState()));
    }

    private static StreamTestGraph plan(
            StreamKeyedProcessFunction function, int maximumOutputs,
            boolean withTag, List<StreamManagedStateSpec> states) {
        return plan(function, maximumOutputs, withTag, states,
                Collections.emptyList(), false);
    }

    private static StreamTestGraph plan(
            StreamKeyedProcessFunction function, int maximumOutputs,
            boolean withTag, List<StreamManagedStateSpec> states,
            List<StreamTimerSpec> timers, boolean eventTime) {
        return plan(function, maximumOutputs, withTag, states, timers,
                eventTime, 10_000);
    }

    private static StreamTestGraph plan(
            StreamKeyedProcessFunction function, int maximumOutputs,
            boolean withTag, List<StreamManagedStateSpec> states,
            List<StreamTimerSpec> timers, boolean eventTime,
            int timerBudget) {
        return plan(function, maximumOutputs, withTag, states, timers,
                eventTime, timerBudget, 1);
    }

    private static StreamTestGraph plan(
            StreamKeyedProcessFunction function, int maximumOutputs,
            boolean withTag, List<StreamManagedStateSpec> states,
            List<StreamTimerSpec> timers, boolean eventTime,
            int timerBudget, int parallelism) {
        StreamSession session = RowWeave.createStreamSession(
                "keyed-process-runtime", "keyed-process-runtime");
        session.setParallelism(parallelism);
        if (eventTime) session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 8_192L, timerBudget, 0,
                65_536L, 8, 64, 1_048_576L));
        StreamProcessSpec spec = StreamProcessSpec.keyed(
                reference(function.getClass().getName()), schema(),
                Collections.emptyList(), states,
                timers,
                StreamEmissionLimit.of(maximumOutputs, 4_096L),
                StreamEmissionLimit.of(1, 4_096L));
        if (withTag) {
            spec = spec.withOutputTags(Collections.singletonList(REJECTS));
        }
        StreamSourceSpec source = eventTime
                ? StreamSourceSpec.exactOrderedEventTime(schema(),
                "source-v1", digest("source"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split"), "sequence-v1", new byte[] {2})),
                StreamWatermarkPolicy.providerEvents(1_000L))
                : StreamSourceSpec.exactOrdered(schema(), "source-v1",
                digest("source"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split"), "sequence-v1", new byte[] {2})));
        io.rowweave.DataStream keyed = session.fromSource(source)
                .uid("source").keyBy("id").uid("key");
        if (withTag) {
            StreamProcessResult result = keyed.keyedProcessWithOutputs(
                    spec, "process");
            result.main().sinkTo("main-sink", sink());
            result.output("rejects").sinkTo("tag-sink", sink());
        } else {
            keyed.keyedProcess(spec).uid("process")
                    .sinkTo("main-sink", sink());
        }
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    private static StreamManagedStateSpec valueState() {
        return managed("latest", StreamStateSpec.Kind.KEYED_VALUE, 1);
    }

    private static StreamManagedStateSpec eventTtlState() {
        return StreamManagedStateSpec.bounded(new StreamStateSpec(
                        "latest", StreamStateSpec.Kind.KEYED_VALUE,
                        "signed-long-v1", 1,
                        StreamStateSpec.TtlMode.EVENT_TIME, 10L),
                1, 1_000L, 1_048_576L, 32L);
    }

    private static List<StreamManagedStateSpec> allStates(int mapEntries) {
        return Arrays.asList(
                managed("latest", StreamStateSpec.Kind.KEYED_VALUE, 1),
                managed("by-value", StreamStateSpec.Kind.KEYED_MAP,
                        mapEntries),
                managed("history", StreamStateSpec.Kind.KEYED_LIST, 3),
                managed("total", StreamStateSpec.Kind.KEYED_AGGREGATING, 1));
    }

    private static StreamManagedStateSpec managed(String name,
            StreamStateSpec.Kind kind, int entriesPerKey) {
        return StreamManagedStateSpec.bounded(new StreamStateSpec(
                        name, kind, "signed-long-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                entriesPerKey, 1_000L, 1_048_576L, 32L);
    }

    private static StreamTimerSpec eventTimer(int perKey) {
        return StreamTimerSpec.eventTime(
                "expiry", perKey, 1_000L, 1_048_576L);
    }

    private static StreamTimerSpec processingTimer() {
        return StreamTimerSpec.processingTime(
                "wake", 1, 1_000L, 1_048_576L);
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", digest("sink"), new byte[] {3});
    }

    private static StreamSchema schema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.LongType, false),
                new StreamField("value", "value", "value",
                        DataType.LongType, false)));
    }

    private static FunctionDeploymentReference reference(String className) {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(digest("deployment"),
                "functions", digest("jar"), className,
                FunctionAbi.KEYED_PROCESS_V1, "config-v1", digest(config),
                config, new FunctionResourceProfile(1_024L, 0L, 8));
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : bytes) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static byte[] encode(long value) {
        return ByteBuffer.allocate(8).putLong(value).array();
    }

    private static long decode(byte[] value) {
        return value == null ? 0L : ByteBuffer.wrap(value).getLong();
    }

    private static final class RunningTotal
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            DataRow key = context.currentKey();
            assertEquals(value.getLong(0), key.getLong(0));
            key.set(0, -1L);
            assertEquals(value.getLong(0), context.currentKey().getLong(0));
            assertTrue(context.hasRecordTimestamp());
            assertTrue(context.currentProcessingTime() > 0L);
            long delta = value.getLong(1);
            if (delta < 0L) {
                output.collect(REJECTS, value);
                return;
            }
            StreamValueState state = context.valueState("latest");
            long total = Math.addExact(decode(state.value()), delta);
            state.update(encode(total));
            output.collect(row(value.getLong(0), total));
        }
    }

    private static final class UndeclaredState
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            context.valueState("missing").value();
        }
    }

    private static final class ExcessOutput
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            output.collect(value);
            output.collect(value);
        }
    }

    private static final class AllStateFunction
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            long delta = value.getLong(1);
            StreamValueState latest = context.valueState("latest");
            long count = decode(latest.value()) + 1L;
            latest.update(encode(count));

            StreamMapState map = context.mapState("by-value");
            map.put(encode(delta), encode(delta * 10L));
            long mapSum = 0L;
            try (StreamStateCursor cursor = map.entries()) {
                while (cursor.advance()) {
                    assertTrue(cursor.hasKey());
                    mapSum += decode(cursor.value());
                }
            }

            StreamListState list = context.listState("history");
            list.add(encode(delta));
            long listSum = 0L;
            try (StreamStateCursor cursor = list.values()) {
                while (cursor.advance()) {
                    assertTrue(!cursor.hasKey());
                    listSum += decode(cursor.value());
                }
            }

            StreamAggregatingState total =
                    context.aggregatingState("total");
            long aggregate = decode(total.value()) + delta;
            total.update(encode(aggregate));
            output.collect(row(value.getLong(0),
                    count + mapSum + listSum + aggregate));
        }
    }

    private static final class MapQuotaFunction
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            StreamMapState map = context.mapState("by-value");
            map.put(encode(1L), encode(1L));
            map.put(encode(2L), encode(2L));
        }
    }

    private static final class TimerFunction
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            context.valueState("latest").update(encode(value.getLong(1)));
            context.timerService().registerTimer(
                    "expiry", encode(7L), 100L);
            context.timerService().registerTimer(
                    "expiry", encode(7L), 100L);
            context.timerService().deleteTimer(
                    "expiry", encode(8L), 100L);
        }

        @Override
        public void onTimer(long timestamp, StreamTimerContext context,
                StreamCollector output) {
            assertEquals("expiry", context.timerFamilyId());
            assertEquals(StreamTimerSpec.TimeDomain.EVENT_TIME,
                    context.timeDomain());
            assertEquals(timestamp, context.timerTimestamp());
            assertEquals(7L, decode(context.timerNamespace()));
            output.collect(row(context.currentKey().getLong(0),
                    decode(context.valueState("latest").value())));
        }
    }

    private static final class TimerQuotaFunction
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            context.timerService().registerTimer("expiry", encode(1L), 100L);
            context.timerService().registerTimer("expiry", encode(2L), 101L);
        }
    }

    private static final class ProcessingTimerFunction
            implements StreamKeyedProcessFunction {
        private volatile Thread processThread;
        private volatile Thread timerThread;
        private volatile int callbacks;

        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            processThread = Thread.currentThread();
            context.valueState("latest").update(encode(value.getLong(1)));
            context.timerService().registerTimer("wake", new byte[0],
                    context.currentProcessingTime() + 50L);
        }

        @Override
        public void onTimer(long timestamp, StreamTimerContext context,
                StreamCollector output) {
            timerThread = Thread.currentThread();
            callbacks++;
            assertEquals(StreamTimerSpec.TimeDomain.PROCESSING_TIME,
                    context.timeDomain());
            output.collect(row(context.currentKey().getLong(0),
                    decode(context.valueState("latest").value())));
        }
    }

    private static final class EventTtlFunction
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            StreamValueState state = context.valueState("latest");
            if (value.getLong(1) != 0L) {
                state.update(encode(value.getLong(1)));
            }
            output.collect(row(value.getLong(0), decode(state.value())));
        }
    }

    private static final class ReentrantTimerFunction
            implements StreamKeyedProcessFunction {
        private volatile int callbacks;

        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            context.timerService().registerTimer(
                    "expiry", encode(1L), 100L);
        }

        @Override
        public void onTimer(long timestamp, StreamTimerContext context,
                StreamCollector output) {
            callbacks++;
            if (callbacks == 1) {
                context.timerService().registerTimer(
                        "expiry", encode(1L), timestamp);
            } else {
                throw new AssertionError("timer reentered its own boundary");
            }
        }
    }

    private static final class OrderedTimerFunction
            implements StreamKeyedProcessFunction {
        @Override
        public void processElement(DataRow value,
                StreamProcessContext context, StreamCollector output) {
            context.timerService().registerTimer("alpha", encode(2L), 100L);
            context.timerService().registerTimer("zeta", encode(0L), 100L);
            context.timerService().registerTimer("alpha", encode(1L), 100L);
            context.timerService().registerTimer("alpha", encode(9L), 99L);
            context.timerService().deleteTimer("alpha", encode(9L), 99L);
        }

        @Override
        public void onTimer(long timestamp, StreamTimerContext context,
                StreamCollector output) {
            long prefix = "alpha".equals(context.timerFamilyId()) ? 10L : 20L;
            output.collect(row(context.currentKey().getLong(0),
                    prefix + decode(context.timerNamespace())));
        }
    }

    private static final class Channels {
        final LocalStreamChannel input;
        final LocalStreamChannel main;
        final LocalStreamChannel tag;

        private Channels(LocalStreamChannel input,
                LocalStreamChannel main, LocalStreamChannel tag) {
            this.input = input;
            this.main = main;
            this.tag = tag;
        }
    }
}
