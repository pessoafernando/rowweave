package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.StreamWindowAggregateFunction;
import io.rowweave.StreamWindowCollector;
import io.rowweave.StreamWindowContext;
import io.rowweave.StreamWindowProcessFunction;
import io.rowweave.StreamWindowDataRowCursor;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamAccumulationMode;
import io.rowweave.stream.api.StreamWindowAssignerSpec;
import io.rowweave.stream.api.StreamWindowTriggerSpec;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamStateDescriptor;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.FinalEventTimeDecimalWindow;
import io.rowweave.stream.runtime.LocalStreamChannel;
import io.rowweave.stream.runtime.StreamChannelInput;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** generic aggregate matches the decimal window fast path. */
class StreamGenericWindowAggregateOperatorTaskTest {

    @Test
    void terminalEventWindowFiresWithoutProviderFinalWatermark(
            @TempDir Path temp) throws Exception {
        StreamTestGraph plan = StreamGenericWindowProductAdmissionTest.plan();
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(temp.resolve("payloads"),
                        1_048_576L);
        StreamGenericWindowOperatorTask task = task(plan, input, output,
                payloads, null, "terminal-event-window");
        task.start();
        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 1L));
        input.send(StreamEnvelope.endOfInput(input.channelIdentity(), 2L));
        DataRow result = row(plan, output.poll(5_000L));
        assertEquals(new BigDecimal("1.0000000000"),
                result.getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
        StreamCheckpointMember first = task.triggerCheckpoint(
                checkpoint(plan, "attempt")).get(5L, TimeUnit.SECONDS);
        assertNotNull(first);
    }

    @Test
    void terminalCountWindowCanCheckpointAfterEnd(@TempDir Path temp)
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.countTumbling(3L),
                        StreamWindowTriggerSpec.count(2L),
                        StreamAccumulationMode.DISCARDING, 0L, false);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        FileCheckpointPayloadStore payloads =
                new FileCheckpointPayloadStore(temp.resolve("payloads"),
                        1_048_576L);
        StreamGenericWindowOperatorTask task = task(plan, input, output,
                payloads, null, "terminal-count-window");
        task.start();
        input.send(recordWithoutTimestamp(plan, input, 1L,
                "A", BigDecimal.ONE));
        input.send(StreamEnvelope.endOfInput(input.channelIdentity(), 2L));
        assertEquals(StreamEnvelope.Kind.RECORD, output.poll(5_000L).kind());
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
        StreamCheckpointMember first = task.triggerCheckpoint(
                checkpoint(plan, "attempt")).get(5L, TimeUnit.SECONDS);
        assertNotNull(first);
    }

    @Test
    void eventTimeTumblingAggregateMatchesDecimalOracle(@TempDir Path temp)
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan();
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask task =
                new StreamGenericWindowOperatorTask("generic",
                        context(plan), input,
                        Collections.singletonMap(
                                plan.onlySink().node.operatorUid(), output),
                        ignored -> new DecimalAggregate(), null, null,
                        0, 0, plan.plan.maxParallelism() - 1);
        task.start();

        input.send(record(plan, input, 1L, "A",
                new BigDecimal("1.00000000006"), 10L));
        input.send(record(plan, input, 2L, "A",
                new BigDecimal("2.00000000005"), 20L));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 3L, 100L));

        StreamEnvelope resultEnvelope = output.receive();
        DataRow result = new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.genericWindow.outputSchema())).deserialize(
                resultEnvelope.recordFrame());
        assertEquals(StreamEnvelope.Kind.RECORD, resultEnvelope.kind());
        assertFalse(resultEnvelope.hasEventTimestamp(),
                "generic fast-path parity must not invent an output timestamp");
        assertEquals("A", result.getString(0));

        List<FinalEventTimeDecimalWindow.Result> oracle = new ArrayList<>();
        try (FinalEventTimeDecimalWindow legacy =
                     new FinalEventTimeDecimalWindow(
                             "artifact", "legacy-window",
                             decimalDescriptor(), 1, 0, 0,
                             FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                             100L, 100L, 1, 8,
                             new MemoryManager(512L),
                             temp.resolve("legacy-spill"), 32_768L,
                             1_024, 4_096, oracle::add)) {
            legacy.process("A".getBytes(StandardCharsets.UTF_8),
                    new BigDecimal("1.00000000006"), 10L);
            legacy.process("A".getBytes(StandardCharsets.UTF_8),
                    new BigDecimal("2.00000000005"), 20L);
            legacy.advanceWatermark(100L);
        }

        assertEquals(oracle.get(0).windowStart(), result.getLong(1));
        assertEquals(oracle.get(0).windowEnd(), result.getLong(2));
        assertEquals(oracle.get(0).sum(), result.getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.receive().kind());
        StreamTaskResourceUsage usage = task.resourceUsage();
        assertTrue(usage.complete());
        assertTrue(usage.managedStateBytes() > 0L);
        assertEquals(1, usage.timerHighWaterCount());
        assertEquals(1, usage.windowHighWaterCount());

        task.cancel();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void eventTimeSlidingAssignsOneRowToEveryOverlappingWindow()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.eventTimeSliding(
                                "event-time", 100L, 50L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING, 0L, false);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask task = task(plan, input, output,
                null, null, "event-sliding");
        task.start();
        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 75L));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 2L, 100L));
        DataRow first = row(plan, output.poll(5_000L));
        assertEquals(0L, first.getLong(1));
        assertEquals(100L, first.getLong(2));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.poll(5_000L).kind());
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 3L, 150L));
        DataRow second = row(plan, output.poll(5_000L));
        assertEquals(50L, second.getLong(1));
        assertEquals(150L, second.getLong(2));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.poll(5_000L).kind());
        task.cancel();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void checkpointRestoresAccumulatorNamespaceAndWatermark(@TempDir Path temp)
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan();
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temp.resolve("payloads"), 65_536L);
        LocalStreamChannel firstInput = channel(plan, "attempt-1",
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel firstOutput = channel(plan, "attempt-1",
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask initial = task(plan, firstInput,
                firstOutput, payloads, null, "initial");
        initial.start();
        firstInput.send(record(plan, firstInput, 1L, "A",
                new BigDecimal("3"), 10L));
        firstInput.send(StreamEnvelope.watermark(
                firstInput.channelIdentity(), 2L, 50L));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                firstOutput.receive().kind());

        CheckpointIdentity checkpoint = checkpoint(plan, "attempt-1");
        java.util.concurrent.CompletableFuture<StreamCheckpointMember> frozen =
                initial.triggerCheckpoint(checkpoint);
        firstInput.send(StreamEnvelope.checkpointBarrier(
                firstInput.channelIdentity(), 3L, checkpoint));
        StreamCheckpointMember member = frozen.get(5L, TimeUnit.SECONDS);
        assertEquals(StreamCheckpointMember.Kind.STATE, member.kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                firstOutput.receive().kind());
        initial.cancel();
        initial.awaitCompletion(5_000L);
        assertNull(initial.failure());

        LocalStreamChannel nextInput = channel(plan, "attempt-2",
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel nextOutput = channel(plan, "attempt-2",
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask restored = task(plan, nextInput,
                nextOutput, payloads, member, "restored");
        restored.start();
        nextInput.send(record(plan, nextInput, 1L, "A",
                new BigDecimal("4"), 60L));
        nextInput.send(StreamEnvelope.watermark(
                nextInput.channelIdentity(), 2L, 100L));
        DataRow result = new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.genericWindow.outputSchema())).deserialize(
                nextOutput.receive().recordFrame());
        assertEquals(new BigDecimal("7.0000000000"),
                result.getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                nextOutput.receive().kind());
        restored.cancel();
        restored.awaitCompletion(5_000L);
        assertNull(restored.failure());

        LocalStreamChannel lateInput = channel(plan, "attempt-3",
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel lateOutput = channel(plan, "attempt-3",
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask lateRestore = task(plan, lateInput,
                lateOutput, payloads, member, "late-restored");
        lateRestore.start();
        lateInput.send(record(plan, lateInput, 1L, "A",
                BigDecimal.ONE, -10L));
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(5L);
        while (lateRestore.failure() == null
                && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertEquals(IllegalStateException.class,
                lateRestore.failure().getClass(),
                "restored watermark must reject an already-cleaned window");
        lateRestore.awaitCompletion(5_000L);
    }

    @Test
    void accumulatingLateRecordRefiresUntilDeterministicCleanup()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamAccumulationMode.ACCUMULATING, 50L, false);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask task = task(plan, input, output,
                null, null, "late-accumulating");
        task.start();
        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 10L));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 2L, 100L));
        assertEquals(new BigDecimal("1.0000000000"),
                amount(plan, output.receive()));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.receive().kind());

        input.send(record(plan, input, 3L, "A",
                new BigDecimal("2"), 20L));
        assertEquals(new BigDecimal("3.0000000000"),
                amount(plan, output.receive()));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 4L, 150L));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.receive().kind());

        input.send(record(plan, input, 5L, "A", BigDecimal.TEN, 20L));
        awaitFailure(task);
        assertEquals(IllegalStateException.class, task.failure().getClass());
        task.awaitCompletion(5_000L);
    }

    @Test
    void expiredRecordUsesDeclaredLatePortWithoutMutatingMain()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamAccumulationMode.DISCARDING, 10L, true);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        Map<String, StreamChannelOutputPair> channels = new LinkedHashMap<>();
        Map<String, io.rowweave.stream.runtime.StreamChannelOutput> outputs =
                new LinkedHashMap<>();
        for (StreamProductExecutionGraph.SinkRoute sink : plan.sinks) {
            LocalStreamChannel output = channel(plan,
                    sink.edge.logicalChannelId());
            channels.put(sink.edge.sourceOutputTagId() == null
                    ? "main" : sink.edge.sourceOutputTagId(),
                    new StreamChannelOutputPair(output));
            outputs.put(sink.node.operatorUid(), output);
        }
        StreamGenericWindowOperatorTask task =
                new StreamGenericWindowOperatorTask("late-tag",
                        context(plan), input,
                        outputs, ignored -> new DecimalAggregate(),
                        null, null, 0, 0,
                        plan.plan.maxParallelism() - 1);
        task.start();
        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 10L));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 2L, 110L));
        LocalStreamChannel main = channels.get("main").channel;
        LocalStreamChannel late = channels.get("late").channel;
        assertEquals(new BigDecimal("1.0000000000"),
                amount(plan, main.receive()));
        assertEquals(StreamEnvelope.Kind.WATERMARK, main.receive().kind());
        assertEquals(StreamEnvelope.Kind.WATERMARK, late.receive().kind());

        input.send(record(plan, input, 3L, "A", BigDecimal.TEN, 20L));
        StreamEnvelope routed = late.receive();
        DataRow lateRow = new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.keyBy.outputSchema())).deserialize(
                routed.recordFrame());
        assertEquals(BigDecimal.TEN, lateRow.getBigDecimal(2));
        assertNull(task.failure());
        task.cancel();
        task.awaitCompletion(5_000L);
    }

    @Test
    void countTriggerDiscardsPanesAndEndFlushesPartialWindow()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.countTumbling(3L),
                        StreamWindowTriggerSpec.count(2L),
                        StreamAccumulationMode.DISCARDING, 0L, false);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask task = task(plan, input, output,
                null, null, "count-window");
        task.start();
        for (int value = 1; value <= 5; value++) {
            input.send(recordWithoutTimestamp(plan, input, value,
                    "A", new BigDecimal(value)));
        }
        input.send(StreamEnvelope.endOfInput(
                input.channelIdentity(), 6L));

        DataRow first = row(plan, output.receive());
        DataRow second = row(plan, output.receive());
        DataRow third = row(plan, output.receive());
        assertEquals(Arrays.asList(0L, 3L,
                        new BigDecimal("3.0000000000")),
                Arrays.asList(first.getLong(1), first.getLong(2),
                        first.getBigDecimal(3)));
        assertEquals(Arrays.asList(0L, 3L,
                        new BigDecimal("3.0000000000")),
                Arrays.asList(second.getLong(1), second.getLong(2),
                        second.getBigDecimal(3)));
        assertEquals(Arrays.asList(3L, 6L,
                        new BigDecimal("9.0000000000")),
                Arrays.asList(third.getLong(1), third.getLong(2),
                        third.getBigDecimal(3)));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.receive().kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void processingTimeTriggerRunsFromMailboxClockWithoutDuplicateFinal()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.processingTimeTumbling(100L),
                        StreamWindowTriggerSpec.processingTime(20L),
                        StreamAccumulationMode.ACCUMULATING, 0L, false);
        LocalStreamChannel inputLane = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        StreamChannelInput input = new TransportNeutralInput(inputLane);
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        AtomicLong clock = new AtomicLong(1_000L);
        Semaphore added = new Semaphore(0);
        StreamGenericWindowOperatorTask task =
                new StreamGenericWindowOperatorTask("processing-window",
                        context(plan), input, Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), output),
                        ignored -> new DecimalAggregate(added::release),
                        null, null,
                        0, 0, plan.plan.maxParallelism() - 1, clock::get);
        task.start();
        inputLane.send(recordWithoutTimestamp(
                plan, inputLane, 1L, "A", BigDecimal.ONE));
        assertTrue(added.tryAcquire(5L, TimeUnit.SECONDS));
        clock.set(1_020L);
        StreamEnvelope first = output.poll(5_000L);
        assertNotNull(first);
        assertEquals(new BigDecimal("1.0000000000"), amount(plan, first));

        clock.set(1_025L);
        inputLane.send(recordWithoutTimestamp(
                plan, inputLane, 2L, "A", new BigDecimal("2")));
        assertTrue(added.tryAcquire(5L, TimeUnit.SECONDS));
        clock.set(1_040L);
        StreamEnvelope second = output.poll(5_000L);
        assertNotNull(second);
        assertEquals(new BigDecimal("3.0000000000"), amount(plan, second));

        clock.set(1_100L);
        inputLane.send(StreamEnvelope.endOfInput(
                inputLane.channelIdentity(), 3L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind(),
                "unchanged accumulating pane must not duplicate at cleanup");
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void processingTimeSlidingKeepsIndependentWindowUpsertKeys()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.processingTimeSliding(
                                100L, 50L),
                        StreamWindowTriggerSpec.processingTime(100L),
                        StreamAccumulationMode.ACCUMULATING, 0L, false);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        AtomicLong clock = new AtomicLong(1_075L);
        Semaphore added = new Semaphore(0);
        StreamGenericWindowOperatorTask task =
                new StreamGenericWindowOperatorTask("processing-sliding",
                        context(plan), input, Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), output),
                        ignored -> new DecimalAggregate(added::release),
                        null, null, 0, 0,
                        plan.plan.maxParallelism() - 1, clock::get);
        task.start();
        input.send(recordWithoutTimestamp(
                plan, input, 1L, "A", BigDecimal.ONE));
        assertTrue(added.tryAcquire(2, 5L, TimeUnit.SECONDS));
        clock.set(1_100L);
        DataRow first = row(plan, output.poll(5_000L));
        assertEquals(1_000L, first.getLong(1));
        assertEquals(1_100L, first.getLong(2));
        clock.set(1_150L);
        DataRow second = row(plan, output.poll(5_000L));
        assertEquals(1_050L, second.getLong(1));
        assertEquals(1_150L, second.getLong(2));
        input.send(StreamEnvelope.endOfInput(
                input.channelIdentity(), 2L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void eventTimeSessionBridgesAndMergesExistingAccumulators()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.eventTimeSession(
                                "event-time", 10L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING, 0L, false);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask task = task(plan, input, output,
                null, null, "event-session");
        task.start();

        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 0L));
        input.send(record(plan, input, 2L, "A",
                new BigDecimal("3"), 18L));
        input.send(record(plan, input, 3L, "A",
                new BigDecimal("2"), 9L));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 4L, 28L));

        StreamEnvelope result = output.poll(5_000L);
        assertNotNull(result);
        DataRow row = row(plan, result);
        assertEquals(0L, row.getLong(1));
        assertEquals(28L, row.getLong(2));
        assertEquals(new BigDecimal("6.0000000000"),
                row.getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.poll(5_000L).kind());
        task.cancel();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void processingTimeSessionExtendsCleanupAndRetainsOneTimer()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.processingTimeSession(10L),
                        StreamWindowTriggerSpec.processingTime(20L),
                        StreamAccumulationMode.ACCUMULATING, 0L, false);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        AtomicLong clock = new AtomicLong(1_000L);
        Semaphore added = new Semaphore(0);
        StreamGenericWindowOperatorTask task =
                new StreamGenericWindowOperatorTask("processing-session",
                        context(plan), input, Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), output),
                        ignored -> new DecimalAggregate(added::release),
                        null, null, 0, 0,
                        plan.plan.maxParallelism() - 1, clock::get);
        task.start();

        input.send(recordWithoutTimestamp(
                plan, input, 1L, "A", BigDecimal.ONE));
        assertTrue(added.tryAcquire(5L, TimeUnit.SECONDS));
        clock.set(1_008L);
        input.send(recordWithoutTimestamp(
                plan, input, 2L, "A", new BigDecimal("2")));
        assertTrue(added.tryAcquire(5L, TimeUnit.SECONDS));
        clock.set(1_018L);

        StreamEnvelope result = output.poll(5_000L);
        assertNotNull(result);
        assertEquals(new BigDecimal("3.0000000000"), amount(plan, result));
        input.send(StreamEnvelope.endOfInput(
                input.channelIdentity(), 3L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void sessionMergeFanInIsEnforcedBeforeStateMutation()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.eventTimeSession(
                                "event-time", 10L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING, 0L, false, 1);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask task = task(plan, input, output,
                null, null, "session-fan-in");
        task.start();

        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 0L));
        input.send(record(plan, input, 2L, "A", BigDecimal.ONE, 18L));
        input.send(record(plan, input, 3L, "A", BigDecimal.ONE, 9L));
        awaitFailure(task);
        assertTrue(task.failure().getMessage().contains("merge fan-in"));
        task.awaitCompletion(5_000L);
    }

    @Test
    void fullWindowProcessReadsSpillCursorAndCollectorIsCallbackScoped()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.processPlan(
                        StreamWindowAssignerSpec.eventTimeTumbling(
                                "event-time", 100L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        AtomicReference<StreamWindowDataRowCursor> escaped =
                new AtomicReference<>();
        StreamWindowProcessFunction function = (context, elements, collector) -> {
            escaped.set(elements);
            BigDecimal sum = BigDecimal.ZERO;
            while (elements.advance()) {
                BigDecimal amount = elements.row().getBigDecimal(2);
                if (amount != null) sum = sum.add(amount);
            }
            DataRow result = new DataRow(4);
            result.set(0, context.currentKey().get(0));
            result.set(1, context.windowStart());
            result.set(2, context.windowEnd());
            result.set(3, sum.setScale(10,
                    java.math.RoundingMode.HALF_UP));
            collector.collect(result);
        };
        StreamGenericWindowOperatorTask task =
                new StreamGenericWindowOperatorTask("full-process",
                        context(plan), input, Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), output),
                        ignored -> function, null, null, 0, 0,
                        plan.plan.maxParallelism() - 1);
        task.start();

        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 10L));
        input.send(record(plan, input, 2L, "A",
                new BigDecimal("2"), 20L));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 3L, 100L));

        StreamEnvelope result = output.poll(5_000L);
        assertNotNull(result);
        assertEquals(new BigDecimal("3.0000000000"),
                row(plan, result).getBigDecimal(3));
        assertThrows(IllegalStateException.class,
                () -> escaped.get().advance());
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.poll(5_000L).kind());
        task.cancel();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void fullWindowProcessMigratesRowsAcrossSessionMerge()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.processPlan(
                        StreamWindowAssignerSpec.eventTimeSession(
                                "event-time", 10L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamWindowProcessFunction function = (context, elements, collector) -> {
            BigDecimal sum = BigDecimal.ZERO;
            while (elements.advance()) {
                sum = sum.add(elements.row().getBigDecimal(2));
            }
            DataRow result = new DataRow(4);
            result.set(0, context.currentKey().get(0));
            result.set(1, context.windowStart());
            result.set(2, context.windowEnd());
            result.set(3, sum.setScale(10,
                    java.math.RoundingMode.HALF_UP));
            collector.collect(result);
        };
        StreamGenericWindowOperatorTask task =
                new StreamGenericWindowOperatorTask("session-process",
                        context(plan), input, Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), output),
                        ignored -> function, null, null, 0, 0,
                        plan.plan.maxParallelism() - 1);
        task.start();

        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 0L));
        input.send(record(plan, input, 2L, "A",
                new BigDecimal("3"), 18L));
        input.send(record(plan, input, 3L, "A",
                new BigDecimal("2"), 9L));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 4L, 28L));

        StreamEnvelope result = output.poll(5_000L);
        assertNotNull(result);
        DataRow row = row(plan, result);
        assertEquals(0L, row.getLong(1));
        assertEquals(28L, row.getLong(2));
        assertEquals(new BigDecimal("6.0000000000"),
                row.getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                output.poll(5_000L).kind());
        task.cancel();
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void fullWindowProcessRowsSurviveCheckpointRestore(@TempDir Path temp)
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.processPlan(
                        StreamWindowAssignerSpec.eventTimeTumbling(
                                "event-time", 100L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING);
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temp.resolve("process-payloads"), 65_536L);
        LocalStreamChannel firstInput = channel(plan, "attempt-1",
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel firstOutput = channel(plan, "attempt-1",
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask initial = processTask(plan,
                firstInput, firstOutput, payloads, null,
                "process-initial", summingProcess());
        initial.start();
        firstInput.send(record(plan, firstInput, 1L, "A",
                new BigDecimal("3"), 10L));
        CheckpointIdentity checkpoint = checkpoint(plan, "attempt-1");
        java.util.concurrent.CompletableFuture<StreamCheckpointMember> frozen =
                initial.triggerCheckpoint(checkpoint);
        firstInput.send(StreamEnvelope.checkpointBarrier(
                firstInput.channelIdentity(), 2L, checkpoint));
        StreamCheckpointMember member = frozen.get(5L, TimeUnit.SECONDS);
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                firstOutput.poll(5_000L).kind());
        initial.cancel();
        initial.awaitCompletion(5_000L);
        assertNull(initial.failure());

        LocalStreamChannel nextInput = channel(plan, "attempt-2",
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel nextOutput = channel(plan, "attempt-2",
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask restored = processTask(plan,
                nextInput, nextOutput, payloads, member,
                "process-restored", summingProcess());
        restored.start();
        nextInput.send(record(plan, nextInput, 1L, "A",
                new BigDecimal("4"), 20L));
        nextInput.send(StreamEnvelope.watermark(
                nextInput.channelIdentity(), 2L, 100L));
        assertEquals(new BigDecimal("7.0000000000"),
                row(plan, nextOutput.poll(5_000L)).getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.WATERMARK,
                nextOutput.poll(5_000L).kind());
        restored.cancel();
        restored.awaitCompletion(5_000L);
        assertNull(restored.failure());
    }

    @Test
    void processingWindowTimerSurvivesCheckpointRestore(@TempDir Path temp)
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.processingTimeTumbling(100L),
                        StreamWindowTriggerSpec.processingTime(20L),
                        StreamAccumulationMode.ACCUMULATING, 0L, false);
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temp.resolve("timer-payloads"), 65_536L);
        AtomicLong firstClock = new AtomicLong(1_000L);
        Semaphore added = new Semaphore(0);
        LocalStreamChannel firstInput = channel(plan, "attempt-1",
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel firstOutput = channel(plan, "attempt-1",
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask initial =
                new StreamGenericWindowOperatorTask("timer-initial",
                        context(plan), firstInput, Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), firstOutput),
                        ignored -> new DecimalAggregate(added::release),
                        payloads, null, 0, 0,
                        plan.plan.maxParallelism() - 1, firstClock::get);
        initial.start();
        firstInput.send(recordWithoutTimestamp(
                plan, firstInput, 1L, "A", BigDecimal.ONE));
        assertTrue(added.tryAcquire(5L, TimeUnit.SECONDS));
        CheckpointIdentity checkpoint = checkpoint(plan, "attempt-1");
        java.util.concurrent.CompletableFuture<StreamCheckpointMember> frozen =
                initial.triggerCheckpoint(checkpoint);
        firstInput.send(StreamEnvelope.checkpointBarrier(
                firstInput.channelIdentity(), 2L, checkpoint));
        StreamCheckpointMember member = frozen.get(5L, TimeUnit.SECONDS);
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                firstOutput.poll(5_000L).kind());
        initial.cancel();
        initial.awaitCompletion(5_000L);
        assertNull(initial.failure());

        AtomicLong nextClock = new AtomicLong(1_020L);
        LocalStreamChannel nextInput = channel(plan, "attempt-2",
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel nextOutput = channel(plan, "attempt-2",
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask restored =
                new StreamGenericWindowOperatorTask("timer-restored",
                        context(plan), nextInput, Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), nextOutput),
                        ignored -> new DecimalAggregate(), payloads, member,
                        0, 0, plan.plan.maxParallelism() - 1, nextClock::get);
        restored.start();
        assertEquals(new BigDecimal("1.0000000000"),
                amount(plan, nextOutput.poll(5_000L)));
        nextInput.send(StreamEnvelope.endOfInput(
                nextInput.channelIdentity(), 1L));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                nextOutput.poll(5_000L).kind());
        restored.awaitCompletion(5_000L);
        assertNull(restored.failure());
    }

    @Test
    void fullProcessCountTriggerDiscardsOnlyTheFiredPane()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.processPlan(
                        StreamWindowAssignerSpec.countTumbling(3L),
                        StreamWindowTriggerSpec.count(2L),
                        StreamAccumulationMode.DISCARDING);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask task = processTask(plan, input,
                output, null, null, "count-process", summingProcess());
        task.start();

        input.send(recordWithoutTimestamp(
                plan, input, 1L, "A", BigDecimal.ONE));
        input.send(recordWithoutTimestamp(
                plan, input, 2L, "A", new BigDecimal("2")));
        input.send(recordWithoutTimestamp(
                plan, input, 3L, "A", new BigDecimal("3")));
        input.send(StreamEnvelope.endOfInput(
                input.channelIdentity(), 4L));

        assertEquals(new BigDecimal("3.0000000000"),
                row(plan, output.poll(5_000L)).getBigDecimal(3));
        assertEquals(new BigDecimal("3.0000000000"),
                row(plan, output.poll(5_000L)).getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void countSlidingCompletesAndFlushesOverlappingWindows()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.plan(
                        StreamWindowAssignerSpec.countSliding(3L, 2L),
                        StreamWindowTriggerSpec.count(3L),
                        StreamAccumulationMode.DISCARDING, 0L, false);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        StreamGenericWindowOperatorTask task = task(plan, input, output,
                null, null, "count-sliding");
        task.start();
        for (int value = 1; value <= 4; value++) {
            input.send(recordWithoutTimestamp(plan, input, value,
                    "A", BigDecimal.valueOf(value)));
        }
        input.send(StreamEnvelope.endOfInput(
                input.channelIdentity(), 5L));
        DataRow first = row(plan, output.poll(5_000L));
        assertEquals(0L, first.getLong(1));
        assertEquals(3L, first.getLong(2));
        assertEquals(new BigDecimal("6.0000000000"),
                first.getBigDecimal(3));
        DataRow second = row(plan, output.poll(5_000L));
        assertEquals(2L, second.getLong(1));
        assertEquals(5L, second.getLong(2));
        assertEquals(new BigDecimal("7.0000000000"),
                second.getBigDecimal(3));
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                output.poll(5_000L).kind());
        task.awaitCompletion(5_000L);
        assertNull(task.failure());
    }

    @Test
    void processCollectorRejectsOutputBeyondDeclaredTriggerQuota()
            throws Exception {
        StreamTestGraph plan =
                StreamGenericWindowProductAdmissionTest.processPlan(
                        StreamWindowAssignerSpec.eventTimeTumbling(
                                "event-time", 100L),
                        StreamWindowTriggerSpec.eventTimeFinal(),
                        StreamAccumulationMode.DISCARDING);
        LocalStreamChannel input = channel(plan,
                plan.sourceInputEdge().logicalChannelId());
        LocalStreamChannel output = channel(plan,
                plan.onlySink().edge.logicalChannelId());
        Semaphore firstEmitted = new Semaphore(0);
        Semaphore continueCallback = new Semaphore(0);
        StreamWindowProcessFunction function = (context, elements, collector) -> {
            while (elements.advance()) elements.row();
            DataRow result = new DataRow(4);
            result.set(0, context.currentKey().get(0));
            result.set(1, context.windowStart());
            result.set(2, context.windowEnd());
            result.set(3, BigDecimal.ONE.setScale(10));
            collector.collect(result);
            firstEmitted.release();
            try {
                continueCallback.acquire();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(interrupted);
            }
            collector.collect(result.copy());
        };
        StreamGenericWindowOperatorTask task = processTask(plan, input,
                output, null, null, "process-output-quota", function);
        task.start();
        input.send(record(plan, input, 1L, "A", BigDecimal.ONE, 10L));
        input.send(StreamEnvelope.watermark(
                input.channelIdentity(), 2L, 100L));
        assertTrue(firstEmitted.tryAcquire(5L, TimeUnit.SECONDS));
        assertNotNull(output.poll(5_000L));
        continueCallback.release();
        awaitFailure(task);
        assertTrue(task.failure().getMessage().contains("output quota"));
        task.awaitCompletion(5_000L);
    }

    private static BigDecimal amount(StreamTestGraph plan,
            StreamEnvelope envelope) {
        assertEquals(StreamEnvelope.Kind.RECORD, envelope.kind());
        byte[] frame = plan.plan.changelogMode()
                == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.UPSERT
                ? StreamUpsertRecordCodec.decode(envelope.recordFrame(),
                plan.maximumFrameBytes(), plan.maximumFrameBytes())
                .afterImageFrame() : envelope.recordFrame();
        return new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.genericWindow.outputSchema())).deserialize(frame)
                .getBigDecimal(3);
    }

    private static void awaitFailure(StreamGenericWindowOperatorTask task) {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(5L);
        while (task.failure() == null && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertNotNull(task.failure());
    }

    private static final class StreamChannelOutputPair {
        final LocalStreamChannel channel;
        private StreamChannelOutputPair(LocalStreamChannel channel) {
            this.channel = channel;
        }
    }

    private static StreamGenericWindowOperatorTask task(
            StreamTestGraph plan, LocalStreamChannel input,
            LocalStreamChannel output, FileCheckpointPayloadStore payloads,
            StreamCheckpointMember restore, String name) {
        return new StreamGenericWindowOperatorTask(name, context(plan),
                input,
                Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), output),
                ignored -> new DecimalAggregate(), payloads, restore,
                0, 0, plan.plan.maxParallelism() - 1);
    }

    private static StreamGenericWindowOperatorTask processTask(
            StreamTestGraph plan, LocalStreamChannel input,
            LocalStreamChannel output, FileCheckpointPayloadStore payloads,
            StreamCheckpointMember restore, String name,
            StreamWindowProcessFunction function) {
        return new StreamGenericWindowOperatorTask(name, context(plan),
                input,
                Collections.singletonMap(
                        plan.onlySink().node.operatorUid(), output),
                ignored -> function, payloads, restore,
                0, 0, plan.plan.maxParallelism() - 1);
    }

    private static StreamWindowProcessFunction summingProcess() {
        return (context, elements, collector) -> {
            BigDecimal sum = BigDecimal.ZERO;
            while (elements.advance()) {
                sum = sum.add(elements.row().getBigDecimal(2));
            }
            DataRow result = new DataRow(4);
            result.set(0, context.currentKey().get(0));
            result.set(1, context.windowStart());
            result.set(2, context.windowEnd());
            result.set(3, sum.setScale(10,
                    java.math.RoundingMode.HALF_UP));
            collector.collect(result);
        };
    }

    private static StreamNodeTaskContext context(
            StreamTestGraph plan) {
        return StreamNodeTaskContext.from(plan.artifact.artifactId(),
                plan.executionGraph, plan.genericWindow.operatorUid());
    }

    private static LocalStreamChannel channel(
            StreamTestGraph plan, String channelId) {
        return channel(plan, "attempt", channelId);
    }

    private static LocalStreamChannel channel(
            StreamTestGraph plan, String attempt,
            String channelId) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run", attempt, 1L, 1L, channelId, 1L),
                plan.maximumEvents(), plan.maximumFrameBytes(), 8_192L);
    }

    private static CheckpointIdentity checkpoint(
            StreamTestGraph plan, String attempt) {
        return new CheckpointIdentity(plan.artifact.definitionId(),
                plan.artifact.artifactId(), "run", attempt, 1L, 1L,
                "coordinator", 1L, plan.commandDigest,
                plan.artifact.artifactId(), 1, 1);
    }

    private static StreamEnvelope record(StreamTestGraph plan,
            LocalStreamChannel channel, long sequence, String groupKey,
            BigDecimal amount, long timestamp) {
        DataRow row = new DataRow(3);
        row.set(0, groupKey);
        row.set(1, new Timestamp(timestamp));
        row.set(2, amount);
        byte[] frame = new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.keyBy.outputSchema())).serialize(row);
        return StreamEnvelope.record(channel.channelIdentity(), sequence,
                timestamp, frame);
    }

    private static StreamEnvelope recordWithoutTimestamp(
            StreamTestGraph plan, LocalStreamChannel channel,
            long sequence, String groupKey, BigDecimal amount) {
        DataRow row = new DataRow(3);
        row.set(0, groupKey);
        row.set(1, new Timestamp(sequence));
        row.set(2, amount);
        byte[] frame = new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.keyBy.outputSchema())).serialize(row);
        return StreamEnvelope.record(
                channel.channelIdentity(), sequence, frame);
    }

    private static DataRow row(StreamTestGraph plan,
            StreamEnvelope envelope) {
        assertEquals(StreamEnvelope.Kind.RECORD, envelope.kind());
        byte[] frame = plan.plan.changelogMode()
                == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.UPSERT
                ? StreamUpsertRecordCodec.decode(envelope.recordFrame(),
                plan.maximumFrameBytes(), plan.maximumFrameBytes())
                .afterImageFrame() : envelope.recordFrame();
        return new RowSerializer(PortablePlanCompiler.toDataSchema(
                plan.genericWindow.outputSchema())).deserialize(frame);
    }

    private static StreamStateDescriptor decimalDescriptor() {
        return new StreamStateDescriptor("sum",
                StreamStateDescriptor.Kind.KEYED_VALUE,
                StreamStateSpec.DECIMAL_WINDOW_SERIALIZER_CONTRACT, 1,
                StreamStateDescriptor.TtlMode.NONE, 0L);
    }

    private static final class DecimalAggregate
            implements StreamWindowAggregateFunction {
        private final Runnable afterAdd;

        private DecimalAggregate() {
            this(() -> { });
        }

        private DecimalAggregate(Runnable afterAdd) {
            this.afterAdd = afterAdd;
        }

        @Override public byte[] createAccumulator() {
            return bytes(BigDecimal.ZERO);
        }

        @Override public byte[] add(DataRow value, byte[] accumulator) {
            BigDecimal amount = value.getBigDecimal(2);
            BigDecimal current = decimal(accumulator);
            byte[] result = bytes(
                    amount == null ? current : current.add(amount));
            afterAdd.run();
            return result;
        }

        @Override public byte[] merge(byte[] left, byte[] right) {
            return bytes(decimal(left).add(decimal(right)));
        }

        @Override public DataRow getResult(byte[] accumulator,
                StreamWindowContext context) {
            DataRow result = new DataRow(4);
            result.set(0, context.currentKey().get(0));
            result.set(1, context.windowStart());
            result.set(2, context.windowEnd());
            result.set(3, decimal(accumulator).setScale(
                    10, java.math.RoundingMode.HALF_UP));
            return result;
        }

        private static BigDecimal decimal(byte[] value) {
            return new BigDecimal(new String(value, StandardCharsets.UTF_8));
        }

        private static byte[] bytes(BigDecimal value) {
            return value.toPlainString().getBytes(StandardCharsets.UTF_8);
        }
    }

    private static final class TransportNeutralInput
            implements StreamChannelInput {
        private final StreamChannelInput delegate;

        private TransportNeutralInput(StreamChannelInput delegate) {
            this.delegate = delegate;
        }

        @Override public StreamChannelIdentity channelIdentity() {
            return delegate.channelIdentity();
        }

        @Override public StreamEnvelope receive() throws InterruptedException {
            return delegate.receive();
        }

        @Override public StreamEnvelope poll(long timeoutMillis)
                throws InterruptedException {
            return delegate.poll(timeoutMillis);
        }

        @Override public void cancel() { delegate.cancel(); }
        @Override public void fail(Throwable cause) { delegate.fail(cause); }
    }
}
