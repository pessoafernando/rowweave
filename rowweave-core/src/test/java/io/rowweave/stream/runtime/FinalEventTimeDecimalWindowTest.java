package io.rowweave.stream.runtime;

import io.rowweave.mem.MemoryManager;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamStateDescriptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FinalEventTimeDecimalWindowTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void tumblingWindowEmitsFinalDecimalSumOnceAndCleansState() {
        List<FinalEventTimeDecimalWindow.Result> output = new ArrayList<>();
        try (FinalEventTimeDecimalWindow window = window(
                FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                100L, 100L, 8, 16, output)) {
            byte[] groupKey = bytes("adaptor-a");
            byte[] nullGroup = new byte[] {0};
            window.process(groupKey,
                    new BigDecimal("999999999999999999999999.00000000004"), 1L);
            window.process(groupKey, new BigDecimal("0.00000000004"), 2L);
            window.process(groupKey, null, 3L);
            window.process(nullGroup, new BigDecimal("-1.55555555555"), 4L);

            window.advanceWatermark(99L);
            assertEquals(0, output.size());
            window.advanceWatermark(100L);
            assertEquals(2, output.size());
            assertResult(output, groupKey, 0L, 100L,
                    "999999999999999999999999.0000000001");
            assertResult(output, nullGroup, 0L, 100L,
                    "-1.5555555556");
            assertEquals(0L, window.stateSpillBytes(),
                    "final trigger must delete window state");

            StreamWindowException late = assertThrows(
                    StreamWindowException.class,
                    () -> window.process(groupKey, BigDecimal.ONE, 99L));
            assertEquals(StreamWindowException.Code.LATE_RECORD, late.code());
            assertEquals(2, output.size());
        }
    }

    @Test
    void slidingMembershipAndFinalOrderFollowWindowEnd() {
        List<FinalEventTimeDecimalWindow.Result> output = new ArrayList<>();
        try (FinalEventTimeDecimalWindow window = window(
                FinalEventTimeDecimalWindow.WindowType.SLIDING,
                100L, 50L, 4, 8, output)) {
            byte[] groupKey = bytes("adaptor");
            window.process(groupKey, new BigDecimal("2.25"), 75L);
            assertEquals(2, window.windowsPerRecord());
            assertEquals(2, window.activeWindowCount());
            assertEquals(2, window.maximumObservedWindowCount());
            assertEquals(1, window.ownedKeyGroupCount());
            assertEquals(64 * 1024L, window.maximumStateSpillBytes());

            window.advanceWatermark(100L);
            assertEquals(1, output.size());
            assertEquals(1, window.activeWindowCount());
            assertEquals(0L, output.get(0).windowStart());
            assertEquals(100L, output.get(0).windowEnd());
            window.advanceWatermark(149L);
            assertEquals(1, output.size());
            window.advanceWatermark(150L);
            assertEquals(2, output.size());
            assertEquals(50L, output.get(1).windowStart());
            assertEquals(150L, output.get(1).windowEnd());
            assertEquals(new BigDecimal("2.2500000000"),
                    output.get(1).sum());
            assertEquals(0, window.activeWindowCount());
            assertEquals(0L, window.stateSpillBytes());
        }
    }

    @Test
    void checkpointRestoresPendingStateTimersAndWatermark() {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("checkpoint-payloads"),
                64 * 1024L);
        StreamCheckpointMember checkpoint;
        List<FinalEventTimeDecimalWindow.Result> before = new ArrayList<>();
        try (FinalEventTimeDecimalWindow window = window(
                FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                100L, 100L, 8, 16, before)) {
            window.process(bytes("adaptor"),
                    new BigDecimal("1.00000000006"), 10L);
            window.process(bytes("adaptor"),
                    new BigDecimal("2.00000000005"), 20L);
            window.advanceWatermark(50L);
            checkpoint = window.snapshot(0, payloads);
            assertEquals(1, window.activeWindowCount());
            assertEquals(0, before.size());
        }

        List<FinalEventTimeDecimalWindow.Result> after = new ArrayList<>();
        try (FinalEventTimeDecimalWindow restored = window(
                FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                100L, 100L, 8, 16, after)) {
            restored.restore(0, checkpoint, payloads);
            assertEquals(1, restored.activeWindowCount());
            assertThrows(StreamWindowException.class,
                    () -> restored.process(bytes("late"), BigDecimal.ONE, 50L));
        }

        try (FinalEventTimeDecimalWindow restored = window(
                FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                100L, 100L, 8, 16, after)) {
            restored.restore(0, checkpoint, payloads);
            restored.advanceWatermark(100L);
            assertEquals(1, after.size());
            assertResult(after, bytes("adaptor"), 0L, 100L,
                    "3.0000000001");
        }
    }

    @Test
    void emptyCanonicalGroupKeySurvivesFinalTimerIdentityRoundTrip() {
        List<FinalEventTimeDecimalWindow.Result> output = new ArrayList<>();
        try (FinalEventTimeDecimalWindow window = window(
                FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                100L, 100L, 8, 8, output)) {
            byte[] emptyGroup = new byte[0];
            window.process(emptyGroup, new BigDecimal("1.25"), 1L);

            window.advanceWatermark(100L);

            assertEquals(1, output.size());
            assertResult(output, emptyGroup, 0L, 100L, "1.2500000000");
            assertEquals(0L, window.stateSpillBytes());
        }
    }

    @Test
    void extremeDecimalScaleFailsBeforeStateOrTimerMutation() {
        List<FinalEventTimeDecimalWindow.Result> output = new ArrayList<>();
        try (FinalEventTimeDecimalWindow window = window(
                FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                100L, 100L, 8, 8, output)) {
            StreamWindowException bounded = assertThrows(
                    StreamWindowException.class,
                    () -> window.process(bytes("adaptor"),
                            new BigDecimal(BigInteger.ONE, -100_000), 1L));
            assertEquals(StreamWindowException.Code.WINDOW_LIMIT,
                    bounded.code());
            assertEquals(0L, window.stateSpillBytes());
            assertEquals(0, output.size());
        }
    }

    @Test
    void timerQuotaRejectsBeforeCreatingOrphanWindowState() {
        List<FinalEventTimeDecimalWindow.Result> output = new ArrayList<>();
        try (FinalEventTimeDecimalWindow window = window(
                FinalEventTimeDecimalWindow.WindowType.TUMBLING,
                100L, 100L, 8, 1, output)) {
            window.process(bytes("first"), BigDecimal.ONE, 1L);
            long before = window.stateSpillBytes();

            StreamTimeException limit = assertThrows(
                    StreamTimeException.class,
                    () -> window.process(
                            bytes("second"), BigDecimal.TEN, 1L));
            assertEquals(StreamTimeException.Code.TIMER_LIMIT, limit.code());
            assertEquals(before, window.stateSpillBytes());
            window.advanceWatermark(100L);
            assertEquals(1, output.size());
            assertEquals("first",
                    new String(output.get(0).canonicalKey(),
                            StandardCharsets.UTF_8));
        }
    }

    @Test
    void invalidWindowFanoutFailsBeforeOpeningManagedState() {
        StreamStateDescriptor descriptor = descriptor();
        assertThrows(StreamWindowException.class,
                () -> new FinalEventTimeDecimalWindow(
                        "artifact", "window", descriptor,
                        1, 0, 0,
                        FinalEventTimeDecimalWindow.WindowType.SLIDING,
                        1_000L, 1L, 10, 16,
                        new MemoryManager(512), temporaryDirectory,
                        64 * 1024L, 128, 1024, ignored -> { }));
    }

    private FinalEventTimeDecimalWindow window(
            FinalEventTimeDecimalWindow.WindowType type,
            long size, long slide, int maxWindowsPerRecord,
            int maximumTimers,
            List<FinalEventTimeDecimalWindow.Result> output) {
        return new FinalEventTimeDecimalWindow(
                "artifact", "window", descriptor(),
                1, 0, 0, type, size, slide,
                maxWindowsPerRecord, maximumTimers,
                new MemoryManager(384), temporaryDirectory,
                64 * 1024L, 128, 1024, output::add);
    }

    private static StreamStateDescriptor descriptor() {
        return new StreamStateDescriptor(
                "sum", StreamStateDescriptor.Kind.KEYED_VALUE,
                FinalEventTimeDecimalWindow.DECIMAL_STATE_SERIALIZER,
                1, StreamStateDescriptor.TtlMode.NONE, 0L);
    }

    private static void assertResult(
            List<FinalEventTimeDecimalWindow.Result> output,
            byte[] key, long start, long end, String expected) {
        FinalEventTimeDecimalWindow.Result result = output.stream()
                .filter(candidate -> java.util.Arrays.equals(
                        key, candidate.canonicalKey()))
                .findFirst().orElseThrow(AssertionError::new);
        assertEquals(start, result.windowStart());
        assertEquals(end, result.windowEnd());
        assertEquals(new BigDecimal(expected), result.sum());
        assertEquals(10, result.sum().scale());
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
