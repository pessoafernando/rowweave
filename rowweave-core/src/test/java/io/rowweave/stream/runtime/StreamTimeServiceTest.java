package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamEnvelope;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamTimeServiceTest {

    @Test
    void activeMinimumIdleAndReactivationDriveCanonicalEventTimers() {
        List<String> fired = new ArrayList<>();
        StreamTimeService time = new StreamTimeService(
                2, 8, () -> 0L,
                timer -> fired.add(timer.timerId()));

        time.register(StreamTimeService.TimerKind.EVENT_TIME, 100L, "b");
        time.register(StreamTimeService.TimerKind.EVENT_TIME, 100L, "a");
        time.register(StreamTimeService.TimerKind.EVENT_TIME, 120L, "c");
        time.onWatermark(0, 100L);
        assertEquals(Long.MIN_VALUE, time.operatorWatermark());
        time.onWatermark(1, 90L);
        assertEquals(90L, time.operatorWatermark());
        assertEquals(0, fired.size());

        time.onWatermarkStatus(
                1, StreamEnvelope.WatermarkStatus.IDLE, 1L);
        assertEquals(100L, time.operatorWatermark());
        assertEquals(Arrays.asList("a", "b"), fired);
        time.onWatermark(1, 100L);
        time.onWatermarkStatus(
                1, StreamEnvelope.WatermarkStatus.ACTIVE, 2L);
        assertEquals(100L, time.operatorWatermark());

        StreamTimeException generation = assertThrows(
                StreamTimeException.class,
                () -> time.onWatermarkStatus(
                        1, StreamEnvelope.WatermarkStatus.IDLE, 2L));
        assertEquals(StreamTimeException.Code.STATUS_GENERATION,
                generation.code());
    }

    @Test
    void lowWatermarkReactivationFailsBeforeChangingOperatorTime() {
        StreamTimeService time = new StreamTimeService(
                2, 8, () -> 0L, ignored -> { });
        time.onWatermark(0, 200L);
        time.onWatermark(1, 100L);
        time.onWatermarkStatus(
                1, StreamEnvelope.WatermarkStatus.IDLE, 1L);
        assertEquals(200L, time.operatorWatermark());
        time.onWatermark(1, 199L);

        StreamTimeException low = assertThrows(
                StreamTimeException.class,
                () -> time.onWatermarkStatus(
                        1, StreamEnvelope.WatermarkStatus.ACTIVE, 2L));
        assertEquals(StreamTimeException.Code.ACTIVATION_WATERMARK, low.code());
        assertEquals(200L, time.operatorWatermark());
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                time.inputStatus(1));
    }

    @Test
    void processingTimersAreMonotonicBoundedAndSingleThreadOwned()
            throws Exception {
        AtomicLong clock = new AtomicLong(1_000L);
        List<String> fired = new ArrayList<>();
        StreamTimeService time = new StreamTimeService(
                1, 3, clock::get, timer -> fired.add(timer.timerId()));
        time.register(StreamTimeService.TimerKind.PROCESSING_TIME,
                1_010L, "z");
        time.register(StreamTimeService.TimerKind.PROCESSING_TIME,
                1_010L, "a");
        time.register(StreamTimeService.TimerKind.PROCESSING_TIME,
                1_020L, "later");
        time.register(StreamTimeService.TimerKind.PROCESSING_TIME,
                1_010L, "a");
        assertEquals(3, time.pendingTimerCount());
        assertEquals(3, time.maximumTimerCount());
        assertEquals(3, time.maximumObservedTimerCount());

        clock.set(1_010L);
        time.advanceProcessingTime();
        assertEquals(Arrays.asList("a", "z"), fired);
        clock.set(1L);
        time.advanceProcessingTime();
        assertEquals(Arrays.asList("a", "z"), fired,
                "clock rollback must not refire timers");
        time.register(StreamTimeService.TimerKind.PROCESSING_TIME,
                1_005L, "already-due");
        assertEquals(Arrays.asList("a", "z", "already-due"), fired);
        assertEquals(3, time.maximumObservedTimerCount());

        AtomicReference<Throwable> crossThread = new AtomicReference<>();
        Thread intruder = new Thread(() -> {
            try {
                time.pendingTimerCount();
            } catch (Throwable failure) {
                crossThread.set(failure);
            }
        });
        intruder.start();
        intruder.join(3_000L);
        assertFalse(intruder.isAlive());
        assertEquals(StreamTimeException.Code.THREAD_OWNERSHIP,
                ((StreamTimeException) crossThread.get()).code());
    }

    @Test
    void timerQuotaAndCallbackFailureAreFailClosed() {
        StreamTimeService bounded = new StreamTimeService(
                1, 1, () -> 0L, ignored -> { });
        bounded.register(StreamTimeService.TimerKind.EVENT_TIME, 10L, "one");
        StreamTimeException limit = assertThrows(
                StreamTimeException.class,
                () -> bounded.register(
                        StreamTimeService.TimerKind.EVENT_TIME, 20L, "two"));
        assertEquals(StreamTimeException.Code.TIMER_LIMIT, limit.code());

        RuntimeException cause = new RuntimeException("timer failed");
        StreamTimeService poisoned = new StreamTimeService(
                1, 1, () -> 0L, timer -> { throw cause; });
        poisoned.register(StreamTimeService.TimerKind.EVENT_TIME, 5L, "bad");
        StreamTimeException callback = assertThrows(
                StreamTimeException.class,
                () -> poisoned.onWatermark(0, 5L));
        assertEquals(StreamTimeException.Code.CALLBACK_FAILURE,
                callback.code());
        assertSame(cause, callback.getCause());
        assertSame(callback, assertThrows(
                StreamTimeException.class,
                () -> poisoned.onWatermark(0, 6L)));
    }
}
