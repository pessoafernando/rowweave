package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CriticalReportRetrierTest {

    @Test
    void retriesUntilAckWithCappedExponentialBackoff() {
        AtomicInteger calls = new AtomicInteger();
        List<Long> pauses = new ArrayList<>();

        CriticalReportRetrier.Result result = CriticalReportRetrier.deliver(
                () -> {
                    if (calls.incrementAndGet() <= 7) {
                        throw new RuntimeException("transient-" + calls.get());
                    }
                },
                () -> false,
                pauses::add);

        assertTrue(result.delivered());
        assertEquals(8, result.attempts());
        assertNotNull(result.firstFailure());
        assertEquals("transient-1", result.firstFailure().getMessage());
        assertEquals(Arrays.asList(25L, 50L, 100L, 200L, 400L, 500L, 500L), pauses);
    }

    @Test
    void closingStopsAfterLastFailedAttemptWithoutAnotherPause() {
        AtomicInteger calls = new AtomicInteger();
        List<Long> pauses = new ArrayList<>();

        CriticalReportRetrier.Result result = CriticalReportRetrier.deliver(
                () -> {
                    calls.incrementAndGet();
                    throw new RuntimeException("master unavailable");
                },
                () -> calls.get() >= 3,
                pauses::add);

        assertFalse(result.delivered());
        assertEquals(3, result.attempts());
        assertEquals(Arrays.asList(25L, 50L), pauses);
    }

    @Test
    void interruptedPauseRestoresInterruptAndStops() {
        AtomicBoolean pauseCalled = new AtomicBoolean();
        try {
            CriticalReportRetrier.Result result = CriticalReportRetrier.deliver(
                    () -> { throw new RuntimeException("retry"); },
                    () -> false,
                    millis -> {
                        pauseCalled.set(true);
                        throw new InterruptedException("stop");
                    });

            assertFalse(result.delivered());
            assertEquals(1, result.attempts());
            assertTrue(pauseCalled.get());
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }
}

