package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HeartbeatSchedulerTest {

    @Test
    void schedulingFailureShutsNewExecutorAndPreservesPrimary() throws Exception {
        RuntimeException primary = new RuntimeException("injected schedule failure");
        RejectingScheduler scheduler = new RejectingScheduler(primary, null);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> HeartbeatScheduler.activate(scheduler, () -> { }, 10L));

        assertSame(primary, actual);
        assertEquals(1, scheduler.shutdownCalls.get());
        assertTrue(scheduler.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void shutdownFailureIsSuppressedOnSchedulingFailure() {
        RuntimeException primary = new RuntimeException("injected schedule failure");
        RuntimeException cleanup = new RuntimeException("injected shutdown failure");
        RejectingScheduler scheduler = new RejectingScheduler(primary, cleanup);

        RuntimeException actual = assertThrows(RuntimeException.class,
                () -> HeartbeatScheduler.activate(scheduler, () -> { }, 10L));

        assertSame(primary, actual);
        assertEquals(1, actual.getSuppressed().length);
        assertSame(cleanup, actual.getSuppressed()[0]);
    }

    private static final class RejectingScheduler extends ScheduledThreadPoolExecutor {
        private final RuntimeException scheduleFailure;
        private final RuntimeException shutdownFailure;
        private final AtomicInteger shutdownCalls = new AtomicInteger();

        private RejectingScheduler(RuntimeException scheduleFailure,
                                   RuntimeException shutdownFailure) {
            super(1);
            this.scheduleFailure = scheduleFailure;
            this.shutdownFailure = shutdownFailure;
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
                                                       long period, TimeUnit unit) {
            throw scheduleFailure;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdownCalls.incrementAndGet();
            List<Runnable> pending = super.shutdownNow();
            if (shutdownFailure != null) throw shutdownFailure;
            return pending;
        }
    }
}
