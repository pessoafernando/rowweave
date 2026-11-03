package io.rowweave.cluster;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Atomic activation boundary for newly owned heartbeat schedulers. */
final class HeartbeatScheduler {

    private HeartbeatScheduler() {
    }

    static void activate(ScheduledExecutorService scheduler, Runnable task, long intervalMs) {
        if (scheduler == null || task == null) {
            throw new IllegalArgumentException("heartbeat scheduler and task are required");
        }
        if (intervalMs <= 0L) {
            throw new IllegalArgumentException("intervalMs must be > 0");
        }
        try {
            scheduler.scheduleAtFixedRate(
                    task, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
        } catch (Throwable primary) {
            try {
                scheduler.shutdownNow();
            } catch (Throwable cleanup) {
                if (cleanup != primary) primary.addSuppressed(cleanup);
            }
            rethrow(primary);
        }
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }
}
