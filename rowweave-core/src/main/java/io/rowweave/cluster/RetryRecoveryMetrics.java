package io.rowweave.cluster;

import io.rowweave.monitor.JobMonitorRegistry;
import io.rowweave.monitor.RetryRecoveryView;
import io.rowweave.run.WholeAttemptRetryDecision;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** In-process advisory counters for the retry coordinator, published as a bounded snapshot. */
final class RetryRecoveryMetrics {
    private static final int MAX_SAMPLES = 16;

    private final JobMonitorRegistry monitor;
    private final int scanPageCapacity;
    private final AtomicLong scheduled = new AtomicLong();
    private final AtomicLong exhausted = new AtomicLong();
    private final AtomicLong notRetryable = new AtomicLong();
    private volatile InventoryGauge gauge = InventoryGauge.notRunning();

    RetryRecoveryMetrics(JobMonitorRegistry monitor, int scanPageCapacity) {
        if (monitor == null) throw new IllegalArgumentException("monitor is required");
        if (scanPageCapacity < 1) {
            throw new IllegalArgumentException("scanPageCapacity must be positive");
        }
        this.monitor = monitor;
        this.scanPageCapacity = scanPageCapacity;
    }

    void record(WholeAttemptRetryDecision decision) {
        if (decision == null) return;
        switch (decision.code()) {
            case SCHEDULED:
                incrementSaturated(scheduled);
                break;
            case EXHAUSTED:
                incrementSaturated(exhausted);
                break;
            case NOT_RETRYABLE:
                incrementSaturated(notRetryable);
                break;
            default:
                break;
        }
        publish();
    }

    void observe(LeaderRecoveryInventory.Snapshot snapshot) {
        if (snapshot == null) return;
        gauge = new InventoryGauge(
                snapshot.state().name(), snapshot.activeRuns(),
                snapshot.waitingRetries(),
                delayBucket(snapshot.waitingRetries(),
                        snapshot.nextRetryDelayMillis()),
                cursorHealth(snapshot.state(), snapshot.maintenanceCursor()),
                bounded(snapshot.samples()));
        publish();
    }

    private void publish() {
        InventoryGauge current = gauge;
        try {
            monitor.onRetryRecovery(new RetryRecoveryView(
                    current.state, current.activeRuns, current.waitingRetries,
                    current.nextDueDelayBucket, scheduled.get(), exhausted.get(),
                    notRetryable.get(), current.cursorHealth, scanPageCapacity,
                    current.samples));
        } catch (RuntimeException ignored) {
            // Monitoring is advisory: an observation failure must not change what retry authority decides.
        }
    }

    private static void incrementSaturated(AtomicLong counter) {
        while (true) {
            long current = counter.get();
            if (current == Long.MAX_VALUE) return;
            if (counter.compareAndSet(current, current + 1L)) return;
        }
    }

    private static String delayBucket(int waitingRetries, long delayMillis) {
        if (waitingRetries == 0 || delayMillis == Long.MAX_VALUE) return "NONE";
        if (delayMillis <= 0L) return "DUE";
        if (delayMillis <= 1_000L) return "LE_1S";
        if (delayMillis <= 5_000L) return "LE_5S";
        if (delayMillis <= 30_000L) return "LE_30S";
        return "GT_30S";
    }

    private static String cursorHealth(
            LeaderRecoveryInventory.State state, String cursor) {
        if (state == LeaderRecoveryInventory.State.IDLE
                || state == LeaderRecoveryInventory.State.LEADERSHIP_LOST
                || state == LeaderRecoveryInventory.State.CLOSED) {
            return "NOT_RUNNING";
        }
        return cursor == null ? "CYCLE_BOUNDARY" : "CONTINUING";
    }

    private static List<String> bounded(List<String> samples) {
        if (samples == null || samples.isEmpty()) return Collections.emptyList();
        int size = Math.min(MAX_SAMPLES, samples.size());
        return Collections.unmodifiableList(new ArrayList<>(samples.subList(0, size)));
    }

    private static final class InventoryGauge {
        final String state;
        final int activeRuns;
        final int waitingRetries;
        final String nextDueDelayBucket;
        final String cursorHealth;
        final List<String> samples;

        InventoryGauge(String state, int activeRuns, int waitingRetries,
                       String nextDueDelayBucket, String cursorHealth,
                       List<String> samples) {
            this.state = state;
            this.activeRuns = activeRuns;
            this.waitingRetries = waitingRetries;
            this.nextDueDelayBucket = nextDueDelayBucket;
            this.cursorHealth = cursorHealth;
            this.samples = samples;
        }

        static InventoryGauge notRunning() {
            return new InventoryGauge("IDLE", 0, 0, "NONE",
                    "NOT_RUNNING", Collections.emptyList());
        }
    }
}
