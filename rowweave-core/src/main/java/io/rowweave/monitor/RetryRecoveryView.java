package io.rowweave.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Read-only bounded health view for the whole-attempt retry coordinator.  */
public final class RetryRecoveryView {
    private final String state;
    private final int activeRuns;
    private final int waitingRetries;
    private final String nextDueDelayBucket;
    private final long scheduledRetries;
    private final long exhaustedRetries;
    private final long notRetryableClosures;
    private final String maintenanceCursorHealth;
    private final int scanPageCapacity;
    private final List<String> diagnosticSamples;

    public RetryRecoveryView(
            String state, int activeRuns, int waitingRetries,
            String nextDueDelayBucket, long scheduledRetries,
            long exhaustedRetries, long notRetryableClosures,
            String maintenanceCursorHealth, int scanPageCapacity,
            List<String> diagnosticSamples) {
        requireOneOf(state, "state", "IDLE", "SCANNING", "READY",
                "EMPTY_READY", "BLOCKED_PENDING_RECOVERY",
                "BLOCKED_CORRUPT", "BLOCKED_UNAVAILABLE",
                "BLOCKED_CAPABILITY", "LEADERSHIP_LOST", "NOT_LEADER",
                "CLOSED");
        requireOneOf(nextDueDelayBucket, "nextDueDelayBucket", "NONE",
                "DUE", "LE_1S", "LE_5S", "LE_30S", "GT_30S");
        requireOneOf(maintenanceCursorHealth, "maintenanceCursorHealth",
                "NOT_RUNNING", "CYCLE_BOUNDARY", "CONTINUING");
        if (activeRuns < 0 || waitingRetries < 0 || scheduledRetries < 0L
                || exhaustedRetries < 0L || notRetryableClosures < 0L
                || scanPageCapacity < 1) {
            throw new IllegalArgumentException("invalid retry recovery metrics");
        }
        if (diagnosticSamples == null || diagnosticSamples.size() > 16) {
            throw new IllegalArgumentException(
                    "invalid retry recovery diagnostic samples");
        }
        this.state = state;
        this.activeRuns = activeRuns;
        this.waitingRetries = waitingRetries;
        this.nextDueDelayBucket = nextDueDelayBucket;
        this.scheduledRetries = scheduledRetries;
        this.exhaustedRetries = exhaustedRetries;
        this.notRetryableClosures = notRetryableClosures;
        this.maintenanceCursorHealth = maintenanceCursorHealth;
        this.scanPageCapacity = scanPageCapacity;
        this.diagnosticSamples = Collections.unmodifiableList(
                new ArrayList<>(diagnosticSamples));
    }

    private static void requireOneOf(String value, String field,
                                     String... allowed) {
        for (String candidate : allowed) {
            if (candidate.equals(value)) return;
        }
        throw new IllegalArgumentException(field + " is invalid");
    }

    public String state() { return state; }
    public int activeRuns() { return activeRuns; }
    public int waitingRetries() { return waitingRetries; }
    public String nextDueDelayBucket() { return nextDueDelayBucket; }
    public long scheduledRetries() { return scheduledRetries; }
    public long exhaustedRetries() { return exhaustedRetries; }
    public long notRetryableClosures() { return notRetryableClosures; }
    public String maintenanceCursorHealth() { return maintenanceCursorHealth; }
    public int scanPageCapacity() { return scanPageCapacity; }
    public List<String> diagnosticSamples() { return diagnosticSamples; }
}
