package io.rowweave.monitor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Read-only bounded observation view for the terminal run janitor.  */
public final class RunRetentionView {
    private final int backlog;
    private final int lifecycleCapacityPerShard;
    private final int scanPageCapacity;
    private final long oldestTerminalAgeMillis;
    private final long claimedRuns;
    private final long purgedRuns;
    private final long errorRuns;
    private final boolean cycleComplete;
    private final List<String> diagnosticSamples;

    public RunRetentionView(int backlog, int lifecycleCapacityPerShard,
                            int scanPageCapacity, long oldestTerminalAgeMillis,
                            long claimedRuns, long purgedRuns, long errorRuns,
                            boolean cycleComplete, List<String> diagnosticSamples) {
        if (backlog < 0 || lifecycleCapacityPerShard < 1 || scanPageCapacity < 1
                || oldestTerminalAgeMillis < 0L || claimedRuns < 0L
                || purgedRuns < 0L || errorRuns < 0L) {
            throw new IllegalArgumentException("invalid run retention metrics");
        }
        if (diagnosticSamples == null || diagnosticSamples.size() > 16) {
            throw new IllegalArgumentException("invalid retention diagnostic samples");
        }
        this.backlog = backlog;
        this.lifecycleCapacityPerShard = lifecycleCapacityPerShard;
        this.scanPageCapacity = scanPageCapacity;
        this.oldestTerminalAgeMillis = oldestTerminalAgeMillis;
        this.claimedRuns = claimedRuns;
        this.purgedRuns = purgedRuns;
        this.errorRuns = errorRuns;
        this.cycleComplete = cycleComplete;
        this.diagnosticSamples = Collections.unmodifiableList(
                new ArrayList<>(diagnosticSamples));
    }

    /** Terminal refs observed in the most recent bounded page scan, not a globally exact total.  */
    public int backlog() { return backlog; }
    public int lifecycleCapacityPerShard() { return lifecycleCapacityPerShard; }
    public int scanPageCapacity() { return scanPageCapacity; }
    public long oldestTerminalAgeMillis() { return oldestTerminalAgeMillis; }
    public long claimedRuns() { return claimedRuns; }
    public long purgedRuns() { return purgedRuns; }
    public long errorRuns() { return errorRuns; }
    public boolean cycleComplete() { return cycleComplete; }
    public List<String> diagnosticSamples() { return diagnosticSamples; }
}
