package io.rowweave.monitor;

import io.rowweave.plan.ExecutionReport;

import java.util.Collections;
import java.util.List;

/**
 * Runtime monitoring collection interface: the engine pushes observation events through the job lifecycle, and the monitor service (rowweave-monitor) reads snapshots for display.
 * This is the sole seam between core and monitoring, in the same position as ServiceRegistry and MetricsListener. Core depends only on this interface plus {@link InMemoryJobMonitor},
 * default implementation,**zero HTTP dependency**(HTTP lives in the separate rowweave-monitor module).
 *
 * <p>Collection is **advisory**:Callers(MasterServer/local execution)must try/catch isolate exceptions,never affect job execution(invariant).
 * Default {@link #NOOP} is zero-overhead; engine behavior unchanged when no monitor is injected.
 */
public interface JobMonitorRegistry {

    /** Job submission (initial DAG snapshot plus the stage list). */
    void onJobSubmit(JobView job);

    /** Stage state change (state, outputRows, workerId, error). */
    void onStageUpdate(String jobId, StageView stage);

    /** Job completion(FINISHED/FAILED + optional error).  */
    void onJobComplete(String jobId, String state, String error);

    /** Includes the Master-validated aggregate report on completion; prior overload retained for source compatibility.  */
    default void onJobComplete(String jobId, String state, String error,
                               ExecutionReport report) {
        onJobComplete(jobId, state, error);
    }

    /** replace the bounded Sink control-plane projection for one job. */
    default void onBatchSinkUpdate(String jobId, BatchSinkView view) { }

    /** Read-only:current + historical job snapshots(bounded).  */
    List<JobView> jobs();

    /** Read-only: one job snapshot; null when absent. */
    JobView job(String jobId);

    /** Read-only: worker list snapshot. */
    List<WorkerView> workers();

    /** Read-only latest runtime metric values; implementations keep this view bounded. */
    default List<MetricSet> metrics() {
        return Collections.emptyList();
    }

    /** Configured bound for retained job history; useful only for sanitized diagnostics. */
    default int historyLimit() {
        return 200;
    }

    /** Terminal retention observation is advisory; implementations should atomically replace the latest snapshot. */
    default void onRunRetention(RunRetentionView view) { }

    /** Latest terminal retention snapshot; null when not published or when NOOP. */
    default RunRetentionView runRetention() { return null; }

    /** Artifact retention and migration observation is advisory. */
    default void onArtifactRetention(ArtifactRetentionView view) { }

    /** Latest artifact retention snapshot; null when not published or when NOOP. */
    default ArtifactRetentionView artifactRetention() { return null; }

    /** Whole-attempt retry and recovery coordinator observation is advisory. */
    default void onRetryRecovery(RetryRecoveryView view) { }

    /** Latest retry and recovery snapshot; null when not published or when NOOP. */
    default RetryRecoveryView retryRecovery() { return null; }

    /**
     * Runtime metric snapshot pushed periodically during execution.
     * Implementations should replace the latest snapshot per (scope, tags, name).
     * Advisory: must never block or fail the calling thread.
     */
    default void onMetrics(String jobId, MetricSet metrics) { }

    /** Zero-overhead empty implementation: used when no monitor is injected, and engine behaviour is unchanged. */
    JobMonitorRegistry NOOP = new JobMonitorRegistry() {
        @Override public void onJobSubmit(JobView job) { }
        @Override public void onStageUpdate(String jobId, StageView stage) { }
        @Override public void onJobComplete(String jobId, String state, String error) { }
        @Override public List<JobView> jobs() { return Collections.emptyList(); }
        @Override public JobView job(String jobId) { return null; }
        @Override public List<WorkerView> workers() { return Collections.emptyList(); }
    };
}
