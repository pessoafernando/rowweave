package io.rowweave.monitor;

import io.rowweave.plan.ExecutionReport;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link JobMonitorRegistry} Pure in-memory implementation:collects job/stage observations in-process for read-only monitor access.
 * Bounded history retains admitted active observations and evicts the oldest
 * terminal observation first. New observations are skipped when all slots are active.
 *
 * <p>Thread safety: all reads and writes happen under this object's monitor lock (the collection is not on the hot path and job-level events are infrequent; snapshot reads copy so that an outside caller cannot mutate state).
 * State changes rebuild an immutable snapshot through {@link JobView#toBuilder()}.
 */
public final class InMemoryJobMonitor implements JobMonitorRegistry {

    private final int capacity;
    private final LinkedHashMap<String, JobView> jobs;
    private volatile List<WorkerView> workers = new ArrayList<>();
    private volatile RunRetentionView runRetention;
    private volatile ArtifactRetentionView artifactRetention;
    private volatile RetryRecoveryView retryRecovery;
    private final LinkedHashMap<String, MetricEntry> metricValues;

    public InMemoryJobMonitor(int capacity) {
        this.capacity = capacity;
        if (capacity < 1) throw new IllegalArgumentException("monitor capacity must be positive");
        this.jobs = new LinkedHashMap<>();
        this.metricValues = new LinkedHashMap<String, MetricEntry>() {
            @Override
            protected boolean removeEldestEntry(Map.Entry<String, MetricEntry> eldest) {
                return size() > InMemoryJobMonitor.this.maxMetricValues();
            }
        };
    }

    private int maxMetricValues() {
        return Math.max(256, capacity * 8);
    }

    @Override
    public synchronized void onJobSubmit(JobView job) {
        JobView previous = jobs.get(job.jobId());
        if (previous != null) {
            if (terminal(previous.state()) && !terminal(job.state())) return;
            JobView.Builder merged = job.toBuilder().submitTime(previous.submitTime());
            if (previous.endTime() > 0L) merged.endTime(previous.endTime());
            if (job.stages().isEmpty() && !previous.stages().isEmpty()) {
                merged.dagJson(previous.dagJson());
                for (StageView stage : previous.stages()) merged.replaceStage(stage);
            }
            job = merged.build();
        }
        if (!jobs.containsKey(job.jobId()) && jobs.size() >= capacity) {
            java.util.Iterator<Map.Entry<String, JobView>> entries = jobs.entrySet().iterator();
            boolean evicted = false;
            while (entries.hasNext()) {
                if (terminal(entries.next().getValue().state())) {
                    entries.remove();
                    evicted = true;
                    break;
                }
            }
            if (!evicted) return;
        }
        jobs.put(job.jobId(), job);
    }

    private static boolean terminal(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state)
                || "CANCELED".equals(state) || "STOPPED_WITH_SAVEPOINT".equals(state);
    }

    @Override
    public synchronized void onStageUpdate(String jobId, StageView stage) {
        JobView cur = jobs.get(jobId);
        if (cur == null) {
            return; // unknown job(may have been evicted),  advisory, ignoring
        }
        jobs.put(jobId, cur.toBuilder().replaceStage(stage).build());
    }

    @Override
    public synchronized void onJobComplete(String jobId, String state, String error) {
        onJobComplete(jobId, state, error, null);
    }

    @Override
    public synchronized void onJobComplete(String jobId, String state, String error,
                                           ExecutionReport report) {
        JobView cur = jobs.get(jobId);
        if (cur == null) {
            return;
        }
        jobs.put(jobId, cur.toBuilder()
                .state(state).error(error).executionReport(report)
                .endTime(cur.endTime() > 0L ? cur.endTime()
                        : System.currentTimeMillis()).build());
    }

    @Override
    public synchronized void onBatchSinkUpdate(String jobId, BatchSinkView view) {
        if (view == null) throw new IllegalArgumentException("view is required");
        JobView cur = jobs.get(jobId);
        if (cur == null) return;
        jobs.put(jobId, cur.toBuilder().batchSink(view).build());
    }

    @Override
    public synchronized List<JobView> jobs() {
        return new ArrayList<>(jobs.values());
    }

    @Override
    public synchronized JobView job(String jobId) {
        return jobs.get(jobId);
    }

    @Override
    public List<WorkerView> workers() {
        return new ArrayList<>(workers);
    }

    @Override
    public synchronized void onMetrics(String jobId, MetricSet metrics) {
        if (metrics == null) {
            return;
        }
        for (MetricValue value : metrics.values()) {
            metricValues.put(metricKey(metrics, value),
                    new MetricEntry(metrics.scope(), metrics.tags(), value));
        }
    }

    @Override
    public int historyLimit() {
        return capacity;
    }

    @Override
    public synchronized List<MetricSet> metrics() {
        List<MetricSet> result = new ArrayList<>(metricValues.size());
        for (MetricEntry entry : metricValues.values()) {
            MetricSet.Builder builder = MetricSet.builder(entry.scope).add(entry.value);
            for (Map.Entry<String, String> tag : entry.tags.entrySet()) {
                builder.tag(tag.getKey(), tag.getValue());
            }
            result.add(builder.build());
        }
        return result;
    }

    private static String metricKey(MetricSet metrics, MetricValue value) {
        return metrics.scope() + '|' + metrics.tags() + '|' + value.name();
    }

    private static final class MetricEntry {
        private final String scope;
        private final Map<String, String> tags;
        private final MetricValue value;

        private MetricEntry(String scope, Map<String, String> tags, MetricValue value) {
            this.scope = scope;
            this.tags = tags;
            this.value = value;
        }
    }

    @Override
    public void onRunRetention(RunRetentionView view) {
        if (view == null) throw new IllegalArgumentException("view is required");
        runRetention = view;
    }

    @Override
    public RunRetentionView runRetention() {
        return runRetention;
    }

    @Override
    public void onArtifactRetention(ArtifactRetentionView view) {
        if (view == null) throw new IllegalArgumentException("view is required");
        artifactRetention = view;
    }

    @Override
    public ArtifactRetentionView artifactRetention() {
        return artifactRetention;
    }

    @Override
    public void onRetryRecovery(RetryRecoveryView view) {
        if (view == null) throw new IllegalArgumentException("view is required");
        retryRecovery = view;
    }

    @Override
    public RetryRecoveryView retryRecovery() {
        return retryRecovery;
    }

    /** Updates the worker snapshot: Master reads registry.liveWorkers() periodically and refreshes it with heartbeat liveness. */
    public void setWorkers(List<WorkerView> ws) {
        this.workers = new ArrayList<>(ws);
    }
}
