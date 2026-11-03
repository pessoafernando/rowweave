package io.rowweave.monitor;

import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PartitionDistribution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Monitor view:Observation snapshot for one job. Immutable value object(state changes rebuild snapshots via InMemoryJobMonitor).
 * state=RUNNING/FINISHED/FAILED;dagJson=frontend-renderable DAG topology ({nodes,edges}); stages=per-stage snapshots.
 */
public final class JobView {

    private final String jobId;
    private final String name;
    private final String state;
    private final long submitTime;
    private final long endTime;
    private final String dagJson;
    private final String error;
    private final List<StageView> stages;
    // Desired and effective job parallelism: desired is what setParallelism asked for, effective is what Master assigned after degrading for available slots.
    private final int desiredParallelism;
    private final int effectiveParallelism;
    private final BatchSinkView batchSink;
    private final ExecutionReport executionReport;
    private final StreamMonitorView stream;

    private JobView(Builder b) {
        this.jobId = b.jobId;
        this.name = b.name;
        this.state = b.state;
        this.submitTime = b.submitTime;
        this.endTime = b.endTime;
        this.dagJson = b.dagJson;
        this.error = b.error;
        this.stages = Collections.unmodifiableList(new ArrayList<>(b.stages));
        this.desiredParallelism = b.desiredParallelism;
        this.effectiveParallelism = b.effectiveParallelism;
        this.batchSink = b.batchSink;
        this.executionReport = b.executionReport;
        this.stream = b.stream;
    }

    public String jobId() { return jobId; }
    public String name() { return name; }
    public String state() { return state; }
    public long submitTime() { return submitTime; }
    public long endTime() { return endTime; }
    public String dagJson() { return dagJson; }
    public String error() { return error; }
    public List<StageView> stages() { return stages; }
    /** User-desired parallelism (setParallelism).  */
    public int desiredParallelism() { return desiredParallelism; }
    /** Effective parallelism assigned by Master, which may be below desired after a degradation. */
    public int effectiveParallelism() { return effectiveParallelism; }
    /** Whether parallelism degradation occurred (effective below desired). */
    public boolean downgraded() { return effectiveParallelism > 0 && effectiveParallelism < desiredParallelism; }
    /** bounded and redacted batch Sink control-plane summary. */
    public BatchSinkView batchSink() { return batchSink; }
    /** Master-validated terminal telemetry; null while running or when no trusted report on failure.  */
    public ExecutionReport executionReport() { return executionReport; }
    /** Stream operational projection; null for Batch jobs. */
    public StreamMonitorView stream() { return stream; }

    /**
     * Projects the ExecutionReport into a typed MetricSet for the job scope.
     * Returns null when no report is available (job still running or failed without trusted report).
     */
    public MetricSet metrics() {
        if (executionReport == null) return null;
        MetricSet.Builder builder = MetricSet.builder("job")
                .tag("jobId", jobId)
                .counter("job.peak_reserved_bytes", executionReport.peakReservedBytes(), "bytes")
                .counter("job.acquired_bytes", executionReport.acquiredBytes(), "bytes")
                .counter("job.spill_count", executionReport.spillCount(), "count")
                .counter("job.rows_out", executionReport.rowsOut(), "rows");
        long[] partitions = executionReport.partitionRowCounts();
        if (partitions.length > 0) {
            long sum = 0;
            for (long count : partitions) sum += count;
            builder.counter("job.partition_input_rows", sum, "rows");
            builder.gauge("job.partition_count", partitions.length, "count");
        }
        return builder.build();
    }

    public static Builder builder(String jobId, String name) { return new Builder(jobId, name); }

    /** Derives a mutable builder from an existing JobView (used to rebuild the snapshot on a state change). */
    public Builder toBuilder() {
        Builder b = new Builder(jobId, name);
        b.state = state;
        b.submitTime = submitTime;
        b.endTime = endTime;
        b.dagJson = dagJson;
        b.error = error;
        b.stages.addAll(stages);
        b.desiredParallelism = desiredParallelism;   // retains parallelism on snapshot rebuild
        b.effectiveParallelism = effectiveParallelism;
        b.batchSink = batchSink;
        b.executionReport = executionReport;
        b.stream = stream;
        return b;
    }

    public static final class Builder {
        private final String jobId;
        private final String name;
        private String state = "RUNNING";
        private long submitTime = System.currentTimeMillis();
        private long endTime = 0;
        private String dagJson = "{}";
        private String error;
        private final List<StageView> stages = new ArrayList<>();
        private int desiredParallelism = 1;   // default 1 (non-degraded job effective==desired)
        private int effectiveParallelism = 1;
        private BatchSinkView batchSink;
        private ExecutionReport executionReport;
        private StreamMonitorView stream;

        private Builder(String jobId, String name) {
            this.jobId = jobId;
            this.name = name;
        }

        public Builder state(String s) { this.state = s; return this; }
        public Builder submitTime(long t) { this.submitTime = t; return this; }
        public Builder endTime(long t) { this.endTime = t; return this; }
        public Builder dagJson(String j) { this.dagJson = j; return this; }
        public Builder error(String e) { this.error = e; return this; }
        public Builder addStage(StageView s) { this.stages.add(s); return this; }
        /** Set job desired/effective parallelism (degradation visible).  */
        public Builder parallelism(int desired, int effective) {
            this.desiredParallelism = desired; this.effectiveParallelism = effective; return this;
        }
        public Builder batchSink(BatchSinkView view) {
            this.batchSink = view;
            return this;
        }
        public Builder executionReport(ExecutionReport report) {
            this.executionReport = report;
            return this;
        }
        public Builder stream(StreamMonitorView view) {
            this.stream = view;
            return this;
        }

        /** Replace the stage at the given index (for state updates); appends if absent.  */
        public Builder replaceStage(StageView s) {
            for (int i = 0; i < stages.size(); i++) {
                if (stages.get(i).index() == s.index()) {
                    stages.set(i, s);
                    return this;
                }
            }
            stages.add(s);
            return this;
        }

        public JobView build() { return new JobView(this); }
    }
}
