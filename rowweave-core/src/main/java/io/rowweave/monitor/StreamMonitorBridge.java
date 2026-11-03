package io.rowweave.monitor;

import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;

import java.util.Objects;

/**
 * Converts public Stream lifecycle evidence into registry snapshots and typed
 * metrics. Monitoring is advisory: registry failures are logged and isolated.
 */
public final class StreamMonitorBridge {

    private final JobMonitorRegistry registry;

    public StreamMonitorBridge(JobMonitorRegistry registry) {
        if (registry == null) {
            throw new IllegalArgumentException("registry is required");
        }
        this.registry = registry;
    }

    public void submit(StreamJobId jobId, String name, StreamJobStatus status,
                       StreamJobEvidence evidence) {
        publish(jobId, name, status, evidence, null, false);
    }

    public void progress(StreamJobId jobId, String name, StreamJobStatus status,
                         StreamJobEvidence evidence) {
        publish(jobId, name, status, evidence, null, false);
    }

    public void complete(StreamJobId jobId, String name, StreamJobStatus status,
                         String error, StreamJobEvidence evidence) {
        if (!status.terminal()) {
            throw new IllegalArgumentException("terminal Stream status is required");
        }
        publish(jobId, name, status, evidence, error, true);
    }

    /** Canonical stream metric names for HTTP export and Prometheus exposition. */
    public MetricSet metrics(StreamJobEvidence evidence) {
        Objects.requireNonNull(evidence, "evidence");
        StreamJobProgress lifetime = evidence.lifetimeProgress();
        StreamJobProgress attempt = evidence.attemptProgress();
        MetricSet.Builder builder = MetricSet.builder("stream")
                .tag("jobId", evidence.jobId().value());
        if (evidence.runId() != null) builder.tag("runId", evidence.runId());
        if (evidence.attemptId() != null) {
            builder.tag("attemptId", evidence.attemptId());
        }
        builder
                .counter("stream.checkpoints_completed",
                        lifetime.completedCheckpoints(), "count")
                .counter("stream.attempt_checkpoints_completed",
                        attempt.completedCheckpoints(), "count")
                .gauge("stream.operator_watermark_millis",
                        lifetime.operatorWatermark(), "milliseconds")
                .gauge("stream.cdc_phase",
                        lifetime.cdcPhase().ordinal(), "count")
                .counter("stream.restarts", lifetime.restarts(), "count")
                .counter("stream.records_read", lifetime.recordsRead(), "events")
                .counter("stream.records_written",
                        lifetime.recordsWritten(), "events")
                .counter("stream.bytes_read", lifetime.bytesRead(), "bytes")
                .counter("stream.bytes_written", lifetime.bytesWritten(), "bytes")
                .counter("stream.backpressured_millis",
                        lifetime.backpressuredMillis(), "milliseconds")
                .gauge("stream.backlog_events", lifetime.backlogEvents(), "events")
                .gauge("stream.backlog_bytes", lifetime.backlogBytes(), "bytes")
                .gauge("stream.mailbox_high_water_events",
                        lifetime.mailboxHighWaterEvents(), "events")
                .gauge("stream.channel_high_water_bytes",
                        lifetime.channelHighWaterBytes(), "bytes")
                .gauge("stream.managed_state_bytes",
                        lifetime.managedStateBytes(), "bytes")
                .gauge("stream.timer_high_water_count",
                        lifetime.timerHighWaterCount(), "count")
                .gauge("stream.window_high_water_count",
                        lifetime.windowHighWaterCount(), "count")
                .gauge("stream.last_checkpoint_duration_millis",
                        evidence.latestCheckpointDurationMillis(), "milliseconds")
                .gauge("stream.placement_task_count",
                        evidence.placementTaskCount(), "count")
                .gauge("stream.reported_task_count",
                        evidence.reportedTaskCount(), "count");
        return builder.build();
    }

    private void publish(StreamJobId jobId, String name, StreamJobStatus status,
                         StreamJobEvidence evidence, String error, boolean terminal) {
        if (jobId == null || status == null) {
            throw new IllegalArgumentException("Stream job identity and status are required");
        }
        if (name == null || name.codePoints().allMatch(Character::isWhitespace)) {
            throw new IllegalArgumentException("Stream job name is required");
        }
        if (evidence == null) {
            throw new IllegalArgumentException("Stream evidence is required");
        }
        if (!jobId.value().equals(evidence.jobId().value())) {
            throw new IllegalArgumentException("Stream job identity does not match evidence");
        }
        synchronized (registry) {
            JobView previous = null;
            try {
                previous = registry.job(jobId.value());
            } catch (RuntimeException ignored) {
                // Snapshot reads are advisory.
            }
            if (previous != null && isTerminal(previous.state()) && !status.terminal()) return;
            JobView.Builder builder = (previous == null
                    ? JobView.builder(jobId.value(), name) : previous.toBuilder())
                    .state(status.name()).stream(new StreamMonitorView(evidence));
            if (terminal) {
                builder.endTime(previous != null && previous.endTime() > 0L
                        ? previous.endTime() : System.currentTimeMillis());
                if (error != null) builder.error(error);
            }
            JobView view = builder.build();
            safe(() -> registry.onJobSubmit(view));
            safeMetrics(evidence);
            if (terminal) {
                safe(() -> registry.onJobComplete(jobId.value(), status.name(), error));
            }
        }
    }

    private static boolean isTerminal(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state)
                || "CANCELED".equals(state) || "STOPPED_WITH_SAVEPOINT".equals(state);
    }

    private void safeMetrics(StreamJobEvidence evidence) {
        try {
            registry.onMetrics(evidence.jobId().value(), metrics(evidence));
        } catch (RuntimeException ignored) {
            // Monitoring is advisory and must never affect job execution.
        }
    }

    private void safe(Runnable action) {
        try {
            action.run();
        } catch (RuntimeException ignored) {
            // Monitoring is advisory and must never affect job execution.
        }
    }
}
