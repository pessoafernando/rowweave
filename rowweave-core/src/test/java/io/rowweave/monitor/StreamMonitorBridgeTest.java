package io.rowweave.monitor;

import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Stream evidence becomes a live monitor snapshot without exposing raw job data. */
class StreamMonitorBridgeTest {

    @Test
    void progressPreservesSubmissionTimeAndRuntimeTopology() {
        InMemoryJobMonitor registry = new InMemoryJobMonitor(4);
        StreamJobId id = StreamJobId.of("stable-view");
        registry.onJobSubmit(JobView.builder(id.value(), "stable")
                .submitTime(1234L).dagJson("{\"nodes\":[{\"id\":0}],\"edges\":[]}")
                .addStage(StageView.builder(0).role("SOURCE").state("RUNNING").build())
                .build());
        new StreamMonitorBridge(registry).progress(id, "stable",
                StreamJobStatus.RUNNING, evidence(id));
        assertEquals(1234L, registry.job(id.value()).submitTime());
        assertEquals(1, registry.job(id.value()).stages().size());
        assertTrue(registry.job(id.value()).dagJson().contains("nodes"));
    }

    @Test
    void progressUpdatesTypedViewAndMetricSet() {
        RecordingRegistry registry = new RecordingRegistry();
        StreamMonitorBridge bridge = new StreamMonitorBridge(registry);
        StreamJobId jobId = StreamJobId.of("stream-live");
        StreamJobEvidence evidence = evidence(jobId);

        bridge.submit(jobId, "Orders", StreamJobStatus.SUBMITTED, evidence);
        bridge.progress(jobId, "Orders", StreamJobStatus.RUNNING, evidence);

        JobView job = registry.job("stream-live");
        assertNotNull(job);
        assertEquals("Orders", job.name());
        assertEquals("RUNNING", job.state());
        assertNotNull(job.stream());

        StreamMonitorView view = job.stream();
        assertEquals(3, view.attemptProgress().attempt());
        assertEquals(1L, view.attemptProgress().completedCheckpoints());
        assertEquals(5L, view.lifetimeProgress().completedCheckpoints());
        assertEquals(123_456L, view.lifetimeProgress().operatorWatermark());
        assertEquals(StreamCdcPhase.LOG, view.lifetimeProgress().cdcPhase());
        assertEquals(6, view.placementTaskCount());
        assertEquals(6, view.reportedTaskCount());
        assertTrue(view.available());
        assertTrue(view.complete());

        MetricSet metrics = findMetrics(registry, "stream-live");
        assertEquals("stream", metrics.scope());
        assertEquals("stream-live", metrics.tags().get("jobId"));
        assertEquals(5L, value(metrics, "stream.checkpoints_completed").longValue());
        assertEquals(123_456L, value(metrics, "stream.operator_watermark_millis").longValue());
        assertEquals(StreamCdcPhase.LOG.ordinal(),
                value(metrics, "stream.cdc_phase").longValue());
    }

    @Test
    void terminalFailureKeepsLastTypedViewAndPublishesError() {
        InMemoryJobMonitor registry = new InMemoryJobMonitor(4);
        StreamMonitorBridge bridge = new StreamMonitorBridge(registry);
        StreamJobId jobId = StreamJobId.of("stream-failed");
        StreamJobEvidence evidence = evidence(jobId);

        bridge.submit(jobId, "Orders", StreamJobStatus.RUNNING, evidence);
        bridge.complete(jobId, "Orders", StreamJobStatus.FAILED,
                "source stopped unexpectedly", evidence);

        JobView job = registry.job("stream-failed");
        assertEquals("FAILED", job.state());
        assertEquals("source stopped unexpectedly", job.error());
        assertNotNull(job.stream());
        assertEquals(5L, job.stream().lifetimeProgress().completedCheckpoints());
    }

    @Test
    void registryFailuresNeverEscapeExecutionPath() {
        AtomicBoolean failed = new AtomicBoolean();
        JobMonitorRegistry registry = new JobMonitorRegistry() {
            @Override public void onJobSubmit(JobView job) {
                failed.set(true);
                throw new IllegalStateException("monitor unavailable");
            }
            @Override public void onJobComplete(String jobId, String state, String error) {
                throw new IllegalStateException("monitor unavailable");
            }
            @Override public void onStageUpdate(String jobId, StageView stage) {
                throw new IllegalStateException("monitor unavailable");
            }
            @Override public void onMetrics(String jobId, MetricSet metrics) {
                throw new IllegalStateException("monitor unavailable");
            }
            @Override public List<JobView> jobs() {
                throw new IllegalStateException("monitor unavailable");
            }
            @Override public JobView job(String jobId) {
                throw new IllegalStateException("monitor unavailable");
            }
            @Override public List<WorkerView> workers() {
                throw new IllegalStateException("monitor unavailable");
            }
        };
        StreamMonitorBridge bridge = new StreamMonitorBridge(registry);
        StreamJobId jobId = StreamJobId.of("stream-isolated");
        StreamJobEvidence evidence = evidence(jobId);

        bridge.submit(jobId, "Orders", StreamJobStatus.RUNNING, evidence);
        bridge.progress(jobId, "Orders", StreamJobStatus.STOPPING, evidence);
        bridge.complete(jobId, "Orders", StreamJobStatus.CANCELED, null, evidence);
        assertTrue(failed.get());
    }

    @Test
    void inputsAreValidatedBeforeRegistryCalls() {
        StreamMonitorBridge bridge = new StreamMonitorBridge(new InMemoryJobMonitor(1));
        StreamJobId jobId = StreamJobId.of("stream-invalid");
        assertThrows(IllegalArgumentException.class,
                () -> bridge.submit(jobId, " ", StreamJobStatus.RUNNING, evidence(jobId)));
        assertThrows(IllegalArgumentException.class,
                () -> new StreamMonitorBridge(null));
    }

    private static StreamJobEvidence evidence(StreamJobId jobId) {
        StreamJobProgress attempt = new StreamJobProgress(
                3, 1L, 123_456L, StreamCdcPhase.LOG, 2,
                1_000L, 900L, 8_000L, 7_000L,
                12L, 4_096L, 20, 32_768L, 65_536L, 40, 8, 5_000L);
        StreamJobProgress lifetime = new StreamJobProgress(
                3, 5L, 123_456L, StreamCdcPhase.LOG, 2,
                11_000L, 10_900L, 88_000L, 77_000L,
                12L, 4_096L, 20, 32_768L, 65_536L, 40, 8, 15_000L);
        return new StreamJobEvidence(jobId, "run-1", "attempt-3", 3,
                9L, 4L, 7L, "stream-checkpoint:" + repeat('a', 64), 5L, 123L, 4_096L,
                attempt, lifetime, 6, 6, 1_000L);
    }

    private static MetricSet findMetrics(RecordingRegistry registry, String jobId) {
        MetricSet metrics = registry.metrics.get(jobId);
        if (metrics == null) throw new AssertionError("missing metric set for " + jobId);
        return metrics;
    }

    private static final class RecordingRegistry implements JobMonitorRegistry {
        private final Map<String, JobView> jobs = new LinkedHashMap<>();
        private final Map<String, MetricSet> metrics = new LinkedHashMap<>();

        @Override public void onJobSubmit(JobView job) {
            jobs.put(job.jobId(), job);
        }
        @Override public void onStageUpdate(String jobId, StageView stage) { }
        @Override public void onJobComplete(String jobId, String state, String error) { }
        @Override public void onMetrics(String jobId, MetricSet value) {
            metrics.put(jobId, value);
        }
        @Override public List<JobView> jobs() {
            return new ArrayList<>(jobs.values());
        }
        @Override public JobView job(String jobId) {
            return jobs.get(jobId);
        }
        @Override public List<WorkerView> workers() {
            return new ArrayList<>();
        }
    }

    private static String repeat(char value, int count) {
        StringBuilder text = new StringBuilder(count);
        for (int index = 0; index < count; index++) text.append(value);
        return text.toString();
    }

    private static MetricValue value(MetricSet metrics, String name) {
        for (MetricValue metric : metrics.values()) {
            if (metric.name().equals(name)) return metric;
        }
        throw new AssertionError("missing metric " + name);
    }
}
