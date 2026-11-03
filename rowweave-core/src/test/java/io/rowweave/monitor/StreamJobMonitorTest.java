package io.rowweave.monitor;

import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamCommandHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamOfflineRescaleRequest;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamSubmissionId;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every Stream client implementation shares one monitoring decoration. */
class StreamJobMonitorTest {

    @Test
    void closeDoesNotInterruptAnInFlightLifecycleObservation() throws Exception {
        FakeClient delegate = new FakeClient();
        delegate.handle.sampleStarted = new java.util.concurrent.CountDownLatch(1);
        delegate.handle.finishSample = new java.util.concurrent.CountDownLatch(1);
        try (StreamJobClient client = new MonitoredStreamJobClient(
                delegate, new InMemoryJobMonitor(4))) {
            client.attach(StreamJobId.of("job-live"));
            assertTrue(delegate.handle.sampleStarted.await(3, java.util.concurrent.TimeUnit.SECONDS));
            client.close();
            assertTrue(delegate.handle.sampleFinished.await(3, java.util.concurrent.TimeUnit.SECONDS));
            assertFalse(delegate.handle.sampleInterrupted.get(),
                    "advisory shutdown must not interrupt authoritative file or lifecycle calls");
        }
    }

    @Test
    void terminalStatusIsObservedWithoutApplicationPolling() throws Exception {
        InMemoryJobMonitor registry = new InMemoryJobMonitor(4);
        FakeClient delegate = new FakeClient();
        try (StreamJobClient client = new MonitoredStreamJobClient(delegate, registry)) {
            client.attach(StreamJobId.of("job-live"));
            delegate.nextStatus(StreamJobStatus.FAILED);
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(3);
            while (!"FAILED".equals(registry.job("job-live").state())
                    && System.nanoTime() < deadline) Thread.sleep(10L);
            assertEquals("FAILED", registry.job("job-live").state());
        }
    }

    @Test
    void submitAndStatusTransitionsFeedSameRegistry() {
        RecordingRegistry registry = new RecordingRegistry();
        FakeClient delegate = new FakeClient();
        StreamJobClient client = new MonitoredStreamJobClient(delegate, registry);

        StreamJobHandle handle = client.submit(
                StreamSubmissionId.of("submission-live"), null);
        assertEquals("SUBMITTED", registry.job("job-live").state());

        delegate.nextStatus(StreamJobStatus.RUNNING);
        assertEquals(StreamJobStatus.RUNNING, handle.status());
        assertEquals("RUNNING", registry.job("job-live").state());

        delegate.nextStatus(StreamJobStatus.FAILED);
        assertEquals(StreamJobStatus.FAILED, handle.awaitTermination(1L));
        assertEquals("FAILED", registry.job("job-live").state());
        assertNotNull(registry.metric("job-live"));
        assertTrue(registry.job("job-live").stream() != null);
        client.close();
    }

    @Test
    void closeIsDelegatedAndHandleEvidenceFallbackIsBounded() {
        RecordingRegistry registry = new RecordingRegistry();
        FakeClient delegate = new FakeClient();
        StreamJobClient client = new MonitoredStreamJobClient(delegate, registry);
        StreamJobHandle handle = client.attach(StreamJobId.of("job-live"));

        delegate.nextStatus(StreamJobStatus.STOPPING);
        assertEquals(StreamJobStatus.STOPPING, handle.status());
        assertTrue(registry.job("job-live").stream() != null);

        client.close();
        assertTrue(delegate.closed.get());
    }

    private static final class FakeClient implements StreamJobClient {
        private final FakeHandle handle = new FakeHandle();
        private final AtomicBoolean closed = new AtomicBoolean();

        void nextStatus(StreamJobStatus status) {
            handle.nextStatus.set(status);
        }

        @Override public StreamJobHandle submit(
                StreamSubmissionId submissionId, PlanArtifact artifact) {
            handle.statusCalls.set(0);
            return handle;
        }
        @Override public StreamJobHandle attachSubmission(
                StreamSubmissionId submissionId) {
            handle.statusCalls.set(0);
            return handle;
        }
        @Override public StreamJobHandle attach(StreamJobId jobId) {
            handle.statusCalls.set(0);
            return handle;
        }
        @Override public StreamCommandHandle attachCancel(
                StreamJobId jobId, StreamCommandId commandId) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamCheckpointHandle attachCheckpoint(
                StreamJobId jobId, StreamCommandId commandId) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamSavepointHandle attachSavepoint(
                StreamJobId jobId, StreamCommandId commandId) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamJobHandle restore(
                StreamSubmissionId submissionId, PlanArtifact artifact,
                StreamRestoreRequest request) {
            handle.statusCalls.set(0);
            return handle;
        }
        @Override public StreamJobHandle offlineRescale(
                StreamSubmissionId submissionId, PlanArtifact artifact,
                StreamOfflineRescaleRequest request) {
            handle.statusCalls.set(0);
            return handle;
        }
        @Override public void close() {
            closed.set(true);
            if (handle.finishSample != null) handle.finishSample.countDown();
        }
    }

    private static final class FakeHandle implements StreamJobHandle {
        private java.util.concurrent.CountDownLatch sampleStarted;
        private java.util.concurrent.CountDownLatch finishSample;
        private final java.util.concurrent.CountDownLatch sampleFinished =
                new java.util.concurrent.CountDownLatch(1);
        private final AtomicBoolean sampleInterrupted = new AtomicBoolean();
        private final AtomicInteger statusCalls = new AtomicInteger();
        private final AtomicReference<StreamJobStatus> nextStatus =
                new AtomicReference<>(StreamJobStatus.SUBMITTED);

        @Override public StreamJobId id() { return StreamJobId.of("job-live"); }
        @Override public String artifactId() { return "artifact-live"; }
        @Override public StreamJobStatus status() {
            int call = statusCalls.getAndIncrement();
            if (call == 0) return StreamJobStatus.SUBMITTED;
            if (finishSample != null) {
                sampleStarted.countDown();
                try {
                    finishSample.await();
                } catch (InterruptedException interrupted) {
                    sampleInterrupted.set(true);
                } finally {
                    sampleFinished.countDown();
                }
            }
            return nextStatus.get();
        }
        @Override public StreamJobProgress progress() {
            return new StreamJobProgress(
                    1, 2L, 456L, StreamCdcPhase.LOG, 0,
                    20L, 18L, 200L, 180L, 0, 0L,
                    0, 0L, 100L, 0, 0, 0L);
        }
        @Override public StreamJobEvidence evidence() {
            throw new UnsupportedOperationException("not available");
        }
        @Override public StreamCommandHandle cancel(StreamCommandId commandId) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamCheckpointHandle triggerCheckpoint(
                StreamCommandId commandId) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamSavepointHandle stopWithSavepoint(
                StreamCommandId commandId, StreamSavepointRequest request) {
            throw new UnsupportedOperationException();
        }
        @Override public StreamJobStatus awaitRunning(long timeoutMillis) {
            return status();
        }
        @Override public StreamJobStatus awaitTermination(long timeoutMillis) {
            return status();
        }
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
        MetricSet metric(String jobId) {
            return metrics.get(jobId);
        }
    }
}
