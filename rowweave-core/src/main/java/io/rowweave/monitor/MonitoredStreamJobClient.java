package io.rowweave.monitor;

import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamCommandHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamOfflineRescaleRequest;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamRescaleHandle;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamSubmissionId;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * One monitoring facade for every Stream client transport. Execution methods
 * delegate unchanged; status observations are translated into registry events.
 */
public final class MonitoredStreamJobClient implements StreamJobClient {

    private final StreamJobClient delegate;
    private final StreamMonitorBridge bridge;
    private final JobMonitorRegistry registry;
    private final java.util.Map<String, MonitoredHandle> observed = new java.util.LinkedHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private java.util.concurrent.ScheduledExecutorService sampler;

    public MonitoredStreamJobClient(StreamJobClient delegate,
                                    JobMonitorRegistry registry) {
        if (delegate == null) {
            throw new IllegalArgumentException("delegate client is required");
        }
        this.delegate = delegate;
        this.registry = registry;
        this.bridge = new StreamMonitorBridge(registry);
    }

    @Override public StreamJobHandle submit(StreamSubmissionId submissionId,
                                            PlanArtifact streamArtifact) {
        return monitored(delegate.submit(submissionId, streamArtifact));
    }

    @Override public StreamJobHandle attachSubmission(
            StreamSubmissionId submissionId) {
        return monitored(delegate.attachSubmission(submissionId));
    }

    @Override public StreamJobHandle attach(StreamJobId jobId) {
        return monitored(delegate.attach(jobId));
    }

    @Override public StreamCommandHandle attachCancel(
            StreamJobId jobId, StreamCommandId commandId) {
        return delegate.attachCancel(jobId, commandId);
    }

    @Override public StreamCheckpointHandle attachCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return delegate.attachCheckpoint(jobId, commandId);
    }

    @Override public StreamSavepointHandle attachSavepoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return delegate.attachSavepoint(jobId, commandId);
    }

    @Override public StreamRescaleHandle attachRescale(
            StreamJobId jobId, StreamCommandId commandId) {
        return delegate.attachRescale(jobId, commandId);
    }

    @Override public StreamJobHandle restore(StreamSubmissionId submissionId,
            PlanArtifact streamArtifact, StreamRestoreRequest request) {
        return monitored(delegate.restore(submissionId, streamArtifact, request));
    }

    @Override public StreamJobHandle offlineRescale(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact,
            StreamOfflineRescaleRequest request) {
        return monitored(delegate.offlineRescale(
                submissionId, streamArtifact, request));
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        synchronized (observed) {
            observed.clear();
            // A sample may be inside an authoritative lifecycle or file call.
            // Stop scheduling without injecting an interrupt into that operation.
            if (sampler != null) sampler.shutdown();
            sampler = null;
        }
        delegate.close();
    }

    private StreamJobHandle monitored(StreamJobHandle handle) {
        Objects.requireNonNull(handle, "Stream job handle");
        if (registry == JobMonitorRegistry.NOOP) return handle;
        MonitoredHandle result = new MonitoredHandle(handle);
        result.sample();
        try {
            synchronized (observed) {
                if (!closed.get() && !result.terminalPublished.get()
                        && observed.size() < Math.max(1, Math.min(4096, registry.historyLimit()))) {
                    observed.put(handle.id().value(), result);
                    if (sampler == null) {
                        sampler = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(action -> {
                            Thread thread = new Thread(action, "rowweave-stream-monitor");
                            thread.setDaemon(true);
                            return thread;
                        });
                        sampler.scheduleWithFixedDelay(this::sampleHandles, 250L, 250L,
                                java.util.concurrent.TimeUnit.MILLISECONDS);
                    }
                }
            }
        } catch (RuntimeException | LinkageError ignored) {
            // Registry capacity or sampler startup failure cannot reject a job.
        }
        return result;
    }


    private void sampleHandles() {
        java.util.List<MonitoredHandle> handles;
        synchronized (observed) {
            handles = new java.util.ArrayList<>(observed.values());
        }
        for (MonitoredHandle handle : handles) {
            if (closed.get()) return;
            handle.sample();
            if (handle.terminalPublished.get()) {
                synchronized (observed) { observed.remove(handle.id().value(), handle); }
            }
        }
    }

    private void observe(StreamJobHandle handle, StreamJobStatus status) {
        StreamJobEvidence evidence = evidence(handle);
        if (status.terminal()) {
            bridge.complete(handle.id(), handle.id().value(), status,
                    null, evidence);
        } else {
            bridge.progress(handle.id(), handle.id().value(), status, evidence);
        }
    }

    private StreamJobEvidence evidence(StreamJobHandle handle) {
        try {
            return handle.evidence();
        } catch (UnsupportedOperationException unavailable) {
            return StreamJobEvidence.unavailable(
                    handle.id(), handle.progress(), System.currentTimeMillis());
        } catch (RuntimeException observationFailure) {
            return StreamJobEvidence.unavailable(
                    handle.id(), handle.progress(), System.currentTimeMillis());
        }
    }

    private final class MonitoredHandle implements StreamJobHandle {
        private final StreamJobHandle delegate;
        private final AtomicBoolean terminalPublished = new AtomicBoolean();

        private MonitoredHandle(StreamJobHandle delegate) {
            this.delegate = delegate;
        }

        @Override public StreamJobId id() { return delegate.id(); }
        @Override public String artifactId() { return delegate.artifactId(); }

        @Override public StreamJobStatus status() {
            StreamJobStatus status = delegate.status();
            observeOnce(status);
            return status;
        }

        @Override public StreamJobProgress progress() {
            return delegate.progress();
        }

        @Override public StreamJobEvidence evidence() {
            return delegate.evidence();
        }

        @Override public StreamCommandHandle cancel(StreamCommandId commandId) {
            return delegate.cancel(commandId);
        }

        @Override public StreamCheckpointHandle triggerCheckpoint(
                StreamCommandId commandId) {
            return delegate.triggerCheckpoint(commandId);
        }

        @Override public StreamSavepointHandle stopWithSavepoint(
                StreamCommandId commandId, StreamSavepointRequest request) {
            return delegate.stopWithSavepoint(commandId, request);
        }

        @Override public StreamRescaleHandle rescale(
                StreamCommandId commandId, int targetParallelism) {
            return delegate.rescale(commandId, targetParallelism);
        }

        @Override public StreamJobStatus awaitRunning(long timeoutMillis) {
            StreamJobStatus status = delegate.awaitRunning(timeoutMillis);
            observeOnce(status);
            return status;
        }

        @Override public StreamJobStatus awaitTermination(long timeoutMillis) {
            StreamJobStatus status = delegate.awaitTermination(timeoutMillis);
            observeOnce(status);
            return status;
        }

        private void sample() {
            try {
                observeOnce(delegate.status());
            } catch (RuntimeException | LinkageError ignored) {
                // Sampling cannot affect an execution or replace its last good snapshot.
            }
        }

        private synchronized void observeOnce(StreamJobStatus status) {
            if (status.terminal()
                    ? terminalPublished.compareAndSet(false, true)
                    : !terminalPublished.get()) {
                try {
                    observe(delegate, status);
                } catch (RuntimeException | LinkageError ignored) {
                    // Monitoring remains isolated from client lifecycle calls.
                }
            }
        }
    }
}
