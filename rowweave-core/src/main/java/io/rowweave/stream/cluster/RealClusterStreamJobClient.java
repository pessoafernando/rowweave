package io.rowweave.stream.cluster;

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
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.registry.ServiceRegistry;

import java.util.concurrent.atomic.AtomicBoolean;

/** Public RealCluster Stream client backed only by stable control identities. */
public final class RealClusterStreamJobClient implements StreamJobClient {
    private static final long WAIT_POLL_MILLIS = 5L;

    private final StreamProductControlTransport transport;
    private final AtomicBoolean closed = new AtomicBoolean();

    public RealClusterStreamJobClient(String masterHost, int masterPort) {
        this(new SocketStreamProductControlTransport(masterHost, masterPort));
    }

    public RealClusterStreamJobClient(String masterHost, int masterPort,
            ControlSecurityConfig security) {
        this(new SocketStreamProductControlTransport(
                masterHost, masterPort, security));
    }

    /** Creates an HA client that resolves the active Stream Master per call. */
    public RealClusterStreamJobClient(ServiceRegistry registry,
            ControlSecurityConfig security) {
        this(new RegistryStreamProductControlTransport(registry, security));
    }

    RealClusterStreamJobClient(StreamProductControlTransport transport) {
        if (transport == null) {
            throw new IllegalArgumentException(
                    "Stream product control transport is required");
        }
        this.transport = transport;
    }

    @Override
    public StreamJobHandle submit(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact) {
        if (submissionId == null || streamArtifact == null) {
            throw new IllegalArgumentException(
                    "submission identity and Stream Artifact are required");
        }
        PlanArtifactValidator.validateForStreamProductExecution(streamArtifact);
        StreamProductJobView view = call(StreamProductControlWire.Request.submit(
                submissionId.value(), PlanArtifactCodec.encode(streamArtifact)));
        return new JobHandle(this, view.jobId(), view.artifactId());
    }

    @Override
    public StreamJobHandle attachSubmission(StreamSubmissionId submissionId) {
        if (submissionId == null) {
            throw new IllegalArgumentException("submissionId is required");
        }
        StreamProductJobView view = call(
                StreamProductControlWire.Request.attachSubmission(
                        submissionId.value()));
        return new JobHandle(this, view.jobId(), view.artifactId());
    }

    @Override
    public StreamJobHandle attach(StreamJobId jobId) {
        StreamProductJobView view = jobView(jobId);
        return new JobHandle(this, view.jobId(), view.artifactId());
    }

    @Override
    public StreamCommandHandle attachCancel(
            StreamJobId jobId, StreamCommandId commandId) {
        requireIds(jobId, commandId);
        StreamProductJobView view = call(
                StreamProductControlWire.Request.attachCancel(
                        jobId.value(), commandId.value()));
        return new CancelHandle(this, jobId, commandId, view.cancelStatus());
    }

    @Override
    public StreamCheckpointHandle attachCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        requireIds(jobId, commandId);
        StreamProductJobView view = call(
                StreamProductControlWire.Request.attachCheckpoint(
                        jobId.value(), commandId.value()));
        return checkpointHandle(jobId, commandId, view);
    }

    @Override
    public StreamSavepointHandle attachSavepoint(
            StreamJobId jobId, StreamCommandId commandId) {
        requireIds(jobId, commandId);
        StreamProductJobView view = call(
                StreamProductControlWire.Request.attachSavepoint(
                        jobId.value(), commandId.value()));
        return savepointHandle(jobId, commandId, view);
    }

    @Override
    public StreamRescaleHandle attachRescale(
            StreamJobId jobId, StreamCommandId commandId) {
        requireIds(jobId, commandId);
        StreamProductJobView view = call(
                StreamProductControlWire.Request.attachRescale(
                        jobId.value(), commandId.value()));
        return rescaleHandle(jobId, commandId, view);
    }

    @Override
    public StreamJobHandle restore(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact,
            StreamRestoreRequest request) {
        if (submissionId == null || streamArtifact == null
                || request == null) {
            throw new IllegalArgumentException(
                    "Stream restore submission is required");
        }
        PlanArtifactValidator.validateForStreamProductExecution(streamArtifact);
        StreamProductRecovery recovery = new StreamProductRecovery(
                request.sourceJobId(), request.recoveryPointId(),
                request.targetParallelism(), request.targetMaxParallelism(),
                false);
        StreamProductJobView view = call(
                StreamProductControlWire.Request.restore(
                        submissionId.value(),
                        StreamRecoverySubmissionCodec.encode(
                                streamArtifact, recovery)));
        return new JobHandle(this, view.jobId(), view.artifactId());
    }

    @Override
    public StreamJobHandle offlineRescale(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact,
            StreamOfflineRescaleRequest request) {
        if (submissionId == null || streamArtifact == null
                || request == null) {
            throw new IllegalArgumentException(
                    "Stream offline-rescale submission is required");
        }
        PlanArtifactValidator.validateForStreamProductExecution(streamArtifact);
        StreamProductRecovery recovery = new StreamProductRecovery(
                request.sourceJobId(), request.recoveryPointId(),
                request.targetParallelism(), request.targetMaxParallelism(),
                true);
        StreamProductJobView view = call(
                StreamProductControlWire.Request.offlineRescale(
                        submissionId.value(),
                        StreamRecoverySubmissionCodec.encode(
                                streamArtifact, recovery)));
        return new JobHandle(this, view.jobId(), view.artifactId());
    }

    private StreamProductJobView jobView(StreamJobId jobId) {
        if (jobId == null) throw new IllegalArgumentException("jobId is required");
        return call(StreamProductControlWire.Request.attachJob(jobId.value()));
    }

    private StreamJobEvidence evidence(StreamJobId jobId) {
        if (jobId == null) throw new IllegalArgumentException("jobId is required");
        StreamJobEvidence evidence = call(
                StreamProductControlWire.Request.attachEvidence(
                        jobId.value())).evidence();
        if (evidence == null) {
            throw new StreamControlException(
                    StreamControlException.Code.INVALID_RESPONSE,
                    "Stream evidence response is absent");
        }
        return evidence;
    }

    private StreamProductJobView cancel(
            StreamJobId jobId, StreamCommandId commandId) {
        requireIds(jobId, commandId);
        return call(StreamProductControlWire.Request.cancel(
                jobId.value(), commandId.value()));
    }

    private StreamCheckpointHandle checkpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        requireIds(jobId, commandId);
        StreamProductJobView view = call(
                StreamProductControlWire.Request.triggerCheckpoint(
                        jobId.value(), commandId.value()));
        return checkpointHandle(jobId, commandId, view);
    }

    private StreamSavepointHandle savepoint(
            StreamJobId jobId, StreamCommandId commandId,
            StreamSavepointRequest request) {
        requireIds(jobId, commandId);
        if (request == null) {
            throw new IllegalArgumentException(
                    "Stream savepoint request is required");
        }
        StreamProductJobView view = call(
                StreamProductControlWire.Request.stopWithSavepoint(
                        jobId.value(), commandId.value(),
                        StreamSavepointRequestCodec.encode(request)));
        return savepointHandle(jobId, commandId, view);
    }

    private StreamRescaleHandle rescale(
            StreamJobId jobId, StreamCommandId commandId,
            int targetParallelism) {
        requireIds(jobId, commandId);
        StreamProductJobView view = call(
                StreamProductControlWire.Request.rescale(
                        jobId.value(), commandId.value(),
                        StreamLiveRescaleRequestCodec.encode(
                                targetParallelism)));
        return rescaleHandle(jobId, commandId, view);
    }

    private StreamRescaleHandle rescaleHandle(
            StreamJobId jobId, StreamCommandId commandId,
            StreamProductJobView view) {
        if (!commandId.equals(view.rescaleCommandId())
                || view.rescaleId() == null
                || view.targetParallelism() < 1
                || view.rescaleStatus() == null) {
            throw new StreamControlException(
                    StreamControlException.Code.INVALID_RESPONSE,
                    "Stream Master returned a different rescale command");
        }
        return new RescaleHandle(this, jobId, commandId,
                view.rescaleId(), view.targetParallelism(),
                view.rescaleStatus());
    }

    private StreamSavepointHandle savepointHandle(
            StreamJobId jobId, StreamCommandId commandId,
            StreamProductJobView view) {
        if (!commandId.equals(view.savepointCommandId())
                || view.savepointId() == null
                || view.savepointStatus() == null) {
            throw new StreamControlException(
                    StreamControlException.Code.INVALID_RESPONSE,
                    "Stream Master returned a different savepoint command");
        }
        return new SavepointHandle(this, jobId, commandId,
                view.savepointId(), view.savepointStatus());
    }

    private StreamCheckpointHandle checkpointHandle(
            StreamJobId jobId, StreamCommandId commandId,
            StreamProductJobView view) {
        if (!commandId.equals(view.checkpointCommandId())
                || view.checkpointId() == null
                || view.checkpointStatus() == null) {
            throw new StreamControlException(
                    StreamControlException.Code.INVALID_RESPONSE,
                    "Stream Master returned a different checkpoint command");
        }
        return new CheckpointHandle(this, jobId, commandId,
                view.checkpointId(), view.checkpointStatus());
    }

    private StreamProductJobView call(StreamProductControlWire.Request request) {
        if (closed.get()) {
            throw new StreamControlException(
                    StreamControlException.Code.TRANSPORT_FAILURE,
                    "Stream product client is closed");
        }
        return transport.call(request);
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) transport.close();
    }

    private static void requireIds(
            StreamJobId jobId, StreamCommandId commandId) {
        if (jobId == null || commandId == null) {
            throw new IllegalArgumentException(
                    "Stream job and command identities are required");
        }
    }

    private static final class JobHandle implements StreamJobHandle {
        private final RealClusterStreamJobClient client;
        private final StreamJobId jobId;
        private final String artifactId;

        private JobHandle(RealClusterStreamJobClient client,
                StreamJobId jobId, String artifactId) {
            this.client = client;
            this.jobId = jobId;
            this.artifactId = artifactId;
        }

        @Override public StreamJobId id() { return jobId; }
        @Override public String artifactId() { return artifactId; }
        @Override public StreamJobStatus status() {
            return client.jobView(jobId).status();
        }
        @Override public StreamJobProgress progress() {
            return client.jobView(jobId).progress();
        }
        @Override public StreamJobEvidence evidence() {
            return client.evidence(jobId);
        }
        @Override public StreamCommandHandle cancel(StreamCommandId commandId) {
            StreamProductJobView view = client.cancel(jobId, commandId);
            return new CancelHandle(
                    client, jobId, commandId, view.cancelStatus());
        }
        @Override public StreamCheckpointHandle triggerCheckpoint(
                StreamCommandId commandId) {
            return client.checkpoint(jobId, commandId);
        }
        @Override public StreamSavepointHandle stopWithSavepoint(
                StreamCommandId commandId, StreamSavepointRequest request) {
            return client.savepoint(jobId, commandId, request);
        }
        @Override public StreamRescaleHandle rescale(
                StreamCommandId commandId, int targetParallelism) {
            return client.rescale(jobId, commandId, targetParallelism);
        }
        @Override public StreamJobStatus awaitRunning(long timeoutMillis) {
            return await(timeoutMillis, false);
        }
        @Override public StreamJobStatus awaitTermination(long timeoutMillis) {
            return await(timeoutMillis, true);
        }

        private StreamJobStatus await(
                long timeoutMillis, boolean terminal) {
            if (timeoutMillis <= 0L) {
                throw new IllegalArgumentException(
                        "Stream wait timeout must be positive and finite");
            }
            long timeoutNanos;
            try {
                timeoutNanos = Math.multiplyExact(timeoutMillis, 1_000_000L);
            } catch (ArithmeticException overflow) {
                timeoutNanos = Long.MAX_VALUE;
            }
            long started = System.nanoTime();
            while (true) {
                StreamJobStatus current = status();
                if (terminal ? current.terminal()
                        : current == StreamJobStatus.RUNNING
                        || current.terminal()) {
                    return current;
                }
                if (System.nanoTime() - started >= timeoutNanos) {
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_TIMEOUT,
                            "Stream job did not reach the requested state before timeout");
                }
                try {
                    Thread.sleep(WAIT_POLL_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_INTERRUPTED,
                            "Stream job wait was interrupted", interrupted);
                }
            }
        }
    }

    private static final class CancelHandle implements StreamCommandHandle {
        private final RealClusterStreamJobClient client;
        private final StreamJobId jobId;
        private final StreamCommandId commandId;
        private final StreamOperationStatus initial;

        private CancelHandle(RealClusterStreamJobClient client,
                StreamJobId jobId, StreamCommandId commandId,
                StreamOperationStatus initial) {
            this.client = client;
            this.jobId = jobId;
            this.commandId = commandId;
            this.initial = initial;
        }

        @Override public StreamCommandId id() { return commandId; }
        @Override public StreamOperationStatus status() {
            if (initial == StreamOperationStatus.SUCCEEDED
                    || initial == StreamOperationStatus.FAILED) {
                return initial;
            }
            return client.call(
                    StreamProductControlWire.Request.attachCancel(
                            jobId.value(), commandId.value())).cancelStatus();
        }
        @Override public StreamOperationStatus awaitCompletion(
                long timeoutMillis) {
            if (timeoutMillis <= 0L) {
                throw new IllegalArgumentException(
                        "Stream wait timeout must be positive and finite");
            }
            long started = System.nanoTime();
            long timeoutNanos = timeoutMillis > Long.MAX_VALUE / 1_000_000L
                    ? Long.MAX_VALUE : timeoutMillis * 1_000_000L;
            while (true) {
                StreamOperationStatus current = status();
                if (current == StreamOperationStatus.SUCCEEDED
                        || current == StreamOperationStatus.FAILED) {
                    return current;
                }
                if (System.nanoTime() - started >= timeoutNanos) {
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_TIMEOUT,
                            "Stream command did not complete before timeout");
                }
                try {
                    Thread.sleep(WAIT_POLL_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_INTERRUPTED,
                            "Stream command wait was interrupted", interrupted);
                }
            }
        }
    }

    private static final class CheckpointHandle
            implements StreamCheckpointHandle {
        private final RealClusterStreamJobClient client;
        private final StreamJobId jobId;
        private final StreamCommandId commandId;
        private final String checkpointId;
        private final StreamOperationStatus initial;

        private CheckpointHandle(RealClusterStreamJobClient client,
                StreamJobId jobId, StreamCommandId commandId,
                String checkpointId, StreamOperationStatus initial) {
            this.client = client;
            this.jobId = jobId;
            this.commandId = commandId;
            this.checkpointId = checkpointId;
            this.initial = initial;
        }

        @Override public StreamCommandId id() { return commandId; }
        @Override public String checkpointId() { return checkpointId; }
        @Override public StreamOperationStatus status() {
            if (terminal(initial)) return initial;
            StreamProductJobView view = client.call(
                    StreamProductControlWire.Request.attachCheckpoint(
                            jobId.value(), commandId.value()));
            if (!checkpointId.equals(view.checkpointId())
                    || !commandId.equals(view.checkpointCommandId())) {
                throw new StreamControlException(
                        StreamControlException.Code.INVALID_RESPONSE,
                        "checkpoint identity changed while attaching");
            }
            return view.checkpointStatus();
        }
        @Override public StreamOperationStatus awaitCompletion(
                long timeoutMillis) {
            if (timeoutMillis <= 0L) {
                throw new IllegalArgumentException(
                        "Stream wait timeout must be positive and finite");
            }
            long started = System.nanoTime();
            long timeoutNanos = timeoutMillis > Long.MAX_VALUE / 1_000_000L
                    ? Long.MAX_VALUE : timeoutMillis * 1_000_000L;
            while (true) {
                StreamOperationStatus current = status();
                if (terminal(current)) return current;
                if (System.nanoTime() - started >= timeoutNanos) {
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_TIMEOUT,
                            "Stream checkpoint did not complete before timeout");
                }
                try {
                    Thread.sleep(WAIT_POLL_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_INTERRUPTED,
                            "Stream checkpoint wait was interrupted",
                            interrupted);
                }
            }
        }

        private static boolean terminal(StreamOperationStatus status) {
            return status == StreamOperationStatus.SUCCEEDED
                    || status == StreamOperationStatus.FAILED;
        }
    }

    private static final class SavepointHandle
            implements StreamSavepointHandle {
        private final RealClusterStreamJobClient client;
        private final StreamJobId jobId;
        private final StreamCommandId commandId;
        private final String savepointId;
        private final StreamOperationStatus initial;

        private SavepointHandle(RealClusterStreamJobClient client,
                StreamJobId jobId, StreamCommandId commandId,
                String savepointId, StreamOperationStatus initial) {
            this.client = client;
            this.jobId = jobId;
            this.commandId = commandId;
            this.savepointId = savepointId;
            this.initial = initial;
        }

        @Override public StreamCommandId id() { return commandId; }
        @Override public String savepointId() { return savepointId; }
        @Override public StreamOperationStatus status() {
            if (terminal(initial)) return initial;
            StreamProductJobView view = client.call(
                    StreamProductControlWire.Request.attachSavepoint(
                            jobId.value(), commandId.value()));
            if (!savepointId.equals(view.savepointId())
                    || !commandId.equals(view.savepointCommandId())) {
                throw new StreamControlException(
                        StreamControlException.Code.INVALID_RESPONSE,
                        "savepoint identity changed while attaching");
            }
            return view.savepointStatus();
        }
        @Override public StreamOperationStatus awaitCompletion(
                long timeoutMillis) {
            if (timeoutMillis <= 0L) {
                throw new IllegalArgumentException(
                        "Stream wait timeout must be positive and finite");
            }
            long started = System.nanoTime();
            long timeoutNanos = timeoutMillis > Long.MAX_VALUE / 1_000_000L
                    ? Long.MAX_VALUE : timeoutMillis * 1_000_000L;
            while (true) {
                StreamOperationStatus current = status();
                if (terminal(current)) return current;
                if (System.nanoTime() - started >= timeoutNanos) {
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_TIMEOUT,
                            "Stream savepoint did not complete before timeout");
                }
                try {
                    Thread.sleep(WAIT_POLL_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_INTERRUPTED,
                            "Stream savepoint wait was interrupted",
                            interrupted);
                }
            }
        }

        private static boolean terminal(StreamOperationStatus status) {
            return status == StreamOperationStatus.SUCCEEDED
                    || status == StreamOperationStatus.FAILED;
        }
    }

    private static final class RescaleHandle
            implements StreamRescaleHandle {
        private final RealClusterStreamJobClient client;
        private final StreamJobId jobId;
        private final StreamCommandId commandId;
        private final String rescaleId;
        private final int targetParallelism;
        private final StreamOperationStatus initial;

        private RescaleHandle(RealClusterStreamJobClient client,
                StreamJobId jobId, StreamCommandId commandId,
                String rescaleId, int targetParallelism,
                StreamOperationStatus initial) {
            this.client = client;
            this.jobId = jobId;
            this.commandId = commandId;
            this.rescaleId = rescaleId;
            this.targetParallelism = targetParallelism;
            this.initial = initial;
        }

        @Override public StreamCommandId id() { return commandId; }
        @Override public String rescaleId() { return rescaleId; }
        @Override public int targetParallelism() { return targetParallelism; }
        @Override public StreamOperationStatus status() {
            if (terminal(initial)) return initial;
            StreamProductJobView view = client.call(
                    StreamProductControlWire.Request.attachRescale(
                            jobId.value(), commandId.value()));
            if (!rescaleId.equals(view.rescaleId())
                    || !commandId.equals(view.rescaleCommandId())
                    || targetParallelism != view.targetParallelism()) {
                throw new StreamControlException(
                        StreamControlException.Code.INVALID_RESPONSE,
                        "rescale identity changed while attaching");
            }
            return view.rescaleStatus();
        }
        @Override public StreamOperationStatus awaitCompletion(
                long timeoutMillis) {
            if (timeoutMillis <= 0L) {
                throw new IllegalArgumentException(
                        "Stream wait timeout must be positive and finite");
            }
            long started = System.nanoTime();
            long timeoutNanos = timeoutMillis > Long.MAX_VALUE / 1_000_000L
                    ? Long.MAX_VALUE : timeoutMillis * 1_000_000L;
            while (true) {
                StreamOperationStatus current = status();
                if (terminal(current)) return current;
                if (System.nanoTime() - started >= timeoutNanos) {
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_TIMEOUT,
                            "Stream rescale did not complete before timeout");
                }
                try {
                    Thread.sleep(WAIT_POLL_MILLIS);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StreamControlException(
                            StreamControlException.Code.WAIT_INTERRUPTED,
                            "Stream rescale wait was interrupted",
                            interrupted);
                }
            }
        }

        private static boolean terminal(StreamOperationStatus status) {
            return status == StreamOperationStatus.SUCCEEDED
                    || status == StreamOperationStatus.FAILED;
        }
    }
}
