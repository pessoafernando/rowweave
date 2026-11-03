package io.rowweave.stream.cluster;

import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamCommandHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamOfflineRescaleRequest;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamRescaleHandle;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamSubmissionId;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicBoolean;

/** No-listener same-JVM adapter over the durable Stream product Master. */
final class InProcessStreamJobClient implements StreamJobClient {
    private final StreamProductJobMasterServer master;
    private final ClusterStreamJobClient delegate;
    private final AtomicBoolean closed = new AtomicBoolean();

    InProcessStreamJobClient(
            Path authorityRoot, StreamProductRunFactory runFactory) {
        StreamProductJobMasterServer created =
                StreamProductJobMasterServer.inProcess(
                        authorityRoot, runFactory);
        this.master = created;
        this.delegate = new ClusterStreamJobClient(
                new InProcessStreamProductControlTransport(created));
    }

    int controlListenerCountForDiagnostics() {
        return master.controlListenerCountForDiagnostics();
    }

    @Override
    public StreamJobHandle submit(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact) {
        return delegate.submit(submissionId, streamArtifact);
    }

    @Override
    public StreamJobHandle attachSubmission(
            StreamSubmissionId submissionId) {
        return delegate.attachSubmission(submissionId);
    }

    @Override
    public StreamJobHandle attach(StreamJobId jobId) {
        return delegate.attach(jobId);
    }

    @Override
    public StreamCommandHandle attachCancel(
            StreamJobId jobId, StreamCommandId commandId) {
        return delegate.attachCancel(jobId, commandId);
    }

    @Override
    public StreamCheckpointHandle attachCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return delegate.attachCheckpoint(jobId, commandId);
    }

    @Override
    public StreamSavepointHandle attachSavepoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return delegate.attachSavepoint(jobId, commandId);
    }

    @Override
    public StreamRescaleHandle attachRescale(
            StreamJobId jobId, StreamCommandId commandId) {
        return delegate.attachRescale(jobId, commandId);
    }

    @Override
    public StreamJobHandle restore(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact,
            StreamRestoreRequest request) {
        return delegate.restore(submissionId, streamArtifact, request);
    }

    @Override
    public StreamJobHandle offlineRescale(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact,
            StreamOfflineRescaleRequest request) {
        return delegate.offlineRescale(
                submissionId, streamArtifact, request);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        try {
            delegate.close();
        } finally {
            master.close();
        }
    }
}
