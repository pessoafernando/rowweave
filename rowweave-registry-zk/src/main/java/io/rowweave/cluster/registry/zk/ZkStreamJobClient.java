package io.rowweave.cluster.registry.zk;

import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamCommandHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamOfflineRescaleRequest;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.cluster.RealClusterStreamJobClient;

import java.util.concurrent.atomic.AtomicBoolean;

/** ZooKeeper-discovered Stream client with an independently owned session. */
public final class ZkStreamJobClient implements StreamJobClient {
    private final ZkServiceRegistry registry;
    private final RealClusterStreamJobClient delegate;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ZkStreamJobClient(String zkConnectString) {
        this(zkConnectString, ControlSecurityConfig.fromSystemProperties());
    }

    public ZkStreamJobClient(String zkConnectString,
            ControlSecurityConfig security) {
        if (zkConnectString == null || zkConnectString.trim().isEmpty()
                || security == null) {
            throw new IllegalArgumentException(
                    "ZooKeeper connection and Stream client security are required");
        }
        ZkServiceRegistry created = null;
        try {
            created = new ZkServiceRegistry(zkConnectString);
            this.registry = created;
            this.delegate = new RealClusterStreamJobClient(created, security);
        } catch (RuntimeException | Error failure) {
            if (created != null) created.close();
            throw failure;
        }
    }

    @Override
    public StreamJobHandle submit(StreamSubmissionId submissionId,
            PlanArtifact streamArtifact) {
        return delegate.submit(submissionId, streamArtifact);
    }

    @Override
    public StreamJobHandle attachSubmission(StreamSubmissionId submissionId) {
        return delegate.attachSubmission(submissionId);
    }

    @Override
    public StreamJobHandle attach(StreamJobId jobId) {
        return delegate.attach(jobId);
    }

    @Override
    public StreamCommandHandle attachCancel(StreamJobId jobId,
            StreamCommandId commandId) {
        return delegate.attachCancel(jobId, commandId);
    }

    @Override
    public StreamCheckpointHandle attachCheckpoint(StreamJobId jobId,
            StreamCommandId commandId) {
        return delegate.attachCheckpoint(jobId, commandId);
    }

    @Override
    public StreamSavepointHandle attachSavepoint(StreamJobId jobId,
            StreamCommandId commandId) {
        return delegate.attachSavepoint(jobId, commandId);
    }

    @Override
    public StreamJobHandle restore(StreamSubmissionId submissionId,
            PlanArtifact streamArtifact, StreamRestoreRequest request) {
        return delegate.restore(submissionId, streamArtifact, request);
    }

    @Override
    public StreamJobHandle offlineRescale(StreamSubmissionId submissionId,
            PlanArtifact streamArtifact,
            StreamOfflineRescaleRequest request) {
        return delegate.offlineRescale(
                submissionId, streamArtifact, request);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Throwable failure = null;
        try {
            delegate.close();
        } catch (Throwable closeFailure) {
            failure = closeFailure;
        }
        try {
            registry.close();
        } catch (Throwable closeFailure) {
            if (failure == null) failure = closeFailure;
            else failure.addSuppressed(closeFailure);
        }
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) throw (Error) failure;
        if (failure != null) throw new IllegalStateException(
                "ZooKeeper Stream client shutdown failed", failure);
    }
}
