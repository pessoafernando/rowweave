package io.rowweave.stream.cluster;

import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamCommandHandle;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamOfflineRescaleRequest;
import io.rowweave.StreamRestoreRequest;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Same-JVM Stream product roles sharing the cluster compiler and run factory. */
final class InProcessStreamCluster implements StreamJobClient {
    private final List<StreamRemoteWorkerServer> workers;
    private final InProcessStreamJobClient client;
    private final AtomicBoolean closed = new AtomicBoolean();

    InProcessStreamCluster(
            Path root,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders) {
        this(root, sourceProviders, sinkProviders, reference -> {
            throw new UnsupportedOperationException(
                    "in-process Stream runtime has no function resolver");
        }, Collections.emptyList());
    }

    InProcessStreamCluster(
            Path root,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            StreamFunctionResolver functionResolver,
            Collection<String> deploymentSetIds) {
        this(root, sourceProviders, sinkProviders, Collections.emptyMap(),
                functionResolver, deploymentSetIds);
    }

    InProcessStreamCluster(
            Path root,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            Map<String, TransactionalStreamSinkProvider>
                    transactionalSinkProviders,
            StreamFunctionResolver functionResolver,
            Collection<String> deploymentSetIds) {
        if (root == null || sourceProviders == null || sinkProviders == null) {
            throw new IllegalArgumentException(
                    "in-process Stream root and providers are required");
        }
        if (functionResolver == null || deploymentSetIds == null) {
            throw new IllegalArgumentException(
                    "in-process Stream function registry is required");
        }
        List<StreamRemoteWorkerServer> createdWorkers = new ArrayList<>();
        InProcessStreamJobClient createdClient = null;
        try {
            Path checkpoints = root.resolve("checkpoints");
            int workerCount = Math.max(2, Math.addExact(1,
                    Math.addExact(sinkProviders.size(),
                            transactionalSinkProviders.size())));
            List<RemoteStreamWorkerEndpoint> endpoints = new ArrayList<>();
            for (int index = 0; index < workerCount; index++) {
                StreamRemoteWorkerServer worker =
                        StreamRemoteWorkerServer.inProcess(
                                profile("in-process-worker-" + index,
                                        deploymentSetIds),
                                sourceProviders, sinkProviders,
                                transactionalSinkProviders,
                                functionResolver, checkpoints);
                createdWorkers.add(worker);
                endpoints.add(RemoteStreamWorkerEndpoint.inProcess(worker));
            }
            RemoteStreamSourceSinkRunFactory runs =
                    new RemoteStreamSourceSinkRunFactory(
                            root.resolve("runs"), 1L,
                            "in-process-master-1", endpoints,
                            checkpoints);
            createdClient = new InProcessStreamJobClient(
                    root.resolve("jobs"), runs);
        } catch (RuntimeException | Error failure) {
            closeAfterFailure(createdClient, failure);
            for (int index = createdWorkers.size() - 1;
                    index >= 0; index--) {
                closeAfterFailure(createdWorkers.get(index), failure);
            }
            throw failure;
        }
        this.workers = Collections.unmodifiableList(
                new ArrayList<>(createdWorkers));
        this.client = createdClient;
    }

    int controlListenerCountForDiagnostics() {
        int count = client.controlListenerCountForDiagnostics();
        for (StreamRemoteWorkerServer worker : workers) {
            count = Math.addExact(count,
                    worker.controlListenerCountForDiagnostics());
        }
        return count;
    }

    private static StreamWorkerCapabilityProfile profile(
            String workerId, Collection<String> deploymentSetIds) {
        List<String> capabilities = new ArrayList<>(Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                PortableStreamPlan.TEMPORAL_JOIN_CAPABILITY,
                PortableStreamPlan.UPSERT_CAPABILITY,
                PortableStreamPlan.RESIDENT_CDC_CAPABILITY,
                PortableStreamPlan.RESIDENT_CDC_V2_CAPABILITY,
                PortableStreamPlan.WINDOW_FIELD_BINDING_CAPABILITY,
                PortableStreamPlan.SOURCE_TIME_POLICY_CAPABILITY,
                PortableStreamPlan.SOURCE_END_OF_INPUT_CAPABILITY,
                PortableStreamPlan.CALC_CAPABILITY,
                PortableStreamPlan.FLAT_MAP_CAPABILITY,
                PortableStreamPlan.UNION_CAPABILITY,
                PortableStreamPlan.EVENT_TIME_ASSIGNMENT_CAPABILITY,
                PortableStreamPlan.KEYED_PROCESS_CAPABILITY,
                PortableStreamPlan.KEYED_CO_PROCESS_CAPABILITY,
                PortableStreamPlan.BROADCAST_STATE_CAPABILITY,
                PortableStreamPlan.GENERIC_WINDOW_CAPABILITY,
                PortableStreamPlan.ASYNC_WAIT_CAPABILITY,
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY,
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY,
                PortableStreamPlan.DYNAMIC_SPLIT_DISCOVERY_CAPABILITY,
                PortableStreamPlan.RETRACT_CHANGELOG_CAPABILITY,
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY,
                StreamWorkerCapabilityProfile
                        .CHECKPOINT_TRANSFER_CAPABILITY,
                StreamWorkerCapabilityProfile.PROGRESS_CAPABILITY));
        for (String deploymentSetId : deploymentSetIds) {
            capabilities.add(PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                    + deploymentSetId);
        }
        return new StreamWorkerCapabilityProfile(
                workerId, workerId + "-incarnation-1", 3, 1,
                capabilities,
                32, 64, 4 * 1024 * 1024,
                1024L * 1024L * 1024L, 128);
    }

    @Override
    public StreamJobHandle submit(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact) {
        return client.submit(submissionId, streamArtifact);
    }

    @Override
    public StreamJobHandle attachSubmission(
            StreamSubmissionId submissionId) {
        return client.attachSubmission(submissionId);
    }

    @Override
    public StreamJobHandle attach(StreamJobId jobId) {
        return client.attach(jobId);
    }

    @Override
    public StreamCommandHandle attachCancel(
            StreamJobId jobId, StreamCommandId commandId) {
        return client.attachCancel(jobId, commandId);
    }

    @Override
    public StreamCheckpointHandle attachCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return client.attachCheckpoint(jobId, commandId);
    }

    @Override
    public StreamSavepointHandle attachSavepoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return client.attachSavepoint(jobId, commandId);
    }

    @Override
    public StreamJobHandle restore(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact,
            StreamRestoreRequest request) {
        return client.restore(submissionId, streamArtifact, request);
    }

    @Override
    public StreamJobHandle offlineRescale(
            StreamSubmissionId submissionId, PlanArtifact streamArtifact,
            StreamOfflineRescaleRequest request) {
        return client.offlineRescale(
                submissionId, streamArtifact, request);
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        RuntimeException first = close(client, null);
        for (int index = workers.size() - 1; index >= 0; index--) {
            first = close(workers.get(index), first);
        }
        if (first != null) throw first;
    }

    private static RuntimeException close(
            AutoCloseable value, RuntimeException first) {
        try {
            value.close();
        } catch (RuntimeException failure) {
            if (first == null) return failure;
            first.addSuppressed(failure);
        } catch (Exception failure) {
            RuntimeException wrapped = new IllegalStateException(
                    "in-process Stream runtime close failed", failure);
            if (first == null) return wrapped;
            first.addSuppressed(wrapped);
        }
        return first;
    }

    private static void closeAfterFailure(
            AutoCloseable value, Throwable failure) {
        if (value == null) return;
        try {
            value.close();
        } catch (Throwable closeFailure) {
            if (closeFailure != failure) failure.addSuppressed(closeFailure);
        }
    }
}
