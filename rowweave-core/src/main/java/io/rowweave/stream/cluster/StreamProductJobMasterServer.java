package io.rowweave.stream.cluster;

import io.rowweave.StreamSavepointRequest;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.monitor.JobMonitorRegistry;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.MonitorRuntime;
import io.rowweave.monitor.StageView;
import io.rowweave.monitor.StreamMonitorBridge;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.cluster.rpc.ControlRole;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.RoleAuthenticatedSocketTransport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.Properties;
import java.util.function.BooleanSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** RealCluster Stream product Master endpoint for submission and lifecycle control. */
public final class StreamProductJobMasterServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(
            StreamProductJobMasterServer.class);
    private static final int SOCKET_TIMEOUT_MILLIS = 30_000;
    private static final Throwable DISCOVER_DURABLE_WORKER_LOSS =
            new IllegalStateException(
                    "discover the unavailable Worker from durable placement");

    private final FileStreamProductJobAuthority authority;
    private final StreamProductRunFactory runFactory;
    private final StreamConnectorProviderRegistry connectorRegistry;
    private final JobMonitorRegistry monitor;
    private final StreamMonitorBridge monitorBridge;
    private final Map<StreamJobId, StreamProductRun> activeRuns =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, Throwable> restartFailures =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, Long> restartReadyNanos =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, Long> nextPeriodicCheckpointNanos =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, Future<?>> ownedCheckpointTasks =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, StreamCommandId> activeCheckpointCommands =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, ReentrantLock> checkpointLocks =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, ReentrantLock> evidenceLocks =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, String> recoveryDiagnostics =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, Long> nextTerminalCheckpointNanos =
            new ConcurrentHashMap<>();
    private final Map<StreamJobId, Long> nextEvidenceSampleNanos =
            new ConcurrentHashMap<>();
    private final java.util.Set<StreamJobId> retiredRuns =
            ConcurrentHashMap.newKeySet();
    private final java.util.Set<StreamJobId> cancellationIntents =
            ConcurrentHashMap.newKeySet();
    private final ExecutorService checkpointExecutor;
    private final ExecutorService controlExecutor;
    private final ServerSocket server;
    private final ControlSecurityConfig security;
    private final BooleanSupplier activeLeader;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread acceptThread;
    private final Thread recoveryThread;

    public StreamProductJobMasterServer(
            Path authorityRoot, StreamProductRunFactory runFactory) {
        this(authorityRoot, runFactory, loopbackSecurity(), 0,
                () -> true, true, null);
    }

    public StreamProductJobMasterServer(
            Path authorityRoot, StreamProductRunFactory runFactory,
            StreamConnectorProviderRegistry connectorRegistry) {
        this(authorityRoot, runFactory, loopbackSecurity(), 0,
                () -> true, true, connectorRegistry);
    }

    public StreamProductJobMasterServer(Path authorityRoot,
            StreamProductRunFactory runFactory,
            ControlSecurityConfig security, int port) {
        this(authorityRoot, runFactory, security, port, () -> true, true, null);
    }

    public StreamProductJobMasterServer(Path authorityRoot,
            StreamProductRunFactory runFactory,
            ControlSecurityConfig security, int port,
            BooleanSupplier activeLeader) {
        this(authorityRoot, runFactory, security, port, activeLeader, true,
                null);
    }

    public StreamProductJobMasterServer(Path authorityRoot,
            StreamProductRunFactory runFactory,
            ControlSecurityConfig security, int port,
            BooleanSupplier activeLeader,
            StreamConnectorProviderRegistry connectorRegistry) {
        this(authorityRoot, runFactory, security, port, activeLeader, true,
                connectorRegistry);
    }

    public StreamProductJobMasterServer(
            StreamProductJobStateStore authorityStore,
            StreamProductRunFactory runFactory,
            ControlSecurityConfig security, int port,
            BooleanSupplier activeLeader) {
        this(new FileStreamProductJobAuthority(authorityStore), runFactory,
                security, port, activeLeader, true, null);
    }

    public StreamProductJobMasterServer(
            StreamProductJobStateStore authorityStore,
            StreamProductRunFactory runFactory,
            ControlSecurityConfig security, int port,
            BooleanSupplier activeLeader,
            StreamConnectorProviderRegistry connectorRegistry) {
        this(new FileStreamProductJobAuthority(authorityStore), runFactory,
                security, port, activeLeader, true, connectorRegistry);
    }

    static StreamProductJobMasterServer inProcess(
            Path authorityRoot, StreamProductRunFactory runFactory) {
        return new StreamProductJobMasterServer(
                authorityRoot, runFactory, loopbackSecurity(), 0,
                () -> true, false, null);
    }

    private StreamProductJobMasterServer(
            Path authorityRoot, StreamProductRunFactory runFactory,
            ControlSecurityConfig security, int port,
            BooleanSupplier activeLeader, boolean listen,
            StreamConnectorProviderRegistry connectorRegistry) {
        this(new FileStreamProductJobAuthority(authorityRoot), runFactory,
                security, port, activeLeader, listen, connectorRegistry);
    }

    private StreamProductJobMasterServer(
            FileStreamProductJobAuthority authority,
            StreamProductRunFactory runFactory,
            ControlSecurityConfig security, int port,
            BooleanSupplier activeLeader, boolean listen,
            StreamConnectorProviderRegistry connectorRegistry) {
        if (authority == null || runFactory == null || security == null
                || activeLeader == null) {
            throw new IllegalArgumentException(
                    "Stream Master authority and run factory are required");
        }
        this.authority = authority;
        this.runFactory = runFactory;
        this.connectorRegistry = connectorRegistry;
        this.monitor = MonitorRuntime.shared();
        this.monitorBridge = new StreamMonitorBridge(monitor);
        this.security = security;
        this.activeLeader = activeLeader;
        this.checkpointExecutor = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(
                    runnable, "rowweave-stream-checkpoint-cut");
            thread.setDaemon(true);
            return thread;
        });
        if (!listen) {
            this.server = null;
            this.acceptThread = null;
            this.controlExecutor = null;
        } else {
            try {
                this.server = RoleAuthenticatedSocketTransport.bind(
                        security, ControlRole.MASTER, port, 32);
            } catch (RuntimeException failure) {
                throw new IllegalStateException(
                        "Stream product Master could not bind control endpoint",
                        failure);
            }
            this.controlExecutor = new ThreadPoolExecutor(
                    4, 4, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<Runnable>(32), runnable -> {
                        Thread thread = new Thread(runnable,
                                "rowweave-stream-master-control-request");
                        thread.setDaemon(true);
                        return thread;
                    }, new ThreadPoolExecutor.AbortPolicy());
            this.acceptThread = new Thread(
                    this::acceptLoop, "rowweave-stream-product-master");
            acceptThread.setDaemon(true);
            acceptThread.start();
        }
        this.recoveryThread = new Thread(
                this::recoveryLoop, "rowweave-stream-product-recovery");
        recoveryThread.setDaemon(true);
        recoveryThread.start();
    }

    public int port() {
        if (server == null) {
            throw new IllegalStateException(
                    "in-process Stream Master has no control listener");
        }
        return server.getLocalPort();
    }

    int controlListenerCountForDiagnostics() {
        return server == null ? 0 : 1;
    }

    StreamProductControlWire.Response dispatchInProcess(
            StreamProductControlWire.Request request) {
        if (closed.get()) {
            return StreamProductControlWire.Response.failure(
                    StreamControlException.Code.TRANSPORT_FAILURE,
                    "in-process Stream Master is closed");
        }
        try {
            return dispatch(request);
        } catch (Throwable failure) {
            return failureResponse(failure);
        }
    }

    private void acceptLoop() {
        while (!closed.get()) {
            try {
                final Socket socket = server.accept();
                try {
                    controlExecutor.execute(() -> handle(socket));
                } catch (RejectedExecutionException saturated) {
                    closeSocket(socket);
                }
            } catch (IOException failure) {
                if (!closed.get()) {
                    // A malformed or reset connection cannot terminate the Master listener.
                }
            }
        }
    }

    private static void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // The peer may already own a durable command outcome.
        }
    }

    private void handle(Socket socket) {
        try (Socket connection = socket) {
            RoleAuthenticatedSocketTransport.authenticateAccepted(
                    security, ControlRole.CLIENT, connection,
                    SOCKET_TIMEOUT_MILLIS);
            DataInputStream input = new DataInputStream(
                    new BufferedInputStream(connection.getInputStream()));
            DataOutputStream output = new DataOutputStream(
                    new BufferedOutputStream(connection.getOutputStream()));
            StreamProductControlWire.Response response;
            try {
                response = dispatch(StreamProductControlWire.readRequest(input));
            } catch (Throwable failure) {
                response = failureResponse(failure);
            }
            StreamProductControlWire.writeResponse(output, response);
        } catch (IOException ignored) {
            // The request may already be durable when its response ACK is lost.
        }
    }

    private static ControlSecurityConfig loopbackSecurity() {
        return ControlSecurityConfig.fromProperties(new Properties());
    }

    private void requireConnectorAdmission(PlanArtifact artifact) {
        if (!artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1)) {
            return;
        }
        if (connectorRegistry == null) {
            throw new IllegalStateException(
                    "STREAM_CONNECTOR_DEPLOYMENT_UNAVAILABLE");
        }
        connectorRegistry.verifyArtifact(artifact);
    }

    private StreamProductControlWire.Response dispatch(
            StreamProductControlWire.Request request) {
        if (!activeLeader.getAsBoolean()) {
            return StreamProductControlWire.Response.failure(
                    StreamControlException.Code.TRANSPORT_FAILURE,
                    "Stream Master is not the active leader");
        }
        StreamProductJobSnapshot snapshot;
        StreamProductCheckpointOperation checkpoint = null;
        StreamProductSavepointOperation savepoint = null;
        StreamProductRescaleOperation rescale = null;
        boolean evidence = false;
        switch (request.type) {
            case SUBMIT:
                PlanArtifact artifact = PlanArtifactCodec.decode(request.artifact);
                requireConnectorAdmission(artifact);
                snapshot = authority.publish(
                        StreamSubmissionId.of(request.firstId), artifact);
                snapshot = ensureActive(snapshot);
                break;
            case ATTACH_SUBMISSION:
                snapshot = attachActive(authority.bySubmission(
                        StreamSubmissionId.of(request.firstId)));
                break;
            case ATTACH_JOB:
                snapshot = attachActive(authority.byJob(
                        StreamJobId.of(request.firstId)));
                break;
            case ATTACH_EVIDENCE:
                snapshot = authority.byJob(StreamJobId.of(request.firstId));
                evidence = true;
                break;
            case CANCEL:
                snapshot = cancel(
                        StreamJobId.of(request.firstId),
                        StreamCommandId.of(request.secondId));
                break;
            case ATTACH_CANCEL:
                snapshot = authority.byJob(StreamJobId.of(request.firstId));
                snapshot.cancelStatus(StreamCommandId.of(request.secondId));
                break;
            case TRIGGER_CHECKPOINT:
                snapshot = checkpoint(
                        StreamJobId.of(request.firstId),
                        StreamCommandId.of(request.secondId));
                checkpoint = snapshot.checkpoint(
                        StreamCommandId.of(request.secondId));
                break;
            case ATTACH_CHECKPOINT:
                snapshot = authority.byJob(StreamJobId.of(request.firstId));
                checkpoint = snapshot.checkpoint(
                        StreamCommandId.of(request.secondId));
                break;
            case STOP_WITH_SAVEPOINT:
                StreamSavepointRequest savepointRequest =
                        StreamSavepointRequestCodec.decode(request.artifact);
                snapshot = savepoint(
                        StreamJobId.of(request.firstId),
                        StreamCommandId.of(request.secondId),
                        savepointRequest);
                savepoint = snapshot.savepoint(
                        StreamCommandId.of(request.secondId));
                break;
            case ATTACH_SAVEPOINT:
                snapshot = authority.byJob(StreamJobId.of(request.firstId));
                savepoint = snapshot.savepoint(
                        StreamCommandId.of(request.secondId));
                break;
            case RESCALE:
                snapshot = rescale(
                        StreamJobId.of(request.firstId),
                        StreamCommandId.of(request.secondId),
                        StreamLiveRescaleRequestCodec.decode(
                                request.artifact));
                rescale = snapshot.rescale(
                        StreamCommandId.of(request.secondId));
                break;
            case ATTACH_RESCALE:
                snapshot = authority.byJob(StreamJobId.of(request.firstId));
                rescale = snapshot.rescale(
                        StreamCommandId.of(request.secondId));
                break;
            case RESTORE:
            case OFFLINE_RESCALE:
                boolean offline = request.type
                        == StreamProductControlWire.Type.OFFLINE_RESCALE;
                StreamRecoverySubmissionCodec.Decoded recovery =
                        StreamRecoverySubmissionCodec.decode(
                                request.artifact, offline);
                requireConnectorAdmission(recovery.artifact);
                snapshot = authority.publishRecovery(
                        StreamSubmissionId.of(request.firstId),
                        recovery.artifact, recovery.recovery);
                snapshot = ensureActive(snapshot);
                break;
            default:
                throw new AssertionError(request.type);
        }
        return StreamProductControlWire.Response.success(
                evidence
                        ? StreamProductJobView.fromEvidence(snapshot)
                        : checkpoint != null
                        ? StreamProductJobView.from(snapshot, checkpoint)
                        : savepoint != null
                        ? StreamProductJobView.from(snapshot, savepoint)
                        : rescale != null
                        ? StreamProductJobView.from(snapshot, rescale)
                        : StreamProductJobView.from(snapshot));
    }

    private StreamProductJobSnapshot attachActive(
            StreamProductJobSnapshot observed) {
        // RUNNING is already durable and the recovery loop owns liveness.
        // A read-only attach must not queue behind a long Worker checkpoint
        // cut merely to rediscover the same active run.
        if (observed.status() == StreamJobStatus.RUNNING) {
            return observed;
        }
        return ensureActive(observed);
    }

    private synchronized StreamProductJobSnapshot ensureActive(
            StreamProductJobSnapshot observed) {
        return ensureActive(observed, false);
    }

    private synchronized StreamProductJobSnapshot ensureActive(
            StreamProductJobSnapshot observed, boolean recoverRestart) {
        if (closed.get()) return observed;
        observed = authority.byJob(observed.jobId());
        requireConnectorAdmission(observed.artifact());
        if (observed.status().terminal()
                || observed.status() == StreamJobStatus.STOPPING) {
            nextPeriodicCheckpointNanos.remove(observed.jobId());
            cancelOwnedCheckpoint(observed.jobId());
            retireTerminal(observed);
            return observed;
        }
        StreamProductRun existing = activeRuns.get(observed.jobId());
        if (existing != null) {
            boolean checkpointActive = activeCheckpointCommands.containsKey(
                    observed.jobId());
            Throwable failure = checkpointActive
                    ? existing.failureDuringCheckpoint() : existing.failure();
            if (checkpointActive && failure == null) {
                // The cut owns its deadline and may hold the run monitor while
                // waiting for remote Workers. Do not hold the Master monitor
                // behind it: unrelated job controls must remain independent.
                return observed;
            }
            boolean runFinished = failure == null && existing.finished();
            if (failure == null) {
                sampleEvidence(observed.jobId(), existing, runFinished);
                if (runFinished) {
                    StreamProductJobSnapshot finished = authority.transition(
                            observed.jobId(), StreamJobStatus.FINISHED);
                    activeRuns.remove(observed.jobId(), existing);
                    cancelOwnedCheckpoint(observed.jobId());
                    closeQuietly(existing);
                    retireTerminal(finished);
                    return finished;
                }
                schedulePeriodic(observed);
                return authority.byJob(observed.jobId());
            }
            PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                    observed.artifact());
            if (observed.progress().restarts()
                    >= job.restartPolicy().maximumAttempts()) {
                try {
                    observed = completeConvergedDecision(observed);
                } catch (CheckpointCommitPendingException pending) {
                    return authority.byJob(observed.jobId());
                }
                activeRuns.remove(observed.jobId(), existing);
                cancelOwnedCheckpoint(observed.jobId());
                closeQuietly(existing);
                if (observed.status().terminal()) {
                    retireTerminal(observed);
                    return observed;
                }
                StreamProductJobSnapshot failed = authority.transition(
                        observed.jobId(), StreamJobStatus.FAILED);
                retireTerminal(failed);
                return failed;
            }
            activeRuns.remove(observed.jobId(), existing);
            cancelOwnedCheckpoint(observed.jobId());
            closeQuietly(existing);
            return beginRestart(observed, failure, job);
        }
        if (observed.status() == StreamJobStatus.RESTARTING) {
            restartFailures.putIfAbsent(
                    observed.jobId(), DISCOVER_DURABLE_WORKER_LOSS);
            restartReadyNanos.putIfAbsent(
                    observed.jobId(), System.nanoTime());
            if (!recoverRestart) return observed;
            Long ready = restartReadyNanos.get(observed.jobId());
            if (ready != null && System.nanoTime() - ready < 0L) {
                return observed;
            }
        }

        StreamProductJobSnapshot starting = observed.status()
                == StreamJobStatus.SUBMITTED
                ? authority.transition(
                        observed.jobId(), StreamJobStatus.STARTING)
                : observed;
        StreamProductRun run = null;
        try {
            if (starting.status() == StreamJobStatus.RESTARTING) {
                run = runFactory.restart(starting,
                        restartFailures.get(starting.jobId()));
            } else {
                run = runFactory.create(starting);
            }
            if (run == null) {
                throw new IllegalStateException(
                        "Stream run factory returned null");
            }
            activeRuns.put(starting.jobId(), run);
            run.start();
            StreamProductJobSnapshot running = authority.transition(
                    starting.jobId(), StreamJobStatus.RUNNING);
            running = authority.updateCdcPhase(
                    running.jobId(), run.cdcPhase());
            convergeInFlightOperations(starting.jobId());
            StreamProductJobSnapshot converged =
                    authority.byJob(starting.jobId());
            if (converged.status().terminal()) {
                activeRuns.remove(starting.jobId(), run);
                closeQuietly(run);
                retireTerminal(converged);
                return converged;
            }
            restartFailures.remove(starting.jobId());
            restartReadyNanos.remove(starting.jobId());
            running = authority.byJob(starting.jobId());
            sampleEvidence(starting.jobId(), run, true);
            schedulePeriodic(running);
            return authority.byJob(starting.jobId());
        } catch (Throwable failure) {
            if (run != null) activeRuns.remove(starting.jobId(), run);
            closeQuietly(run);
            if (failure instanceof CheckpointCommitPendingException) {
                return authority.byJob(starting.jobId());
            }
            if ((starting.status() == StreamJobStatus.STARTING
                    || starting.status() == StreamJobStatus.RESTARTING)
                    && (failure instanceof StreamBoundProviderUnavailableException
                    || failure instanceof StreamClusterProtocolException
                    && ((StreamClusterProtocolException) failure).code()
                    == StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY)) {
                // Worker/provider registration may lag first placement or a
                // rolling replacement. No attempt started: keep the accepted
                // submission pending and preserve its retry budget.
                return authority.byJob(starting.jobId());
            }
            if (starting.status() == StreamJobStatus.RESTARTING
                    || starting.status() == StreamJobStatus.RUNNING) {
                try {
                    starting = completeConvergedDecision(starting);
                } catch (CheckpointCommitPendingException pending) {
                    return authority.byJob(starting.jobId());
                }
                if (starting.status().terminal()) {
                    retireTerminal(starting);
                    return starting;
                }
            }
            PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                    starting.artifact());
            if (failure instanceof StreamWorkerLossException
                    && starting.status() == StreamJobStatus.RUNNING
                    && starting.progress().restarts()
                    < job.restartPolicy().maximumAttempts()) {
                return beginRestart(starting, failure, job);
            }
            StreamProductJobSnapshot failed = authority.transition(
                    starting.jobId(), StreamJobStatus.FAILED);
            retireTerminal(failed);
            if (starting.status() == StreamJobStatus.RUNNING
                    || starting.status() == StreamJobStatus.RESTARTING) {
                LOG.warn("Stream job {} failed while recovering",
                        starting.jobId().value(), failure);
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) throw (Error) failure;
            throw new IllegalStateException("Stream run start failed", failure);
        }
    }

    private void recoveryLoop() {
        while (!closed.get()) {
            if (!activeLeader.getAsBoolean()) {
                if (!pauseRecoveryLoop()) return;
                continue;
            }
            try {
                for (StreamProductJobSnapshot snapshot : authority.all()) {
                    try {
                        if (snapshot.status().terminal()) {
                            retireTerminal(snapshot);
                        } else if (snapshot.status()
                                != StreamJobStatus.STOPPING
                                && !activeRuns.containsKey(snapshot.jobId())) {
                            ensureActive(snapshot, true);
                        }
                        recoveryDiagnostics.remove(snapshot.jobId());
                    } catch (Throwable failure) {
                        logRecoveryFailure(snapshot.jobId(), failure);
                    }
                }
            } catch (Throwable ignored) {
                // A corrupt catalog fails closed; healthy active runs remain monitored.
            }
            for (StreamJobId jobId : activeRuns.keySet()) {
                try {
                    ensureActive(authority.byJob(jobId), true);
                    if (activeCheckpointCommands.containsKey(jobId)) {
                        continue;
                    }
                    convergeInFlightOperations(jobId);
                    triggerTerminalIfReady(jobId);
                    triggerPeriodicIfDue(jobId);
                    recoveryDiagnostics.remove(jobId);
                } catch (Throwable failure) {
                    logRecoveryFailure(jobId, failure);
                    // ensureActive durably decides RUNNING/RESTARTING/FAILED.
                }
            }
            for (StreamJobId jobId : restartFailures.keySet()) {
                if (activeRuns.containsKey(jobId)) continue;
                try {
                    ensureActive(authority.byJob(jobId), true);
                    recoveryDiagnostics.remove(jobId);
                } catch (Throwable failure) {
                    logRecoveryFailure(jobId, failure);
                    // A failed replacement is reflected by the durable state.
                }
            }
            if (!pauseRecoveryLoop()) return;
        }
    }

    private boolean pauseRecoveryLoop() {
        try {
            Thread.sleep(50L);
            return true;
        } catch (InterruptedException interrupted) {
            if (closed.get()) return false;
            // An interrupt that is not paired with shutdown must not retire the
            // only thread that reconciles durable Stream decisions.
            LOG.warn("Stream product recovery pause interrupted while running",
                    interrupted);
            return true;
        }
    }

    private void logRecoveryFailure(StreamJobId jobId, Throwable failure) {
        String fingerprint = failure.getClass().getName() + ':'
                + String.valueOf(failure.getMessage());
        String previous = recoveryDiagnostics.put(jobId, fingerprint);
        if (!fingerprint.equals(previous)) {
            LOG.warn("Stream recovery is pending job={}", jobId, failure);
        }
    }

    private void convergeInFlightOperations(StreamJobId jobId) {
        StreamProductJobSnapshot snapshot = authority.byJob(jobId);
        for (StreamProductCheckpointOperation operation
                : snapshot.checkpoints()) {
            if (operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                    || operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.RUNNING) {
                StreamProductRun run = activeRuns.get(jobId);
                if (run != null && run.ownsCheckpointDeadline()) {
                    submitOwnedCheckpoint(jobId, operation.commandId());
                } else {
                    checkpoint(jobId, operation.commandId());
                }
                break;
            }
        }
        snapshot = authority.byJob(jobId);
        StreamProductSavepointOperation savepoint = snapshot.savepointOrNull();
        if (savepoint != null
                && (savepoint.status()
                == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                || savepoint.status()
                == io.rowweave.stream.api.StreamOperationStatus.RUNNING)) {
            StreamProductRun run = activeRuns.get(jobId);
            if (run == null) {
                return;
            }
            try {
                if (!run.recoverSavepoint(
                        savepoint.commandId(), savepoint.savepointId())) {
                    authority.failSavepoint(jobId, savepoint.commandId());
                    return;
                }
                activeRuns.remove(jobId, run);
                nextPeriodicCheckpointNanos.remove(jobId);
                closeQuietly(run);
                retireTerminal(authority.completeSavepoint(
                        jobId, savepoint.commandId()));
            } catch (CheckpointCommitPendingException pending) {
                // The durable decision remains authoritative and is retried.
            } catch (Throwable failure) {
                authority.failSavepoint(jobId, savepoint.commandId());
            }
        }
        snapshot = authority.byJob(jobId);
        StreamProductRescaleOperation rescale = snapshot.rescaleOrNull();
        if (rescale != null
                && (rescale.status()
                == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                || rescale.status()
                == io.rowweave.stream.api.StreamOperationStatus.RUNNING)) {
            rescale(jobId, rescale.commandId(),
                    rescale.targetParallelism());
        }
    }

    private StreamProductJobSnapshot beginRestart(
            StreamProductJobSnapshot observed, Throwable failure,
            PortableStreamJob job) {
        StreamProductJobSnapshot restarting =
                authority.beginRestart(observed.jobId());
        nextTerminalCheckpointNanos.remove(observed.jobId());
        restartFailures.put(observed.jobId(), failure);
        restartReadyNanos.put(observed.jobId(), saturatingAdd(
                System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(
                        job.restartPolicy().backoffMillis())));
        return restarting;
    }

    private void retireTerminal(StreamProductJobSnapshot snapshot) {
        if (snapshot == null || !snapshot.status().terminal()
                || !retiredRuns.add(snapshot.jobId())) return;
        publishMonitor(snapshot, java.util.Collections.emptyList());
        nextTerminalCheckpointNanos.remove(snapshot.jobId());
        nextEvidenceSampleNanos.remove(snapshot.jobId());
        cancelOwnedCheckpoint(snapshot.jobId());
        try {
            runFactory.retire(snapshot);
        } catch (RuntimeException failure) {
            retiredRuns.remove(snapshot.jobId());
        }
    }

    private static long saturatingAdd(long left, long right) {
        long result = left + right;
        if (((left ^ result) & (right ^ result)) < 0L) {
            return Long.MAX_VALUE;
        }
        return result;
    }

    private void sampleEvidence(StreamJobId jobId,
            StreamProductRun run, boolean force) {
        if (jobId == null || run == null) return;
        ReentrantLock lock = evidenceLocks.computeIfAbsent(
                jobId, ignored -> new ReentrantLock());
        lock.lock();
        try {
            long nowNanos = System.nanoTime();
            Long due = nextEvidenceSampleNanos.get(jobId);
            if (!force && due != null && nowNanos - due < 0L) return;
            nextEvidenceSampleNanos.put(jobId, saturatingAdd(nowNanos,
                    TimeUnit.MILLISECONDS.toNanos(250L)));
            try {
                StreamRunProgressSnapshot observed = run.progressSnapshot();
                if (observed == null) return;
                StreamProductJobSnapshot current = authority.byJob(jobId);
                StreamJobEvidence prior = current.evidence();
                StreamJobProgress attempt = withPublicMetadata(
                        observed.aggregate().progress(), current.progress());
                attempt = StreamJobProgressAccumulator.attempt(
                        prior, observed, attempt);
                StreamJobProgress lifetime =
                        StreamJobProgressAccumulator.lifetime(
                        prior, current.progress(), observed, attempt);
                StreamJobEvidence evidence = new StreamJobEvidence(
                        jobId, observed.runId(), observed.attemptId(),
                        current.progress().attempt(), observed.fenceToken(),
                        observed.coordinatorTerm(),
                        observed.assignmentGeneration(),
                        prior.available()
                                ? prior.latestCompletedCheckpointId() : null,
                        prior.available()
                                ? prior.latestCompletedCheckpointSequence() : 0L,
                        prior.available()
                                ? prior.latestCheckpointDurationMillis() : 0L,
                        prior.available() ? prior.latestCheckpointBytes() : 0L,
                        attempt, lifetime,
                        observed.aggregate().placementTaskCount(),
                        observed.aggregate().reportedTaskCount(),
                        System.currentTimeMillis());
                authority.updateEvidence(jobId, evidence);
                publishMonitor(authority.byJob(jobId), observed.stages());
                publishLatestCompletedCheckpointEvidence(jobId, run);
            } catch (RuntimeException ignored) {
                // Operational observation is advisory and never changes execution.
            }
        } finally {
            lock.unlock();
        }
    }

    private void publishMonitor(StreamProductJobSnapshot snapshot,
            java.util.List<StageView> stages) {
        if (monitor == JobMonitorRegistry.NOOP) return;
        try {
            synchronized (monitor) {
                if (snapshot.status().terminal()) {
                    monitorBridge.complete(snapshot.jobId(), snapshot.jobId().value(),
                            snapshot.status(), null, snapshot.evidence());
                } else {
                    monitorBridge.progress(snapshot.jobId(), snapshot.jobId().value(),
                            snapshot.status(), snapshot.evidence());
                }
                JobView current = monitor.job(snapshot.jobId().value());
                if (current == null) return;
                JobView.Builder builder = current.toBuilder();
                if (!stages.isEmpty()) {
                    builder.dagJson(StreamRuntimeMonitorProjection.dag(stages));
                    int parallelism = 1;
                    for (StageView stage : stages) {
                        parallelism = Math.max(parallelism, stage.parallelism());
                        builder.replaceStage(stage);
                    }
                    builder.parallelism(parallelism, parallelism);
                }
                if (snapshot.status().terminal()) {
                    for (StageView stage : current.stages()) {
                        builder.replaceStage(StreamRuntimeMonitorProjection.terminal(
                                stage, snapshot.status().name(), current.endTime()));
                    }
                }
                monitor.onJobSubmit(builder.build());
            }
        } catch (RuntimeException | LinkageError ignored) {
            // Dashboard projection cannot alter durable job lifecycle decisions.
        }
    }

    private void publishLatestCompletedCheckpointEvidence(StreamJobId jobId,
            StreamProductRun run) {
        StreamCompletedCheckpointEvidence observed =
                run.completedCheckpointEvidence();
        if (observed == null) return;
        StreamProductJobSnapshot current = authority.byJob(jobId);
        for (StreamProductCheckpointOperation operation
                : current.checkpoints()) {
            if (!operation.checkpointId().equals(observed.checkpointId())
                    || operation.status()
                    != io.rowweave.stream.api.StreamOperationStatus
                    .SUCCEEDED) {
                continue;
            }
            long durationMillis = operation.acceptedAtEpochMillis() > 0L
                    && operation.completedAtEpochMillis() > 0L
                    ? Math.max(0L, operation.completedAtEpochMillis()
                    - operation.acceptedAtEpochMillis()) : 0L;
            publishCompletedCheckpointEvidence(jobId, run,
                    operation.checkpointId(), durationMillis);
            return;
        }
    }

    private static StreamJobProgress withPublicMetadata(
            StreamJobProgress observed, StreamJobProgress current) {
        return new StreamJobProgress(current.attempt(),
                current.completedCheckpoints(), observed.operatorWatermark(),
                observed.cdcPhase(), current.restarts(),
                observed.recordsRead(), observed.recordsWritten(),
                observed.bytesRead(), observed.bytesWritten(),
                observed.backlogEvents(), observed.backlogBytes(),
                observed.mailboxHighWaterEvents(),
                observed.channelHighWaterBytes(),
                observed.managedStateBytes(),
                observed.timerHighWaterCount(),
                observed.windowHighWaterCount(),
                observed.backpressuredMillis());
    }

    private void schedulePeriodic(StreamProductJobSnapshot snapshot) {
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                snapshot.artifact());
        if (!job.checkpointConfig().periodic()) {
            nextPeriodicCheckpointNanos.remove(snapshot.jobId());
            return;
        }
        long interval = TimeUnit.MILLISECONDS.toNanos(
                job.checkpointConfig().intervalMillis());
        nextPeriodicCheckpointNanos.putIfAbsent(snapshot.jobId(),
                saturatingAdd(System.nanoTime(), interval));
    }

    private void triggerPeriodicIfDue(StreamJobId jobId) {
        if (cancellationIntents.contains(jobId)) {
            nextPeriodicCheckpointNanos.remove(jobId);
            return;
        }
        Long due = nextPeriodicCheckpointNanos.get(jobId);
        if (due == null || System.nanoTime() - due < 0L) return;
        StreamProductJobSnapshot snapshot = authority.byJob(jobId);
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                snapshot.artifact());
        if (snapshot.status() != StreamJobStatus.RUNNING
                || !job.checkpointConfig().periodic()) {
            nextPeriodicCheckpointNanos.remove(jobId);
            cancelOwnedCheckpoint(jobId);
            return;
        }
        if (hasInFlightCheckpoint(snapshot)) return;
        StreamProductRun run = activeRuns.get(jobId);
        if (run != null && run.ownsCheckpointDeadline()) {
            Future<?> inFlight = ownedCheckpointTasks.get(jobId);
            if (inFlight != null && !inFlight.isDone()) return;
            nextPeriodicCheckpointNanos.put(jobId, saturatingAdd(
                    System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(
                            job.checkpointConfig().intervalMillis())));
            submitOwnedCheckpoint(jobId, StreamCommandId.random());
            return;
        }
        nextPeriodicCheckpointNanos.put(jobId, saturatingAdd(
                System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(
                        job.checkpointConfig().intervalMillis())));
        checkpoint(jobId, StreamCommandId.random());
    }

    private void cancelOwnedCheckpoint(StreamJobId jobId) {
        Future<?> task = ownedCheckpointTasks.remove(jobId);
        if (task != null) task.cancel(true);
    }

    private static boolean hasInFlightCheckpoint(
            StreamProductJobSnapshot snapshot) {
        for (StreamProductCheckpointOperation operation
                : snapshot.checkpoints()) {
            if (operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                    || operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.RUNNING) {
                return true;
            }
        }
        return false;
    }

    private void submitOwnedCheckpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        if (activeCheckpointCommands.containsKey(jobId)) return;
        Future<?> inFlight = ownedCheckpointTasks.get(jobId);
        if (inFlight != null && !inFlight.isDone()) return;
        Future<?> submitted = checkpointExecutor.submit(() -> {
            try {
                checkpoint(jobId, commandId, false);
            } catch (RuntimeException failure) {
                LOG.warn("Asynchronous Stream checkpoint failed job={} command={}",
                        jobId, commandId, failure);
            }
        });
        ownedCheckpointTasks.put(jobId, submitted);
    }

    private void triggerTerminalIfReady(StreamJobId jobId) {
        Long next = nextTerminalCheckpointNanos.get(jobId);
        long now = System.nanoTime();
        if (next != null && now - next < 0L) return;
        StreamProductJobSnapshot snapshot = authority.byJob(jobId);
        if (snapshot.status() != StreamJobStatus.RUNNING) return;
        StreamProductRun run = activeRuns.get(jobId);
        if (run == null || !run.terminalCheckpointReady()) return;
        if (next == null) {
            nextTerminalCheckpointNanos.put(jobId, saturatingAdd(now,
                    TimeUnit.SECONDS.toNanos(1L)));
            return;
        }
        for (StreamProductCheckpointOperation operation
                : snapshot.checkpoints()) {
            if (operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                    || operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.RUNNING) {
                return;
            }
        }
        nextTerminalCheckpointNanos.put(jobId, saturatingAdd(now,
                TimeUnit.SECONDS.toNanos(1L)));
        StreamCommandId commandId = StreamCommandId.random();
        checkpoint(jobId, commandId);
    }

    private synchronized StreamProductJobSnapshot cancel(
            StreamJobId jobId, StreamCommandId commandId) {
        cancellationIntents.add(jobId);
        nextPeriodicCheckpointNanos.remove(jobId);
        try {
            convergeBeforeCancel(jobId);
            requireNoInFlightDecision(authority.byJob(jobId));
            StreamProductJobSnapshot accepted =
                    authority.acceptCancel(jobId, commandId);
            if (accepted.status() == StreamJobStatus.CANCELED) return accepted;
            StreamProductRun run = activeRuns.remove(jobId);
            restartFailures.remove(jobId);
            restartReadyNanos.remove(jobId);
            cancelOwnedCheckpoint(jobId);
            sampleEvidence(jobId, run, true);
            try {
                if (run != null) run.cancel();
            } finally {
                closeQuietly(run);
            }
            StreamProductJobSnapshot canceled =
                    authority.completeCancel(jobId, commandId);
            retireTerminal(canceled);
            return canceled;
        } finally {
            cancellationIntents.remove(jobId);
            StreamProductJobSnapshot current = authority.byJob(jobId);
            if (current.status() == StreamJobStatus.RUNNING
                    && activeRuns.containsKey(jobId)) {
                schedulePeriodic(current);
            }
        }
    }

    private void convergeBeforeCancel(StreamJobId jobId) {
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                authority.byJob(jobId).artifact());
        long deadline = saturatingAdd(System.nanoTime(),
                TimeUnit.MILLISECONDS.toNanos(
                        job.checkpointConfig().timeoutMillis()));
        while (hasInFlightCheckpoint(authority.byJob(jobId))) {
            convergeInFlightOperations(jobId);
            if (!hasInFlightCheckpoint(authority.byJob(jobId))) return;
            Future<?> task = ownedCheckpointTasks.get(jobId);
            if (task == null) break;
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) break;
            try {
                task.get(remaining, TimeUnit.NANOSECONDS);
            } catch (ExecutionException ignored) {
                // The task records its durable outcome before completing.
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new StreamProductControlException(
                        StreamProductControlException.Code.INVALID_STATE,
                        "Stream checkpoint convergence was interrupted");
            } catch (TimeoutException timeout) {
                break;
            }
        }
    }

    private StreamProductJobSnapshot completeConvergedDecision(
            StreamProductJobSnapshot snapshot) {
        StreamProductDecisionConvergence converged =
                runFactory.convergeDurableDecision(snapshot);
        if (converged == null) return snapshot;
        for (StreamProductCheckpointOperation operation
                : snapshot.checkpoints()) {
            if (!operation.checkpointId().equals(converged.referenceId())) {
                continue;
            }
            if (operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED) {
                authority.startCheckpoint(snapshot.jobId(),
                        operation.commandId());
            }
            return authority.completeCheckpoint(snapshot.jobId(),
                    operation.commandId(), converged.cdcPhase());
        }
        StreamProductSavepointOperation savepoint = snapshot.savepointOrNull();
        if (savepoint != null
                && savepoint.savepointId().equals(converged.referenceId())) {
            if (savepoint.status()
                    == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED) {
                authority.startSavepoint(snapshot.jobId(),
                        savepoint.commandId());
            }
            return authority.completeSavepoint(snapshot.jobId(),
                    savepoint.commandId());
        }
        throw new IllegalStateException(
                "durable Stream decision has no matching public operation");
    }

    private static void requireNoInFlightDecision(
            StreamProductJobSnapshot snapshot) {
        for (StreamProductCheckpointOperation operation
                : snapshot.checkpoints()) {
            if (operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                    || operation.status()
                    == io.rowweave.stream.api.StreamOperationStatus.RUNNING) {
                throw new StreamProductControlException(
                        StreamProductControlException.Code.INVALID_STATE,
                        "Stream checkpoint decision must converge before cancellation");
            }
        }
        StreamProductSavepointOperation savepoint = snapshot.savepointOrNull();
        if (savepoint != null
                && (savepoint.status()
                == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                || savepoint.status()
                == io.rowweave.stream.api.StreamOperationStatus.RUNNING)) {
            throw new StreamProductControlException(
                    StreamProductControlException.Code.INVALID_STATE,
                    "Stream savepoint decision must converge before cancellation");
        }
        StreamProductRescaleOperation rescale = snapshot.rescaleOrNull();
        if (rescale != null
                && (rescale.status()
                == io.rowweave.stream.api.StreamOperationStatus.ACCEPTED
                || rescale.status()
                == io.rowweave.stream.api.StreamOperationStatus.RUNNING)) {
            throw new StreamProductControlException(
                    StreamProductControlException.Code.INVALID_STATE,
                    "Stream rescale decision must converge before cancellation");
        }
    }

    private synchronized StreamProductJobSnapshot rescale(
            StreamJobId jobId, StreamCommandId commandId,
            int targetParallelism) {
        StreamProductJobSnapshot current = ensureActive(
                authority.byJob(jobId));
        StreamProductRun run = activeRuns.get(jobId);
        StreamProductRescaleOperation existing = current.rescaleOrNull();
        long sourceGeneration = existing != null
                && existing.commandId().equals(commandId)
                ? existing.sourceGeneration()
                : run == null ? 1L : run.assignmentGeneration();
        StreamProductJobSnapshot accepted = authority.acceptRescale(
                jobId, commandId, targetParallelism, sourceGeneration);
        StreamProductRescaleOperation operation =
                accepted.rescale(commandId);
        if (operation.status()
                == io.rowweave.stream.api.StreamOperationStatus.SUCCEEDED
                || operation.status()
                == io.rowweave.stream.api.StreamOperationStatus.FAILED) {
            return accepted;
        }
        if (run == null || current.status() != StreamJobStatus.RUNNING) {
            return authority.failRescale(jobId, commandId);
        }
        StreamProductJobSnapshot running = operation.phase()
                == StreamProductRescaleOperation.Phase.INTENT_DURABLE
                ? authority.startRescale(jobId, commandId)
                : accepted;
        operation = running.rescale(commandId);
        try {
            if (operation.phase()
                    == StreamProductRescaleOperation.Phase.CUTOVER_DURABLE) {
                return authority.completeRescale(jobId, commandId);
            }
            if (run.assignmentGeneration()
                    == operation.targetGeneration()) {
                authority.decideRescaleCutover(jobId, commandId);
                return authority.completeRescale(jobId, commandId);
            }
            StreamProductRun replacement = run.liveRescale(
                    commandId, operation.rescaleId(), targetParallelism,
                    operation.sourceGeneration(), operation.targetGeneration());
            if (replacement == null) {
                throw new IllegalStateException(
                        "live rescale returned no target generation");
            }
            authority.decideRescaleCutover(jobId, commandId);
            if (replacement != run) {
                sampleEvidence(jobId, run, true);
                activeRuns.put(jobId, replacement);
                closeQuietly(run);
            }
            return authority.completeRescale(jobId, commandId);
        } catch (Throwable failure) {
            LOG.warn("Stream live rescale failed job={} command={}",
                    jobId, commandId, failure);
            if (failure instanceof StreamLiveRescaleCutoverException) {
                authority.decideRescaleCutover(jobId, commandId);
                activeRuns.remove(jobId, run);
                closeQuietly(run);
                StreamProductJobSnapshot completed =
                        authority.completeRescale(jobId, commandId);
                ensureActive(completed);
                return authority.byJob(jobId);
            }
            StreamProductRescaleOperation durable = authority.byJob(jobId)
                    .rescale(commandId);
            if (durable.phase()
                    == StreamProductRescaleOperation.Phase.CUTOVER_DURABLE) {
                return authority.completeRescale(jobId, commandId);
            }
            return authority.failRescale(jobId, commandId);
        }
    }

    private StreamProductJobSnapshot checkpoint(
            StreamJobId jobId, StreamCommandId commandId) {
        return checkpoint(jobId, commandId, true);
    }

    private StreamProductJobSnapshot checkpoint(
            StreamJobId jobId, StreamCommandId commandId,
            boolean ensureLiveness) {
        if (ensureLiveness) ensureActive(authority.byJob(jobId));
        ReentrantLock lock = checkpointLocks.computeIfAbsent(
                jobId, ignored -> new ReentrantLock(true));
        lock.lock();
        try {
            activeCheckpointCommands.put(jobId, commandId);
            return checkpointExclusive(jobId, commandId);
        } finally {
            activeCheckpointCommands.remove(jobId, commandId);
            lock.unlock();
        }
    }

    private StreamProductJobSnapshot checkpointExclusive(
            StreamJobId jobId, StreamCommandId commandId) {
        StreamProductJobSnapshot current = authority.byJob(jobId);
        StreamProductJobSnapshot accepted =
                authority.acceptCheckpoint(jobId, commandId);
        StreamProductCheckpointOperation operation =
                accepted.checkpoint(commandId);
        if (operation.status() ==
                io.rowweave.stream.api.StreamOperationStatus.SUCCEEDED
                || operation.status() ==
                io.rowweave.stream.api.StreamOperationStatus.FAILED) {
            return accepted;
        }
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                accepted.artifact());
        if (job.resourceProfile().checkpointBytes() <= 0L) {
            return authority.failCheckpoint(jobId, commandId);
        }
        StreamProductRun run = activeRuns.get(jobId);
        if (run == null || current.status() != StreamJobStatus.RUNNING) {
            if (current.status() == StreamJobStatus.STARTING
                    || current.status() == StreamJobStatus.RESTARTING
                    || current.status() == StreamJobStatus.SUBMITTED) {
                return accepted;
            }
            return authority.failCheckpoint(jobId, commandId);
        }
        StreamProductJobSnapshot running =
                authority.startCheckpoint(jobId, commandId);
        try {
            Runnable cut = () -> run.triggerCheckpoint(commandId,
                    running.checkpoint(commandId).checkpointId());
            if (run.ownsCheckpointDeadline()) cut.run();
            else invokeWithinCheckpointTimeout(job, cut);
            StreamProductJobSnapshot completed = authority.completeCheckpoint(
                    jobId, commandId, run.cdcPhase());
            sampleEvidence(jobId, run, true);
            if (run.finished()) {
                sampleEvidence(jobId, run, true);
                completed = authority.transition(
                        jobId, StreamJobStatus.FINISHED);
                activeRuns.remove(jobId, run);
                closeQuietly(run);
                retireTerminal(completed);
            }
            return completed;
        } catch (CheckpointCommitPendingException pending) {
            return authority.byJob(jobId);
        } catch (Throwable failure) {
            LOG.warn("Stream checkpoint failed job={} command={}",
                    jobId, commandId, failure);
            boolean interrupted = Thread.interrupted();
            try {
                if (closed.get()) {
                    // Master handoff must not turn an irreversible Worker cut
                    // into FAILED; the replacement owns reconciliation.
                    return authority.byJob(jobId);
                }
                StreamProductJobSnapshot converged =
                        completeConvergedDecision(authority.byJob(jobId));
                if (converged.checkpoint(commandId).status()
                        == io.rowweave.stream.api.StreamOperationStatus
                        .SUCCEEDED) {
                    return converged;
                }
                return authority.failCheckpoint(jobId, commandId);
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }

    private void publishCompletedCheckpointEvidence(StreamJobId jobId,
            StreamProductRun run, String expectedCheckpointId,
            long durationMillis) {
        try {
            StreamCompletedCheckpointEvidence completed =
                    run.completedCheckpointEvidence();
            if (completed == null
                    || !expectedCheckpointId.equals(completed.checkpointId())) {
                return;
            }
            StreamProductJobSnapshot current = authority.byJob(jobId);
            StreamJobEvidence prior = current.evidence();
            if (!prior.available()) return;
            if (prior.latestCompletedCheckpointSequence()
                    > completed.checkpointSequence()) {
                return;
            }
            if (prior.latestCompletedCheckpointSequence()
                    == completed.checkpointSequence()
                    && prior.latestCompletedCheckpointId() != null
                    && !prior.latestCompletedCheckpointId().equals(
                    completed.checkpointId())) {
                return;
            }
            StreamJobProgress attempt = withManagedStateFloor(
                    prior.attemptProgress(), completed.managedStateBytes());
            StreamJobProgress lifetime = withManagedStateFloor(
                    prior.lifetimeProgress(), completed.managedStateBytes());
            authority.updateEvidence(jobId, new StreamJobEvidence(
                    jobId, prior.runId(), prior.attemptId(),
                    prior.attemptOrdinal(), prior.fenceToken(),
                    prior.coordinatorTerm(), prior.assignmentGeneration(),
                    completed.checkpointId(), completed.checkpointSequence(),
                    durationMillis, completed.contentBytes(), attempt, lifetime,
                    prior.placementTaskCount(), prior.reportedTaskCount(),
                    System.currentTimeMillis()));
        } catch (RuntimeException ignored) {
            // Operational observation is advisory and never changes execution.
        }
    }

    private static StreamJobProgress withManagedStateFloor(
            StreamJobProgress progress, long managedStateBytes) {
        return new StreamJobProgress(progress.attempt(),
                progress.completedCheckpoints(), progress.operatorWatermark(),
                progress.cdcPhase(), progress.restarts(),
                progress.recordsRead(), progress.recordsWritten(),
                progress.bytesRead(), progress.bytesWritten(),
                progress.backlogEvents(), progress.backlogBytes(),
                progress.mailboxHighWaterEvents(),
                progress.channelHighWaterBytes(),
                Math.max(progress.managedStateBytes(), managedStateBytes),
                progress.timerHighWaterCount(),
                progress.windowHighWaterCount(),
                progress.backpressuredMillis());
    }

    private synchronized StreamProductJobSnapshot savepoint(
            StreamJobId jobId, StreamCommandId commandId,
            io.rowweave.StreamSavepointRequest request) {
        StreamProductJobSnapshot current = ensureActive(
                authority.byJob(jobId));
        String requestDigest = StreamSavepointRequestCodec.digest(request);
        StreamProductJobSnapshot accepted = authority.acceptSavepoint(
                jobId, commandId, requestDigest);
        StreamProductSavepointOperation operation =
                accepted.savepoint(commandId);
        if (operation.status()
                == io.rowweave.stream.api.StreamOperationStatus.SUCCEEDED
                || operation.status()
                == io.rowweave.stream.api.StreamOperationStatus.FAILED) {
            return accepted;
        }
        StreamProductRun run = activeRuns.get(jobId);
        if (run == null || current.status() != StreamJobStatus.RUNNING) {
            if (current.status() == StreamJobStatus.STARTING
                    || current.status() == StreamJobStatus.RESTARTING
                    || current.status() == StreamJobStatus.SUBMITTED) {
                return accepted;
            }
            return authority.failSavepoint(jobId, commandId);
        }
        StreamProductJobSnapshot running =
                authority.startSavepoint(jobId, commandId);
        try {
            PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                    running.artifact());
            if (job.resourceProfile().checkpointBytes() <= 0L) {
                return authority.failSavepoint(jobId, commandId);
            }
            Runnable cut = () -> run.stopWithSavepoint(
                    commandId, running.savepoint(commandId).savepointId(),
                    request);
            if (run.ownsCheckpointDeadline()) cut.run();
            else invokeWithinCheckpointTimeout(job, cut);
            sampleEvidence(jobId, run, true);
            activeRuns.remove(jobId, run);
            nextPeriodicCheckpointNanos.remove(jobId);
            cancelOwnedCheckpoint(jobId);
            closeQuietly(run);
            StreamProductJobSnapshot stopped =
                    authority.completeSavepoint(jobId, commandId);
            retireTerminal(stopped);
            return stopped;
        } catch (CheckpointCommitPendingException pending) {
            return authority.byJob(jobId);
        } catch (Throwable failure) {
            LOG.warn("Stream savepoint failed job={} command={}",
                    jobId, commandId, failure);
            boolean interrupted = Thread.interrupted();
            try {
                if (closed.get()) {
                    // Master handoff must not turn an irreversible Worker cut
                    // into FAILED; the replacement owns reconciliation.
                    return authority.byJob(jobId);
                }
                StreamProductJobSnapshot converged =
                        completeConvergedDecision(authority.byJob(jobId));
                io.rowweave.stream.api.StreamOperationStatus status =
                        converged.savepoint(commandId).status();
                if (status
                        == io.rowweave.stream.api.StreamOperationStatus
                        .SUCCEEDED) {
                    activeRuns.remove(jobId, run);
                    nextPeriodicCheckpointNanos.remove(jobId);
                    cancelOwnedCheckpoint(jobId);
                    closeQuietly(run);
                    retireTerminal(converged);
                    return converged;
                }
                return authority.failSavepoint(jobId, commandId);
            } catch (CheckpointCommitPendingException pending) {
                // The durable savepoint decision remains authoritative and is
                // retried rather than published as a terminal failure.
                return authority.byJob(jobId);
            } finally {
                if (interrupted) Thread.currentThread().interrupt();
            }
        }
    }

    private void invokeWithinCheckpointTimeout(
            PortableStreamJob job, Runnable operation) {
        Future<?> future = checkpointExecutor.submit(operation);
        try {
            future.get(job.checkpointConfig().timeoutMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            future.cancel(true);
            throw new IllegalStateException(
                    "Stream checkpoint cut exceeded its configured timeout",
                    timeout);
        } catch (InterruptedException interrupted) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Stream checkpoint cut was interrupted", interrupted);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) throw (Error) cause;
            throw new IllegalStateException(
                    "Stream checkpoint cut failed", cause);
        }
    }

    private static StreamProductControlWire.Response failureResponse(
            Throwable failure) {
        if (failure instanceof StreamProductControlException) {
            StreamProductControlException control =
                    (StreamProductControlException) failure;
            return StreamProductControlWire.Response.failure(
                    publicCode(control.code()), control.getMessage());
        }
        if (failure instanceof UnsupportedOperationException) {
            return StreamProductControlWire.Response.failure(
                    StreamControlException.Code.UNSUPPORTED_OPERATION,
                    "Stream operation is unsupported");
        }
        return StreamProductControlWire.Response.failure(
                StreamControlException.Code.INVALID_RESPONSE,
                "Stream Master rejected the control request"
                        + (failure.getMessage() == null
                        || failure.getMessage().isEmpty()
                        ? "" : ": " + failure.getMessage()));
    }

    private static StreamControlException.Code publicCode(
            StreamProductControlException.Code code) {
        switch (code) {
            case SUBMISSION_CONFLICT:
                return StreamControlException.Code.SUBMISSION_CONFLICT;
            case COMMAND_CONFLICT:
                return StreamControlException.Code.COMMAND_CONFLICT;
            case COMMAND_NOT_FOUND:
                return StreamControlException.Code.COMMAND_NOT_FOUND;
            case JOB_NOT_FOUND:
                return StreamControlException.Code.JOB_NOT_FOUND;
            default:
                return StreamControlException.Code.INVALID_RESPONSE;
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            if (!closed.compareAndSet(false, true)) return;
            if (server != null) {
                try {
                    server.close();
                } catch (IOException ignored) {
                    // Idempotent close.
                }
            }
            for (Future<?> task : ownedCheckpointTasks.values()) {
                task.cancel(true);
            }
            ownedCheckpointTasks.clear();
            activeCheckpointCommands.clear();
            cancellationIntents.clear();
            recoveryDiagnostics.clear();
            for (StreamProductRun run : activeRuns.values()) closeQuietly(run);
            activeRuns.clear();
            restartFailures.clear();
            restartReadyNanos.clear();
            nextPeriodicCheckpointNanos.clear();
            nextEvidenceSampleNanos.clear();
            retiredRuns.clear();
            checkpointExecutor.shutdownNow();
            if (controlExecutor != null) controlExecutor.shutdownNow();
        }
        recoveryThread.interrupt();
        if (acceptThread != null) {
            try {
                acceptThread.join(2_000L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            recoveryThread.join(2_000L);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(StreamProductRun run) {
        if (run == null) return;
        try {
            run.close();
        } catch (RuntimeException ignored) {
            // Closing a superseded Master must not rewrite durable job state.
        }
    }
}
