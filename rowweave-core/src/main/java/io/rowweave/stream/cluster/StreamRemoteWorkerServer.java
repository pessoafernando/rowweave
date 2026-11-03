package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.deployment.PortableFunctionProviderRegistry;
import io.rowweave.deployment.StreamConnectorProviderRegistry;
import io.rowweave.deployment.VerifiedDeploymentSet;
import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.stream.api.StreamCheckpointMode;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor;
import io.rowweave.stream.contract.StreamSplitDiscoveryCodec;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.DynamicDurableStreamSourceProvider;
import io.rowweave.stream.runtime.StreamSourceEnumeratorOpenRequest;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSourceEnumerator;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import io.rowweave.stream.runtime.EagerStreamSinkProvider;
import io.rowweave.stream.runtime.FileCheckpointPayloadStore;
import io.rowweave.stream.runtime.FileCheckpointAuthority;
import io.rowweave.stream.runtime.FileCheckpointMemberStore;
import io.rowweave.stream.runtime.CheckpointTransactionalSink;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.StreamChannelInput;
import io.rowweave.stream.runtime.StreamSourceCheckpointRuntime;
import io.rowweave.stream.runtime.StreamUpsertKeyValidator;
import io.rowweave.stream.runtime.StreamChangeKeyValidator;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;
import io.rowweave.stream.runtime.SignedStreamConnectorAdapters;
import io.rowweave.stream.spi.StreamSecretResolver;
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
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Independent Stream Worker control endpoint backed by Worker-local providers. */
public final class StreamRemoteWorkerServer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(
            StreamRemoteWorkerServer.class);
    private static final int SOCKET_TIMEOUT_MILLIS = 30_000;

    private final StreamClusterWorker worker;
    private final Map<String, String> sourceContracts;
    private final Map<String, String> sinkContracts;
    private final StreamFunctionResolver functionResolver;
    private final Path checkpointRoot;
    private final ServerSocket server;
    private final ControlSecurityConfig security;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread acceptThread;
    private final ExecutorService inProcessControlExecutor;
    private final ExecutorService socketControlExecutor;
    private final ExecutorService sourceDiscoveryExecutor;
    private final AtomicBoolean sourceDiscoveryPoisoned = new AtomicBoolean();
    private final Map<OperatorTaskKey, OperatorTaskSlot> operatorTasks =
            new HashMap<>();
    private final Map<EnumeratorKey, StreamSourceEnumerator> enumerators =
            new ConcurrentHashMap<>();
    private final Map<EnumeratorKey, DiscoveryReplay> discoveryReplays =
            new HashMap<>();
    private final WorkerResourceLeases resourceLeases;
    private final StreamConnectorProviderRegistry connectorRegistry;

    /** Signed Worker bootstrap derives advertised providers from verified metadata. */
    public static StreamRemoteWorkerServer signed(
            StreamWorkerCapabilityProfile profile,
            VerifiedDeploymentSet deployments,
            Path checkpointRoot, ControlSecurityConfig security, int port) {
        return signed(profile, deployments, null, checkpointRoot,
                security, port);
    }

    public static StreamRemoteWorkerServer signed(
            StreamWorkerCapabilityProfile profile,
            VerifiedDeploymentSet deployments,
            StreamSecretResolver resolver,
            Path checkpointRoot, ControlSecurityConfig security, int port) {
        if (profile == null || deployments == null) {
            throw new IllegalArgumentException(
                    "signed Stream Worker profile and deployments are required");
        }
        deployments.requireAdmissionOpen();
        ProviderActionSupervisor supervisor =
                ProviderActionSupervisor.processWide(
                        Math.min(profile.taskSlots(), 64));
        StreamConnectorProviderRegistry registry =
                StreamConnectorProviderRegistry.scan(deployments,
                        StreamConnectorProviderRegistry.Role.WORKER);
        PortableFunctionProviderRegistry functions =
                PortableFunctionProviderRegistry.scan(deployments,
                        PortableFunctionProviderRegistry.Role.WORKER);
        SignedStreamConnectorAdapters adapters =
                new SignedStreamConnectorAdapters(
                        registry, supervisor, resolver);
        StreamFunctionResolver functionResolver = reference ->
                functions.resolve(reference, supervisor,
                        Math.addExact(System.nanoTime(),
                                TimeUnit.SECONDS.toNanos(30L)));
        return new StreamRemoteWorkerServer(profile, adapters.sources(),
                adapters.eagerSinks(), adapters.transactionalSinks(),
                functionResolver, checkpointRoot, security, port, true,
                registry);
    }

    public StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders) {
        this(profile, sourceProviders, sinkProviders, reference -> {
            throw new UnsupportedOperationException(
                    "Stream Worker has no function deployment resolver");
        }, null);
    }

    public StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            Path checkpointRoot) {
        this(profile, sourceProviders, sinkProviders, reference -> {
            throw new UnsupportedOperationException(
                    "Stream Worker has no function deployment resolver");
        }, checkpointRoot);
    }

    public StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            StreamFunctionResolver functionResolver) {
        this(profile, sourceProviders, sinkProviders, functionResolver, null);
    }

    public StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot) {
        this(profile, sourceProviders, sinkProviders, functionResolver,
                checkpointRoot, loopbackSecurity(), 0, true);
    }

    public StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot, ControlSecurityConfig security, int port) {
        this(profile, sourceProviders, sinkProviders, Collections.emptyMap(),
                functionResolver, checkpointRoot, security, port, true);
    }

    public StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            Map<String, TransactionalStreamSinkProvider>
                    transactionalSinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot, ControlSecurityConfig security, int port) {
        this(profile, sourceProviders, sinkProviders,
                transactionalSinkProviders, functionResolver,
                checkpointRoot, security, port, true);
    }

    public StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            Map<String, TransactionalStreamSinkProvider>
                    transactionalSinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot) {
        this(profile, sourceProviders, sinkProviders,
                transactionalSinkProviders, functionResolver,
                checkpointRoot, loopbackSecurity(), 0, true);
    }

    static StreamRemoteWorkerServer inProcess(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot) {
        return new StreamRemoteWorkerServer(profile, sourceProviders,
                sinkProviders, Collections.emptyMap(), functionResolver,
                checkpointRoot, loopbackSecurity(), 0, false);
    }

    static StreamRemoteWorkerServer inProcess(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            Map<String, TransactionalStreamSinkProvider>
                    transactionalSinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot) {
        return new StreamRemoteWorkerServer(profile, sourceProviders,
                sinkProviders, transactionalSinkProviders, functionResolver,
                checkpointRoot, loopbackSecurity(), 0, false);
    }

    private StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot, ControlSecurityConfig security,
            int port, boolean listen) {
        this(profile, sourceProviders, sinkProviders, Collections.emptyMap(),
                functionResolver, checkpointRoot, security, port, listen);
    }

    private StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            Map<String, TransactionalStreamSinkProvider>
                    transactionalSinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot, ControlSecurityConfig security,
            int port, boolean listen) {
        this(profile, sourceProviders, sinkProviders,
                transactionalSinkProviders, functionResolver,
                checkpointRoot, security, port, listen, null);
    }

    private StreamRemoteWorkerServer(
            StreamWorkerCapabilityProfile profile,
            Map<String, DurableStreamSourceProvider> sourceProviders,
            Map<String, EagerStreamSinkProvider> sinkProviders,
            Map<String, TransactionalStreamSinkProvider>
                    transactionalSinkProviders,
            StreamFunctionResolver functionResolver,
            Path checkpointRoot, ControlSecurityConfig security,
            int port, boolean listen,
            StreamConnectorProviderRegistry connectorRegistry) {
        if (functionResolver == null || security == null) {
            throw new IllegalArgumentException(
                    "Stream function resolver is required");
        }
        boolean signedCapability = profile != null
                && profile.capabilities().contains(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
        if (signedCapability != (connectorRegistry != null)) {
            throw new IllegalArgumentException(
                    "signed Stream Worker capability requires a signed registry");
        }
        if (connectorRegistry != null) {
            connectorRegistry.requireWorkerRole();
            if (!profile.capabilities().contains(
                    PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                            + connectorRegistry.deploymentSetId())) {
                throw new IllegalArgumentException(
                        "signed Stream Worker deployment set differs from profile");
            }
        }
        this.connectorRegistry = connectorRegistry;
        this.worker = new StreamClusterWorker(
                profile, sourceProviders, sinkProviders,
                transactionalSinkProviders);
        this.resourceLeases = new WorkerResourceLeases(profile);
        this.sourceContracts = sourceContracts(sourceProviders);
        this.sinkContracts = sinkContracts(
                sinkProviders, transactionalSinkProviders);
        this.functionResolver = functionResolver;
        this.security = security;
        this.checkpointRoot = checkpointRoot == null ? null
                : checkpointRoot.toAbsolutePath().normalize();
        this.sourceDiscoveryExecutor =
                Executors.newSingleThreadExecutor(runnable -> {
                    Thread thread = new Thread(runnable,
                            "rowweave-stream-worker-source-discovery-"
                                    + profile.workerId());
                    thread.setDaemon(true);
                    return thread;
                });
        if (!listen) {
            this.server = null;
            this.acceptThread = null;
            this.socketControlExecutor = null;
            this.inProcessControlExecutor =
                    Executors.newSingleThreadExecutor(runnable -> {
                        Thread thread = new Thread(runnable,
                                "rowweave-stream-worker-in-process-control-"
                                        + profile.workerId());
                        thread.setDaemon(true);
                        return thread;
                    });
            return;
        }
        this.inProcessControlExecutor = null;
        try {
            this.server = RoleAuthenticatedSocketTransport.bind(
                    security, ControlRole.WORKER, port, 32);
        } catch (RuntimeException failure) {
            throw new IllegalStateException(
                    "Stream Worker could not bind its control endpoint", failure);
        }
        this.socketControlExecutor = new ThreadPoolExecutor(
                4, 4, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(32), runnable -> {
                    Thread thread = new Thread(runnable,
                            "rowweave-stream-worker-control-request-"
                                    + profile.workerId());
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
        this.acceptThread = new Thread(
                this::acceptLoop, "rowweave-stream-worker-control-"
                        + profile.workerId());
        acceptThread.setDaemon(true);
        acceptThread.start();
    }

    public int port() {
        if (server == null) {
            throw new IllegalStateException(
                    "in-process Stream Worker has no control listener");
        }
        return server.getLocalPort();
    }

    int controlListenerCountForDiagnostics() {
        return server == null ? 0 : 1;
    }

    StreamRemoteWorkerWire.Response dispatchInProcess(
            StreamRemoteWorkerWire.Request request) {
        if (closed.get()) {
            return StreamRemoteWorkerWire.Response.failure(
                    StreamRemoteWorkerWire.ErrorCode.INTERNAL_FAILURE,
                    "Stream Worker is closed");
        }
        try {
            return dispatch(request);
        } catch (Throwable failure) {
            return failure(failure);
        }
    }

    StreamRemoteWorkerWire.Response dispatchInProcess(
            StreamRemoteWorkerWire.Request request, int timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Stream Worker RPC timeout must be positive");
        }
        if (inProcessControlExecutor == null) {
            throw new IllegalStateException(
                    "socket Stream Worker has no in-process transport");
        }
        Future<StreamRemoteWorkerWire.Response> future =
                inProcessControlExecutor.submit(
                        () -> dispatchInProcess(request));
        try {
            return future.get(timeoutMillis, TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            future.cancel(true);
            throw new IllegalStateException(
                    "in-process Stream Worker control RPC timed out", timeout);
        } catch (InterruptedException interrupted) {
            future.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "in-process Stream Worker control RPC was interrupted",
                    interrupted);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) throw (Error) cause;
            throw new IllegalStateException(
                    "in-process Stream Worker control RPC failed", cause);
        }
    }

    private void acceptLoop() {
        while (!closed.get()) {
            try {
                final Socket socket = server.accept();
                try {
                    socketControlExecutor.execute(() -> handle(socket));
                } catch (RejectedExecutionException saturated) {
                    closeSocket(socket);
                }
            } catch (IOException failure) {
                if (!closed.get()) {
                    // One reset control connection cannot terminate the Worker.
                }
            }
        }
    }

    private static void closeSocket(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // The peer already owns any durable command result.
        }
    }

    private void handle(Socket socket) {
        try (Socket connection = socket) {
            RoleAuthenticatedSocketTransport.authenticateAccepted(
                    security, ControlRole.MASTER, connection,
                    SOCKET_TIMEOUT_MILLIS);
            DataInputStream input = new DataInputStream(
                    new BufferedInputStream(connection.getInputStream()));
            DataOutputStream output = new DataOutputStream(
                    new BufferedOutputStream(connection.getOutputStream()));
            StreamRemoteWorkerWire.Response response;
            try {
                response = dispatch(StreamRemoteWorkerWire.readRequest(input));
            } catch (Throwable failure) {
                response = failure(failure);
            }
            StreamRemoteWorkerWire.writeResponse(output, response);
        } catch (IOException ignored) {
            // The fenced command may already be accepted when its ACK is lost.
        }
    }

    private static ControlSecurityConfig loopbackSecurity() {
        return ControlSecurityConfig.fromProperties(new Properties());
    }

    private StreamRemoteWorkerWire.Response dispatch(
            StreamRemoteWorkerWire.Request request) throws IOException {
        if (request.type == StreamRemoteWorkerWire.Type.CHECKPOINT) {
            return dispatchCheckpoint(request);
        }
        synchronized (this) {
            return dispatchLocked(request);
        }
    }

    private StreamRemoteWorkerWire.Response dispatchLocked(
            StreamRemoteWorkerWire.Request request) throws IOException {
        if (closed.get()) {
            return StreamRemoteWorkerWire.Response.failure(
                    StreamRemoteWorkerWire.ErrorCode.INTERNAL_FAILURE,
                    "Stream Worker is closed");
        }
        if (request.type == StreamRemoteWorkerWire.Type.DESCRIBE) {
            return StreamRemoteWorkerWire.Response.profile(
                    worker.profile(), sourceContracts, sinkContracts);
        }
        if (request.type == StreamRemoteWorkerWire.Type.START_SINK
                || request.type == StreamRemoteWorkerWire.Type.START_SOURCE
                || request.type
                == StreamRemoteWorkerWire.Type.START_SINK_TASK
                || request.type
                == StreamRemoteWorkerWire.Type.START_SOURCE_FANOUT) {
            throw new UnsupportedOperationException(
                    "legacy role-shaped Stream start RPC is disabled; "
                            + "deploy one operator task per node/subtask");
        }
        if (request.type == StreamRemoteWorkerWire.Type.DISCOVER_SOURCE
                || request.type == StreamRemoteWorkerWire.Type.PREPARE_OPERATOR_TASK
                || request.type == StreamRemoteWorkerWire.Type.START_OPERATOR_TASK
                || request.type == StreamRemoteWorkerWire.Type.RECOVER_CHECKPOINT) {
            PlanArtifact artifact = PlanArtifactCodec.decode(
                    request.artifactFrame);
            if (connectorRegistry != null) {
                connectorRegistry.verifyArtifact(artifact);
            } else if (artifact.requiredFeatures().contains(
                    PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1)) {
                throw new IllegalArgumentException(
                        "signed Stream connector Artifact requires a signed Worker");
            }
        }
        StreamControlMessage command = StreamControlMessageCodec.decode(
                request.controlFrame);
        if (request.type
                == StreamRemoteWorkerWire.Type.DISCOVER_SOURCE) {
            if ((command.kind() != StreamControlMessage.Kind.START
                    && command.kind() != StreamControlMessage.Kind.RESTORE
                    && command.kind()
                    != StreamControlMessage.Kind.TASK_PROGRESS)
                    || !worker.profile().workerIncarnation().equals(
                    command.workerIncarnation())) {
                throw new IllegalArgumentException(
                        "Stream source discovery attempt fence is invalid");
            }
            StreamProductExecutionGraph admitted;
            if (command.kind() == StreamControlMessage.Kind.TASK_PROGRESS) {
                admitted = StreamProductExecutionGraph.decode(
                        request.artifactFrame);
            } else {
                StreamTaskLaunchPayload launch =
                        StreamTaskLaunchPayload.decode(
                                command.payload(), command.kind());
                admitted = StreamProductExecutionGraph.decode(
                        request.artifactFrame,
                        launch.parallelismOverride(),
                        launch.nodeMinimumParallelism());
            }
            if (!admitted.commandDigest.equals(command.commandDigest())) {
                throw new IllegalArgumentException(
                        "Stream source discovery differs from the Artifact");
            }
            StreamExecutionGraph.Node node = admitted.executionGraph.node(
                    request.operatorUid);
            if (node.portableNode().opcode() != StreamOperatorCode.SOURCE) {
                throw new IllegalArgumentException(
                        "Stream source discovery target is not a Source");
            }
            StreamSourceBinding binding = (StreamSourceBinding)
                    node.portableNode().binding();
            StreamSourceDiscoveryDescriptor limits =
                    binding.discoveryDescriptor();
            if (request.discoveryLimit
                    > limits.maximumChangesPerDiscovery()
                    || request.discoveryCursorFrame.length
                    > limits.maximumCursorBytes()) {
                throw new IllegalArgumentException(
                        "Stream source discovery exceeds its limits");
            }
            EnumeratorKey key = new EnumeratorKey(command,
                    request.operatorUid);
            DiscoveryReplay replay = discoveryReplays.get(key);
            if (replay != null && replay.command.equals(command)) {
                replay.requireSameRequest(request);
                return StreamRemoteWorkerWire.Response.sourceDiscovery(
                        replay.responseFrame);
            }
            if (sourceDiscoveryPoisoned.get()) {
                throw new IllegalStateException(
                        "Stream source discovery facility is poisoned");
            }
            worker.acceptParticipantControl(request.controlFrame);
            StreamSplitDiscovery discovery = discoverSource(
                    key, binding, request, limits);
            byte[] response = StreamSplitDiscoveryCodec.encode(discovery,
                    limits.maximumChangesPerDiscovery(),
                    limits.maximumCursorBytes(),
                    limits.maximumDescriptorBytes());
            discoveryReplays.put(key, new DiscoveryReplay(command,
                    request.discoveryCursorFrame,
                    request.discoveryLimit, response));
            return StreamRemoteWorkerWire.Response.sourceDiscovery(response);
        }
        if (request.type
                == StreamRemoteWorkerWire.Type.ASSIGN_SOURCE_SPLIT) {
            if (command.kind()
                    != StreamControlMessage.Kind.TASK_PROGRESS
                    || !worker.profile().workerIncarnation().equals(
                    command.workerIncarnation())) {
                throw new IllegalArgumentException(
                        "Stream source assignment attempt fence is invalid");
            }
            OperatorTaskSlot target = operatorTask(command,
                    request.operatorUid, request.subtaskIndex);
            if (target == null || !target.matchesAttempt(command)) {
                throw new IllegalArgumentException(
                        "Stream source assignment task is unavailable");
            }
            target.assignDynamicSource(request, command,
                    () -> worker.acceptParticipantControl(
                            request.controlFrame));
            return StreamRemoteWorkerWire.Response.ack();
        }
        if (request.type
                == StreamRemoteWorkerWire.Type.REVOKE_SOURCE_SPLIT) {
            if (command.kind()
                    != StreamControlMessage.Kind.TASK_PROGRESS
                    || !worker.profile().workerIncarnation().equals(
                    command.workerIncarnation())) {
                throw new IllegalArgumentException(
                        "Stream source revocation attempt fence is invalid");
            }
            OperatorTaskSlot target = operatorTask(command,
                    request.operatorUid, request.subtaskIndex);
            if (target == null || !target.matchesAttempt(command)) {
                throw new IllegalArgumentException(
                        "Stream source revocation task is unavailable");
            }
            target.revokeDynamicSource(request, command,
                    () -> worker.acceptParticipantControl(
                            request.controlFrame));
            return StreamRemoteWorkerWire.Response.ack();
        }
        if (request.type
                == StreamRemoteWorkerWire.Type.SOURCE_SPLIT_PROGRESS) {
            if (command.kind()
                    != StreamControlMessage.Kind.TASK_PROGRESS
                    || !worker.profile().workerIncarnation().equals(
                    command.workerIncarnation())) {
                throw new IllegalArgumentException(
                        "Stream source split progress fence is invalid");
            }
            OperatorTaskSlot target = operatorTask(command,
                    request.operatorUid, request.subtaskIndex);
            if (target == null || !target.matchesAttempt(command)) {
                throw new IllegalArgumentException(
                        "Stream source split progress task is unavailable");
            }
            worker.acceptParticipantControl(request.controlFrame);
            return StreamRemoteWorkerWire.Response.sourceSplitProgress(
                    target.sourceSplitProgress());
        }
        if (request.type
                == StreamRemoteWorkerWire.Type.FETCH_CHECKPOINT_PAYLOAD
                || request.type
                == StreamRemoteWorkerWire.Type.STORE_CHECKPOINT_PAYLOAD) {
            if (checkpointRoot == null
                    || !worker.profile().workerIncarnation().equals(
                    command.workerIncarnation())) {
                throw new IllegalArgumentException(
                        "Stream checkpoint payload transfer is unavailable");
            }
            StreamCheckpointPayloadCommandCodec.Command payload =
                    StreamCheckpointPayloadCommandCodec.decode(
                            request.artifactFrame);
            FileCheckpointPayloadStore store =
                    new FileCheckpointPayloadStore(checkpointRoot,
                            worker.profile().maximumCheckpointBytes());
            FileCheckpointPayloadStore.StoredPayload identity =
                    new FileCheckpointPayloadStore.StoredPayload(
                            payload.digest(), payload.contentBytes());
            if (request.type
                    == StreamRemoteWorkerWire.Type.FETCH_CHECKPOINT_PAYLOAD) {
                if (command.kind()
                        != StreamControlMessage.Kind.CHECKPOINT_TRIGGER
                        || payload.chunk().length != 0
                        || !hasAttempt(command)) {
                    throw new IllegalArgumentException(
                            "Stream checkpoint payload fetch fence is invalid");
                }
                return StreamRemoteWorkerWire.Response
                        .checkpointPayloadChunk(store.readChunk(identity,
                                payload.offset(),
                                FileCheckpointPayloadStore
                                        .TRANSFER_CHUNK_BYTES));
            }
            if ((command.kind() != StreamControlMessage.Kind.RESTORE
                    && command.kind()
                    != StreamControlMessage.Kind.CHECKPOINT_TRIGGER)
                    || payload.chunk().length == 0) {
                throw new IllegalArgumentException(
                        "Stream checkpoint payload store fence is invalid");
            }
            store.acceptChunk(identity, payload.offset(), payload.chunk());
            return StreamRemoteWorkerWire.Response.ack();
        }
        if (request.type == StreamRemoteWorkerWire.Type.TASK_LIFECYCLE) {
            OperatorTaskSlot operator = operatorTask(
                    command, request.operatorUid, request.subtaskIndex);
            if (operator == null || !operator.matchesAttempt(command)) {
                throw new IllegalArgumentException(
                        "Stream Worker task lifecycle attempt is unavailable");
            }
            return StreamRemoteWorkerWire.Response.taskLifecycle(
                    operator.failure() != null, operator.completed());
        }
        if (request.type == StreamRemoteWorkerWire.Type.TASK_PROGRESS) {
            OperatorTaskSlot operator = operatorTask(
                    command, request.operatorUid, request.subtaskIndex);
            if (operator == null || !operator.matchesAttempt(command)) {
                throw new IllegalArgumentException(
                        "Stream Worker task progress attempt is unavailable");
            }
            return StreamRemoteWorkerWire.Response.taskProgress(
                    operator.progress());
        }
        if (request.type == StreamRemoteWorkerWire.Type.STATUS) {
            if (request.operatorUid != null) {
                OperatorTaskSlot operator = operatorTask(
                        command, request.operatorUid,
                        request.subtaskIndex);
                if (operator == null || !operator.matchesAttempt(command)) {
                    throw new IllegalArgumentException(
                            "Stream Worker operator status attempt is unavailable");
                }
                Throwable failure = operator.failure();
                return StreamRemoteWorkerWire.Response.taskStatus(
                        failure != null,
                        failure == null ? null : failureDetail(failure));
            }
            boolean found = false;
            for (OperatorTaskSlot operator : operatorTasks.values()) {
                if (operator.matchesAttempt(command)) {
                    found = true;
                    if (operator.failure() != null) {
                        return StreamRemoteWorkerWire.Response.taskStatus(true,
                                "operator " + operator.identity.operatorUid()
                                        + "#" + operator.identity.subtaskIndex()
                                        + " failed: "
                                + failureDetail(operator.failure()));
                    }
                }
            }
            if (!found) {
                throw new IllegalArgumentException(
                        "Stream Worker status attempt is unavailable");
            }
            return StreamRemoteWorkerWire.Response.taskStatus(false);
        }
        if (request.type == StreamRemoteWorkerWire.Type.CANCEL) {
            if (command.kind() != StreamControlMessage.Kind.CANCEL) {
                throw new IllegalArgumentException(
                        "Stream Worker cancel RPC requires a CANCEL command");
            }
            worker.acceptControl(request.controlFrame);
            closeOperatorTasks(command);
            closeEnumerators(command);
            return StreamRemoteWorkerWire.Response.ack();
        }
        if (request.type == StreamRemoteWorkerWire.Type.CHECKPOINT) {
            throw new IllegalStateException(
                    "blocking checkpoint request bypassed its control lane");
        }
        if (request.type
                == StreamRemoteWorkerWire.Type.CHECKPOINT_COMPLETE) {
            if (command.kind()
                    != StreamControlMessage.Kind.CHECKPOINT_TRIGGER) {
                throw new IllegalArgumentException(
                        "Stream Worker checkpoint completion requires a trigger");
            }
            if (request.operatorUid == null) {
                throw new UnsupportedOperationException(
                        "Stream checkpoint completion RPC must target one "
                                + "operator task");
            }
            CheckpointIdentity checkpoint =
                    StreamCheckpointControlCodec.decode(command.payload());
            if (checkpointRoot == null || operatorTasks.isEmpty()) {
                throw new IllegalArgumentException(
                        "Stream Worker transactional completion scope is invalid");
            }
            OperatorTaskSlot target = operatorTask(
                    command, request.operatorUid,
                    request.subtaskIndex);
            if (target == null || !target.matchesAttempt(command)) {
                throw new IllegalArgumentException(
                        "Stream Worker transactional completion task is unavailable");
            }
            StreamExecutionGraph.Node node = target.admitted.executionGraph
                    .node(target.identity.operatorUid());
            boolean transactionalSink = node.portableNode().opcode()
                    == StreamOperatorCode.SINK
                    && ((StreamSinkBinding) node.portableNode().binding())
                    .visibilityProfile()
                    == StreamSinkBinding.VisibilityProfile
                    .CHECKPOINT_TRANSACTIONAL;
            boolean residentCdcV2 = node.portableNode().opcode()
                    == StreamOperatorCode.SOURCE
                    && "STREAM_CDC_ENUMERATOR_V2".equals(
                    node.portableNode().scalarParameters().get(
                            "source.enumeratorStateFormat"));
            if (!transactionalSink && !residentCdcV2) {
                throw new IllegalArgumentException(
                        "Stream Worker checkpoint completion task is invalid");
            }
            worker.acceptParticipantControl(request.controlFrame);
            io.rowweave.stream.runtime.StreamCheckpointAuthority authority =
                    checkpointAuthority(request.checkpointBundleFrame,
                            checkpoint);
            if (transactionalSink) {
                target.completeTransactionalCheckpoint(
                        checkpoint, request.maximumEvents, authority);
            } else {
                if (authority.decision(checkpoint).status()
                        != CheckpointDecision.Status.COMPLETED) {
                    throw new IllegalArgumentException(
                            "resident CDC source completion requires an authoritative completed checkpoint");
                }
                target.completeResidentSourceCheckpoint(
                        checkpoint, request.maximumEvents);
            }
            return StreamRemoteWorkerWire.Response.ack();
        }
        if (request.type
                == StreamRemoteWorkerWire.Type.RECOVER_CHECKPOINT) {
            if (command.kind()
                    != StreamControlMessage.Kind.CHECKPOINT_TRIGGER
                    || checkpointRoot == null) {
                throw new IllegalArgumentException(
                        "Stream Worker checkpoint recovery scope is invalid");
            }
            CheckpointIdentity checkpoint =
                    StreamCheckpointControlCodec.decode(command.payload());
            StreamProductExecutionGraph admitted =
                    StreamProductExecutionGraph.decode(request.artifactFrame);
            if (!admitted.commandDigest.equals(command.commandDigest())
                    || !admitted.artifact.artifactId().equals(
                    checkpoint.artifactId())
                    || !command.runId().equals(checkpoint.runId())
                    || !command.attemptId().equals(checkpoint.attemptId())
                    || command.fenceToken() != checkpoint.fenceToken()) {
                throw new IllegalArgumentException(
                        "Stream Worker checkpoint recovery differs from its decision");
            }
            StreamExecutionGraph.Node node = admitted.executionGraph.node(
                    request.operatorUid);
            if (node.portableNode().opcode() != StreamOperatorCode.SINK
                    || ((StreamSinkBinding) node.portableNode().binding())
                    .visibilityProfile()
                    != StreamSinkBinding.VisibilityProfile
                    .CHECKPOINT_TRANSACTIONAL) {
                throw new IllegalArgumentException(
                        "Stream Worker checkpoint recovery target is invalid");
            }
            io.rowweave.stream.runtime.StreamCheckpointAuthority authority =
                    checkpointAuthority(request.checkpointBundleFrame,
                            checkpoint);
            CheckpointDecision decision = authority.decision(checkpoint);
            if (decision == null || decision.status()
                    != CheckpointDecision.Status.COMMIT_DECIDED) {
                throw new IllegalArgumentException(
                        "Stream Worker recovery requires a commit decision");
            }
            StreamCheckpointMember prepared = null;
            List<StreamCheckpointMember> recoveryMembers =
                    checkpointMembers(request.checkpointBundleFrame,
                            decision, authority, admitted,
                            request.operatorUid, request.subtaskIndex);
            for (StreamCheckpointMember member : recoveryMembers) {
                if (member.kind()
                        == StreamCheckpointMember.Kind.TRANSACTIONAL_SINK
                        && request.operatorUid.equals(member.operatorUid())
                        && request.subtaskIndex == member.subtaskIndex()) {
                    if (prepared != null) {
                        throw new IllegalArgumentException(
                                "Stream Worker recovery member is ambiguous");
                    }
                    prepared = member;
                }
            }
            if (prepared == null) {
                throw new IllegalArgumentException(
                        "Stream Worker recovery member is unavailable");
            }
            worker.acceptRecoveryControl(request.controlFrame);
            StreamSinkBinding binding =
                    (StreamSinkBinding) node.portableNode().binding();
            try (CheckpointTransactionalSink recovered =
                         CheckpointTransactionalSink.recover(
                                 request.operatorUid,
                                 request.subtaskIndex, checkpoint.runId(),
                                 checkpoint.attemptId(), checkpoint.fenceToken(),
                                 command.assignmentGeneration(), binding,
                                 worker.transactionalSinkProvider(binding),
                                 checkpoint, prepared)) {
                recovered.reconcile(decision, authority);
            }
            return StreamRemoteWorkerWire.Response.ack();
        }
        if (command.kind() != StreamControlMessage.Kind.START
                && command.kind() != StreamControlMessage.Kind.RESTORE) {
            throw new IllegalArgumentException(
                    "Stream Worker start RPC requires START or RESTORE");
        }
        StreamTaskLaunchPayload launch = StreamTaskLaunchPayload.decode(
                command.payload(), command.kind());
        StreamProductExecutionGraph admitted =
                StreamProductExecutionGraph.decode(request.artifactFrame,
                        launch.parallelismOverride(),
                        launch.nodeMinimumParallelism());
        if (!admitted.commandDigest.equals(command.commandDigest())) {
            throw new IllegalArgumentException(
                    "Stream Worker command digest differs from the Artifact");
        }
        if (request.maximumEvents != admitted.maximumEvents()) {
            throw new IllegalArgumentException(
                    "Stream Worker event bound differs from the Artifact");
        }
        List<StreamCheckpointMember> restoreMembers =
                restoreMembers(request, command, admitted);
        if (request.type == StreamRemoteWorkerWire.Type.PREPARE_OPERATOR_TASK
                || request.type
                == StreamRemoteWorkerWire.Type.START_OPERATOR_TASK) {
            return dispatchOperatorTask(request, command, admitted,
                    restoreMembers);
        }
        throw new IllegalArgumentException(
                "Stream Worker start RPC type is invalid");
    }

    /**
     * Captures the checkpoint target under the Worker control fence, then
     * waits for the task barrier without monopolizing that fence. A newer
     * coordinator must be able to take over while an old coordinator's
     * aligned cut is still waiting or timing out.
     */
    private StreamRemoteWorkerWire.Response dispatchCheckpoint(
            StreamRemoteWorkerWire.Request request) throws IOException {
        final CheckpointIdentity checkpoint;
        final OperatorTaskSlot operator;
        synchronized (this) {
            if (closed.get()) {
                return StreamRemoteWorkerWire.Response.failure(
                        StreamRemoteWorkerWire.ErrorCode.INTERNAL_FAILURE,
                        "Stream Worker is closed");
            }
            StreamControlMessage command = StreamControlMessageCodec.decode(
                    request.controlFrame);
            if (command.kind()
                    != StreamControlMessage.Kind.CHECKPOINT_TRIGGER) {
                throw new IllegalArgumentException(
                        "Stream Worker checkpoint RPC requires a trigger");
            }
            if (request.operatorUid == null) {
                throw new UnsupportedOperationException(
                        "Stream checkpoint RPC must target one operator task");
            }
            checkpoint = StreamCheckpointControlCodec.decode(
                    command.payload());
            operator = operatorTask(command, request.operatorUid,
                    request.subtaskIndex);
            if (operator == null
                    || !operator.matchesAttempt(command)
                    || !operator.admitted.artifact.artifactId().equals(
                    checkpoint.artifactId())
                    || !command.commandDigest().equals(
                    checkpoint.planDigest())
                    || !command.runId().equals(checkpoint.runId())
                    || !command.attemptId().equals(checkpoint.attemptId())
                    || command.fenceToken() != checkpoint.fenceToken()
                    || command.coordinatorTerm()
                    != checkpoint.coordinatorTerm()
                    || !command.coordinatorIncarnation().equals(
                    checkpoint.coordinatorIncarnation())) {
                throw new IllegalArgumentException(
                        "Stream Worker operator checkpoint scope differs");
            }
            worker.acceptParticipantControl(request.controlFrame);
        }
        return StreamRemoteWorkerWire.Response.checkpointMembers(
                operator.checkpoint(checkpoint));
    }

    private StreamSplitDiscovery discoverSource(EnumeratorKey key,
            StreamSourceBinding binding,
            StreamRemoteWorkerWire.Request request,
            StreamSourceDiscoveryDescriptor limits) {
        Future<StreamSplitDiscovery> action;
        try {
            action = sourceDiscoveryExecutor.submit(() -> {
                StreamSourceEnumerator enumerator = enumerators.get(key);
                if (enumerator == null) {
                    DynamicDurableStreamSourceProvider provider =
                            worker.dynamicSourceProvider(binding);
                    enumerator = provider.openEnumerator(
                            new StreamSourceEnumeratorOpenRequest(
                                    request.operatorUid, binding));
                    if (enumerator == null) {
                        throw new IllegalStateException(
                                "Stream source provider returned no enumerator");
                    }
                    enumerators.put(key, enumerator);
                    if (closed.get() || sourceDiscoveryPoisoned.get()) {
                        if (enumerators.remove(key, enumerator)) {
                            closeEnumerator(enumerator);
                        }
                        throw new IllegalStateException(
                                "Stream source discovery facility is unavailable");
                    }
                }
                return enumerator.discover(StreamDiscoveryCursor.of(
                                request.discoveryCursorFrame,
                                limits.maximumCursorBytes()),
                        request.discoveryLimit);
            });
        } catch (java.util.concurrent.RejectedExecutionException rejected) {
            throw new IllegalStateException(
                    "Stream source discovery facility is unavailable",
                    rejected);
        }
        try {
            return action.get(limits.discoveryTimeoutMillis(),
                    TimeUnit.MILLISECONDS);
        } catch (TimeoutException timeout) {
            sourceDiscoveryPoisoned.set(true);
            action.cancel(true);
            throw new IllegalStateException(
                    "Stream source discovery exceeded its timeout", timeout);
        } catch (InterruptedException interrupted) {
            action.cancel(true);
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Stream source discovery was interrupted", interrupted);
        } catch (ExecutionException failure) {
            Throwable cause = failure.getCause();
            if (cause instanceof RuntimeException) {
                throw new IllegalStateException(
                        "Stream source discovery failed", cause);
            }
            if (cause instanceof Error) throw (Error) cause;
            throw new IllegalStateException(
                    "Stream source discovery failed", cause);
        }
    }

    private boolean hasAttempt(StreamControlMessage command) {
        for (OperatorTaskSlot operator : operatorTasks.values()) {
            if (operator.matchesAttempt(command)) return true;
        }
        return false;
    }

    private StreamRemoteWorkerWire.Response dispatchOperatorTask(
            StreamRemoteWorkerWire.Request request,
            StreamControlMessage command,
            StreamProductExecutionGraph admitted,
            List<StreamCheckpointMember> restoreMembers) {
        StreamExecutionGraph.Node node = admitted.executionGraph.node(
                request.operatorUid);
        if (request.subtaskIndex < 0
                || request.subtaskIndex >= node.parallelism()) {
            throw new IllegalArgumentException(
                    "Stream operator subtask is outside node parallelism");
        }
        StreamTaskIdentity identity = new StreamTaskIdentity(
                request.operatorUid, request.subtaskIndex);
        OperatorTaskKey key = new OperatorTaskKey(command, identity);
        OperatorTaskSlot existing = operatorTasks.get(key);
        String requestIdentity = requestIdentity(request);
        if (request.type == StreamRemoteWorkerWire.Type.PREPARE_OPERATOR_TASK) {
            if (existing != null) {
                if (existing.matches(command, admitted.artifact.artifactId(),
                        request.maximumEvents, requestIdentity)) {
                    return existing.endpointsResponse();
                }
                if (existing.matchesTakeover(
                        command, admitted.artifact.artifactId(),
                        request.maximumEvents, requestIdentity)) {
                    worker.acceptParticipantControl(request.controlFrame);
                    existing.takeOver(command);
                    return existing.endpointsResponse();
                }
                throw new IllegalStateException(
                        "Stream Worker operator task identity is already prepared");
            }
            boolean establishedAttempt = hasOperatorAttempt(command);
            if (establishedAttempt) {
                for (OperatorTaskSlot operator : operatorTasks.values()) {
                    if (!operator.matchesAttempt(command)) continue;
                    if (!operator.admitted.artifact.artifactId().equals(
                            admitted.artifact.artifactId())
                            || operator.maximumEvents
                            != request.maximumEvents) {
                        throw new IllegalStateException(
                                "Stream Worker attempt graph is inconsistent");
                    }
                    // All task slots of one attempt retain one immutable
                    // graph and one physical-lane index, not one copy each.
                    admitted = operator.admitted;
                    break;
                }
            }
            worker.acceptParticipantControl(request.controlFrame);
            if (!establishedAttempt) {
                closeSupersededOperatorTasks(command);
            }
            StreamTaskPlacementRequest lease = taskResource(
                    admitted, identity);
            try {
                resourceLeases.reserve(key, lease);
            } catch (RuntimeException failure) {
                if (!establishedAttempt && !hasOperatorAttempt(command)) {
                    worker.abandonAttempt(command);
                }
                throw failure;
            }
            List<StreamSocketChannel.Listener> listeners = new ArrayList<>();
            List<StreamTaskLaneSpec> endpoints = new ArrayList<>();
            try {
                for (StreamTaskLaneSpec lane : request.inputLanes) {
                    StreamSocketChannel.Listener listener =
                            StreamSocketChannel.bind(security,
                            identity(command, lane.laneId()),
                            request.maximumEvents,
                            lane.maximumFrameBytes(),
                            lane.initialCreditBytes());
                    listeners.add(listener);
                    endpoints.add(StreamTaskLaneSpec.endpoint(
                            lane.laneId(), security.advertisedHost(),
                            listener.localPort(),
                            lane.maximumFrameBytes(),
                            lane.initialCreditBytes()));
                }
            } catch (Throwable failure) {
                for (StreamSocketChannel.Listener listener : listeners) {
                    listener.close();
                }
                if (failure instanceof RuntimeException) {
                    resourceLeases.release(key);
                    if (!establishedAttempt
                            && !hasOperatorAttempt(command)) {
                        worker.abandonAttempt(command);
                    }
                    throw (RuntimeException) failure;
                }
                if (failure instanceof Error) {
                    resourceLeases.release(key);
                    if (!establishedAttempt
                            && !hasOperatorAttempt(command)) {
                        worker.abandonAttempt(command);
                    }
                    throw (Error) failure;
                }
                resourceLeases.release(key);
                if (!establishedAttempt && !hasOperatorAttempt(command)) {
                    worker.abandonAttempt(command);
                }
                throw new IllegalStateException(
                        "generic operator task input lane bind failed",
                        failure);
            }
            try {
                OperatorTaskSlot slot = new OperatorTaskSlot(identity, command,
                        admitted, request, listeners, endpoints,
                        requestIdentity, restoreMembers);
                operatorTasks.put(key, slot);
            } catch (Throwable failure) {
                for (StreamSocketChannel.Listener listener : listeners) {
                    listener.close();
                }
                resourceLeases.release(key);
                if (!establishedAttempt && !hasOperatorAttempt(command)) {
                    worker.abandonAttempt(command);
                }
                if (failure instanceof RuntimeException) {
                    throw (RuntimeException) failure;
                }
                if (failure instanceof Error) throw (Error) failure;
                throw new IllegalStateException(
                        "generic operator task registration failed", failure);
            }
            return StreamRemoteWorkerWire.Response.taskEndpoints(endpoints);
        }
        if (existing == null || !existing.matches(command,
                admitted.artifact.artifactId(), request.maximumEvents,
                requestIdentity)) {
            throw new IllegalStateException(
                    "Stream operator task was not prepared by this attempt");
        }
        existing.start(request);
        return StreamRemoteWorkerWire.Response.ack();
    }

    private boolean hasOperatorAttempt(StreamControlMessage observed) {
        for (OperatorTaskSlot operator : operatorTasks.values()) {
            if (operator.matchesAttempt(observed)) return true;
        }
        return false;
    }

    private OperatorTaskSlot operatorTask(StreamControlMessage command,
            String operatorUid, int subtaskIndex) {
        return operatorTasks.get(new OperatorTaskKey(command,
                new StreamTaskIdentity(operatorUid, subtaskIndex)));
    }

    private StreamTaskPlacementRequest taskResource(
            StreamProductExecutionGraph admitted,
            StreamTaskIdentity identity) {
        for (StreamTaskPlacementRequest request
                : StreamNodeTaskPlacementPlanner.requests(
                admitted.executionGraph,
                worker.profile().controlProtocolVersion(),
                worker.profile().dataProtocolVersion(),
                admitted.executionCapabilities())) {
            if (request.identity().equals(identity)) return request;
        }
        throw new IllegalArgumentException(
                "Stream task resource contract is unavailable");
    }

    private FileCheckpointPayloadStore checkpointPayloadStore(
            StreamProductExecutionGraph admitted,
            StreamExecutionGraph.Node node) {
        long maximumCheckpointBytes =
                node.resourceRequest().checkpointBytes();
        return checkpointRoot == null || maximumCheckpointBytes <= 0L
                ? null : new FileCheckpointPayloadStore(checkpointRoot,
                maximumCheckpointBytes);
    }

    private static StreamChannelIdentity identity(
            StreamControlMessage command, String logicalChannelId) {
        return new StreamChannelIdentity(
                command.runId(), command.attemptId(), command.fenceToken(),
                command.assignmentGeneration(), logicalChannelId,
                commandPayloadEpoch(command));
    }

    private static java.util.function.Consumer<byte[]> recordValidator(
            StreamProductExecutionGraph admitted,
            StreamProductExecutionGraph.SinkRoute sink) {
        io.rowweave.stream.contract.PortableStreamPlan.ChangelogMode mode =
                StreamEdgePartitioning.outputChangelogMode(
                        admitted.executionGraph, sink.edge);
        if (mode == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.APPEND) {
            return ignored -> { };
        }
        if (mode == io.rowweave.stream.contract.PortableStreamPlan
                .ChangelogMode.RETRACT) {
            StreamChangeKeyValidator validator =
                    new StreamChangeKeyValidator(
                            StreamEdgePartitioning.outputSchema(
                                    admitted.executionGraph, sink.edge),
                            StreamEdgePartitioning.outputStableKeyFields(
                                    admitted.executionGraph, sink.edge),
                            sink.edge.maximumFrameBytes());
            return validator::validate;
        }
        StreamUpsertKeyValidator validator = new StreamUpsertKeyValidator(
                StreamEdgePartitioning.outputSchema(
                        admitted.executionGraph, sink.edge),
                StreamEdgePartitioning.outputStableKeyFields(
                        admitted.executionGraph, sink.edge),
                sink.edge.maximumFrameBytes());
        return validator::validate;
    }

    private static String requestIdentity(
            StreamRemoteWorkerWire.Request request) {
        StringBuilder identity = new StringBuilder(
                request.type == StreamRemoteWorkerWire.Type.PREPARE_OPERATOR_TASK
                        || request.type == StreamRemoteWorkerWire.Type.START_OPERATOR_TASK
                        ? "OPERATOR_TASK" : request.type.name());
        if (request.operatorUid != null) identity.append(':').append(
                request.operatorUid);
        if (request.dataPort != 0) identity.append(':').append(request.dataPort);
        for (Map.Entry<String, Integer> route : request.dataPorts.entrySet()) {
            identity.append(':').append(route.getKey()).append('=')
                    .append(route.getValue());
        }
        if (request.subtaskIndex >= 0) {
            identity.append('#').append(request.subtaskIndex);
            for (StreamTaskLaneSpec lane : request.inputLanes) {
                identity.append(":in=").append(lane.laneId());
            }
            for (StreamTaskLaneSpec lane : request.outputLanes) {
                identity.append(":out=").append(lane.laneId())
                        .append('@').append(lane.host()).append(':')
                        .append(lane.port());
            }
            if (request.sourceAssignmentFrame != null) {
                identity.append(":source-assignment=").append(
                        sha256Hex(request.sourceAssignmentFrame));
            }
        }
        return identity.toString();
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format(java.util.Locale.ROOT,
                        "%02x", current & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private void closeOperatorTasks(StreamControlMessage command) {
        Iterator<Map.Entry<OperatorTaskKey, OperatorTaskSlot>> iterator =
                operatorTasks.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<OperatorTaskKey, OperatorTaskSlot> entry =
                    iterator.next();
            if (!entry.getValue().matchesAttempt(command)) continue;
            entry.getValue().close();
            resourceLeases.release(entry.getKey());
            iterator.remove();
        }
    }

    private void closeSupersededOperatorTasks(StreamControlMessage command) {
        Iterator<Map.Entry<OperatorTaskKey, OperatorTaskSlot>> iterator =
                operatorTasks.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<OperatorTaskKey, OperatorTaskSlot> entry =
                    iterator.next();
            if (!entry.getKey().runId.equals(command.runId())
                    || entry.getValue().matchesAttempt(command)) continue;
            entry.getValue().close();
            resourceLeases.release(entry.getKey());
            iterator.remove();
        }
    }

    private void closeOperatorTasks() {
        for (Map.Entry<OperatorTaskKey, OperatorTaskSlot> entry
                : new ArrayList<>(operatorTasks.entrySet())) {
            entry.getValue().close();
            resourceLeases.release(entry.getKey());
        }
        operatorTasks.clear();
    }

    private static long commandPayloadEpoch(StreamControlMessage command) {
        return StreamTaskLaunchPayload.decode(
                command.payload(), command.kind()).channelEpoch();
    }

    private static String attemptStorageId(StreamControlMessage command) {
        String value = command.runId() + '\0' + command.attemptId() + '\0'
                + command.fenceToken() + '\0'
                + command.assignmentGeneration();
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format(java.util.Locale.ROOT,
                        "%02x", current & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private List<StreamCheckpointMember> restoreMembers(
            StreamRemoteWorkerWire.Request request,
            StreamControlMessage command,
            StreamProductExecutionGraph admitted) {
        if (command.kind() != StreamControlMessage.Kind.RESTORE) {
            return Collections.emptyList();
        }
        StreamTaskLaunchPayload launch = StreamTaskLaunchPayload.decode(
                command.payload(), command.kind());
        if (checkpointRoot == null || !launch.restoring()) {
            throw new IllegalArgumentException(
                    "Stream Worker restore storage is unavailable");
        }
        byte[] checkpointFrame = launch.checkpointFrame();
        CheckpointIdentity checkpoint =
                StreamCheckpointControlCodec.decode(checkpointFrame);
        if (!admitted.artifact.artifactId().equals(checkpoint.artifactId())) {
            throw new IllegalArgumentException(
                    "Stream restore Artifact differs from its checkpoint");
        }
        io.rowweave.stream.runtime.StreamCheckpointAuthority authority =
                checkpointAuthority(request.checkpointBundleFrame,
                        checkpoint);
        io.rowweave.stream.contract.CheckpointDecision decision =
                authority.decision(checkpoint);
        return checkpointMembers(request.checkpointBundleFrame,
                decision, authority, admitted,
                request.operatorUid, request.subtaskIndex,
                request.sourceAssignmentFrame);
    }

    private io.rowweave.stream.runtime.StreamCheckpointAuthority
    checkpointAuthority(byte[] bundleFrame,
            CheckpointIdentity checkpoint) {
        if (bundleFrame == null) {
            return new FileCheckpointAuthority(
                    checkpointRoot.resolve("authority"));
        }
        StreamCheckpointTransferBundleCodec.Bundle bundle =
                StreamCheckpointTransferBundleCodec.decode(bundleFrame);
        if (!checkpoint.equals(bundle.decision().checkpointIdentity())) {
            throw new IllegalArgumentException(
                    "checkpoint transfer proof belongs to another cut");
        }
        return bundle.authority();
    }

    private List<StreamCheckpointMember> checkpointMembers(
            byte[] bundleFrame, CheckpointDecision decision,
            io.rowweave.stream.runtime.StreamCheckpointAuthority authority,
            StreamProductExecutionGraph admitted,
            String operatorUid, int subtaskIndex) {
        return checkpointMembers(bundleFrame, decision, authority, admitted,
                operatorUid, subtaskIndex, null);
    }

    private List<StreamCheckpointMember> checkpointMembers(
            byte[] bundleFrame, CheckpointDecision decision,
            io.rowweave.stream.runtime.StreamCheckpointAuthority authority,
            StreamProductExecutionGraph admitted,
            String operatorUid, int subtaskIndex,
            byte[] sourceAssignmentFrame) {
        if (bundleFrame == null) {
            return new FileCheckpointMemberStore(checkpointRoot).resolve(
                    decision, authority);
        }
        StreamCheckpointTransferBundleCodec.Bundle bundle =
                StreamCheckpointTransferBundleCodec.decode(bundleFrame);
        validateBundleMembers(bundle, admitted,
                operatorUid, subtaskIndex, sourceAssignmentFrame);
        return bundle.members();
    }

    static void validateBundleMembers(
            StreamCheckpointTransferBundleCodec.Bundle bundle,
            StreamProductExecutionGraph admitted,
            String operatorUid, int subtaskIndex) {
        validateBundleMembers(bundle, admitted, operatorUid,
                subtaskIndex, null);
    }

    static void validateBundleMembers(
            StreamCheckpointTransferBundleCodec.Bundle bundle,
            StreamProductExecutionGraph admitted,
            String operatorUid, int subtaskIndex,
            byte[] sourceAssignmentFrame) {
        if (admitted.executionGraph.node(operatorUid) == null
                || subtaskIndex < 0) {
            throw new IllegalArgumentException(
                    "checkpoint transfer target task is unknown");
        }
        String syntheticTaskId = operatorUid.length() + ":"
                + operatorUid + ':' + subtaskIndex;
        String coordinatorTaskId = "stream-source-coordinator:"
                + operatorUid;
        List<io.rowweave.stream.contract.CheckpointEvidence> all =
                new ArrayList<>();
        for (io.rowweave.stream.contract.CheckpointTaskReceipt receipt
                : bundle.manifest().taskReceipts()) {
            all.addAll(receipt.evidence());
        }
        boolean wholeGraphProof = all.size() == bundle.members().size();
        List<io.rowweave.stream.contract.CheckpointEvidence> provided =
                new ArrayList<>();
        StreamSourceTaskAssignmentCodec.Value sourceAssignment =
                sourceAssignmentFrame == null
                        || sourceAssignmentFrame.length == 0 ? null
                        : StreamSourceTaskAssignmentCodec.decode(
                                sourceAssignmentFrame);
        if (sourceAssignment != null
                && (!operatorUid.equals(sourceAssignment.assignment()
                .sourceOperatorUid()) || subtaskIndex
                != sourceAssignment.assignment().subtaskIndex())) {
            throw new IllegalArgumentException(
                    "checkpoint source assignment differs from target task");
        }
        for (StreamCheckpointMember member : bundle.members()) {
            boolean sourcePosition = member.kind()
                    == StreamCheckpointMember.Kind.SOURCE_POSITION;
            boolean reassignedSourcePosition = sourcePosition
                    && sourceAssignmentFrame != null
                    && sourceAssignment != null
                    && operatorUid.equals(member.operatorUid())
                    && sourceAssignment.assignment().splitId().equals(
                    member.subjectId())
                    && sourceAssignment.split().positionCodecId().equals(
                    member.codecId());
            if (sourcePosition && sourceAssignmentFrame != null
                    && !reassignedSourcePosition) {
                throw new IllegalArgumentException(
                        "checkpoint source position differs from assigned split");
            }
            if (!wholeGraphProof && !reassignedSourcePosition
                    && (!operatorUid.equals(member.operatorUid())
                    || subtaskIndex != member.subtaskIndex())) {
                throw new IllegalArgumentException(
                        "checkpoint transfer member belongs to another task");
            }
            provided.add(member.toEvidence());
        }
        List<io.rowweave.stream.contract.CheckpointEvidence> expected =
                new ArrayList<>();
        if (wholeGraphProof) {
            expected.addAll(all);
        } else {
            for (io.rowweave.stream.contract.CheckpointTaskReceipt receipt
                    : bundle.manifest().taskReceipts()) {
                boolean exactTask = receipt.taskId().equals(syntheticTaskId)
                        || subtaskIndex == 0
                        && receipt.taskId().equals(coordinatorTaskId);
                if (sourceAssignmentFrame == null) {
                    boolean selected = exactTask;
                    for (io.rowweave.stream.contract.CheckpointEvidence value
                            : receipt.evidence()) {
                        if (provided.contains(value)) selected = true;
                    }
                    if (selected) expected.addAll(receipt.evidence());
                    continue;
                }
                boolean selectedNonPosition = exactTask;
                for (io.rowweave.stream.contract.CheckpointEvidence value
                        : receipt.evidence()) {
                    if (value.kind() != io.rowweave.stream.contract
                            .CheckpointEvidence.Kind.SOURCE_POSITION
                            && provided.contains(value)) {
                        selectedNonPosition = true;
                    }
                }
                for (io.rowweave.stream.contract.CheckpointEvidence value
                        : receipt.evidence()) {
                    if (value.kind()
                            == io.rowweave.stream.contract
                            .CheckpointEvidence.Kind.SOURCE_POSITION) {
                        if (provided.contains(value)) expected.add(value);
                    } else if (selectedNonPosition) {
                        expected.add(value);
                    }
                }
            }
        }
        if (expected.size() != provided.size()) {
            throw new IllegalArgumentException(
                    "checkpoint transfer members do not cover the target task: "
                    + operatorUid + ':' + subtaskIndex + " expected="
                    + expected.size() + " provided=" + provided.size());
        }
        for (io.rowweave.stream.contract.CheckpointEvidence value
                : provided) {
            if (!expected.remove(value)) {
                throw new IllegalArgumentException(
                        "checkpoint transfer member is not authoritative");
            }
        }
        if (!expected.isEmpty()) {
            throw new IllegalArgumentException(
                    "checkpoint transfer manifest has missing members");
        }
    }

    private static StreamCheckpointMember exactRestoreMember(
            List<StreamCheckpointMember> members, String operatorUid,
            StreamCheckpointMember.Kind kind, int subtaskIndex,
            boolean required) {
        StreamCheckpointMember found = null;
        for (StreamCheckpointMember member : members) {
            if (member.kind() != kind
                    || !operatorUid.equals(member.operatorUid())
                    || member.subtaskIndex() != subtaskIndex) continue;
            if (found != null) {
                throw new IllegalArgumentException(
                        "Stream restore contains duplicate task state");
            }
            found = member;
        }
        if (required && found == null) {
            throw new IllegalArgumentException(
                    "Stream restore task state is missing");
        }
        return found;
    }

    private static StreamCheckpointMember sourceRestoreMember(
            List<StreamCheckpointMember> members, String operatorUid,
            StreamSourceSplit split, boolean required) {
        if (split == null) return null;
        StreamCheckpointMember found = null;
        for (StreamCheckpointMember member : members) {
            if (member.kind()
                    != StreamCheckpointMember.Kind.SOURCE_POSITION
                    || !operatorUid.equals(member.operatorUid())
                    || !split.splitId().equals(member.subjectId())) continue;
            if (!split.positionCodecId().equals(member.codecId())
                    || found != null) {
                throw new IllegalArgumentException(
                        "Stream source restore split position is invalid");
            }
            found = member;
        }
        if (required && found == null) {
            throw new IllegalArgumentException(
                    "Stream restore split position is missing");
        }
        return found;
    }

    static StreamRemoteWorkerWire.Response failure(Throwable failure) {
        StreamRemoteWorkerWire.ErrorCode code =
                StreamRemoteWorkerWire.ErrorCode.INTERNAL_FAILURE;
        if (failure instanceof StreamClusterProtocolException) {
            StreamClusterProtocolException.Code cluster =
                    ((StreamClusterProtocolException) failure).code();
            code = cluster == StreamClusterProtocolException.Code.STALE_ATTEMPT
                    || cluster == StreamClusterProtocolException.Code.STALE_COORDINATOR
                    || cluster == StreamClusterProtocolException.Code.COMMAND_REPLAY
                    || cluster == StreamClusterProtocolException.Code
                    .WORKER_INCARNATION_MISMATCH
                    ? StreamRemoteWorkerWire.ErrorCode.STALE_FENCE
                    : StreamRemoteWorkerWire.ErrorCode.ADMISSION_REJECTED;
        } else if (failure instanceof IllegalArgumentException
                || failure instanceof UnsupportedOperationException) {
            code = StreamRemoteWorkerWire.ErrorCode.ADMISSION_REJECTED;
        }
        return StreamRemoteWorkerWire.Response.failure(code,
                "Stream Worker rejected the fenced command: "
                        + code.name());
    }

    static String safeFailureLocations(Throwable failure) {
        StringBuilder result = new StringBuilder();
        Throwable current = failure;
        int causes = 0;
        while (current != null && causes++ < 8) {
            if (result.length() > 0) result.append(" <- ");
            result.append(current.getClass().getSimpleName());
            String message = current.getMessage();
            if (message != null
                    && message.matches("[A-Z][A-Z0-9_]{2,63}")) {
                result.append('[').append(message).append(']');
            }
            int selected = 0;
            for (StackTraceElement frame : current.getStackTrace()) {
                if (frame.getClassName().startsWith("io.rowweave.")) {
                    result.append(selected++ == 0 ? '@' : ',')
                            .append(frame.getClassName())
                            .append(':').append(frame.getLineNumber());
                    if (selected == 3) break;
                }
            }
            current = current.getCause();
        }
        return result.toString();
    }

    private static Map<String, String> sourceContracts(
            Map<String, DurableStreamSourceProvider> providers) {
        if (providers == null) throw new IllegalArgumentException("sources are required");
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, DurableStreamSourceProvider> entry
                : providers.entrySet()) {
            DurableStreamSourceProvider provider = entry.getValue();
            if (entry.getKey() == null || provider == null
                    || !entry.getKey().equals(provider.providerKey())) {
                throw new IllegalArgumentException(
                        "Stream source provider registry entry is invalid");
            }
            result.put(entry.getKey(), provider.contractDigest());
        }
        return result;
    }

    private static Map<String, String> sinkContracts(
            Map<String, EagerStreamSinkProvider> providers,
            Map<String, TransactionalStreamSinkProvider>
                    transactionalProviders) {
        if (providers == null || transactionalProviders == null) {
            throw new IllegalArgumentException("Sinks are required");
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (Map.Entry<String, EagerStreamSinkProvider> entry
                : providers.entrySet()) {
            EagerStreamSinkProvider provider = entry.getValue();
            if (entry.getKey() == null || provider == null
                    || !entry.getKey().equals(provider.providerKey())) {
                throw new IllegalArgumentException(
                        "Stream Sink provider registry entry is invalid");
            }
            result.put(entry.getKey(), provider.contractDigest());
        }
        for (Map.Entry<String, TransactionalStreamSinkProvider> entry
                : transactionalProviders.entrySet()) {
            TransactionalStreamSinkProvider provider = entry.getValue();
            if (entry.getKey() == null || provider == null
                    || !entry.getKey().equals(provider.providerKey())
                    || result.put(entry.getKey(), provider.contractDigest())
                    != null) {
                throw new IllegalArgumentException(
                        "transactional Stream Sink registry entry is invalid");
            }
        }
        return result;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        if (inProcessControlExecutor != null) {
            inProcessControlExecutor.shutdownNow();
        }
        if (socketControlExecutor != null) {
            socketControlExecutor.shutdownNow();
        }
        sourceDiscoveryExecutor.shutdownNow();
        if (server != null) {
            try {
                server.close();
            } catch (IOException ignored) {
                // Idempotent close.
            }
        }
        synchronized (this) {
            closeOperatorTasks();
            closeEnumerators();
        }
        if (acceptThread != null) {
            try {
                acceptThread.join(2_000L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void closeEnumerators(StreamControlMessage command) {
        for (Map.Entry<EnumeratorKey, StreamSourceEnumerator> entry
                : enumerators.entrySet()) {
            if (!entry.getKey().sameAttempt(command)) continue;
            if (enumerators.remove(entry.getKey(), entry.getValue())) {
                closeEnumerator(entry.getValue());
                discoveryReplays.remove(entry.getKey());
            }
        }
    }

    private void closeEnumerators() {
        for (Map.Entry<EnumeratorKey, StreamSourceEnumerator> entry
                : enumerators.entrySet()) {
            if (enumerators.remove(entry.getKey(), entry.getValue())) {
                closeEnumerator(entry.getValue());
            }
        }
        discoveryReplays.clear();
    }

    private static void closeEnumerator(StreamSourceEnumerator enumerator) {
        try {
            enumerator.close();
        } catch (Exception ignored) {
            // Closing the Worker is terminal for the connector instance.
        }
    }

    private static final class EnumeratorKey {
        private final String runId;
        private final String attemptId;
        private final long fenceToken;
        private final long assignmentGeneration;
        private final String operatorUid;

        private EnumeratorKey(StreamControlMessage command,
                String operatorUid) {
            this.runId = command.runId();
            this.attemptId = command.attemptId();
            this.fenceToken = command.fenceToken();
            this.assignmentGeneration = command.assignmentGeneration();
            this.operatorUid = operatorUid;
        }

        private boolean sameAttempt(StreamControlMessage command) {
            return runId.equals(command.runId())
                    && attemptId.equals(command.attemptId())
                    && fenceToken == command.fenceToken()
                    && assignmentGeneration
                    == command.assignmentGeneration();
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof EnumeratorKey)) return false;
            EnumeratorKey that = (EnumeratorKey) other;
            return fenceToken == that.fenceToken
                    && assignmentGeneration == that.assignmentGeneration
                    && runId.equals(that.runId)
                    && attemptId.equals(that.attemptId)
                    && operatorUid.equals(that.operatorUid);
        }

        @Override public int hashCode() {
            return Objects.hash(runId, attemptId, fenceToken,
                    assignmentGeneration, operatorUid);
        }
    }

    private static final class DiscoveryReplay {
        private final StreamControlMessage command;
        private final byte[] cursor;
        private final int limit;
        private final byte[] responseFrame;

        private DiscoveryReplay(StreamControlMessage command,
                byte[] cursor, int limit, byte[] responseFrame) {
            this.command = command;
            this.cursor = cursor.clone();
            this.limit = limit;
            this.responseFrame = responseFrame.clone();
        }

        private void requireSameRequest(
                StreamRemoteWorkerWire.Request request) {
            if (limit != request.discoveryLimit
                    || !java.util.Arrays.equals(
                    cursor, request.discoveryCursorFrame)) {
                throw new IllegalArgumentException(
                        "replayed source discovery command changed its request");
            }
        }
    }

    private static final class OperatorTaskKey {
        final String runId;
        final String attemptId;
        final long fenceToken;
        final long assignmentGeneration;
        final StreamTaskIdentity task;

        private OperatorTaskKey(
                StreamControlMessage command, StreamTaskIdentity task) {
            this.runId = command.runId();
            this.attemptId = command.attemptId();
            this.fenceToken = command.fenceToken();
            this.assignmentGeneration = command.assignmentGeneration();
            this.task = task;
        }

        @Override public boolean equals(Object other) {
            if (!(other instanceof OperatorTaskKey)) return false;
            OperatorTaskKey that = (OperatorTaskKey) other;
            return fenceToken == that.fenceToken
                    && assignmentGeneration == that.assignmentGeneration
                    && runId.equals(that.runId)
                    && attemptId.equals(that.attemptId)
                    && task.equals(that.task);
        }

        @Override public int hashCode() {
            return Objects.hash(runId, attemptId, fenceToken,
                    assignmentGeneration, task);
        }
    }

    private static final class WorkerResourceLeases {
        private final StreamWorkerCapabilityProfile profile;
        private final Map<OperatorTaskKey, StreamTaskPlacementRequest> leases =
                new HashMap<>();
        private int tasks;
        private int channels;
        private int fileDescriptors;
        private long residentBytes;
        private int threads;
        private int timers;
        private int windows;
        private long checkpointBytes;
        private long spillBytes;

        private WorkerResourceLeases(
                StreamWorkerCapabilityProfile profile) {
            this.profile = profile;
        }

        void reserve(OperatorTaskKey key,
                StreamTaskPlacementRequest request) {
            if (leases.containsKey(key)) {
                throw new IllegalStateException(
                        "Stream task resource lease is duplicated");
            }
            long nextTasks = add(tasks, 1L);
            long nextChannels = add(channels, request.channels());
            long nextFileDescriptors = add(
                    fileDescriptors, request.fileDescriptorCount());
            long nextResident = add(
                    residentBytes, request.residentBytes());
            long nextThreads = add(threads, request.threads());
            long nextTimers = add(timers, request.timerCount());
            long nextWindows = add(windows, request.windowCount());
            long nextCheckpoint = add(
                    checkpointBytes, request.checkpointBytes());
            long nextSpill = add(spillBytes, request.spillDiskBytes());
            if (profile.controlProtocolVersion()
                    != request.controlProtocolVersion()
                    || profile.dataProtocolVersion()
                    != request.dataProtocolVersion()
                    || profile.maximumFrameBytes()
                    < request.maximumFrameBytes()
                    || !profile.capabilities().containsAll(
                    request.requiredCapabilities())
                    || nextTasks > profile.taskSlots()
                    || nextChannels > profile.channelCapacity()
                    || nextFileDescriptors
                    > profile.maximumFileDescriptorCount()
                    || nextResident > profile.maximumResidentBytes()
                    || nextThreads > profile.maximumThreads()
                    || nextTimers > profile.maximumTimerCount()
                    || nextWindows > profile.maximumWindowCount()
                    || nextCheckpoint > profile.maximumCheckpointBytes()
                    || nextSpill > profile.maximumSpillDiskBytes()) {
                throw new StreamClusterProtocolException(
                        StreamClusterProtocolException.Code
                                .INSUFFICIENT_CAPACITY,
                        "Stream Worker task resource lease exceeds capacity");
            }
            tasks = (int) nextTasks;
            channels = (int) nextChannels;
            fileDescriptors = (int) nextFileDescriptors;
            residentBytes = nextResident;
            threads = (int) nextThreads;
            timers = (int) nextTimers;
            windows = (int) nextWindows;
            checkpointBytes = nextCheckpoint;
            spillBytes = nextSpill;
            leases.put(key, request);
        }

        void release(OperatorTaskKey key) {
            StreamTaskPlacementRequest request = leases.remove(key);
            if (request == null) return;
            tasks = Math.subtractExact(tasks, 1);
            channels = Math.subtractExact(channels, request.channels());
            fileDescriptors = Math.subtractExact(
                    fileDescriptors, request.fileDescriptorCount());
            residentBytes = Math.subtractExact(
                    residentBytes, request.residentBytes());
            threads = Math.subtractExact(threads, request.threads());
            timers = Math.subtractExact(timers, request.timerCount());
            windows = Math.subtractExact(windows, request.windowCount());
            checkpointBytes = Math.subtractExact(
                    checkpointBytes, request.checkpointBytes());
            spillBytes = Math.subtractExact(
                    spillBytes, request.spillDiskBytes());
        }

        private static long add(long left, long right) {
            try {
                return Math.addExact(left, right);
            } catch (ArithmeticException overflow) {
                return Long.MAX_VALUE;
            }
        }
    }

    private final class OperatorTaskSlot implements AutoCloseable {
        private final StreamTaskIdentity identity;
        private volatile StreamControlMessage command;
        private final StreamProductExecutionGraph admitted;
        private final int maximumEvents;
        private final byte[] sourceAssignmentFrame;
        private final List<StreamSocketChannel.Listener> listeners;
        private final List<StreamTaskLaneSpec> endpoints;
        private final String requestIdentity;
        private final List<StreamCheckpointMember> restoreMembers;
        private final AtomicBoolean started = new AtomicBoolean();
        private final List<StreamSocketChannel.Receiver> receivers =
                new ArrayList<>();
        private final List<StreamSocketChannel.Sender> senders =
                new ArrayList<>();
        private volatile StreamTaskProgressTracker progressTracker;
        private final java.util.concurrent.CompletableFuture<Void>
                checkpointReady = new java.util.concurrent.CompletableFuture<>();
        private volatile StreamOperatorTask task;
        private volatile StreamUnalignedTaskGate unalignedGate;
        private volatile Thread bootstrap;
        private volatile Throwable failure;
        private final AtomicBoolean failureReported = new AtomicBoolean();
        private volatile CheckpointIdentity completedTransactionalCheckpoint;
        private volatile boolean terminalInputCommitted;
        private volatile io.rowweave.stream.contract.StreamSourceAssignment
                lastSourceAssignment;
        private StreamControlMessage lastSourceAssignmentCommand;
        private byte[] lastSourceAssignmentFrame;
        private byte[] lastFinishedSourceAssignmentFrame;
        private StreamControlMessage lastSourceRevocationCommand;
        private byte[] lastSourceRevocationFrame;
        private java.util.concurrent.CompletableFuture<Void>
                lastSourceRevocationCompletion;
        private boolean initialSourceAssignmentRevoked;
        private StreamSourceSplit pendingSourceSplit;

        private OperatorTaskSlot(StreamTaskIdentity identity,
                StreamControlMessage command,
                StreamProductExecutionGraph admitted,
                StreamRemoteWorkerWire.Request prepared,
                List<StreamSocketChannel.Listener> listeners,
                List<StreamTaskLaneSpec> endpoints,
                String requestIdentity,
                List<StreamCheckpointMember> restoreMembers) {
            this.identity = identity;
            this.command = command;
            this.admitted = admitted;
            this.maximumEvents = prepared.maximumEvents;
            this.sourceAssignmentFrame = prepared.sourceAssignmentFrame;
            this.listeners = listeners;
            this.endpoints = endpoints;
            this.requestIdentity = requestIdentity;
            this.restoreMembers = restoreMembers == null
                    ? Collections.emptyList()
                    : Collections.unmodifiableList(
                    new ArrayList<>(restoreMembers));
        }

        boolean matches(StreamControlMessage command, String artifactId,
                int maximumEvents, String requestIdentity) {
            return this.command.equals(command)
                    && admitted.artifact.artifactId().equals(artifactId)
                    && this.maximumEvents == maximumEvents
                    && this.requestIdentity.equals(requestIdentity);
        }

        boolean matchesTakeover(StreamControlMessage replacement,
                String artifactId, int maximumEvents,
                String requestIdentity) {
            StreamControlMessage current = command;
            return replacement.coordinatorTerm() > current.coordinatorTerm()
                    && matchesAttempt(replacement)
                    && admitted.artifact.artifactId().equals(artifactId)
                    && this.maximumEvents == maximumEvents
                    && this.requestIdentity.equals(requestIdentity);
        }

        void takeOver(StreamControlMessage replacement) {
            if (!matchesAttempt(replacement)
                    || replacement.coordinatorTerm()
                    <= command.coordinatorTerm()) {
                throw new IllegalStateException(
                        "Stream operator task takeover fence is stale");
            }
            command = replacement;
            StreamTaskProgressTracker tracker = progressTracker;
            if (tracker != null) tracker.takeOver(replacement);
        }

        boolean matchesAttempt(StreamControlMessage observed) {
            return command.runId().equals(observed.runId())
                    && command.attemptId().equals(observed.attemptId())
                    && command.fenceToken() == observed.fenceToken()
                    && command.assignmentGeneration()
                    == observed.assignmentGeneration()
                    && command.workerIncarnation().equals(
                    observed.workerIncarnation())
                    && command.commandDigest().equals(
                    observed.commandDigest());
        }

        List<StreamCheckpointMember> checkpoint(
                CheckpointIdentity checkpoint) {
            long timeoutMillis = admitted.job.checkpointConfig()
                    .timeoutMillis();
            try {
                checkpointReady.get(timeoutMillis,
                        TimeUnit.MILLISECONDS);
            } catch (TimeoutException timeout) {
                throw new IllegalStateException(
                        "Stream operator task did not become checkpoint-ready before its timeout",
                        timeout);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Stream operator checkpoint readiness was interrupted",
                        interrupted);
            } catch (ExecutionException failure) {
                Throwable cause = failure.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                if (cause instanceof Error) throw (Error) cause;
                throw new IllegalStateException(
                        "Stream operator task failed before checkpoint readiness",
                        cause);
            }
            // Transactional sinks replace their completed segment after every
            // committed checkpoint. The future gates only initial bootstrap;
            // the current task must be read after that gate on every cut.
            StreamOperatorTask current = task;
            if (current == null) {
                throw new IllegalStateException(
                        "Stream operator task is unavailable for checkpoints");
            }
            java.util.concurrent.CompletableFuture<
                    List<StreamCheckpointMember>> cut =
                    current.checkpoint(checkpoint);
            try {
                // The coordinator RPC owns the absolute checkpoint deadline.
                // Once bootstrap is ready, preserve the task's full configured
                // barrier budget instead of shaving time spent crossing the
                // asynchronous readiness hand-off.
                return cut.get(timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException timeout) {
                cut.cancel(true);
                throw new IllegalStateException(
                        "Stream operator checkpoint exceeded its configured timeout",
                        timeout);
            } catch (InterruptedException interrupted) {
                cut.cancel(true);
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Stream operator checkpoint was interrupted", interrupted);
            } catch (ExecutionException failure) {
                Throwable cause = failure.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                if (cause instanceof Error) throw (Error) cause;
                throw new IllegalStateException(
                        "Stream operator checkpoint failed", cause);
            }
        }

        Throwable failure() {
            Throwable current = task == null ? null : task.failure();
            Throwable observed = failure != null ? failure : current;
            if (observed != null
                    && failureReported.compareAndSet(false, true)) {
                LOG.warn("Stream operator task {}#{} failed at {}",
                        identity.operatorUid(), identity.subtaskIndex(),
                        safeFailureLocations(observed));
            }
            return observed;
        }

        boolean completed() {
            StreamOperatorTask current = task;
            return (terminalInputCommitted
                    || current != null && current.completed())
                    && failure() == null;
        }

        synchronized void completeTransactionalCheckpoint(
                CheckpointIdentity checkpoint, int timeoutMillis,
                io.rowweave.stream.runtime.StreamCheckpointAuthority
                        authority) {
            if (checkpoint.equals(completedTransactionalCheckpoint)) return;
            StreamOperatorTask segment = task;
            if (segment == null
                    || !checkpoint.equals(segment.preparedCheckpoint())) {
                throw new IllegalStateException(
                        "transactional Sink checkpoint is not prepared");
            }
            try {
                segment.awaitCompletion(timeoutMillis);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "interrupted while reconciling transactional Sink",
                        interrupted);
            }
            if (!segment.completed() || segment.failure() != null) {
                throw new IllegalStateException(
                        "transactional Sink segment did not complete cleanly");
            }
            segment.reconcile(authority.decision(checkpoint), authority);
            completedTransactionalCheckpoint = checkpoint;
            terminalInputCommitted |= segment.terminalInputSeen();
            StreamExecutionGraph.Node node = admitted.executionGraph.node(
                    identity.operatorUid());
            StreamSinkBinding binding = (StreamSinkBinding)
                    node.portableNode().binding();
            StreamProductExecutionGraph.SinkRoute route = admitted.sink(
                    identity.operatorUid());
            StreamOperatorTask replacement =
                    StreamOperatorTaskFactory.createEndpoint(
                    node.portableNode(), worker.transactionalSinkTask(
                    identity.operatorUid(), identity.subtaskIndex(), binding,
                    progressTracker.observeInput(sinkInput(taskInputs())),
                    ignored -> { }, recordValidator(admitted, route),
                    segment.terminalInputSeen()));
            task = wrapUnaligned(replacement);
            task.start();
        }

        synchronized void completeResidentSourceCheckpoint(
                CheckpointIdentity checkpoint, int timeoutMillis) {
            StreamOperatorTask current = task;
            if (current == null) {
                throw new IllegalStateException(
                        "resident CDC source task is unavailable");
            }
            try {
                current.checkpointCompleted(checkpoint).get(
                        timeoutMillis, TimeUnit.MILLISECONDS);
            } catch (TimeoutException timeout) {
                throw new IllegalStateException(
                        "resident CDC source completion exceeded its timeout",
                        timeout);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "resident CDC source completion was interrupted",
                        interrupted);
            } catch (ExecutionException failure) {
                Throwable cause = failure.getCause();
                if (cause instanceof RuntimeException) {
                    throw (RuntimeException) cause;
                }
                if (cause instanceof Error) throw (Error) cause;
                throw new IllegalStateException(
                        "resident CDC source completion failed", cause);
            }
        }

        StreamRemoteWorkerWire.Response endpointsResponse() {
            return StreamRemoteWorkerWire.Response.taskEndpoints(endpoints);
        }

        synchronized void start(StreamRemoteWorkerWire.Request request) {
            if (!started.compareAndSet(false, true)) return;
            StreamOperatorCode opcode = admitted.executionGraph.node(
                    identity.operatorUid()).portableNode().opcode();
            boolean source = opcode == StreamOperatorCode.SOURCE;
            boolean sink = opcode == StreamOperatorCode.SINK;
            if ((!sink && request.outputLanes.isEmpty())
                    || (sink && !request.outputLanes.isEmpty())
                    || request.inputLanes.size() != listeners.size()
                    || (source && !request.inputLanes.isEmpty())) {
                throw new IllegalArgumentException(
                        "generic operator task lane cardinality is invalid");
            }
            bootstrap = new Thread(() -> {
                try {
                    for (StreamSocketChannel.Listener listener : listeners) {
                        receivers.add(listener.accept());
                    }
                    if (!sink) {
                        for (StreamTaskLaneSpec output : request.outputLanes) {
                            senders.add(StreamSocketChannel.connect(
                                    security, output.host(), output.port(),
                                    identity(command, output.laneId()),
                                    request.maximumEvents,
                                    output.maximumFrameBytes(),
                                    output.initialCreditBytes()));
                        }
                    }
                    StreamExecutionGraph.Node node = admitted.executionGraph
                            .node(identity.operatorUid());
                    List<? extends StreamChannelInput> taskInputs = receivers;
                    List<StreamCheckpointMember> operatorRestoreMembers =
                            restoreMembers;
                    FileCheckpointPayloadStore payloadStore =
                            checkpointPayloadStore(admitted, node);
                    if (node.portableNode().opcode()
                            != StreamOperatorCode.SOURCE
                            && admitted.job.checkpointConfig().mode()
                            == StreamCheckpointMode.UNALIGNED) {
                        unalignedGate = unalignedGate(node, payloadStore);
                        taskInputs = unalignedGate.inputs();
                        operatorRestoreMembers = withoutChannelState(
                                restoreMembers);
                    }
                    if (node.portableNode().opcode()
                            == StreamOperatorCode.SOURCE) {
                      synchronized (this) {
                        StreamSourceBinding binding =
                                (StreamSourceBinding) node.portableNode()
                                        .binding();
                        if (initialSourceAssignmentRevoked
                                && !java.util.Arrays.equals(
                                request.sourceAssignmentFrame,
                                sourceAssignmentFrame)) {
                            throw new IllegalStateException(
                                    "revoked initial Source assignment changed before launch");
                        }
                        List<StreamSourceSplit> owned;
                        if (request.sourceAssignmentFrame != null) {
                            StreamSourceSplit assigned =
                                    initialSourceAssignmentRevoked
                                    || request.sourceAssignmentFrame.length == 0
                                    ? null : dynamicAssignedSplit(
                                    request, command, binding);
                            if (assigned != null) {
                                rememberSourceAssignment(request, command);
                            }
                            owned = assigned == null
                                    ? Collections.<StreamSourceSplit>emptyList()
                                    : Collections.singletonList(assigned);
                        } else if (!binding.dynamicDiscovery()) {
                            List<List<StreamSourceSplit>> splitOwners =
                                    StreamSourceSplitAssigner.assign(binding,
                                            node.parallelism());
                            owned = splitOwners.get(identity.subtaskIndex());
                        } else {
                            owned = Collections.emptyList();
                        }
                        if (owned.size() > 1) {
                            throw new IllegalArgumentException(
                                    "generic source subtask must own at most one split");
                        }
                        StreamSourceCheckpointRuntime checkpointRuntime =
                                payloadStore == null ? null
                                : new StreamSourceCheckpointRuntime(
                                admitted.artifact.artifactId(),
                                identity.subtaskIndex(),
                                checkpointRoot.resolve("working")
                                        .resolve(attemptStorageId(command))
                                        .resolve(admitted.artifact.artifactId()
                                                + '-' + identity.operatorUid()
                                                + '-' + identity.subtaskIndex()),
                                payloadStore, owned.isEmpty() ? null
                                : sourceRestoreMember(restoreMembers,
                                identity.operatorUid(), owned.get(0),
                                command.kind()
                                        == StreamControlMessage.Kind.RESTORE
                                        && !binding.dynamicDiscovery()));
                        StreamProductExecutionGraph.SourceRoute sourceRoute =
                                sourceRoute(node.portableNode().operatorUid());
                        StreamChannelOutput sourceOutput = outputRouter(node,
                                request.outputLanes, senders);
                        progressTracker = new StreamTaskProgressTracker(
                                command, identity, true, false,
                                Collections.singletonList(sourceOutput));
                        sourceOutput = progressTracker.observeOutput(
                                sourceOutput);
                        if (admitted.isResidentCdc(sourceRoute)
                                && !owned.isEmpty()) {
                            task = StreamOperatorTaskFactory.createEndpoint(
                                    node.portableNode(),
                                    worker.residentCdcSourceTask(
                                            node.portableNode().operatorUid(),
                                            binding, owned.get(0), sourceOutput,
                                            checkpointRuntime,
                                            node.resourceRequest().maximumFrameBytes(),
                                            node.portableNode().outputSchema(),
                                            node.portableNode()
                                                    .stableKeyFields()));
                        } else {
                            task = StreamOperatorTaskFactory.createEndpoint(
                                    node.portableNode(), worker.sourceTask(
                                            node.portableNode().operatorUid(),
                                            binding, owned.isEmpty()
                                                    ? null : owned.get(0),
                                            sourceOutput, checkpointRuntime,
                                            admitted.sourceIdleTimeoutMillis(
                                                    sourceRoute),
                                            binding.dynamicDiscovery()
                                                    || owned.isEmpty(),
                                            "EXPLICIT_END_OF_INPUT".equals(
                                                    node.portableNode()
                                                    .scalarParameters().get(
                                                            "source.termination"))));
                        }
                        deliverPendingDynamicSource();
                      }
                        task.start();
                    } else if (node.portableNode().opcode()
                            == StreamOperatorCode.SINK) {
                        StreamProductExecutionGraph.SinkRoute sinkRoute =
                                admitted.sink(node.portableNode().operatorUid());
                        StreamSinkBinding binding = (StreamSinkBinding)
                                node.portableNode().binding();
                        progressTracker = new StreamTaskProgressTracker(
                                command, identity, false, true,
                                Collections.emptyList());
                        StreamChannelInput observedSinkInput =
                                progressTracker.observeInput(
                                        sinkInput(taskInputs));
                        if (binding.visibilityProfile()
                                == StreamSinkBinding.VisibilityProfile
                                .CHECKPOINT_TRANSACTIONAL) {
                            StreamOperatorTask created =
                                    StreamOperatorTaskFactory.createEndpoint(
                                    node.portableNode(),
                                    worker.transactionalSinkTask(
                                    node.portableNode().operatorUid(),
                                    identity.subtaskIndex(), binding,
                                    observedSinkInput, ignored -> { },
                                    recordValidator(admitted, sinkRoute)));
                            task = wrapUnaligned(created);
                        } else {
                            StreamOperatorTask created =
                                    StreamOperatorTaskFactory.createEndpoint(
                                    node.portableNode(), worker.sinkTask(
                                    node.portableNode().operatorUid(),
                                    identity.subtaskIndex(), binding,
                                    observedSinkInput, ignored -> { },
                                    recordValidator(admitted, sinkRoute)));
                            task = wrapUnaligned(created);
                        }
                        task.start();
                    } else {
                    List<StreamTaskOutput> rawOutputs = taskOutputs(
                            node, request.outputLanes, senders);
                    progressTracker = new StreamTaskProgressTracker(
                            command, identity, false, false,
                            taskOutputChannels(rawOutputs));
                    StreamOperatorTask created =
                            StreamRemoteOperatorTaskRunner.start(
                            admitted.executionGraph,
                            admitted.artifact.artifactId(),
                            node.portableNode(), observedTaskInputs(taskInputs, progressTracker),
                            request.inputLanes,
                            observedTaskOutputs(rawOutputs, progressTracker),
                            functionResolver,
                            identity.subtaskIndex(), payloadStore,
                            operatorRestoreMembers, command.kind()
                                    == StreamControlMessage.Kind.RESTORE);
                    task = wrapUnaligned(created);
                    }
                    checkpointReady.complete(null);
                } catch (Throwable failure) {
                    this.failure = failure;
                    checkpointReady.completeExceptionally(failure);
                    close();
                }
            }, "rowweave-stream-generic-task-" + identity);
            bootstrap.setDaemon(true);
            bootstrap.start();
        }

        private StreamSourceSplit dynamicAssignedSplit(
                StreamRemoteWorkerWire.Request request,
                StreamControlMessage activeCommand,
                StreamSourceBinding binding) {
            if (request.sourceAssignmentFrame == null) {
                return null;
            }
            StreamSourceTaskAssignmentCodec.Value decoded =
                    StreamSourceTaskAssignmentCodec.decode(
                            request.sourceAssignmentFrame);
            io.rowweave.stream.contract.StreamSourceAssignment assignment =
                    decoded.assignment();
            if (!activeCommand.runId().equals(assignment.jobId())
                    || !activeCommand.attemptId().equals(
                    assignment.attemptId())
                    || !identity.operatorUid().equals(
                    assignment.sourceOperatorUid())
                    || identity.subtaskIndex()
                    != assignment.subtaskIndex()
                    || !activeCommand.workerIncarnation().equals(
                    assignment.workerIncarnation())
                    || activeCommand.assignmentGeneration()
                    != assignment.assignmentGeneration()) {
                throw new IllegalArgumentException(
                        "dynamic Source split assignment fence differs from its task");
            }
            if (!binding.dynamicDiscovery()
                    && !binding.fixedSplits().contains(decoded.split())) {
                throw new IllegalArgumentException(
                        "fixed Source assignment is not in its contract");
            }
            return decoded.split();
        }

        synchronized void assignDynamicSource(
                StreamRemoteWorkerWire.Request request,
                StreamControlMessage assignmentCommand,
                Runnable acceptControl) {
            StreamExecutionGraph.Node node = admitted.executionGraph.node(
                    identity.operatorUid());
            if (node.portableNode().opcode() != StreamOperatorCode.SOURCE) {
                throw new IllegalStateException(
                        "Source task is not ready for assignment");
            }
            StreamSourceTaskAssignmentCodec.Value decoded =
                    StreamSourceTaskAssignmentCodec.decode(
                            request.sourceAssignmentFrame);
            dynamicAssignedSplit(request, assignmentCommand,
                    (StreamSourceBinding) node.portableNode().binding());
            if (decoded.assignment().equals(lastSourceAssignment)) {
                if (!assignmentCommand.equals(lastSourceAssignmentCommand)
                        || !java.util.Arrays.equals(
                        request.sourceAssignmentFrame,
                        lastSourceAssignmentFrame)) {
                    throw new IllegalStateException(
                            "dynamic Source assignment replay drifted");
                }
                return;
            }
            if (lastSourceAssignment != null && task != null
                    && task.completedSourceSplit() == null) {
                throw new IllegalStateException(
                        "dynamic Source task still owns its previous split");
            }
            acceptControl.run();
            if (task == null) {
                if (pendingSourceSplit != null) {
                    throw new IllegalStateException(
                            "dynamic Source task already has a pending assignment");
                }
                pendingSourceSplit = decoded.split();
            } else {
                task.assignSourceSplit(decoded.split());
            }
            lastSourceAssignment = decoded.assignment();
            lastSourceAssignmentCommand = assignmentCommand;
            lastSourceAssignmentFrame =
                    request.sourceAssignmentFrame.clone();
            lastFinishedSourceAssignmentFrame = null;
        }

        synchronized void revokeDynamicSource(
                StreamRemoteWorkerWire.Request request,
                StreamControlMessage revocationCommand,
                Runnable acceptControl) {
            StreamExecutionGraph.Node node = admitted.executionGraph.node(
                    identity.operatorUid());
            if (node.portableNode().opcode() != StreamOperatorCode.SOURCE) {
                throw new IllegalStateException(
                        "Source task is not ready for revocation");
            }
            StreamSourceTaskAssignmentCodec.Value decoded =
                    StreamSourceTaskAssignmentCodec.decode(
                            request.sourceAssignmentFrame);
            dynamicAssignedSplit(request, revocationCommand,
                    (StreamSourceBinding) node.portableNode().binding());
            if (revocationCommand.equals(lastSourceRevocationCommand)) {
                if (!java.util.Arrays.equals(request.sourceAssignmentFrame,
                        lastSourceRevocationFrame)) {
                    throw new IllegalStateException(
                            "dynamic Source revocation replay drifted");
                }
                awaitSourceRevocation(lastSourceRevocationCompletion,
                        (StreamSourceBinding) node.portableNode()
                                .binding());
                finalizeSourceRevocation(request.sourceAssignmentFrame);
                return;
            }
            boolean currentAssignment =
                    decoded.assignment().equals(lastSourceAssignment)
                    && java.util.Arrays.equals(
                            request.sourceAssignmentFrame,
                            lastSourceAssignmentFrame);
            boolean preparedAssignment = task == null
                    && java.util.Arrays.equals(
                            request.sourceAssignmentFrame,
                            sourceAssignmentFrame);
            if (!currentAssignment && !preparedAssignment) {
                throw new IllegalStateException(
                        "dynamic Source revocation differs from its owner");
            }
            acceptControl.run();
            lastSourceRevocationCommand = revocationCommand;
            lastSourceRevocationFrame = request.sourceAssignmentFrame.clone();
            if (task == null) {
                if (pendingSourceSplit != null
                        && pendingSourceSplit.equals(decoded.split())) {
                    pendingSourceSplit = null;
                } else if (preparedAssignment) {
                    initialSourceAssignmentRevoked = true;
                } else {
                    throw new IllegalStateException(
                            "pending Source split differs from revocation");
                }
                lastSourceRevocationCompletion =
                        java.util.concurrent.CompletableFuture.completedFuture(
                                null);
            } else {
                lastSourceRevocationCompletion = task.revokeSourceSplit(
                        decoded.split().splitId());
            }
            awaitSourceRevocation(lastSourceRevocationCompletion,
                    (StreamSourceBinding) node.portableNode().binding());
            finalizeSourceRevocation(request.sourceAssignmentFrame);
        }

        private void finalizeSourceRevocation(byte[] assignmentFrame) {
            if (java.util.Arrays.equals(lastSourceAssignmentFrame,
                    assignmentFrame)) {
                lastSourceAssignment = null;
                lastSourceAssignmentCommand = null;
                lastSourceAssignmentFrame = null;
            }
            if (java.util.Arrays.equals(lastFinishedSourceAssignmentFrame,
                    assignmentFrame)) {
                lastFinishedSourceAssignmentFrame = null;
            }
        }

        private void awaitSourceRevocation(
                java.util.concurrent.CompletableFuture<Void> completion,
                StreamSourceBinding binding) {
            if (completion == null) {
                throw new IllegalStateException(
                        "dynamic Source revocation has no completion proof");
            }
            try {
                completion.get(binding.discoveryDescriptor()
                                .discoveryTimeoutMillis(),
                        java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "dynamic Source revocation interrupted", interrupted);
            } catch (java.util.concurrent.ExecutionException
                    | java.util.concurrent.TimeoutException failure) {
                throw new IllegalStateException(
                        "dynamic Source reader did not stop", failure);
            }
        }

        private void rememberSourceAssignment(
                StreamRemoteWorkerWire.Request request,
                StreamControlMessage assignmentCommand) {
            StreamSourceTaskAssignmentCodec.Value decoded =
                    StreamSourceTaskAssignmentCodec.decode(
                            request.sourceAssignmentFrame);
            lastSourceAssignment = decoded.assignment();
            lastSourceAssignmentCommand = assignmentCommand;
            lastSourceAssignmentFrame =
                    request.sourceAssignmentFrame.clone();
        }

        synchronized byte[] sourceSplitProgress() {
            if (task == null) {
                return lastFinishedSourceAssignmentFrame == null
                        ? null : lastFinishedSourceAssignmentFrame.clone();
            }
            StreamSourceSplit completed = task.completedSourceSplit();
            if (completed != null) {
                if (lastSourceAssignment == null
                        || !completed.splitId().equals(
                        lastSourceAssignment.splitId())) {
                    throw new IllegalStateException(
                            "completed dynamic split differs from its assignment");
                }
                lastFinishedSourceAssignmentFrame =
                        lastSourceAssignmentFrame.clone();
            }
            return lastFinishedSourceAssignmentFrame == null
                    ? null : lastFinishedSourceAssignmentFrame.clone();
        }

        private synchronized void deliverPendingDynamicSource() {
            if (pendingSourceSplit == null) return;
            task.assignSourceSplit(pendingSourceSplit);
            pendingSourceSplit = null;
        }

        private StreamProductExecutionGraph.SourceRoute sourceRoute(
                String operatorUid) {
            for (StreamProductExecutionGraph.SourceRoute source
                    : admitted.sources) {
                if (source.node.operatorUid().equals(operatorUid)) {
                    return source;
                }
            }
            throw new IllegalArgumentException(
                    "generic source task is not in the admitted Artifact");
        }

        private StreamChannelOutput outputRouter(
                StreamExecutionGraph.Node node,
                List<StreamTaskLaneSpec> specs,
                List<StreamSocketChannel.Sender> lanes) {
            if (lanes.size() == 1) return lanes.get(0);
            if (node.outputEdges().size() > 1) {
                return new StreamMultiEdgeOutput(admitted.executionGraph,
                        node, identity.subtaskIndex(), specs, lanes);
            }
            if (node.outputEdges().size() != 1) {
                throw new IllegalArgumentException(
                        "generic operator task output lanes need one logical edge");
            }
            StreamEdge edge = node.outputEdges().get(0);
            switch (edge.partitioning()) {
                case FORWARD:
                    return new StreamParallelForwardOutput(lanes);
                case BROADCAST:
                    return new StreamParallelBroadcastOutput(lanes);
                case HASH:
                    StreamExecutionGraph.Node targetNode = admitted.executionGraph
                            .node(edge.targetOperatorUid());
                    List<String> partitionKeyFields =
                            StreamEdgePartitioning.keyFields(
                                    admitted.executionGraph, edge);
                    if (partitionKeyFields.isEmpty()) {
                        throw new IllegalArgumentException(
                                "HASH edge has no stable partition key");
                    }
                    return new StreamParallelKeyedOutput(targetNode,
                            StreamEdgePartitioning.outputSchema(
                                    admitted.executionGraph, edge),
                            partitionKeyFields, lanes,
                            StreamEdgePartitioning.outputChangelogMode(
                                    admitted.executionGraph, edge),
                            edge.maximumFrameBytes());
                default:
                    throw new IllegalArgumentException(
                            "unknown generic output partitioning");
            }
        }

        private List<StreamTaskOutput> taskOutputs(
                StreamExecutionGraph.Node node,
                List<StreamTaskLaneSpec> specs,
                List<StreamSocketChannel.Sender> lanes) {
            List<String> tags = new ArrayList<>();
            for (StreamEdge edge : node.outputEdges()) {
                if (!tags.contains(edge.sourceOutputTagId())) {
                    tags.add(edge.sourceOutputTagId());
                }
            }
            List<StreamTaskOutput> outputs = new ArrayList<>(tags.size());
            for (String tag : tags) {
                List<StreamTaskLaneSpec> portSpecs = new ArrayList<>();
                List<StreamChannelOutput> portLanes = new ArrayList<>();
                for (int index = 0; index < specs.size(); index++) {
                    StreamTaskLaneSpec spec = specs.get(index);
                    if (belongsToPort(spec.laneId(), node, tag)) {
                        portSpecs.add(spec);
                        portLanes.add(lanes.get(index));
                    }
                }
                outputs.add(new StreamTaskOutput(tag,
                        new StreamMultiEdgeOutput(admitted.executionGraph,
                                node, identity.subtaskIndex(), tag,
                                portSpecs, portLanes)));
            }
            return Collections.unmodifiableList(outputs);
        }

        private List<StreamChannelOutput> taskOutputChannels(
                List<StreamTaskOutput> outputs) {
            List<StreamChannelOutput> channels = new ArrayList<>(
                    outputs.size());
            for (StreamTaskOutput output : outputs) {
                channels.add(output.output);
            }
            return Collections.unmodifiableList(channels);
        }

        private List<StreamTaskOutput> observedTaskOutputs(
                List<StreamTaskOutput> outputs,
                StreamTaskProgressTracker tracker) {
            List<StreamTaskOutput> observed = new ArrayList<>(outputs.size());
            for (StreamTaskOutput output : outputs) {
                observed.add(new StreamTaskOutput(output.outputTagId,
                        tracker.observeOutput(output.output)));
            }
            return Collections.unmodifiableList(observed);
        }

        private List<StreamChannelInput> observedTaskInputs(
                List<? extends StreamChannelInput> inputs, StreamTaskProgressTracker tracker) {
            List<StreamChannelInput> observed = new ArrayList<>(inputs.size());
            for (StreamChannelInput input : inputs) observed.add(tracker.observeInput(input));
            return Collections.unmodifiableList(observed);
        }

        private StreamTaskProgress progress() {
            StreamTaskProgressTracker tracker = progressTracker;
            if (tracker == null) {
                throw new IllegalStateException(
                        "Stream task progress is not ready");
            }
            StreamOperatorTask current = task;
            StreamTaskResourceUsage usage = current == null
                    ? StreamTaskResourceUsage.unavailable()
                    : current.resourceUsage();
            return tracker.snapshot(usage.mailboxHighWaterEvents(),
                    usage.managedStateBytes(), usage.timerHighWaterCount(),
                    usage.windowHighWaterCount(), usage.complete());
        }

        private boolean belongsToPort(String laneId,
                StreamExecutionGraph.Node node, String outputTagId) {
            StreamEdge edge = StreamPhysicalLaneBinding.output(
                    admitted.executionGraph, node,
                    identity.subtaskIndex(), laneId);
            return java.util.Objects.equals(outputTagId,
                    edge.sourceOutputTagId());
        }

        private StreamChannelInput sinkInput(
                List<? extends StreamChannelInput> inputs) {
            if (inputs == null || inputs.isEmpty()) {
                throw new IllegalStateException(
                        "generic Sink task has no physical input lane");
            }
            return inputs.size() == 1 ? inputs.get(0)
                    : new StreamInputGateChannel(inputs);
        }

        private StreamUnalignedTaskGate unalignedGate(
                StreamExecutionGraph.Node node,
                FileCheckpointPayloadStore payloadStore) {
            long budget = node.resourceRequest().checkpointBytes();
            if (payloadStore == null || budget < 1L) {
                throw new IllegalArgumentException(
                        "unaligned task requires checkpoint storage");
            }
            long framed = Math.min(budget, Math.addExact(
                    (long) node.resourceRequest().maximumFrameBytes(),
                    64L * 1024L));
            return new StreamUnalignedTaskGate(identity.operatorUid(),
                    identity.subtaskIndex(), receivers, payloadStore,
                    restoreMembers, command.kind()
                            == StreamControlMessage.Kind.RESTORE,
                    maximumEvents, budget, budget,
                    (int) Math.min(Integer.MAX_VALUE, framed));
        }

        private StreamOperatorTask wrapUnaligned(
                StreamOperatorTask delegate) {
            StreamUnalignedTaskGate gate = unalignedGate;
            return gate == null ? delegate : gate.wrap(delegate);
        }

        private List<? extends StreamChannelInput> taskInputs() {
            StreamUnalignedTaskGate gate = unalignedGate;
            return gate == null ? receivers : gate.inputs();
        }

        private List<StreamCheckpointMember> withoutChannelState(
                List<StreamCheckpointMember> members) {
            List<StreamCheckpointMember> filtered = new ArrayList<>();
            for (StreamCheckpointMember member : members) {
                if (member.kind()
                        != StreamCheckpointMember.Kind.CHANNEL_STATE) {
                    filtered.add(member);
                }
            }
            return Collections.unmodifiableList(filtered);
        }

        @Override
        public void close() {
            checkpointReady.completeExceptionally(new IllegalStateException(
                    "Stream operator task closed before checkpoint readiness"));
            for (StreamSocketChannel.Listener listener : listeners) {
                listener.close();
            }
            for (StreamSocketChannel.Receiver receiver : receivers) {
                receiver.cancel();
            }
            for (StreamSocketChannel.Sender sender : senders) {
                sender.cancel();
            }
            if (task != null) task.cancel();
            StreamPhysicalLaneBinding.release(admitted.executionGraph,
                    identity);
        }
    }

    static String failureDetail(Throwable failure) {
        return "TASK_FAILURE";
    }

}
