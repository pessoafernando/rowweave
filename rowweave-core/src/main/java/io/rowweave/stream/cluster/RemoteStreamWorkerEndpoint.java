package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamSplitDiscoveryCodec;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSplitDiscovery;
import io.rowweave.cluster.rpc.ControlRole;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.RoleAuthenticatedSocketTransport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Stable address and admitted capabilities for one remote Stream Worker incarnation. */
public final class RemoteStreamWorkerEndpoint {
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final int READ_TIMEOUT_MILLIS = 30_000;

    private final String host;
    private final int port;
    private final ControlTransport transport;
    private final StreamWorkerCapabilityProfile profile;
    private final Map<String, String> sourceContracts;
    private final Map<String, String> sinkContracts;

    private RemoteStreamWorkerEndpoint(String host, int port,
            ControlTransport transport,
            StreamWorkerCapabilityProfile profile,
            Map<String, String> sourceContracts,
            Map<String, String> sinkContracts) {
        this.host = host;
        this.port = port;
        this.transport = transport;
        this.profile = profile;
        this.sourceContracts = Collections.unmodifiableMap(
                new LinkedHashMap<>(sourceContracts));
        this.sinkContracts = Collections.unmodifiableMap(
                new LinkedHashMap<>(sinkContracts));
    }

    public static RemoteStreamWorkerEndpoint connect(String host, int port) {
        return connect(host, port, loopbackSecurity());
    }

    public static RemoteStreamWorkerEndpoint connect(String host, int port,
            ControlSecurityConfig security) {
        requireAddress(host, port);
        if (security == null) {
            throw new IllegalArgumentException(
                    "Stream Worker control security is required");
        }
        ControlTransport transport = new SocketControlTransport(
                host, port, security);
        StreamRemoteWorkerWire.Response response = requireSuccess(
                transport.call(StreamRemoteWorkerWire.Request.describe()));
        if (response.kind != StreamRemoteWorkerWire.ResponseKind.PROFILE
                || response.profile == null) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no capability profile");
        }
        return new RemoteStreamWorkerEndpoint(host, port, transport,
                response.profile, response.sourceContracts,
                response.sinkContracts);
    }

    static RemoteStreamWorkerEndpoint inProcess(
            StreamRemoteWorkerServer worker) {
        if (worker == null) {
            throw new IllegalArgumentException(
                    "in-process Stream Worker is required");
        }
        ControlTransport transport = new ControlTransport() {
            @Override
            public StreamRemoteWorkerWire.Response call(
                    StreamRemoteWorkerWire.Request request) {
                return worker.dispatchInProcess(request);
            }

            @Override
            public StreamRemoteWorkerWire.Response call(
                    StreamRemoteWorkerWire.Request request,
                    int timeoutMillis) {
                return worker.dispatchInProcess(request, timeoutMillis);
            }
        };
        return fromTransport("in-process", 0, transport);
    }

    static RemoteStreamWorkerEndpoint fromTransport(
            String host, int port, ControlTransport transport) {
        if (host == null || host.isEmpty() || transport == null) {
            throw new IllegalArgumentException(
                    "Stream Worker transport identity is required");
        }
        StreamRemoteWorkerWire.Response response = requireSuccess(
                transport.call(StreamRemoteWorkerWire.Request.describe()));
        if (response.kind != StreamRemoteWorkerWire.ResponseKind.PROFILE
                || response.profile == null) {
            throw new IllegalStateException(
                    "in-process Stream Worker returned no capability profile");
        }
        return new RemoteStreamWorkerEndpoint(
                host, port, transport, response.profile,
                response.sourceContracts, response.sinkContracts);
    }

    public String host() { return host; }
    public int port() { return port; }
    public StreamWorkerCapabilityProfile profile() { return profile; }

    boolean supportsSource(String providerKey, String contractDigest) {
        return contractDigest.equals(sourceContracts.get(providerKey));
    }

    boolean supportsSink(String providerKey, String contractDigest) {
        return contractDigest.equals(sinkContracts.get(providerKey));
    }

    void requireReachable() {
        StreamRemoteWorkerWire.Response response = requireSuccess(
                transport.call(StreamRemoteWorkerWire.Request.describe()));
        if (response.kind != StreamRemoteWorkerWire.ResponseKind.PROFILE
                || response.profile == null
                || !profile.workerId().equals(
                        response.profile.workerId())
                || !profile.workerIncarnation().equals(
                        response.profile.workerIncarnation())) {
            throw new IllegalStateException(
                    "remote Stream Worker identity changed");
        }
    }

    StreamSplitDiscovery discoverSource(byte[] control, byte[] artifact,
            String operatorUid, StreamDiscoveryCursor cursor, int limit,
            StreamSourceDiscoverySpec discoverySpec) {
        if (cursor == null || discoverySpec == null
                || limit > discoverySpec.maximumChangesPerDiscovery()) {
            throw new IllegalArgumentException(
                    "Stream source discovery request exceeds its contract");
        }
        StreamRemoteWorkerWire.Response response = idempotentCall(
                StreamRemoteWorkerWire.Request.discoverSource(
                        control, artifact, operatorUid, cursor.frame(), limit),
                timeout(discoverySpec.discoveryTimeoutMillis()));
        if (response.kind
                != StreamRemoteWorkerWire.ResponseKind.SOURCE_DISCOVERY) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no source discovery");
        }
        return StreamSplitDiscoveryCodec.decode(
                response.sourceDiscoveryFrame,
                discoverySpec.maximumChangesPerDiscovery(),
                discoverySpec.maximumCursorBytes(),
                discoverySpec.maximumDescriptorBytes());
    }

    void assignSourceSplit(byte[] control, String operatorUid,
            int subtaskIndex, byte[] sourceAssignmentFrame) {
        requireAck(idempotentCall(
                StreamRemoteWorkerWire.Request.assignSourceSplit(
                control, operatorUid, subtaskIndex,
                sourceAssignmentFrame), READ_TIMEOUT_MILLIS));
    }

    void revokeSourceSplit(byte[] control, String operatorUid,
            int subtaskIndex, byte[] sourceAssignmentFrame,
            long timeoutMillis) {
        long rpcTimeout = timeoutMillis >= Integer.MAX_VALUE - 1_000L
                ? Integer.MAX_VALUE : timeoutMillis + 1_000L;
        requireAck(idempotentCall(
                StreamRemoteWorkerWire.Request.revokeSourceSplit(
                        control, operatorUid, subtaskIndex,
                        sourceAssignmentFrame), timeout(rpcTimeout)));
    }

    StreamSourceTaskAssignmentCodec.Value sourceSplitProgress(
            byte[] control, String operatorUid, int subtaskIndex) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.sourceSplitProgress(
                        control, operatorUid, subtaskIndex));
        if (response.kind
                != StreamRemoteWorkerWire.ResponseKind.SOURCE_SPLIT_PROGRESS) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no source split progress");
        }
        return response.sourceSplitProgressFrame.length == 0 ? null
                : StreamSourceTaskAssignmentCodec.decode(
                        response.sourceSplitProgressFrame);
    }

    private static int timeout(long timeoutMillis) {
        if (timeoutMillis < 1L || timeoutMillis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(
                    "Stream source discovery timeout is invalid");
        }
        return (int) timeoutMillis;
    }

    List<StreamTaskLaneSpec> prepareOperatorTask(byte[] control,
            byte[] artifact, int maximumEvents, String operatorUid,
            int subtaskIndex, List<StreamTaskLaneSpec> inputLanes,
            List<StreamTaskLaneSpec> outputLanes) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.prepareOperatorTask(
                        control, artifact, maximumEvents, operatorUid,
                        subtaskIndex, inputLanes, outputLanes));
        if (response.kind != StreamRemoteWorkerWire.ResponseKind.TASK_ENDPOINTS) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no task endpoints");
        }
        return response.taskEndpoints;
    }

    List<StreamTaskLaneSpec> prepareOperatorTask(byte[] control,
            byte[] artifact, int maximumEvents, String operatorUid,
            int subtaskIndex, List<StreamTaskLaneSpec> inputLanes,
            List<StreamTaskLaneSpec> outputLanes,
            byte[] checkpointBundleFrame) {
        return prepareOperatorTask(control, artifact, maximumEvents,
                operatorUid, subtaskIndex, inputLanes, outputLanes,
                checkpointBundleFrame, null);
    }

    List<StreamTaskLaneSpec> prepareOperatorTask(byte[] control,
            byte[] artifact, int maximumEvents, String operatorUid,
            int subtaskIndex, List<StreamTaskLaneSpec> inputLanes,
            List<StreamTaskLaneSpec> outputLanes,
            byte[] checkpointBundleFrame, byte[] sourceAssignmentFrame) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.prepareOperatorTask(
                        control, artifact, maximumEvents, operatorUid,
                        subtaskIndex, inputLanes, outputLanes,
                        checkpointBundleFrame, sourceAssignmentFrame));
        if (response.kind
                != StreamRemoteWorkerWire.ResponseKind.TASK_ENDPOINTS) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no task endpoints");
        }
        return response.taskEndpoints;
    }

    void startOperatorTask(byte[] control, byte[] artifact,
            int maximumEvents, String operatorUid, int subtaskIndex,
            List<StreamTaskLaneSpec> inputLanes,
            List<StreamTaskLaneSpec> outputLanes) {
        requireAck(call(StreamRemoteWorkerWire.Request.startOperatorTask(
                control, artifact, maximumEvents, operatorUid, subtaskIndex,
                inputLanes, outputLanes)));
    }

    void startOperatorTask(byte[] control, byte[] artifact,
            int maximumEvents, String operatorUid, int subtaskIndex,
            List<StreamTaskLaneSpec> inputLanes,
            List<StreamTaskLaneSpec> outputLanes,
            byte[] checkpointBundleFrame) {
        startOperatorTask(control, artifact, maximumEvents, operatorUid,
                subtaskIndex, inputLanes, outputLanes,
                checkpointBundleFrame, null);
    }

    void startOperatorTask(byte[] control, byte[] artifact,
            int maximumEvents, String operatorUid, int subtaskIndex,
            List<StreamTaskLaneSpec> inputLanes,
            List<StreamTaskLaneSpec> outputLanes,
            byte[] checkpointBundleFrame, byte[] sourceAssignmentFrame) {
        requireAck(call(StreamRemoteWorkerWire.Request.startOperatorTask(
                control, artifact, maximumEvents, operatorUid, subtaskIndex,
                inputLanes, outputLanes, checkpointBundleFrame,
                sourceAssignmentFrame)));
    }

    void cancel(byte[] control) {
        requireAck(call(
                StreamRemoteWorkerWire.Request.cancel(control)));
    }

    List<StreamCheckpointMember> checkpoint(byte[] control) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.checkpoint(control));
        if (response.kind
                != StreamRemoteWorkerWire.ResponseKind.CHECKPOINT_MEMBERS) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no checkpoint members");
        }
        return response.checkpointMembers;
    }

    List<StreamCheckpointMember> checkpoint(byte[] control,
            String operatorUid, int subtaskIndex) {
        return checkpoint(control, operatorUid, subtaskIndex,
                READ_TIMEOUT_MILLIS);
    }

    List<StreamCheckpointMember> checkpoint(byte[] control,
            String operatorUid, int subtaskIndex, int timeoutMillis) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.checkpoint(control,
                        operatorUid, subtaskIndex), timeoutMillis);
        if (response.kind
                != StreamRemoteWorkerWire.ResponseKind.CHECKPOINT_MEMBERS) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no checkpoint members");
        }
        return response.checkpointMembers;
    }

    byte[] fetchCheckpointPayload(byte[] control, String digest,
            long contentBytes, long offset) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.fetchCheckpointPayload(
                        control, StreamCheckpointPayloadCommandCodec.fetch(
                                digest, contentBytes, offset)));
        if (response.kind != StreamRemoteWorkerWire.ResponseKind
                .CHECKPOINT_PAYLOAD_CHUNK) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no checkpoint payload");
        }
        return response.checkpointPayloadChunk;
    }

    void storeCheckpointPayload(byte[] control, String digest,
            long contentBytes, long offset, byte[] chunk) {
        requireAck(call(StreamRemoteWorkerWire.Request.storeCheckpointPayload(
                control, StreamCheckpointPayloadCommandCodec.store(
                        digest, contentBytes, offset, chunk))));
    }

    void completeCheckpoint(byte[] control) {
        requireAck(call(StreamRemoteWorkerWire.Request
                .checkpointComplete(control)));
    }

    void completeCheckpoint(byte[] control, String operatorUid,
            int subtaskIndex) {
        completeCheckpoint(control, operatorUid, subtaskIndex,
                READ_TIMEOUT_MILLIS);
    }

    void completeCheckpoint(byte[] control, String operatorUid,
            int subtaskIndex, int timeoutMillis) {
        requireAck(call(StreamRemoteWorkerWire.Request.checkpointComplete(
                control, operatorUid, subtaskIndex, timeoutMillis),
                timeoutMillis));
    }

    void completeCheckpoint(byte[] control, String operatorUid,
            int subtaskIndex, int timeoutMillis,
            byte[] checkpointBundleFrame) {
        requireAck(call(StreamRemoteWorkerWire.Request.checkpointComplete(
                control, operatorUid, subtaskIndex, timeoutMillis,
                checkpointBundleFrame), timeoutMillis));
    }

    void recoverCheckpoint(byte[] control, byte[] artifact,
            String operatorUid, int subtaskIndex, int timeoutMillis) {
        requireAck(call(StreamRemoteWorkerWire.Request.recoverCheckpoint(
                control, artifact, operatorUid, subtaskIndex,
                timeoutMillis), timeoutMillis));
    }

    void recoverCheckpoint(byte[] control, byte[] artifact,
            String operatorUid, int subtaskIndex, int timeoutMillis,
            byte[] checkpointBundleFrame) {
        requireAck(call(StreamRemoteWorkerWire.Request.recoverCheckpoint(
                control, artifact, operatorUid, subtaskIndex,
                timeoutMillis, checkpointBundleFrame), timeoutMillis));
    }

    boolean taskFailed(byte[] control) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.status(control));
        if (response.kind != StreamRemoteWorkerWire.ResponseKind.TASK_STATUS) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no task status");
        }
        return response.taskFailed;
    }

    boolean taskCompleted(byte[] control, String operatorUid,
            int subtaskIndex) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.taskLifecycle(control,
                        operatorUid, subtaskIndex));
        if (response.kind
                != StreamRemoteWorkerWire.ResponseKind.TASK_LIFECYCLE) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no task lifecycle");
        }
        return response.taskCompleted && !response.taskFailed;
    }

    StreamTaskProgress taskProgress(byte[] control, String operatorUid,
            int subtaskIndex) {
        StreamRemoteWorkerWire.Response response = call(
                StreamRemoteWorkerWire.Request.taskProgress(
                        control, operatorUid, subtaskIndex));
        if (response.kind
                != StreamRemoteWorkerWire.ResponseKind.TASK_PROGRESS
                || response.taskProgress == null) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no task progress");
        }
        return response.taskProgress;
    }

    String taskFailureMessage(byte[] control) {
        return taskFailureMessage(control, null, -1);
    }

    String taskFailureMessage(byte[] control, String operatorUid,
            int subtaskIndex) {
        StreamRemoteWorkerWire.Response response = call(
                operatorUid == null
                        ? StreamRemoteWorkerWire.Request.status(control)
                        : StreamRemoteWorkerWire.Request.status(control,
                                operatorUid, subtaskIndex));
        if (response.kind != StreamRemoteWorkerWire.ResponseKind.TASK_STATUS) {
            throw new IllegalStateException(
                    "remote Stream Worker returned no task status");
        }
        return response.taskFailed ? response.taskFailureMessage : null;
    }

    private StreamRemoteWorkerWire.Response call(
            StreamRemoteWorkerWire.Request request) {
        return requireSuccess(transport.call(request));
    }

    private StreamRemoteWorkerWire.Response call(
            StreamRemoteWorkerWire.Request request, int timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Stream Worker RPC timeout must be positive");
        }
        return requireSuccess(transport.call(request, timeoutMillis));
    }

    private StreamRemoteWorkerWire.Response idempotentCall(
            StreamRemoteWorkerWire.Request request, int timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException(
                    "Stream Worker RPC timeout must be positive");
        }
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        RuntimeException first;
        StreamRemoteWorkerWire.Response response;
        try {
            response = transport.call(request, timeoutMillis);
        } catch (RuntimeException transportFailure) {
            first = transportFailure;
            long remainingNanos = deadline - System.nanoTime();
            if (remainingNanos <= 0L) throw first;
            int remainingMillis = (int) Math.max(1L, Math.min(
                    Integer.MAX_VALUE,
                    TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
            try {
                response = transport.call(request, remainingMillis);
            } catch (RuntimeException retryFailure) {
                retryFailure.addSuppressed(first);
                throw retryFailure;
            }
        }
        return requireSuccess(response);
    }

    private static StreamRemoteWorkerWire.Response requireSuccess(
            StreamRemoteWorkerWire.Response response) {
        if (response.kind == StreamRemoteWorkerWire.ResponseKind.ERROR) {
            throw new IllegalStateException(
                    "Stream Worker rejected request: "
                            + response.errorCode + ": "
                            + response.errorMessage);
        }
        return response;
    }

    private static void requireAck(StreamRemoteWorkerWire.Response response) {
        if (response.kind != StreamRemoteWorkerWire.ResponseKind.ACK) {
            throw new IllegalStateException(
                    "remote Stream Worker returned an unexpected response");
        }
    }

    private static void requireAddress(String host, int port) {
        if (host == null || host.trim().isEmpty()
                || port <= 0 || port > 65_535) {
            throw new IllegalArgumentException(
                    "remote Stream Worker host and port are required");
        }
    }

    interface ControlTransport {
        StreamRemoteWorkerWire.Response call(
                StreamRemoteWorkerWire.Request request);
        default StreamRemoteWorkerWire.Response call(
                StreamRemoteWorkerWire.Request request, int timeoutMillis) {
            return call(request);
        }
    }

    private static final class SocketControlTransport
            implements ControlTransport {
        private final String host;
        private final int port;
        private final ControlSecurityConfig security;

        private SocketControlTransport(String host, int port,
                ControlSecurityConfig security) {
            this.host = host;
            this.port = port;
            this.security = security;
        }

        @Override
        public StreamRemoteWorkerWire.Response call(
                StreamRemoteWorkerWire.Request request) {
            return call(request, READ_TIMEOUT_MILLIS);
        }

        @Override
        public StreamRemoteWorkerWire.Response call(
                StreamRemoteWorkerWire.Request request, int timeoutMillis) {
            try (Socket socket = RoleAuthenticatedSocketTransport.connect(
                    security, ControlRole.MASTER, ControlRole.WORKER,
                    host, port,
                    Math.min(CONNECT_TIMEOUT_MILLIS, timeoutMillis),
                    timeoutMillis)) {
                DataOutputStream output = new DataOutputStream(
                        new BufferedOutputStream(socket.getOutputStream()));
                DataInputStream input = new DataInputStream(
                        new BufferedInputStream(socket.getInputStream()));
                StreamRemoteWorkerWire.writeRequest(output, request);
                return StreamRemoteWorkerWire.readResponse(input);
            } catch (IOException failure) {
                throw new IllegalStateException(
                        "remote Stream Worker control RPC failed", failure);
            }
        }
    }

    private static ControlSecurityConfig loopbackSecurity() {
        return ControlSecurityConfig.fromProperties(new Properties());
    }
}
