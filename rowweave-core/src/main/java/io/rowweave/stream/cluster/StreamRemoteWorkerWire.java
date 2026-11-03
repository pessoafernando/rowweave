package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamCheckpointMemberCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Bounded, checksummed RPC framing for the independent Stream Worker control plane. */
final class StreamRemoteWorkerWire {
    private static final byte[] MAGIC_V1 =
            "RWXXSWC1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MAGIC_V2 =
            "RWXXSWC2".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] MAGIC_V3 =
            "RWXXSWC3".getBytes(StandardCharsets.US_ASCII);
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_TEXT_BYTES = 256;
    private static final int MAX_PROVIDERS = 256;
    private static final int MAX_ROUTES = 64;
    private static final int MAX_CHECKPOINT_MEMBERS = 256;
    static final int MAX_FRAME_BYTES = PlanArtifactCodec.MAX_FRAME_BYTES
            + StreamControlMessageCodec.MAX_FRAME_BYTES
            + StreamCheckpointTransferBundleCodec.MAX_FRAME_BYTES
            + StreamSourceTaskAssignmentCodec.MAX_FRAME_BYTES
            + 64 * 1024;

    enum Type {
        DESCRIBE(1), START_SINK(2), START_SOURCE(3), CANCEL(4),
        START_SINK_TASK(5), START_SOURCE_FANOUT(6), STATUS(7), CHECKPOINT(8),
        CHECKPOINT_COMPLETE(9), PREPARE_OPERATOR_TASK(10),
        START_OPERATOR_TASK(11), RECOVER_CHECKPOINT(12),
        FETCH_CHECKPOINT_PAYLOAD(13), STORE_CHECKPOINT_PAYLOAD(14),
        DISCOVER_SOURCE(15), ASSIGN_SOURCE_SPLIT(16),
        SOURCE_SPLIT_PROGRESS(17), REVOKE_SOURCE_SPLIT(18),
        TASK_LIFECYCLE(19), TASK_PROGRESS(20);
        private final int wireId;
        Type(int wireId) { this.wireId = wireId; }
        static Type fromWireId(int wireId) {
            for (Type type : values()) if (type.wireId == wireId) return type;
            throw invalid("unknown Stream Worker request type");
        }
    }

    enum ResponseKind {
        ACK(1), PROFILE(2), DATA_PORT(3), ERROR(4), TASK_STATUS(5),
        CHECKPOINT_MEMBERS(6), TASK_ENDPOINTS(7),
        CHECKPOINT_PAYLOAD_CHUNK(8), SOURCE_DISCOVERY(9),
        SOURCE_SPLIT_PROGRESS(10), TASK_LIFECYCLE(11), TASK_PROGRESS(12);
        private final int wireId;
        ResponseKind(int wireId) { this.wireId = wireId; }
        static ResponseKind fromWireId(int wireId) {
            for (ResponseKind kind : values()) if (kind.wireId == wireId) return kind;
            throw invalid("unknown Stream Worker response type");
        }
    }

    enum ErrorCode {
        INVALID_REQUEST(1), ADMISSION_REJECTED(2), STALE_FENCE(3),
        TASK_CONFLICT(4), INTERNAL_FAILURE(5);
        private final int wireId;
        ErrorCode(int wireId) { this.wireId = wireId; }
        static ErrorCode fromWireId(int wireId) {
            for (ErrorCode code : values()) if (code.wireId == wireId) return code;
            throw invalid("unknown Stream Worker error code");
        }
    }

    static final class Request {
        final Type type;
        final byte[] controlFrame;
        final byte[] artifactFrame;
        final int dataPort;
        final int maximumEvents;
        final String operatorUid;
        final Map<String, Integer> dataPorts;
        final int subtaskIndex;
        final List<StreamTaskLaneSpec> inputLanes;
        final List<StreamTaskLaneSpec> outputLanes;
        final byte[] checkpointBundleFrame;
        final byte[] sourceAssignmentFrame;
        final byte[] discoveryCursorFrame;
        final int discoveryLimit;

        private Request(Type type, byte[] controlFrame, byte[] artifactFrame,
                int dataPort, int maximumEvents, String operatorUid,
                Map<String, Integer> dataPorts) {
            this(type, controlFrame, artifactFrame, dataPort, maximumEvents,
                    operatorUid, dataPorts, -1, null, null, null);
        }

        private Request(Type type, byte[] controlFrame, byte[] artifactFrame,
                int dataPort, int maximumEvents, String operatorUid,
                Map<String, Integer> dataPorts, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes) {
            this(type, controlFrame, artifactFrame, dataPort, maximumEvents,
                    operatorUid, dataPorts, subtaskIndex, inputLanes,
                    outputLanes, null, null);
        }

        private Request(Type type, byte[] controlFrame, byte[] artifactFrame,
                int dataPort, int maximumEvents, String operatorUid,
                Map<String, Integer> dataPorts, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes,
                byte[] checkpointBundleFrame) {
            this(type, controlFrame, artifactFrame, dataPort, maximumEvents,
                    operatorUid, dataPorts, subtaskIndex, inputLanes,
                    outputLanes, checkpointBundleFrame, null);
        }

        private Request(Type type, byte[] controlFrame, byte[] artifactFrame,
                int dataPort, int maximumEvents, String operatorUid,
                Map<String, Integer> dataPorts, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes,
                byte[] checkpointBundleFrame, byte[] sourceAssignmentFrame) {
            this.type = type;
            this.controlFrame = copyBytes(controlFrame);
            this.artifactFrame = copyBytes(artifactFrame);
            this.dataPort = dataPort;
            this.maximumEvents = maximumEvents;
            this.operatorUid = operatorUid;
            this.dataPorts = dataPorts == null ? java.util.Collections.emptyMap()
                    : java.util.Collections.unmodifiableMap(
                    new LinkedHashMap<>(dataPorts));
            this.subtaskIndex = subtaskIndex;
            this.inputLanes = inputLanes == null
                    ? java.util.Collections.emptyList()
                    : java.util.Collections.unmodifiableList(
                    new ArrayList<>(inputLanes));
            this.outputLanes = outputLanes == null
                    ? java.util.Collections.emptyList()
                    : java.util.Collections.unmodifiableList(
                    new ArrayList<>(outputLanes));
            this.checkpointBundleFrame = copyBytes(checkpointBundleFrame);
            this.sourceAssignmentFrame = copyBytes(sourceAssignmentFrame);
            this.discoveryCursorFrame = type == Type.DISCOVER_SOURCE
                    ? copyBytes(checkpointBundleFrame) : null;
            this.discoveryLimit = type == Type.DISCOVER_SOURCE
                    ? maximumEvents : 0;
        }

        static Request describe() {
            return new Request(Type.DESCRIBE, null, null, 0, 0, null, null);
        }
        static Request startSink(byte[] control, byte[] artifact, int events,
                String operatorUid) {
            return new Request(Type.START_SINK, control, artifact, 0, events,
                    operatorUid, null);
        }
        static Request startSinkTask(byte[] control, byte[] artifact,
                int events, String operatorUid) {
            return new Request(Type.START_SINK_TASK, control, artifact, 0,
                    events, operatorUid, null);
        }
        static Request startSource(byte[] control, byte[] artifact,
                int dataPort, int events, String sinkOperatorUid) {
            return new Request(Type.START_SOURCE, control, artifact,
                    dataPort, events, sinkOperatorUid, null);
        }
        static Request startSourceFanout(byte[] control, byte[] artifact,
                Map<String, Integer> dataPorts, int events) {
            return new Request(Type.START_SOURCE_FANOUT, control, artifact,
                    0, events, null, dataPorts);
        }
        static Request cancel(byte[] control) {
            return new Request(Type.CANCEL, control, null, 0, 0, null, null);
        }
        static Request status(byte[] control) {
            return new Request(Type.STATUS, control, null, 0, 0, null, null);
        }
        static Request status(byte[] control, String operatorUid,
                int subtaskIndex) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || subtaskIndex < 0 || subtaskIndex >= 4_096) {
                throw new IllegalArgumentException(
                        "status task identity is invalid");
            }
            return new Request(Type.STATUS, control, null, 0, 0,
                    operatorUid, null, subtaskIndex, null, null);
        }
        static Request taskLifecycle(byte[] control, String operatorUid,
                int subtaskIndex) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || subtaskIndex < 0 || subtaskIndex >= 4_096) {
                throw new IllegalArgumentException(
                        "task lifecycle identity is invalid");
            }
            return new Request(Type.TASK_LIFECYCLE, control, null, 0, 0,
                    operatorUid, null, subtaskIndex, null, null);
        }
        static Request taskProgress(byte[] control, String operatorUid,
                int subtaskIndex) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || subtaskIndex < 0 || subtaskIndex >= 4_096) {
                throw new IllegalArgumentException(
                        "task progress identity is invalid");
            }
            return new Request(Type.TASK_PROGRESS, control, null, 0, 0,
                    operatorUid, null, subtaskIndex, null, null);
        }
        static Request checkpoint(byte[] control) {
            return new Request(Type.CHECKPOINT, control, null, 0, 0, null, null);
        }
        static Request checkpoint(byte[] control, String operatorUid,
                int subtaskIndex) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || subtaskIndex < 0 || subtaskIndex >= 4_096) {
                throw new IllegalArgumentException(
                        "checkpoint task identity is invalid");
            }
            return new Request(Type.CHECKPOINT, control, null, 0, 0,
                    operatorUid, null, subtaskIndex, null, null);
        }
        static Request fetchCheckpointPayload(byte[] control,
                byte[] payloadCommand) {
            return new Request(Type.FETCH_CHECKPOINT_PAYLOAD, control,
                    payloadCommand, 0, 0, null, null);
        }
        static Request storeCheckpointPayload(byte[] control,
                byte[] payloadCommand) {
            return new Request(Type.STORE_CHECKPOINT_PAYLOAD, control,
                    payloadCommand, 0, 0, null, null);
        }
        static Request checkpointComplete(byte[] control) {
            return new Request(Type.CHECKPOINT_COMPLETE, control,
                    null, 0, 0, null, null);
        }

        static Request discoverSource(byte[] control, byte[] artifact,
                String operatorUid, byte[] cursorFrame, int limit) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || cursorFrame == null || cursorFrame.length
                    > io.rowweave.stream.api.StreamSourceDiscoverySpec
                    .HARD_MAX_FRAME_BYTES
                    || limit < 1
                    || limit > io.rowweave.stream.api
                    .StreamSourceDiscoverySpec.HARD_MAX_CHANGES) {
                throw new IllegalArgumentException(
                        "source discovery request is invalid");
            }
            return new Request(Type.DISCOVER_SOURCE, control, artifact,
                    0, limit, operatorUid, null, -1, null, null,
                    cursorFrame);
        }

        static Request assignSourceSplit(byte[] control, String operatorUid,
                int subtaskIndex, byte[] sourceAssignmentFrame) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || subtaskIndex < 0 || subtaskIndex >= 4_096
                    || sourceAssignmentFrame == null
                    || sourceAssignmentFrame.length < 1
                    || sourceAssignmentFrame.length
                    > StreamSourceTaskAssignmentCodec.MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "source split assignment request is invalid");
            }
            return new Request(Type.ASSIGN_SOURCE_SPLIT, control, null,
                    0, 0, operatorUid, null, subtaskIndex, null, null,
                    null, sourceAssignmentFrame);
        }

        static Request revokeSourceSplit(byte[] control, String operatorUid,
                int subtaskIndex, byte[] sourceAssignmentFrame) {
            Request assignment = assignSourceSplit(control, operatorUid,
                    subtaskIndex, sourceAssignmentFrame);
            return new Request(Type.REVOKE_SOURCE_SPLIT,
                    assignment.controlFrame, null, 0, 0,
                    operatorUid, null, subtaskIndex, null, null,
                    null, sourceAssignmentFrame);
        }

        static Request sourceSplitProgress(byte[] control, String operatorUid,
                int subtaskIndex) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || subtaskIndex < 0 || subtaskIndex >= 4_096) {
                throw new IllegalArgumentException(
                        "source split progress task identity is invalid");
            }
            return new Request(Type.SOURCE_SPLIT_PROGRESS, control, null,
                    0, 0, operatorUid, null, subtaskIndex, null, null);
        }

        static Request checkpointComplete(byte[] control, String operatorUid,
                int subtaskIndex) {
            return checkpointComplete(control, operatorUid, subtaskIndex,
                    2_000);
        }

        static Request recoverCheckpoint(byte[] control, byte[] artifact,
                String operatorUid, int subtaskIndex, int timeoutMillis) {
            return recoverCheckpoint(control, artifact, operatorUid,
                    subtaskIndex, timeoutMillis, null);
        }

        static Request recoverCheckpoint(byte[] control, byte[] artifact,
                String operatorUid, int subtaskIndex, int timeoutMillis,
                byte[] checkpointBundleFrame) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || subtaskIndex < 0 || subtaskIndex >= 4_096
                    || timeoutMillis <= 0) {
                throw new IllegalArgumentException(
                        "checkpoint recovery task identity is invalid");
            }
            return new Request(Type.RECOVER_CHECKPOINT, control, artifact,
                    0, timeoutMillis, operatorUid, null, subtaskIndex,
                    null, null, checkpointBundleFrame);
        }

        static Request checkpointComplete(byte[] control, String operatorUid,
                int subtaskIndex, int timeoutMillis) {
            return checkpointComplete(control, operatorUid, subtaskIndex,
                    timeoutMillis, null);
        }

        static Request checkpointComplete(byte[] control, String operatorUid,
                int subtaskIndex, int timeoutMillis,
                byte[] checkpointBundleFrame) {
            if (operatorUid == null || operatorUid.isEmpty()
                    || subtaskIndex < 0 || subtaskIndex >= 4_096
                    || timeoutMillis <= 0) {
                throw new IllegalArgumentException(
                        "checkpoint completion task identity is invalid");
            }
            return new Request(Type.CHECKPOINT_COMPLETE, control,
                    null, 0, timeoutMillis, operatorUid, null,
                    subtaskIndex, null, null, checkpointBundleFrame);
        }

        static Request prepareOperatorTask(byte[] control, byte[] artifact,
                int events, String operatorUid, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes) {
            return operatorTask(Type.PREPARE_OPERATOR_TASK, control, artifact,
                    events, operatorUid, subtaskIndex, inputLanes,
                    outputLanes, null);
        }

        static Request prepareOperatorTask(byte[] control, byte[] artifact,
                int events, String operatorUid, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes,
                byte[] checkpointBundleFrame) {
            return operatorTask(Type.PREPARE_OPERATOR_TASK, control, artifact,
                    events, operatorUid, subtaskIndex, inputLanes,
                    outputLanes, checkpointBundleFrame);
        }

        static Request prepareOperatorTask(byte[] control, byte[] artifact,
                int events, String operatorUid, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes,
                byte[] checkpointBundleFrame, byte[] sourceAssignmentFrame) {
            return operatorTask(Type.PREPARE_OPERATOR_TASK, control, artifact,
                    events, operatorUid, subtaskIndex, inputLanes,
                    outputLanes, checkpointBundleFrame,
                    sourceAssignmentFrame);
        }

        static Request startOperatorTask(byte[] control, byte[] artifact,
                int events, String operatorUid, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes) {
            return operatorTask(Type.START_OPERATOR_TASK, control, artifact,
                    events, operatorUid, subtaskIndex, inputLanes,
                    outputLanes, null);
        }

        static Request startOperatorTask(byte[] control, byte[] artifact,
                int events, String operatorUid, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes,
                byte[] checkpointBundleFrame) {
            return operatorTask(Type.START_OPERATOR_TASK, control, artifact,
                    events, operatorUid, subtaskIndex, inputLanes,
                    outputLanes, checkpointBundleFrame);
        }

        static Request startOperatorTask(byte[] control, byte[] artifact,
                int events, String operatorUid, int subtaskIndex,
                List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes,
                byte[] checkpointBundleFrame, byte[] sourceAssignmentFrame) {
            return operatorTask(Type.START_OPERATOR_TASK, control, artifact,
                    events, operatorUid, subtaskIndex, inputLanes,
                    outputLanes, checkpointBundleFrame,
                    sourceAssignmentFrame);
        }

        private static Request operatorTask(Type type, byte[] control,
                byte[] artifact, int events, String operatorUid,
                int subtaskIndex, List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes,
                byte[] checkpointBundleFrame) {
            return operatorTask(type, control, artifact, events, operatorUid,
                    subtaskIndex, inputLanes, outputLanes,
                    checkpointBundleFrame, null);
        }

        private static Request operatorTask(Type type, byte[] control,
                byte[] artifact, int events, String operatorUid,
                int subtaskIndex, List<StreamTaskLaneSpec> inputLanes,
                List<StreamTaskLaneSpec> outputLanes,
                byte[] checkpointBundleFrame, byte[] sourceAssignmentFrame) {
            if (type != Type.PREPARE_OPERATOR_TASK
                    && type != Type.START_OPERATOR_TASK) {
                throw new IllegalArgumentException("operator task type required");
            }
            return new Request(type, control, artifact, 0, events,
                    operatorUid, null, subtaskIndex, inputLanes,
                    outputLanes, checkpointBundleFrame,
                    sourceAssignmentFrame);
        }
    }

    static final class Response {
        final ResponseKind kind;
        final StreamWorkerCapabilityProfile profile;
        final Map<String, String> sourceContracts;
        final Map<String, String> sinkContracts;
        final int dataPort;
        final ErrorCode errorCode;
        final String errorMessage;
        final boolean taskFailed;
        final boolean taskCompleted;
        final String taskFailureMessage;
        final List<StreamCheckpointMember> checkpointMembers;
        final List<StreamTaskLaneSpec> taskEndpoints;
        final byte[] checkpointPayloadChunk;
        final byte[] sourceDiscoveryFrame;
        final byte[] sourceSplitProgressFrame;
        final StreamTaskProgress taskProgress;

        private Response(ResponseKind kind,
                StreamWorkerCapabilityProfile profile,
                Map<String, String> sourceContracts,
                Map<String, String> sinkContracts,
                int dataPort, ErrorCode errorCode, String errorMessage,
                boolean taskFailed, String taskFailureMessage,
                List<StreamCheckpointMember> checkpointMembers,
                List<StreamTaskLaneSpec> taskEndpoints,
                byte[] checkpointPayloadChunk) {
            this(kind, profile, sourceContracts, sinkContracts, dataPort,
                    errorCode, errorMessage, taskFailed, false,
                    taskFailureMessage, checkpointMembers, taskEndpoints,
                    checkpointPayloadChunk);
        }

        private Response(ResponseKind kind,
                StreamWorkerCapabilityProfile profile,
                Map<String, String> sourceContracts,
                Map<String, String> sinkContracts,
                int dataPort, ErrorCode errorCode, String errorMessage,
                boolean taskFailed, boolean taskCompleted,
                String taskFailureMessage,
                List<StreamCheckpointMember> checkpointMembers,
                List<StreamTaskLaneSpec> taskEndpoints,
                byte[] checkpointPayloadChunk) {
            this(kind, profile, sourceContracts, sinkContracts, dataPort,
                    errorCode, errorMessage, taskFailed, taskCompleted,
                    taskFailureMessage, checkpointMembers, taskEndpoints,
                    checkpointPayloadChunk, null);
        }

        private Response(ResponseKind kind,
                StreamWorkerCapabilityProfile profile,
                Map<String, String> sourceContracts,
                Map<String, String> sinkContracts,
                int dataPort, ErrorCode errorCode, String errorMessage,
                boolean taskFailed, boolean taskCompleted,
                String taskFailureMessage,
                List<StreamCheckpointMember> checkpointMembers,
                List<StreamTaskLaneSpec> taskEndpoints,
                byte[] checkpointPayloadChunk,
                StreamTaskProgress taskProgress) {
            this.kind = kind;
            this.profile = profile;
            this.sourceContracts = sourceContracts;
            this.sinkContracts = sinkContracts;
            this.dataPort = dataPort;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
            this.taskFailed = taskFailed;
            this.taskCompleted = taskCompleted;
            this.taskFailureMessage = taskFailureMessage;
            this.checkpointMembers = checkpointMembers == null
                    ? java.util.Collections.emptyList()
                    : java.util.Collections.unmodifiableList(
                    new ArrayList<>(checkpointMembers));
            this.taskEndpoints = taskEndpoints == null
                    ? java.util.Collections.emptyList()
                    : java.util.Collections.unmodifiableList(
                    new ArrayList<>(taskEndpoints));
            this.checkpointPayloadChunk = copyBytes(checkpointPayloadChunk);
            this.sourceDiscoveryFrame = kind == ResponseKind.SOURCE_DISCOVERY
                    ? copyBytes(checkpointPayloadChunk) : null;
            this.sourceSplitProgressFrame = kind
                    == ResponseKind.SOURCE_SPLIT_PROGRESS
                    ? copyBytes(checkpointPayloadChunk) : null;
            this.taskProgress = taskProgress;
        }

        static Response ack() {
            return new Response(ResponseKind.ACK, null, null, null,
                    0, null, null, false, null, null, null, null);
        }
        static Response profile(StreamWorkerCapabilityProfile profile,
                Map<String, String> sources, Map<String, String> sinks) {
            return new Response(ResponseKind.PROFILE, profile,
                    new LinkedHashMap<>(sources), new LinkedHashMap<>(sinks),
                    0, null, null, false, null, null, null, null);
        }
        static Response dataPort(int port) {
            return new Response(ResponseKind.DATA_PORT, null, null, null,
                    port, null, null, false, null, null, null, null);
        }
        static Response failure(ErrorCode code, String message) {
            return new Response(ResponseKind.ERROR, null, null, null,
                    0, code, bounded(message), false, null, null, null,
                    null);
        }
        static Response taskStatus(boolean failed) {
            return taskStatus(failed, null);
        }
        static Response taskStatus(boolean failed, String failureMessage) {
            return new Response(ResponseKind.TASK_STATUS, null, null, null,
                    0, null, null, failed, bounded(failureMessage), null,
                    null, null);
        }
        static Response taskLifecycle(boolean failed, boolean completed) {
            if (failed && completed) {
                throw new IllegalArgumentException(
                        "failed Stream task cannot be completed");
            }
            return new Response(ResponseKind.TASK_LIFECYCLE, null, null,
                    null, 0, null, null, failed, completed, null,
                    null, null, null);
        }
        static Response checkpointMembers(
                List<StreamCheckpointMember> members) {
            if (members == null
                    || members.size() > MAX_CHECKPOINT_MEMBERS) {
                throw new IllegalArgumentException(
                        "Stream Worker checkpoint members are invalid");
            }
            return new Response(ResponseKind.CHECKPOINT_MEMBERS,
                    null, null, null, 0, null, null, false, null, members,
                    null, null);
        }

        static Response taskEndpoints(List<StreamTaskLaneSpec> endpoints) {
            if (endpoints == null || endpoints.size() > MAX_ROUTES) {
                throw new IllegalArgumentException(
                        "Stream Worker task endpoints are invalid");
            }
            return new Response(ResponseKind.TASK_ENDPOINTS, null, null, null,
                    0, null, null, false, null, null, endpoints, null);
        }

        static Response checkpointPayloadChunk(byte[] chunk) {
            if (chunk == null || chunk.length < 1
                    || chunk.length > io.rowweave.stream.runtime
                    .FileCheckpointPayloadStore.TRANSFER_CHUNK_BYTES) {
                throw new IllegalArgumentException(
                        "Stream checkpoint payload chunk is invalid");
            }
            return new Response(ResponseKind.CHECKPOINT_PAYLOAD_CHUNK,
                    null, null, null, 0, null, null, false, null, null,
                    null, chunk);
        }

        static Response sourceDiscovery(byte[] frame) {
            if (frame == null || frame.length < 1
                    || frame.length > io.rowweave.stream.contract
                    .StreamSplitDiscoveryCodec.MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "Stream source discovery frame is invalid");
            }
            return new Response(ResponseKind.SOURCE_DISCOVERY,
                    null, null, null, 0, null, null, false, null, null,
                    null, frame);
        }

        static Response sourceSplitProgress(byte[] frame) {
            if (frame != null && frame.length
                    > StreamSourceTaskAssignmentCodec.MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "Stream source split progress frame is invalid");
            }
            return new Response(ResponseKind.SOURCE_SPLIT_PROGRESS,
                    null, null, null, 0, null, null, false, null, null,
                    null, frame == null ? new byte[0] : frame);
        }

        static Response taskProgress(StreamTaskProgress progress) {
            if (progress == null) {
                throw new IllegalArgumentException(
                        "Stream task progress is required");
            }
            return new Response(ResponseKind.TASK_PROGRESS,
                    null, null, null, 0, null, null, false, false,
                    null, null, null, null, progress);
        }
    }

    private StreamRemoteWorkerWire() { }

    static void writeRequest(DataOutputStream output, Request request)
            throws IOException {
        if (request == null) throw new IllegalArgumentException("request is required");
        writeFrame(output, requestBody(request), requestVersion(request));
    }

    static Request readRequest(DataInputStream input) throws IOException {
        Frame frame = readFrame(input);
        byte[] bytes = frame.body;
        try (DataInputStream body = new DataInputStream(
                new ByteArrayInputStream(bytes))) {
            Type type = Type.fromWireId(body.readInt());
            if (frame.version == 1
                    && (type == Type.FETCH_CHECKPOINT_PAYLOAD
                    || type == Type.STORE_CHECKPOINT_PAYLOAD
                    || type == Type.DISCOVER_SOURCE
                    || type == Type.ASSIGN_SOURCE_SPLIT
                    || type == Type.REVOKE_SOURCE_SPLIT
                    || type == Type.SOURCE_SPLIT_PROGRESS
                    || type == Type.TASK_LIFECYCLE)) {
                throw invalid("Stream Worker v1 frame carries a v2 opcode");
            }
            if (frame.version < 3 && type == Type.TASK_PROGRESS) {
                throw invalid("Stream Worker pre-v3 frame carries task progress");
            }
            Request result;
            if (type == Type.DESCRIBE) {
                result = Request.describe();
                } else if (type == Type.CANCEL || type == Type.STATUS
                    || type == Type.TASK_LIFECYCLE
                    || type == Type.TASK_PROGRESS
                    || type == Type.CHECKPOINT
                    || type == Type.CHECKPOINT_COMPLETE) {
                    byte[] control = readBytes(body,
                            StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                String operatorUid = null;
                int subtaskIndex = -1;
                int checkpointTimeoutMillis = 2_000;
                byte[] checkpointBundle = null;
                if ((type == Type.CHECKPOINT || type == Type.STATUS
                        || type == Type.TASK_LIFECYCLE
                        || type == Type.TASK_PROGRESS
                        || type == Type.CHECKPOINT_COMPLETE)
                        && body.available() > 0) {
                    operatorUid = readText(body);
                    subtaskIndex = body.readInt();
                    if (subtaskIndex < 0 || subtaskIndex >= 4_096) {
                        throw invalid("Stream Worker task status/checkpoint subtask is invalid");
                    }
                    if (type == Type.CHECKPOINT_COMPLETE
                            && body.available() > 0) {
                        checkpointTimeoutMillis = body.readInt();
                        if (checkpointTimeoutMillis <= 0) {
                            throw invalid(
                                    "Stream Worker checkpoint completion timeout is invalid");
                        }
                        if (frame.version >= 2 && body.available() > 0) {
                            checkpointBundle = readBytes(body,
                                    StreamCheckpointTransferBundleCodec
                                            .MAX_FRAME_BYTES,
                                    true);
                            if (checkpointBundle.length == 0) {
                                checkpointBundle = null;
                            }
                        }
                    }
                }
                result = type == Type.CANCEL ? Request.cancel(control)
                        : type == Type.TASK_LIFECYCLE
                        ? Request.taskLifecycle(control, operatorUid,
                                subtaskIndex)
                        : type == Type.TASK_PROGRESS
                        ? Request.taskProgress(control, operatorUid,
                                subtaskIndex)
                        : type == Type.STATUS ? operatorUid == null
                        ? Request.status(control)
                        : Request.status(control, operatorUid, subtaskIndex)
                        : type == Type.CHECKPOINT ? operatorUid == null
                        ? Request.checkpoint(control)
                        : Request.checkpoint(control, operatorUid, subtaskIndex)
                        : operatorUid == null
                        ? Request.checkpointComplete(control)
                        : Request.checkpointComplete(
                                control, operatorUid, subtaskIndex,
                                checkpointTimeoutMillis, checkpointBundle);
            } else if (type == Type.FETCH_CHECKPOINT_PAYLOAD
                    || type == Type.STORE_CHECKPOINT_PAYLOAD) {
                byte[] control = readBytes(body,
                        StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                byte[] payloadCommand = readBytes(body,
                        StreamCheckpointPayloadCommandCodec.MAX_FRAME_BYTES,
                        false);
                result = type == Type.FETCH_CHECKPOINT_PAYLOAD
                        ? Request.fetchCheckpointPayload(
                        control, payloadCommand)
                        : Request.storeCheckpointPayload(
                        control, payloadCommand);
            } else if (type == Type.DISCOVER_SOURCE) {
                byte[] control = readBytes(body,
                        StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                byte[] artifact = readBytes(body,
                        PlanArtifactCodec.MAX_FRAME_BYTES, false);
                int limit = body.readInt();
                String operatorUid = readText(body);
                byte[] cursor = readBytes(body,
                        io.rowweave.stream.api.StreamSourceDiscoverySpec
                                .HARD_MAX_FRAME_BYTES, true);
                result = Request.discoverSource(control, artifact,
                        operatorUid, cursor, limit);
            } else if (type == Type.ASSIGN_SOURCE_SPLIT
                    || type == Type.REVOKE_SOURCE_SPLIT) {
                byte[] control = readBytes(body,
                        StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                String operatorUid = readText(body);
                int subtaskIndex = body.readInt();
                byte[] assignment = readBytes(body,
                        StreamSourceTaskAssignmentCodec.MAX_FRAME_BYTES,
                        false);
                result = type == Type.ASSIGN_SOURCE_SPLIT
                        ? Request.assignSourceSplit(control, operatorUid,
                                subtaskIndex, assignment)
                        : Request.revokeSourceSplit(control, operatorUid,
                                subtaskIndex, assignment);
            } else if (type == Type.SOURCE_SPLIT_PROGRESS) {
                byte[] control = readBytes(body,
                        StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                String operatorUid = readText(body);
                int subtaskIndex = body.readInt();
                result = Request.sourceSplitProgress(control, operatorUid,
                        subtaskIndex);
            } else {
                byte[] control = readBytes(body,
                        StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                byte[] artifact = readBytes(body,
                        PlanArtifactCodec.MAX_FRAME_BYTES, false);
                int events = body.readInt();
                if (events <= 0 || events > 1_048_576) {
                    throw invalid("Stream Worker event bound is invalid");
                }
                if (type == Type.PREPARE_OPERATOR_TASK
                        || type == Type.START_OPERATOR_TASK
                        || type == Type.RECOVER_CHECKPOINT) {
                    String operatorUid = readText(body);
                    int subtaskIndex = body.readInt();
                    if (subtaskIndex < 0 || subtaskIndex >= 4_096) {
                        throw invalid("Stream Worker subtask index is invalid");
                    }
                    if (type == Type.RECOVER_CHECKPOINT) {
                        byte[] checkpointBundle = frame.version < 2
                                || body.available() == 0
                                ? null : readBytes(body,
                                StreamCheckpointTransferBundleCodec
                                        .MAX_FRAME_BYTES, true);
                        if (checkpointBundle != null
                                && checkpointBundle.length == 0) {
                            checkpointBundle = null;
                        }
                        result = Request.recoverCheckpoint(control, artifact,
                                operatorUid, subtaskIndex, events,
                                checkpointBundle);
                    } else {
                        List<StreamTaskLaneSpec> inputs = readLaneSpecs(body);
                        List<StreamTaskLaneSpec> outputs = readLaneSpecs(body);
                        byte[] checkpointBundle = frame.version < 2
                                || body.available() == 0
                                ? null : readBytes(body,
                                StreamCheckpointTransferBundleCodec
                                        .MAX_FRAME_BYTES, true);
                        if (checkpointBundle != null
                                && checkpointBundle.length == 0) {
                            checkpointBundle = null;
                        }
                        byte[] sourceAssignment = body.available() == 0
                                ? null : readBytes(body,
                                StreamSourceTaskAssignmentCodec.MAX_FRAME_BYTES,
                                true);
                        result = type == Type.PREPARE_OPERATOR_TASK
                            ? Request.prepareOperatorTask(control, artifact,
                            events, operatorUid, subtaskIndex, inputs, outputs,
                            checkpointBundle, sourceAssignment)
                            : Request.startOperatorTask(control, artifact,
                            events, operatorUid, subtaskIndex, inputs, outputs,
                            checkpointBundle, sourceAssignment);
                    }
                } else if (type == Type.START_SINK) {
                    result = Request.startSink(control, artifact, events,
                            readText(body));
                } else if (type == Type.START_SINK_TASK) {
                    result = Request.startSinkTask(
                            control, artifact, events, readText(body));
                } else if (type == Type.START_SOURCE) {
                    int port = body.readInt();
                    if (port <= 0 || port > 65_535) {
                        throw invalid("Stream Worker data port is invalid");
                    }
                    result = Request.startSource(control, artifact, port, events,
                            readText(body));
                } else {
                    int count = body.readInt();
                    if (count <= 1 || count > MAX_ROUTES) {
                        throw invalid("Stream Worker fanout route count is invalid");
                    }
                    Map<String, Integer> ports = new LinkedHashMap<>();
                    for (int index = 0; index < count; index++) {
                        String operatorUid = readText(body);
                        int port = body.readInt();
                        if (port <= 0 || port > 65_535
                                || ports.put(operatorUid, port) != null) {
                            throw invalid("Stream Worker fanout route is invalid");
                        }
                    }
                    result = Request.startSourceFanout(
                            control, artifact, ports, events);
                }
            }
            if (body.available() != 0) {
                throw invalid("Stream Worker request has trailing bytes");
            }
            return result;
        } catch (EOFException truncated) {
            throw invalid("Stream Worker request is truncated");
        }
    }

    static void writeResponse(DataOutputStream output, Response response)
            throws IOException {
        if (response == null) throw new IllegalArgumentException("response is required");
        writeFrame(output, responseBody(response),
                response.kind == ResponseKind.TASK_PROGRESS ? 3
                        : response.kind == ResponseKind.CHECKPOINT_PAYLOAD_CHUNK
                        || response.kind == ResponseKind.SOURCE_DISCOVERY
                        || response.kind
                        == ResponseKind.SOURCE_SPLIT_PROGRESS
                        || response.kind == ResponseKind.TASK_LIFECYCLE
                        ? 2 : 1);
    }

    static Response readResponse(DataInputStream input) throws IOException {
        Frame frame = readFrame(input);
        byte[] bytes = frame.body;
        try (DataInputStream body = new DataInputStream(
                new ByteArrayInputStream(bytes))) {
            ResponseKind kind = ResponseKind.fromWireId(body.readInt());
            if (frame.version == 1
                    && (kind == ResponseKind.CHECKPOINT_PAYLOAD_CHUNK
                    || kind == ResponseKind.SOURCE_DISCOVERY
                    || kind == ResponseKind.SOURCE_SPLIT_PROGRESS
                    || kind == ResponseKind.TASK_LIFECYCLE)) {
                throw invalid("Stream Worker v1 response carries a v2 payload");
            }
            if (frame.version < 3 && kind == ResponseKind.TASK_PROGRESS) {
                throw invalid(
                        "Stream Worker pre-v3 response carries task progress");
            }
            Response result;
            if (kind == ResponseKind.ACK) {
                result = Response.ack();
            } else if (kind == ResponseKind.TASK_STATUS) {
                result = Response.taskStatus(body.readBoolean(), readText(body));
            } else if (kind == ResponseKind.TASK_LIFECYCLE) {
                result = Response.taskLifecycle(body.readBoolean(),
                        body.readBoolean());
            } else if (kind == ResponseKind.TASK_PROGRESS) {
                result = Response.taskProgress(StreamTaskProgressCodec.decode(
                        readBytes(body,
                                StreamTaskProgressCodec.MAX_FRAME_BYTES,
                                false)));
            } else if (kind == ResponseKind.DATA_PORT) {
                int port = body.readInt();
                if (port <= 0 || port > 65_535) {
                    throw invalid("Stream Worker response port is invalid");
                }
                result = Response.dataPort(port);
            } else if (kind == ResponseKind.ERROR) {
                result = Response.failure(
                        ErrorCode.fromWireId(body.readInt()), readText(body));
            } else if (kind == ResponseKind.CHECKPOINT_MEMBERS) {
                int count = body.readInt();
                if (count < 0 || count > MAX_CHECKPOINT_MEMBERS) {
                    throw invalid(
                            "Stream Worker checkpoint member count is invalid");
                }
                List<StreamCheckpointMember> members = new ArrayList<>(count);
                for (int index = 0; index < count; index++) {
                    members.add(StreamCheckpointMemberCodec.decode(readBytes(
                            body, 8 * 1024, false)));
                }
                result = Response.checkpointMembers(members);
            } else if (kind == ResponseKind.TASK_ENDPOINTS) {
                result = Response.taskEndpoints(readLaneSpecs(body));
            } else if (kind == ResponseKind.CHECKPOINT_PAYLOAD_CHUNK) {
                result = Response.checkpointPayloadChunk(readBytes(body,
                        io.rowweave.stream.runtime.FileCheckpointPayloadStore
                                .TRANSFER_CHUNK_BYTES,
                        false));
            } else if (kind == ResponseKind.SOURCE_DISCOVERY) {
                result = Response.sourceDiscovery(readBytes(body,
                        io.rowweave.stream.contract.StreamSplitDiscoveryCodec
                                .MAX_FRAME_BYTES, false));
            } else if (kind == ResponseKind.SOURCE_SPLIT_PROGRESS) {
                result = Response.sourceSplitProgress(readBytes(body,
                        StreamSourceTaskAssignmentCodec.MAX_FRAME_BYTES,
                        true));
            } else {
                StreamWorkerCapabilityProfile profile = readProfile(body);
                result = Response.profile(profile,
                        readContracts(body), readContracts(body));
            }
            if (body.available() != 0) {
                throw invalid("Stream Worker response has trailing bytes");
            }
            return result;
        } catch (EOFException truncated) {
            throw invalid("Stream Worker response is truncated");
        }
    }

    private static void writeLaneSpecs(DataOutputStream output,
            List<StreamTaskLaneSpec> lanes) throws IOException {
        if (lanes == null || lanes.size() > MAX_ROUTES) {
            throw new IllegalArgumentException(
                    "Stream task lane count exceeds its bound");
        }
        output.writeInt(lanes.size());
        java.util.HashSet<String> ids = new java.util.HashSet<>();
        for (StreamTaskLaneSpec lane : lanes) {
            if (lane == null || !ids.add(lane.laneId())) {
                throw new IllegalArgumentException(
                        "Stream task lane identities must be unique");
            }
            writeText(output, lane.laneId());
            output.writeInt(lane.port());
            if (lane.port() > 0) writeText(output, lane.host());
            output.writeInt(lane.maximumFrameBytes());
            output.writeLong(lane.initialCreditBytes());
        }
    }

    private static List<StreamTaskLaneSpec> readLaneSpecs(
            DataInputStream input) throws IOException {
        int count = input.readInt();
        if (count < 0 || count > MAX_ROUTES) {
            throw invalid("Stream Worker task lane count is invalid");
        }
        List<StreamTaskLaneSpec> lanes = new ArrayList<>(count);
        java.util.HashSet<String> ids = new java.util.HashSet<>();
        for (int index = 0; index < count; index++) {
            String laneId = readText(input);
            int port = input.readInt();
            String host = port > 0 ? readText(input) : "";
            int frameBytes = input.readInt();
            long creditBytes = input.readLong();
            if (!ids.add(laneId)) {
                throw invalid("Stream Worker task lane identity is duplicated");
            }
            try {
                lanes.add(port == 0
                        ? StreamTaskLaneSpec.input(laneId, frameBytes,
                        creditBytes)
                        : StreamTaskLaneSpec.endpoint(laneId, host, port,
                        frameBytes, creditBytes));
            } catch (IllegalArgumentException malformed) {
                throw invalid("Stream Worker task lane is invalid");
            }
        }
        return lanes;
    }

    private static byte[] requestBody(Request request) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bytes)) {
                body.writeInt(request.type.wireId);
                if (request.type == Type.CANCEL
                        || request.type == Type.STATUS
                        || request.type == Type.TASK_LIFECYCLE
                        || request.type == Type.TASK_PROGRESS
                        || request.type == Type.CHECKPOINT
                        || request.type == Type.CHECKPOINT_COMPLETE) {
                    writeBytes(body, request.controlFrame,
                            StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                    if ((request.type == Type.CHECKPOINT
                            || request.type == Type.STATUS
                            || request.type == Type.TASK_LIFECYCLE
                            || request.type == Type.TASK_PROGRESS
                            || request.type == Type.CHECKPOINT_COMPLETE)
                            && request.operatorUid != null) {
                        writeText(body, request.operatorUid);
                        body.writeInt(request.subtaskIndex);
                        if (request.type == Type.CHECKPOINT_COMPLETE) {
                            if (request.maximumEvents <= 0) {
                                throw new IllegalArgumentException(
                                        "checkpoint completion timeout is invalid");
                            }
                            body.writeInt(request.maximumEvents);
                            if (request.checkpointBundleFrame != null) {
                                writeBytes(body,
                                        request.checkpointBundleFrame,
                                        StreamCheckpointTransferBundleCodec
                                                .MAX_FRAME_BYTES,
                                        false);
                            }
                        }
                    }
                } else if (request.type == Type.FETCH_CHECKPOINT_PAYLOAD
                        || request.type == Type.STORE_CHECKPOINT_PAYLOAD) {
                    writeBytes(body, request.controlFrame,
                            StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                    writeBytes(body, request.artifactFrame,
                            StreamCheckpointPayloadCommandCodec.MAX_FRAME_BYTES,
                            false);
                } else if (request.type == Type.DISCOVER_SOURCE) {
                    writeBytes(body, request.controlFrame,
                            StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                    writeBytes(body, request.artifactFrame,
                            PlanArtifactCodec.MAX_FRAME_BYTES, false);
                    body.writeInt(request.discoveryLimit);
                    writeText(body, request.operatorUid);
                    writeBytes(body, request.discoveryCursorFrame,
                            io.rowweave.stream.api.StreamSourceDiscoverySpec
                                    .HARD_MAX_FRAME_BYTES, true);
                } else if (request.type == Type.ASSIGN_SOURCE_SPLIT
                        || request.type == Type.REVOKE_SOURCE_SPLIT) {
                    writeBytes(body, request.controlFrame,
                            StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                    writeText(body, request.operatorUid);
                    body.writeInt(request.subtaskIndex);
                    writeBytes(body, request.sourceAssignmentFrame,
                            StreamSourceTaskAssignmentCodec.MAX_FRAME_BYTES,
                            false);
                } else if (request.type == Type.SOURCE_SPLIT_PROGRESS) {
                    writeBytes(body, request.controlFrame,
                            StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                    writeText(body, request.operatorUid);
                    body.writeInt(request.subtaskIndex);
                } else if (request.type == Type.PREPARE_OPERATOR_TASK
                        || request.type == Type.START_OPERATOR_TASK
                        || request.type == Type.RECOVER_CHECKPOINT) {
                    writeBytes(body, request.controlFrame,
                            StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                    writeBytes(body, request.artifactFrame,
                            PlanArtifactCodec.MAX_FRAME_BYTES, false);
                    if (request.maximumEvents <= 0
                            || request.maximumEvents > 1_048_576
                            || request.operatorUid == null
                            || request.subtaskIndex < 0
                            || request.subtaskIndex >= 4_096) {
                        throw new IllegalArgumentException(
                                "Stream operator task request is invalid");
                    }
                    body.writeInt(request.maximumEvents);
                    writeText(body, request.operatorUid);
                    body.writeInt(request.subtaskIndex);
                    if (request.type != Type.RECOVER_CHECKPOINT) {
                        writeLaneSpecs(body, request.inputLanes);
                        writeLaneSpecs(body, request.outputLanes);
                    }
                    if (request.checkpointBundleFrame != null
                            || request.sourceAssignmentFrame != null) {
                        writeBytes(body, request.checkpointBundleFrame == null
                                        ? new byte[0]
                                        : request.checkpointBundleFrame,
                                StreamCheckpointTransferBundleCodec
                                                .MAX_FRAME_BYTES,
                                true);
                        if (request.sourceAssignmentFrame != null) {
                            writeBytes(body, request.sourceAssignmentFrame,
                                    StreamSourceTaskAssignmentCodec
                                            .MAX_FRAME_BYTES, true);
                        }
                    }
                } else if (request.type == Type.START_SINK
                        || request.type == Type.START_SOURCE
                        || request.type == Type.START_SINK_TASK
                        || request.type == Type.START_SOURCE_FANOUT) {
                    writeBytes(body, request.controlFrame,
                            StreamControlMessageCodec.MAX_FRAME_BYTES, false);
                    writeBytes(body, request.artifactFrame,
                            PlanArtifactCodec.MAX_FRAME_BYTES, false);
                    if (request.maximumEvents <= 0
                            || request.maximumEvents > 1_048_576) {
                        throw new IllegalArgumentException(
                                "Stream Worker event bound is invalid");
                    }
                    body.writeInt(request.maximumEvents);
                    if (request.type == Type.START_SINK) {
                        writeText(body, request.operatorUid);
                    } else if (request.type == Type.START_SOURCE) {
                        if (request.dataPort <= 0 || request.dataPort > 65_535) {
                            throw new IllegalArgumentException(
                                    "Stream Worker data port is invalid");
                        }
                        body.writeInt(request.dataPort);
                        writeText(body, request.operatorUid);
                    } else if (request.type == Type.START_SINK_TASK) {
                        writeText(body, request.operatorUid);
                    } else if (request.type == Type.START_SOURCE_FANOUT) {
                        if (request.dataPorts.size() <= 1
                                || request.dataPorts.size() > MAX_ROUTES) {
                            throw new IllegalArgumentException(
                                    "Stream Worker fanout route count is invalid");
                        }
                        body.writeInt(request.dataPorts.size());
                        for (Map.Entry<String, Integer> route
                                : request.dataPorts.entrySet()) {
                            writeText(body, route.getKey());
                            int port = route.getValue() == null
                                    ? 0 : route.getValue();
                            if (port <= 0 || port > 65_535) {
                                throw new IllegalArgumentException(
                                        "Stream Worker fanout route is invalid");
                            }
                            body.writeInt(port);
                        }
                    }
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static byte[] responseBody(Response response) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bytes)) {
                body.writeInt(response.kind.wireId);
                if (response.kind == ResponseKind.PROFILE) {
                    writeProfile(body, response.profile);
                    writeContracts(body, response.sourceContracts);
                    writeContracts(body, response.sinkContracts);
                } else if (response.kind == ResponseKind.DATA_PORT) {
                    body.writeInt(response.dataPort);
                } else if (response.kind == ResponseKind.TASK_STATUS) {
                    body.writeBoolean(response.taskFailed);
                    writeText(body, response.taskFailureMessage);
                } else if (response.kind == ResponseKind.TASK_LIFECYCLE) {
                    body.writeBoolean(response.taskFailed);
                    body.writeBoolean(response.taskCompleted);
                } else if (response.kind == ResponseKind.TASK_PROGRESS) {
                    writeBytes(body, StreamTaskProgressCodec.encode(
                                    response.taskProgress),
                            StreamTaskProgressCodec.MAX_FRAME_BYTES, false);
                } else if (response.kind
                        == ResponseKind.CHECKPOINT_MEMBERS) {
                    body.writeInt(response.checkpointMembers.size());
                    for (StreamCheckpointMember member
                            : response.checkpointMembers) {
                        writeBytes(body, StreamCheckpointMemberCodec.encode(member),
                                8 * 1024, false);
                    }
                } else if (response.kind == ResponseKind.TASK_ENDPOINTS) {
                    writeLaneSpecs(body, response.taskEndpoints);
                } else if (response.kind
                        == ResponseKind.CHECKPOINT_PAYLOAD_CHUNK) {
                    writeBytes(body, response.checkpointPayloadChunk,
                            io.rowweave.stream.runtime.FileCheckpointPayloadStore
                                    .TRANSFER_CHUNK_BYTES,
                            false);
                } else if (response.kind
                        == ResponseKind.SOURCE_DISCOVERY) {
                    writeBytes(body, response.sourceDiscoveryFrame,
                            io.rowweave.stream.contract
                                    .StreamSplitDiscoveryCodec.MAX_FRAME_BYTES,
                            false);
                } else if (response.kind
                        == ResponseKind.SOURCE_SPLIT_PROGRESS) {
                    writeBytes(body, response.sourceSplitProgressFrame,
                            StreamSourceTaskAssignmentCodec.MAX_FRAME_BYTES,
                            true);
                } else if (response.kind == ResponseKind.ERROR) {
                    body.writeInt(response.errorCode.wireId);
                    writeText(body, response.errorMessage);
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void writeProfile(DataOutputStream output,
            StreamWorkerCapabilityProfile profile) throws IOException {
        if (profile == null) throw new IllegalArgumentException("profile is required");
        writeText(output, profile.workerId());
        writeText(output, profile.workerIncarnation());
        output.writeInt(profile.controlProtocolVersion());
        output.writeInt(profile.dataProtocolVersion());
        output.writeInt(profile.capabilities().size());
        for (String capability : profile.capabilities()) writeText(output, capability);
        output.writeInt(profile.taskSlots());
        output.writeInt(profile.channelCapacity());
        output.writeInt(profile.maximumFrameBytes());
        output.writeLong(profile.maximumResidentBytes());
        output.writeInt(profile.maximumThreads());
        output.writeInt(profile.maximumTimerCount());
        output.writeInt(profile.maximumWindowCount());
        output.writeLong(profile.maximumCheckpointBytes());
        output.writeLong(profile.maximumSpillDiskBytes());
        output.writeInt(profile.maximumFileDescriptorCount());
    }

    private static StreamWorkerCapabilityProfile readProfile(
            DataInputStream input) throws IOException {
        String workerId = readText(input);
        String incarnation = readText(input);
        int controlVersion = input.readInt();
        int dataVersion = input.readInt();
        int count = input.readInt();
        if (count <= 0 || count > 256) {
            throw invalid("Stream Worker capability count is invalid");
        }
        List<String> capabilities = new ArrayList<>(count);
        for (int index = 0; index < count; index++) {
            capabilities.add(readText(input));
        }
        int taskSlots = input.readInt();
        int channelCapacity = input.readInt();
        int maximumFrameBytes = input.readInt();
        long maximumResidentBytes = input.readLong();
        int maximumThreads = input.readInt();
        int maximumTimerCount = input.readInt();
        int maximumWindowCount = input.readInt();
        long maximumCheckpointBytes = input.readLong();
        long maximumSpillDiskBytes = input.readLong();
        int maximumFileDescriptorCount = input.readInt();
        return new StreamWorkerCapabilityProfile(
                workerId, incarnation, controlVersion, dataVersion,
                capabilities, taskSlots, channelCapacity,
                maximumFrameBytes, maximumResidentBytes, maximumThreads,
                maximumTimerCount, maximumWindowCount,
                maximumCheckpointBytes, maximumSpillDiskBytes,
                maximumFileDescriptorCount);
    }

    private static void writeContracts(DataOutputStream output,
            Map<String, String> contracts) throws IOException {
        if (contracts == null || contracts.size() > MAX_PROVIDERS) {
            throw new IllegalArgumentException(
                    "Stream Worker provider registry is out of bounds");
        }
        output.writeInt(contracts.size());
        for (Map.Entry<String, String> entry : contracts.entrySet()) {
            writeText(output, entry.getKey());
            writeText(output, entry.getValue());
        }
    }

    private static Map<String, String> readContracts(DataInputStream input)
            throws IOException {
        int count = input.readInt();
        if (count < 0 || count > MAX_PROVIDERS) {
            throw invalid("Stream Worker provider count is invalid");
        }
        Map<String, String> result = new LinkedHashMap<>();
        for (int index = 0; index < count; index++) {
            String key = readText(input);
            String digest = readText(input);
            if (result.put(key, digest) != null) {
                throw invalid("Stream Worker provider key is duplicated");
            }
        }
        return result;
    }

    private static int requestVersion(Request request) {
        if (request.type == Type.TASK_PROGRESS) return 3;
        return request.type == Type.FETCH_CHECKPOINT_PAYLOAD
                || request.type == Type.STORE_CHECKPOINT_PAYLOAD
                || request.type == Type.DISCOVER_SOURCE
                || request.type == Type.ASSIGN_SOURCE_SPLIT
                || request.type == Type.REVOKE_SOURCE_SPLIT
                || request.type == Type.SOURCE_SPLIT_PROGRESS
                || request.type == Type.TASK_LIFECYCLE
                || request.checkpointBundleFrame != null
                || request.sourceAssignmentFrame != null ? 2 : 1;
    }

    private static void writeFrame(DataOutputStream output, byte[] body,
            int version) throws IOException {
        byte[] magic;
        if (version == 1) {
            magic = MAGIC_V1;
        } else if (version == 2) {
            magic = MAGIC_V2;
        } else if (version == 3) {
            magic = MAGIC_V3;
        } else {
            throw new IllegalArgumentException(
                    "Stream Worker frame version is unsupported");
        }
        long length = magic.length + 8L + body.length + DIGEST_BYTES;
        if (length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("Stream Worker frame exceeds limit");
        }
        output.writeInt((int) length);
        output.write(magic);
        output.writeInt(version);
        output.writeInt(body.length);
        output.write(body);
        output.write(sha256(body));
        output.flush();
    }

    private static Frame readFrame(DataInputStream input) throws IOException {
        int length = input.readInt();
        int minimum = MAGIC_V1.length + 8 + DIGEST_BYTES;
        if (length < minimum || length > MAX_FRAME_BYTES) {
            throw invalid("Stream Worker frame length is invalid");
        }
        byte[] frame = new byte[length];
        input.readFully(frame);
        try (DataInputStream value = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC_V1.length];
            value.readFully(magic);
            int version = value.readInt();
            int bodyLength = value.readInt();
            boolean supportedHeader = version == 1
                    && MessageDigest.isEqual(MAGIC_V1, magic)
                    || version == 2
                    && MessageDigest.isEqual(MAGIC_V2, magic)
                    || version == 3
                    && MessageDigest.isEqual(MAGIC_V3, magic);
            if (!supportedHeader
                    || bodyLength < 0
                    || MAGIC_V1.length + 8L + bodyLength
                    + DIGEST_BYTES != length) {
                throw invalid("Stream Worker frame header is invalid");
            }
            byte[] body = new byte[bodyLength];
            value.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            value.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(body))) {
                throw invalid("Stream Worker frame checksum differs");
            }
            return new Frame(version, body);
        }
    }

    private static final class Frame {
        private final int version;
        private final byte[] body;

        private Frame(int version, byte[] body) {
            this.version = version;
            this.body = body;
        }
    }

    private static void writeBytes(DataOutputStream output, byte[] value,
            int maximum, boolean emptyAllowed) throws IOException {
        if (value == null || value.length > maximum
                || (!emptyAllowed && value.length == 0)) {
            throw new IllegalArgumentException("Stream Worker bytes are invalid");
        }
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readBytes(DataInputStream input,
            int maximum, boolean emptyAllowed) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > maximum || (!emptyAllowed && length == 0)
                || length > input.available()) {
            throw invalid("Stream Worker byte field length is invalid");
        }
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        if (value == null) throw new IllegalArgumentException("text is required");
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException("Stream Worker text is invalid");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_TEXT_BYTES || length > input.available()) {
            throw invalid("Stream Worker text length is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
        } catch (CharacterCodingException malformed) {
            throw invalid("Stream Worker text is malformed UTF-8");
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static byte[] copyBytes(byte[] value) {
        return value == null ? null : value.clone();
    }

    private static String bounded(String message) {
        if (message == null || message.isEmpty()) return "Stream Worker request failed";
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_TEXT_BYTES) return message;
        return "Stream Worker request failed";
    }

    private static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException(message);
    }
}
