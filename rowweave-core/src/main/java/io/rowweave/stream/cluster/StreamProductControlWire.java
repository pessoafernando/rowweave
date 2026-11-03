package io.rowweave.stream.cluster;

import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamControlException;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamSubmissionId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Independent, checksummed and bounded Stream product control protocol v1. */
final class StreamProductControlWire {
    private static final byte[] MAGIC =
            "RWXXSCJ1".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_TEXT_BYTES = 1_024;
    static final int MAX_FRAME_BYTES =
            PlanArtifactCodec.MAX_FRAME_BYTES + 32 * 1_024;

    enum Type {
        SUBMIT(1), ATTACH_SUBMISSION(2), ATTACH_JOB(3),
        CANCEL(4), ATTACH_CANCEL(5), TRIGGER_CHECKPOINT(6),
        ATTACH_CHECKPOINT(7), STOP_WITH_SAVEPOINT(8),
        ATTACH_SAVEPOINT(9), RESTORE(10), OFFLINE_RESCALE(11),
        RESCALE(12), ATTACH_RESCALE(13), ATTACH_EVIDENCE(14);

        private final int wireId;
        Type(int wireId) { this.wireId = wireId; }
        static Type fromWireId(int wireId) {
            for (Type type : values()) {
                if (type.wireId == wireId) return type;
            }
            throw invalid("unknown Stream control request type");
        }
    }

    static final class Request {
        final Type type;
        final String firstId;
        final String secondId;
        final byte[] artifact;

        private Request(Type type, String firstId, String secondId,
                byte[] artifact) {
            this.type = type;
            this.firstId = firstId;
            this.secondId = secondId;
            this.artifact = artifact == null ? null : artifact.clone();
        }

        static Request submit(String submissionId, byte[] artifact) {
            return new Request(Type.SUBMIT, submissionId, null, artifact);
        }
        static Request attachSubmission(String submissionId) {
            return new Request(Type.ATTACH_SUBMISSION, submissionId, null, null);
        }
        static Request attachJob(String jobId) {
            return new Request(Type.ATTACH_JOB, jobId, null, null);
        }
        static Request cancel(String jobId, String commandId) {
            return new Request(Type.CANCEL, jobId, commandId, null);
        }
        static Request attachCancel(String jobId, String commandId) {
            return new Request(Type.ATTACH_CANCEL, jobId, commandId, null);
        }
        static Request triggerCheckpoint(String jobId, String commandId) {
            return new Request(
                    Type.TRIGGER_CHECKPOINT, jobId, commandId, null);
        }
        static Request attachCheckpoint(String jobId, String commandId) {
            return new Request(
                    Type.ATTACH_CHECKPOINT, jobId, commandId, null);
        }
        static Request stopWithSavepoint(
                String jobId, String commandId, byte[] requestFrame) {
            return new Request(Type.STOP_WITH_SAVEPOINT,
                    jobId, commandId, requestFrame);
        }
        static Request attachSavepoint(String jobId, String commandId) {
            return new Request(
                    Type.ATTACH_SAVEPOINT, jobId, commandId, null);
        }
        static Request restore(String submissionId, byte[] payload) {
            return new Request(Type.RESTORE, submissionId, null, payload);
        }
        static Request offlineRescale(String submissionId, byte[] payload) {
            return new Request(
                    Type.OFFLINE_RESCALE, submissionId, null, payload);
        }
        static Request rescale(
                String jobId, String commandId, byte[] requestFrame) {
            return new Request(Type.RESCALE, jobId, commandId, requestFrame);
        }
        static Request attachRescale(String jobId, String commandId) {
            return new Request(Type.ATTACH_RESCALE, jobId, commandId, null);
        }
        static Request attachEvidence(String jobId) {
            return new Request(Type.ATTACH_EVIDENCE, jobId, null, null);
        }
    }

    static final class Response {
        final StreamProductJobView view;
        final StreamControlException.Code errorCode;
        final String errorMessage;

        private Response(StreamProductJobView view,
                StreamControlException.Code errorCode, String errorMessage) {
            this.view = view;
            this.errorCode = errorCode;
            this.errorMessage = errorMessage;
        }

        static Response success(StreamProductJobView view) {
            return new Response(view, null, null);
        }
        static Response failure(
                StreamControlException.Code code, String message) {
            return new Response(null, code, boundedMessage(message));
        }
        boolean success() { return view != null; }
    }

    private StreamProductControlWire() { }

    static void writeRequest(DataOutputStream output, Request request)
            throws IOException {
        if (request == null) throw new IllegalArgumentException("request is required");
        writeTransportFrame(output, requestBody(request));
    }

    static Request readRequest(DataInputStream input) throws IOException {
        byte[] body = readTransportFrame(input);
        try (DataInputStream value = new DataInputStream(
                new ByteArrayInputStream(body))) {
            Type type = Type.fromWireId(value.readInt());
            String first = readText(value);
            String second = null;
            byte[] artifact = null;
            if (hasCommandId(type)) {
                second = readText(value);
                if (type == Type.STOP_WITH_SAVEPOINT) {
                    artifact = readPayload(value,
                            StreamSavepointRequestCodec
                                    .MAX_CONFIGURATION_BYTES + 272,
                            "Stream savepoint request length is invalid");
                } else if (type == Type.RESCALE) {
                    artifact = readPayload(value,
                            StreamLiveRescaleRequestCodec.FRAME_BYTES,
                            "Stream live rescale request length is invalid");
                }
            } else if (type == Type.SUBMIT) {
                artifact = readPayload(value,
                        PlanArtifactCodec.MAX_FRAME_BYTES,
                        "Stream product Artifact length is invalid");
            } else if (type == Type.RESTORE
                    || type == Type.OFFLINE_RESCALE) {
                artifact = readPayload(value,
                        StreamRecoverySubmissionCodec.MAX_FRAME_BYTES,
                        "Stream recovery submission length is invalid");
            }
            if (value.available() != 0) {
                throw invalid("Stream product request has trailing bytes");
            }
            validateRequestIds(type, first, second);
            return new Request(type, first, second, artifact);
        }
    }

    static void writeResponse(DataOutputStream output, Response response)
            throws IOException {
        if (response == null) throw new IllegalArgumentException("response is required");
        writeTransportFrame(output, responseBody(response));
    }

    static Response readResponse(DataInputStream input) throws IOException {
        byte[] body = readTransportFrame(input);
        try (DataInputStream value = new DataInputStream(
                new ByteArrayInputStream(body))) {
            boolean success = value.readBoolean();
            Response result;
            if (!success) {
                StreamControlException.Code code;
                try {
                    code = StreamControlException.Code.fromWireId(
                            value.readInt());
                } catch (IllegalArgumentException unknown) {
                    throw invalid("unknown Stream control error code");
                }
                result = Response.failure(code, readText(value));
            } else {
                result = Response.success(readView(value));
            }
            if (value.available() != 0) {
                throw invalid("Stream product response has trailing bytes");
            }
            return result;
        }
    }

    private static byte[] requestBody(Request request) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeInt(request.type.wireId);
                writeText(output, request.firstId);
                if (hasCommandId(request.type)) {
                    writeText(output, request.secondId);
                    if (request.type == Type.STOP_WITH_SAVEPOINT) {
                        writePayload(output, request.artifact,
                                StreamSavepointRequestCodec
                                        .MAX_CONFIGURATION_BYTES + 272,
                                "Stream savepoint request is out of bounds");
                    } else if (request.type == Type.RESCALE) {
                        writePayload(output, request.artifact,
                                StreamLiveRescaleRequestCodec.FRAME_BYTES,
                                "Stream live rescale request is out of bounds");
                    }
                } else if (request.type == Type.SUBMIT) {
                    writePayload(output, request.artifact,
                            PlanArtifactCodec.MAX_FRAME_BYTES,
                            "Stream product Artifact is out of bounds");
                } else if (request.type == Type.RESTORE
                        || request.type == Type.OFFLINE_RESCALE) {
                    writePayload(output, request.artifact,
                            StreamRecoverySubmissionCodec.MAX_FRAME_BYTES,
                            "Stream recovery submission is out of bounds");
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
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.writeBoolean(response.success());
                if (!response.success()) {
                    output.writeInt(response.errorCode.wireId());
                    writeText(output, response.errorMessage);
                } else {
                    writeView(output, response.view);
                }
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static void writeView(
            DataOutputStream output, StreamProductJobView view)
            throws IOException {
        writeText(output, view.submissionId().value());
        writeText(output, view.jobId().value());
        writeText(output, view.artifactId());
        output.writeInt(view.status().wireId());
        writeProgress(output, view.progress());
        output.writeBoolean(view.cancelCommandId() != null);
        if (view.cancelCommandId() != null) {
            writeText(output, view.cancelCommandId().value());
            output.writeInt(view.cancelStatus().wireId());
        }
        output.writeBoolean(view.checkpointCommandId() != null);
        if (view.checkpointCommandId() != null) {
            writeText(output, view.checkpointCommandId().value());
            writeText(output, view.checkpointId());
            output.writeInt(view.checkpointStatus().wireId());
        }
        if (view.savepointCommandId() != null) {
            output.writeBoolean(true);
            writeText(output, view.savepointCommandId().value());
            writeText(output, view.savepointId());
            output.writeInt(view.savepointStatus().wireId());
        }
        if (view.rescaleCommandId() != null) {
            if (view.savepointCommandId() == null) output.writeBoolean(false);
            output.writeBoolean(true);
            writeText(output, view.rescaleCommandId().value());
            writeText(output, view.rescaleId());
            output.writeInt(view.targetParallelism());
            output.writeInt(view.rescaleStatus().wireId());
        }
        if (view.evidence() != null) {
            if (view.savepointCommandId() == null) output.writeBoolean(false);
            if (view.rescaleCommandId() == null) output.writeBoolean(false);
            output.writeBoolean(true);
            writeEvidence(output, view.evidence());
        }
    }

    private static StreamProductJobView readView(DataInputStream input)
            throws IOException {
        StreamSubmissionId submissionId =
                StreamSubmissionId.of(readText(input));
        StreamJobId jobId = StreamJobId.of(readText(input));
        String artifactId = readText(input);
        StreamJobStatus status = jobStatus(input.readInt());
        StreamJobProgress progress = readProgress(input);
        StreamCommandId cancelId = null;
        StreamOperationStatus cancelStatus = null;
        if (input.available() > 0 && input.readBoolean()) {
            cancelId = StreamCommandId.of(readText(input));
            try {
                cancelStatus = StreamOperationStatus.fromWireId(
                        input.readInt());
            } catch (IllegalArgumentException unknown) {
                throw invalid("unknown Stream operation status");
            }
        }
        StreamCommandId checkpointCommandId = null;
        String checkpointId = null;
        StreamOperationStatus checkpointStatus = null;
        if (input.readBoolean()) {
            checkpointCommandId = StreamCommandId.of(readText(input));
            checkpointId = readText(input);
            try {
                checkpointStatus = StreamOperationStatus.fromWireId(
                        input.readInt());
            } catch (IllegalArgumentException unknown) {
                throw invalid("unknown checkpoint operation status");
            }
        }
        StreamCommandId savepointCommandId = null;
        String savepointId = null;
        StreamOperationStatus savepointStatus = null;
        if (input.available() > 0 && input.readBoolean()) {
            savepointCommandId = StreamCommandId.of(readText(input));
            savepointId = readText(input);
            try {
                savepointStatus = StreamOperationStatus.fromWireId(
                        input.readInt());
            } catch (IllegalArgumentException unknown) {
                throw invalid("unknown savepoint operation status");
            }
        }
        StreamCommandId rescaleCommandId = null;
        String rescaleId = null;
        int targetParallelism = 0;
        StreamOperationStatus rescaleStatus = null;
        if (input.available() > 0 && input.readBoolean()) {
            rescaleCommandId = StreamCommandId.of(readText(input));
            rescaleId = readText(input);
            targetParallelism = input.readInt();
            try {
                rescaleStatus = StreamOperationStatus.fromWireId(
                        input.readInt());
            } catch (IllegalArgumentException unknown) {
                throw invalid("unknown rescale operation status");
            }
        }
        StreamJobEvidence evidence = null;
        if (input.available() > 0 && input.readBoolean()) {
            evidence = readEvidence(input, jobId);
        }
        return new StreamProductJobView(
                submissionId, jobId, artifactId, status, progress,
                cancelId, cancelStatus, checkpointCommandId, checkpointId,
                checkpointStatus, savepointCommandId, savepointId,
                savepointStatus, rescaleCommandId, rescaleId,
                targetParallelism, rescaleStatus, evidence);
    }

    private static void writeProgress(
            DataOutputStream output, StreamJobProgress progress)
            throws IOException {
        output.writeInt(progress.attempt());
        output.writeLong(progress.completedCheckpoints());
        output.writeLong(progress.operatorWatermark());
        output.writeInt(progress.cdcPhase().wireId());
        output.writeInt(progress.restarts());
        output.writeLong(progress.recordsRead());
        output.writeLong(progress.recordsWritten());
        output.writeLong(progress.bytesRead());
        output.writeLong(progress.bytesWritten());
        output.writeInt(progress.mailboxHighWaterEvents());
        output.writeLong(progress.channelHighWaterBytes());
        output.writeLong(progress.managedStateBytes());
        output.writeInt(progress.timerHighWaterCount());
        output.writeInt(progress.windowHighWaterCount());
        output.writeLong(progress.backpressuredMillis());
    }

    private static StreamJobProgress readProgress(DataInputStream input)
            throws IOException {
        return new StreamJobProgress(
                input.readInt(), input.readLong(), input.readLong(),
                cdcPhase(input.readInt()), input.readInt(),
                input.readLong(), input.readLong(), input.readLong(),
                input.readLong(), input.readInt(), input.readLong(),
                input.readLong(), input.readInt(), input.readInt(),
                input.readLong());
    }

    private static void writeEvidence(
            DataOutputStream output, StreamJobEvidence evidence)
            throws IOException {
        output.writeBoolean(evidence.available());
        output.writeLong(evidence.observedAtEpochMillis());
        if (!evidence.available()) {
            writeEvidenceProgress(output, evidence.lifetimeProgress());
            return;
        }
        writeText(output, evidence.runId());
        writeText(output, evidence.attemptId());
        output.writeInt(evidence.attemptOrdinal());
        output.writeLong(evidence.fenceToken());
        output.writeLong(evidence.coordinatorTerm());
        output.writeLong(evidence.assignmentGeneration());
        output.writeBoolean(evidence.latestCompletedCheckpointId() != null);
        if (evidence.latestCompletedCheckpointId() != null) {
            writeText(output, evidence.latestCompletedCheckpointId());
            output.writeLong(evidence.latestCompletedCheckpointSequence());
            output.writeLong(evidence.latestCheckpointDurationMillis());
            output.writeLong(evidence.latestCheckpointBytes());
        }
        writeEvidenceProgress(output, evidence.attemptProgress());
        writeEvidenceProgress(output, evidence.lifetimeProgress());
        output.writeInt(evidence.placementTaskCount());
        output.writeInt(evidence.reportedTaskCount());
    }

    private static StreamJobEvidence readEvidence(
            DataInputStream input, StreamJobId jobId) throws IOException {
        boolean available = input.readBoolean();
        long observedAtEpochMillis = input.readLong();
        if (!available) {
            return StreamJobEvidence.unavailable(jobId,
                    readEvidenceProgress(input), observedAtEpochMillis);
        }
        String runId = readText(input);
        String attemptId = readText(input);
        int attemptOrdinal = input.readInt();
        long fenceToken = input.readLong();
        long coordinatorTerm = input.readLong();
        long assignmentGeneration = input.readLong();
        String checkpointId = null;
        long checkpointSequence = 0L;
        long checkpointDurationMillis = 0L;
        long checkpointBytes = 0L;
        if (input.readBoolean()) {
            checkpointId = readText(input);
            checkpointSequence = input.readLong();
            checkpointDurationMillis = input.readLong();
            checkpointBytes = input.readLong();
        }
        StreamJobProgress attemptProgress = readEvidenceProgress(input);
        StreamJobProgress lifetimeProgress = readEvidenceProgress(input);
        return new StreamJobEvidence(jobId, runId, attemptId,
                attemptOrdinal, fenceToken, coordinatorTerm,
                assignmentGeneration, checkpointId, checkpointSequence,
                checkpointDurationMillis, checkpointBytes,
                attemptProgress, lifetimeProgress, input.readInt(),
                input.readInt(), observedAtEpochMillis);
    }

    private static void writeEvidenceProgress(
            DataOutputStream output, StreamJobProgress progress)
            throws IOException {
        output.writeInt(progress.attempt());
        output.writeLong(progress.completedCheckpoints());
        output.writeLong(progress.operatorWatermark());
        output.writeInt(progress.cdcPhase().wireId());
        output.writeInt(progress.restarts());
        output.writeLong(progress.recordsRead());
        output.writeLong(progress.recordsWritten());
        output.writeLong(progress.bytesRead());
        output.writeLong(progress.bytesWritten());
        output.writeLong(progress.backlogEvents());
        output.writeLong(progress.backlogBytes());
        output.writeInt(progress.mailboxHighWaterEvents());
        output.writeLong(progress.channelHighWaterBytes());
        output.writeLong(progress.managedStateBytes());
        output.writeInt(progress.timerHighWaterCount());
        output.writeInt(progress.windowHighWaterCount());
        output.writeLong(progress.backpressuredMillis());
    }

    private static StreamJobProgress readEvidenceProgress(
            DataInputStream input) throws IOException {
        return new StreamJobProgress(
                input.readInt(), input.readLong(), input.readLong(),
                cdcPhase(input.readInt()), input.readInt(),
                input.readLong(), input.readLong(), input.readLong(),
                input.readLong(), input.readLong(), input.readLong(),
                input.readInt(), input.readLong(), input.readLong(),
                input.readInt(), input.readInt(), input.readLong());
    }

    private static void writeTransportFrame(
            DataOutputStream output, byte[] body) throws IOException {
        long frameLength = MAGIC.length + 8L + body.length + DIGEST_BYTES;
        if (frameLength > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "Stream product control frame exceeds its bound");
        }
        output.writeInt((int) frameLength);
        output.write(MAGIC);
        output.writeInt(VERSION);
        output.writeInt(body.length);
        output.write(body);
        output.write(sha256(body));
        output.flush();
    }

    private static byte[] readTransportFrame(DataInputStream input)
            throws IOException {
        int frameLength = input.readInt();
        int minimum = MAGIC.length + 8 + DIGEST_BYTES;
        if (frameLength < minimum || frameLength > MAX_FRAME_BYTES) {
            throw invalid("Stream product control frame length is invalid");
        }
        byte[] frame = new byte[frameLength];
        input.readFully(frame);
        try (DataInputStream value = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            value.readFully(magic);
            int version = value.readInt();
            int bodyLength = value.readInt();
            if (!MessageDigest.isEqual(MAGIC, magic) || version != VERSION
                    || bodyLength < 0
                    || MAGIC.length + 8L + bodyLength + DIGEST_BYTES
                    != frame.length) {
                throw invalid("Stream product control frame header is invalid");
            }
            byte[] body = new byte[bodyLength];
            value.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            value.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(body))) {
                throw invalid("Stream product control checksum differs");
            }
            return body;
        }
    }

    private static void validateRequestIds(
            Type type, String first, String second) {
        if (type == Type.SUBMIT || type == Type.ATTACH_SUBMISSION
                || type == Type.RESTORE
                || type == Type.OFFLINE_RESCALE) {
            StreamSubmissionId.of(first);
        } else {
            StreamJobId.of(first);
        }
        if (hasCommandId(type)) {
            StreamCommandId.of(second);
        }
    }

    private static boolean hasCommandId(Type type) {
        return type == Type.CANCEL || type == Type.ATTACH_CANCEL
                || type == Type.TRIGGER_CHECKPOINT
                || type == Type.ATTACH_CHECKPOINT
                || type == Type.STOP_WITH_SAVEPOINT
                || type == Type.ATTACH_SAVEPOINT
                || type == Type.RESCALE
                || type == Type.ATTACH_RESCALE;
    }

    private static byte[] readPayload(
            DataInputStream input, int maximum, String message)
            throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maximum || length > input.available()) {
            throw invalid(message);
        }
        byte[] result = new byte[length];
        input.readFully(result);
        return result;
    }

    private static void writePayload(
            DataOutputStream output, byte[] payload, int maximum,
            String message) throws IOException {
        if (payload == null || payload.length == 0
                || payload.length > maximum) {
            throw new IllegalArgumentException(message);
        }
        output.writeInt(payload.length);
        output.write(payload);
    }

    private static StreamJobStatus jobStatus(int wireId) {
        for (StreamJobStatus status : StreamJobStatus.values()) {
            if (status.wireId() == wireId) return status;
        }
        throw invalid("unknown Stream job status");
    }

    private static StreamCdcPhase cdcPhase(int wireId) {
        for (StreamCdcPhase phase : StreamCdcPhase.values()) {
            if (phase.wireId() == wireId) return phase;
        }
        throw invalid("unknown Stream CDC phase");
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        if (value == null) {
            throw new IllegalArgumentException("Stream control text is required");
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= 0 || bytes.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "Stream control text exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > MAX_TEXT_BYTES) {
            throw invalid("Stream control text exceeds its bound");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        try {
            String result = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes)).toString();
            if (!MessageDigest.isEqual(
                    bytes, result.getBytes(StandardCharsets.UTF_8))) {
                throw invalid("Stream control text is noncanonical");
            }
            return result;
        } catch (java.nio.charset.CharacterCodingException malformed) {
            throw invalid("Stream control text is invalid UTF-8");
        }
    }

    private static String boundedMessage(String message) {
        String value = message == null || message.isEmpty()
                ? "Stream control operation failed" : message;
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length <= MAX_TEXT_BYTES) return value;
        return "Stream product control failure";
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static StreamProductControlException invalid(String message) {
        return new StreamProductControlException(
                StreamProductControlException.Code.INVALID_FRAME, message);
    }
}
