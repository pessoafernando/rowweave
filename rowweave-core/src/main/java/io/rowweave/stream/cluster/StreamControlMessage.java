package io.rowweave.stream.cluster;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/** Immutable, fully fenced Stream control-plane message. */
public final class StreamControlMessage {
    public static final int MAX_PAYLOAD_BYTES = 64 * 1024;
    static final int MAX_TEXT_BYTES = 256;

    public enum Kind {
        SUBMIT(1),
        START(2),
        LEASE(3),
        CHECKPOINT_TRIGGER(4),
        CHECKPOINT_ACK(5),
        CHECKPOINT_DECISION(6),
        CHECKPOINT_QUERY(7),
        TASK_PROGRESS(8),
        CANCEL(9),
        RESTORE(10),
        SAVEPOINT(11),
        RESCALE(12);

        private final int wireId;

        Kind(int wireId) {
            this.wireId = wireId;
        }

        public int wireId() {
            return wireId;
        }

        public static Kind fromWireId(int wireId) {
            for (Kind value : values()) {
                if (value.wireId == wireId) {
                    return value;
                }
            }
            throw failure(StreamClusterProtocolException.Code.UNKNOWN_CONTROL_KIND,
                    "unknown Stream control message kind");
        }
    }

    private final Kind kind;
    private final long coordinatorTerm;
    private final String coordinatorIncarnation;
    private final String runId;
    private final String attemptId;
    private final long fenceToken;
    private final long assignmentGeneration;
    private final String workerIncarnation;
    private final long commandSequence;
    private final String commandDigest;
    private final byte[] payload;

    public StreamControlMessage(
            Kind kind, long coordinatorTerm, String coordinatorIncarnation,
            String runId, String attemptId, long fenceToken,
            long assignmentGeneration, String workerIncarnation,
            long commandSequence, String commandDigest, byte[] payload) {
        if (kind == null || coordinatorTerm <= 0L || fenceToken <= 0L
                || assignmentGeneration <= 0L || commandSequence <= 0L) {
            throw new IllegalArgumentException(
                    "Stream control kind and generations must be positive");
        }
        if (commandDigest == null
                || !commandDigest.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException(
                    "Stream control command digest must be canonical SHA-256 hex");
        }
        if (payload == null || payload.length > MAX_PAYLOAD_BYTES) {
            throw new IllegalArgumentException(
                    "Stream control payload exceeds its limit");
        }
        this.kind = kind;
        this.coordinatorTerm = coordinatorTerm;
        this.coordinatorIncarnation = text(
                coordinatorIncarnation, "coordinator incarnation");
        this.runId = text(runId, "run ID");
        this.attemptId = text(attemptId, "attempt ID");
        this.fenceToken = fenceToken;
        this.assignmentGeneration = assignmentGeneration;
        this.workerIncarnation = text(workerIncarnation, "worker incarnation");
        this.commandSequence = commandSequence;
        this.commandDigest = commandDigest;
        this.payload = payload.clone();
    }

    public Kind kind() { return kind; }
    public long coordinatorTerm() { return coordinatorTerm; }
    public String coordinatorIncarnation() { return coordinatorIncarnation; }
    public String runId() { return runId; }
    public String attemptId() { return attemptId; }
    public long fenceToken() { return fenceToken; }
    public long assignmentGeneration() { return assignmentGeneration; }
    public String workerIncarnation() { return workerIncarnation; }
    public long commandSequence() { return commandSequence; }
    public String commandDigest() { return commandDigest; }
    public byte[] payload() { return payload.clone(); }
    byte[] payloadUnsafe() { return payload; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamControlMessage)) {
            return false;
        }
        StreamControlMessage that = (StreamControlMessage) other;
        return coordinatorTerm == that.coordinatorTerm
                && fenceToken == that.fenceToken
                && assignmentGeneration == that.assignmentGeneration
                && commandSequence == that.commandSequence
                && kind == that.kind
                && coordinatorIncarnation.equals(that.coordinatorIncarnation)
                && runId.equals(that.runId)
                && attemptId.equals(that.attemptId)
                && workerIncarnation.equals(that.workerIncarnation)
                && commandDigest.equals(that.commandDigest)
                && Arrays.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(kind, coordinatorTerm, coordinatorIncarnation,
                runId, attemptId, fenceToken, assignmentGeneration,
                workerIncarnation, commandSequence, commandDigest,
                Arrays.hashCode(payload));
    }

    static String text(String value, String field) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(field + " is invalid");
        }
        try {
            int length = StandardCharsets.UTF_8.newEncoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .encode(CharBuffer.wrap(value)).remaining();
            if (length > MAX_TEXT_BYTES) {
                throw new IllegalArgumentException(field + " is invalid");
            }
            return value;
        } catch (CharacterCodingException malformed) {
            throw new IllegalArgumentException(field + " is invalid", malformed);
        }
    }

    static StreamClusterProtocolException failure(
            StreamClusterProtocolException.Code code, String message) {
        return new StreamClusterProtocolException(code, message);
    }
}
