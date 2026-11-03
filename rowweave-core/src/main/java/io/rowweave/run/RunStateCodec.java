package io.rowweave.run;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.function.LongConsumer;

/** Deterministic, bounded Run control-plane codec carrying a SHA-256 envelope. */
public final class RunStateCodec {
    public static final int MAX_FRAME_BYTES = 64 * 1024;

    private static final int MAGIC = 0x52575253; // RWRS
    private static final int LEGACY_VERSION = 1;
    private static final int SUMMARY_VERSION = 2;
    private static final int VERSION = 3;
    private static final int HEADER_BYTES = 12;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_BODY_BYTES = MAX_FRAME_BYTES - HEADER_BYTES - CHECKSUM_BYTES;

    private static final int COMMAND = 1;
    private static final int SNAPSHOT = 2;
    private static final int EVENT = 3;
    private static final int RECORD = 4;

    private RunStateCodec() {
    }

    public static byte[] encodeCommand(RunCommand command) {
        if (command == null) throw new IllegalArgumentException("command is required");
        byte[] canonical = RunCommandCanonical.encodeBody(command);
        return frame(COMMAND, out -> {
            RunWire.writeText(out, command.commandId());
            writeBlob(out, canonical);
            out.write(command.commandDigestUnsafe());
        });
    }

    public static RunCommand decodeCommand(byte[] encoded) {
        FrameBody frame = body(encoded, COMMAND);
        RunWire.Reader in = frame.reader;
        String commandId = in.readText("command.commandId", RunText.MAX_ID_BYTES);
        byte[] canonical = readBlob(in, "command.canonical", MAX_BODY_BYTES);
        byte[] storedDigest = in.readFixed(CHECKSUM_BYTES, "command.digest");
        in.finish("command frame");
        RunCommand command = RunCommandCanonical.decodeBody(commandId, canonical);
        if (!MessageDigest.isEqual(storedDigest, command.commandDigestUnsafe())) {
            throw new IllegalArgumentException("command digest mismatch");
        }
        return command;
    }

    public static byte[] encodeSnapshot(RunSnapshot snapshot) {
        validateSnapshot(snapshot);
        return frame(SNAPSHOT, out -> writeSnapshotBody(out, snapshot));
    }

    /**
     * RPC producer path: reserves the conservative peak of the whole 64 KiB envelope before any codec byte[] is allocated,
     * at six times the size to cover body and frame backing, the toByteArray copy, checksum input and digest, and the returned array.
     */
    public static byte[] encodeSnapshot(RunSnapshot snapshot, LongConsumer reservation) {
        if (reservation == null) throw new IllegalArgumentException("reservation must not be null");
        reservation.accept(6L * MAX_FRAME_BYTES);
        return encodeSnapshot(snapshot);
    }

    public static RunSnapshot decodeSnapshot(byte[] encoded) {
        FrameBody frame = body(encoded, SNAPSHOT);
        RunWire.Reader in = frame.reader;
        RunSnapshot snapshot = readSnapshotBody(in, frame.version);
        in.finish("snapshot frame");
        validateSnapshot(snapshot, frame.version == LEGACY_VERSION);
        return snapshot;
    }

    public static byte[] encodeEvent(ExecutionEvent event) {
        validateEvent(event);
        return frame(EVENT, out -> writeEventBody(out, event));
    }

    public static ExecutionEvent decodeEvent(byte[] encoded) {
        RunWire.Reader in = body(encoded, EVENT).reader;
        ExecutionEvent event = readEventBody(in);
        in.finish("event frame");
        validateEvent(event);
        return event;
    }

    public static byte[] encodeRecord(RunJournalRecord record) {
        if (record == null) throw new IllegalArgumentException("record is required");
        byte[] command = encodeCommand(record.command());
        byte[] snapshot = encodeSnapshot(record.snapshot());
        byte[] event = encodeEvent(record.event());
        return frame(RECORD, out -> {
            writeBlob(out, command);
            writeBlob(out, snapshot);
            writeBlob(out, event);
        });
    }

    public static RunJournalRecord decodeRecord(byte[] encoded) {
        RunWire.Reader in = body(encoded, RECORD).reader;
        RunCommand command = decodeCommand(readBlob(in, "record.command", MAX_FRAME_BYTES));
        RunSnapshot snapshot = decodeSnapshot(readBlob(in, "record.snapshot", MAX_FRAME_BYTES));
        ExecutionEvent event = decodeEvent(readBlob(in, "record.event", MAX_FRAME_BYTES));
        in.finish("record frame");
        return new RunJournalRecord(command, snapshot, event);
    }

    private static void writeSnapshotBody(DataOutputStream out, RunSnapshot snapshot) throws IOException {
        RunWire.writeText(out, snapshot.runId());
        RunWire.writeText(out, snapshot.definitionId());
        RunWire.writeText(out, snapshot.artifactId());
        out.writeInt(snapshot.maxAttempts());
        out.writeInt(snapshot.attemptCount());
        out.writeLong(snapshot.revision());
        out.writeByte(RunWire.runStatusId(snapshot.status()));
        out.writeLong(snapshot.deadlineEpochMillis());
        out.writeLong(snapshot.createdAtMillis());
        out.writeLong(snapshot.updatedAtMillis());
        AttemptSnapshot attempt = snapshot.currentAttempt();
        out.writeBoolean(attempt != null);
        if (attempt != null) {
            out.writeInt(attempt.attemptNumber());
            RunWire.writeText(out, attempt.attemptId());
            out.writeLong(attempt.fenceToken());
            RunWire.writeText(out, attempt.publicationId());
            out.writeByte(RunWire.attemptStatusId(attempt.status()));
            out.writeByte(RunWire.cancellationCauseId(attempt.cancellationCause()));
            out.writeLong(attempt.createdAtMillis());
            out.writeLong(attempt.updatedAtMillis());
            RunWire.writeNullableText(out, attempt.reasonCode());
            RunWire.writeNullableText(out, attempt.message());
        }
        writeResultSummary(out, snapshot.resultSummary());
    }

    private static RunSnapshot readSnapshotBody(RunWire.Reader in, int version) {
        String runId = in.readText("snapshot.runId", RunText.MAX_ID_BYTES);
        String definitionId = in.readText("snapshot.definitionId", RunText.MAX_ID_BYTES);
        String artifactId = in.readText("snapshot.artifactId", RunText.MAX_ID_BYTES);
        int maxAttempts = in.readInt("snapshot.maxAttempts");
        int attemptCount = in.readInt("snapshot.attemptCount");
        long revision = in.readLong("snapshot.revision");
        RunStatus status = RunWire.runStatus(in.readUnsignedByte("snapshot.status"), false);
        long deadline = in.readLong("snapshot.deadline");
        long created = in.readLong("snapshot.createdAt");
        long updated = in.readLong("snapshot.updatedAt");
        AttemptSnapshot attempt = null;
        if (in.readBoolean("snapshot.hasAttempt")) {
            int number = in.readInt("snapshot.attempt.number");
            String attemptId = in.readText("snapshot.attempt.id", RunText.MAX_ID_BYTES);
            long fence = in.readLong("snapshot.attempt.fence");
            String publication = in.readText("snapshot.attempt.publication", RunText.MAX_ID_BYTES);
            AttemptStatus attemptStatus = RunWire.attemptStatus(
                    in.readUnsignedByte("snapshot.attempt.status"), false);
            CancellationCause cause = RunWire.cancellationCause(
                    in.readUnsignedByte("snapshot.attempt.cancelCause"), true);
            long attemptCreated = in.readLong("snapshot.attempt.createdAt");
            long attemptUpdated = in.readLong("snapshot.attempt.updatedAt");
            String reason = in.readNullableText(
                    "snapshot.attempt.reason", RunText.MAX_REASON_CODE_BYTES);
            String message = in.readNullableText(
                    "snapshot.attempt.message", RunText.MAX_MESSAGE_BYTES);
            attempt = new AttemptSnapshot(number, attemptId, fence, publication,
                    attemptStatus, cause, attemptCreated, attemptUpdated, reason, message);
        }
        RunResultSummary summary = version >= SUMMARY_VERSION
                ? readResultSummary(in, version) : null;
        if (attempt != null && summary != null) {
            attempt = new AttemptSnapshot(attempt.attemptNumber(), attempt.attemptId(),
                    attempt.fenceToken(), attempt.publicationId(), attempt.status(),
                    attempt.cancellationCause(), attempt.createdAtMillis(),
                    attempt.updatedAtMillis(), attempt.reasonCode(), attempt.message(), summary);
        }
        return new RunSnapshot(runId, definitionId, artifactId, maxAttempts, attemptCount,
                revision, status, deadline, created, updated, attempt);
    }

    private static void writeEventBody(DataOutputStream out, ExecutionEvent event) throws IOException {
        out.writeLong(event.sequence());
        out.writeByte(RunWire.eventTypeId(event.type()));
        RunWire.writeText(out, event.runId());
        RunWire.writeNullableText(out, event.attemptId());
        out.writeLong(event.fenceToken());
        out.writeByte(RunWire.runStatusId(event.runFrom()));
        out.writeByte(RunWire.runStatusId(event.runTo()));
        out.writeByte(RunWire.attemptStatusId(event.attemptFrom()));
        out.writeByte(RunWire.attemptStatusId(event.attemptTo()));
        out.writeLong(event.eventTimeMillis());
        RunWire.writeNullableText(out, event.reasonCode());
        RunWire.writeNullableText(out, event.message());
        RunWire.writeText(out, event.commandId());
        out.write(event.commandDigestUnsafe());
    }

    private static ExecutionEvent readEventBody(RunWire.Reader in) {
        long sequence = in.readLong("event.sequence");
        ExecutionEvent.Type type = RunWire.eventType(in.readUnsignedByte("event.type"));
        String runId = in.readText("event.runId", RunText.MAX_ID_BYTES);
        String attemptId = in.readNullableText("event.attemptId", RunText.MAX_ID_BYTES);
        long fence = in.readLong("event.fence");
        RunStatus runFrom = RunWire.runStatus(in.readUnsignedByte("event.runFrom"), true);
        RunStatus runTo = RunWire.runStatus(in.readUnsignedByte("event.runTo"), false);
        AttemptStatus attemptFrom = RunWire.attemptStatus(
                in.readUnsignedByte("event.attemptFrom"), true);
        AttemptStatus attemptTo = RunWire.attemptStatus(
                in.readUnsignedByte("event.attemptTo"), true);
        long eventTime = in.readLong("event.time");
        String reason = in.readNullableText("event.reason", RunText.MAX_REASON_CODE_BYTES);
        String message = in.readNullableText("event.message", RunText.MAX_MESSAGE_BYTES);
        String commandId = in.readText("event.commandId", RunText.MAX_ID_BYTES);
        byte[] digest = in.readFixed(CHECKSUM_BYTES, "event.commandDigest");
        return new ExecutionEvent(sequence, type, runId, attemptId, fence,
                runFrom, runTo, attemptFrom, attemptTo, eventTime,
                reason, message, commandId, digest);
    }

    private static void validateSnapshot(RunSnapshot snapshot) {
        validateSnapshot(snapshot, false);
    }

    private static void validateSnapshot(RunSnapshot snapshot, boolean allowLegacyMissingSummary) {
        if (snapshot == null) throw new IllegalArgumentException("snapshot is required");
        RunText.required(snapshot.runId(), "snapshot.runId", RunText.MAX_ID_BYTES);
        RunText.required(snapshot.definitionId(), "snapshot.definitionId", RunText.MAX_ID_BYTES);
        RunText.required(snapshot.artifactId(), "snapshot.artifactId", RunText.MAX_ID_BYTES);
        if (snapshot.maxAttempts() < 1 || snapshot.maxAttempts() > 1000
                || snapshot.attemptCount() < 0
                || snapshot.attemptCount() > snapshot.maxAttempts()
                || snapshot.revision() <= 0L || snapshot.status() == null
                || snapshot.createdAtMillis() < 0L
                || snapshot.updatedAtMillis() < snapshot.createdAtMillis()
                || snapshot.deadlineEpochMillis() < 0L
                || (snapshot.deadlineEpochMillis() != 0L
                && snapshot.deadlineEpochMillis() < snapshot.createdAtMillis())) {
            throw new IllegalArgumentException("invalid snapshot scalar invariants");
        }
        AttemptSnapshot attempt = snapshot.currentAttempt();
        if ((snapshot.attemptCount() == 0) != (attempt == null)) {
            throw new IllegalArgumentException("snapshot attempt presence mismatch");
        }
        if (attempt != null) {
            RunText.required(attempt.attemptId(), "snapshot.attemptId", RunText.MAX_ID_BYTES);
            RunText.required(attempt.publicationId(), "snapshot.publicationId", RunText.MAX_ID_BYTES);
            RunText.optional(attempt.reasonCode(), "snapshot.reason", RunText.MAX_REASON_CODE_BYTES);
            RunText.optional(attempt.message(), "snapshot.message", RunText.MAX_MESSAGE_BYTES);
            if (attempt.attemptNumber() != snapshot.attemptCount()
                    || attempt.fenceToken() <= 0L || attempt.status() == null
                    || attempt.createdAtMillis() < snapshot.createdAtMillis()
                    || attempt.updatedAtMillis() < attempt.createdAtMillis()
                    || attempt.updatedAtMillis() > snapshot.updatedAtMillis()) {
                throw new IllegalArgumentException("invalid attempt snapshot invariants");
            }
        }
        if (snapshot.status() == RunStatus.SUCCEEDED) {
            if (snapshot.resultSummary() == null && !allowLegacyMissingSummary) {
                throw new IllegalArgumentException("succeeded snapshot requires result summary");
            }
        } else if (snapshot.resultSummary() != null) {
            throw new IllegalArgumentException("non-succeeded snapshot cannot carry result summary");
        }
    }

    private static void validateEvent(ExecutionEvent event) {
        if (event == null || event.sequence() <= 0L || event.type() == null
                || event.runTo() == null || event.eventTimeMillis() < 0L
                || event.commandDigestUnsafe().length != CHECKSUM_BYTES) {
            throw new IllegalArgumentException("invalid event scalar invariants");
        }
        RunText.required(event.runId(), "event.runId", RunText.MAX_ID_BYTES);
        RunText.optional(event.attemptId(), "event.attemptId", RunText.MAX_ID_BYTES);
        RunText.optional(event.reasonCode(), "event.reason", RunText.MAX_REASON_CODE_BYTES);
        RunText.optional(event.message(), "event.message", RunText.MAX_MESSAGE_BYTES);
        RunText.required(event.commandId(), "event.commandId", RunText.MAX_ID_BYTES);
        if (event.attemptId() == null && event.fenceToken() != 0L) {
            throw new IllegalArgumentException("event fence requires attempt identity");
        }
    }

    private static byte[] frame(int kind, BodyWriter writer) {
        byte[] body = writeBody(writer);
        if (body.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("run state frame body exceeds limit: " + body.length);
        }
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(HEADER_BYTES + body.length + CHECKSUM_BYTES);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeShort(VERSION);
            out.writeByte(kind);
            out.writeByte(0);
            out.writeInt(body.length);
            out.write(body);
            out.flush();
            out.write(sha256(bytes.toByteArray()));
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        byte[] encoded = bytes.toByteArray();
        if (encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("run state frame exceeds limit");
        }
        return encoded;
    }

    private static FrameBody body(byte[] encoded, int expectedKind) {
        if (encoded == null || encoded.length < HEADER_BYTES + CHECKSUM_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("run state frame length out of bounds");
        }
        RunWire.Reader in = new RunWire.Reader(encoded);
        if (in.readInt("frame.magic") != MAGIC) throw new IllegalArgumentException("invalid frame magic");
        int version = (in.readUnsignedByte("frame.version.high") << 8)
                | in.readUnsignedByte("frame.version.low");
        if (version < LEGACY_VERSION || version > VERSION) {
            throw new IllegalArgumentException("unsupported frame version: " + version);
        }
        int kind = in.readUnsignedByte("frame.kind");
        if (kind < COMMAND || kind > RECORD) throw new IllegalArgumentException("unknown frame kind: " + kind);
        if (kind != expectedKind) throw new IllegalArgumentException("unexpected frame kind: " + kind);
        if (in.readUnsignedByte("frame.reserved") != 0) throw new IllegalArgumentException("non-zero reserved byte");
        int bodyLength = in.readInt("frame.bodyLength");
        if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                || encoded.length != HEADER_BYTES + bodyLength + CHECKSUM_BYTES) {
            throw new IllegalArgumentException("frame body length mismatch: " + bodyLength);
        }
        byte[] body = in.readFixed(bodyLength, "frame.body");
        byte[] checksum = in.readFixed(CHECKSUM_BYTES, "frame.checksum");
        in.finish("frame");
        byte[] expected = sha256(Arrays.copyOf(encoded, encoded.length - CHECKSUM_BYTES));
        if (!MessageDigest.isEqual(expected, checksum)) {
            throw new IllegalArgumentException("run state frame checksum mismatch");
        }
        return new FrameBody(version, new RunWire.Reader(body));
    }

    private static void writeResultSummary(DataOutputStream out, RunResultSummary summary)
            throws IOException {
        out.writeBoolean(summary != null);
        if (summary == null) return;
        out.writeLong(summary.rowsOut());
        out.writeByte(RunWire.publicationKindId(summary.publicationKind()));
        if (summary.publicationKind() == RunResultSummary.PublicationKind.MANIFEST) {
            out.writeLong(summary.manifestGeneration());
            out.write(summary.manifestSha256Unsafe());
            out.writeBoolean(summary.sinkIdentityDigestUnsafe() != null);
            if (summary.sinkIdentityDigestUnsafe() != null) {
                out.write(summary.sinkIdentityDigestUnsafe());
            }
        }
    }

    private static RunResultSummary readResultSummary(RunWire.Reader in, int version) {
        if (!in.readBoolean("snapshot.hasResultSummary")) return null;
        long rowsOut = in.readLong("snapshot.result.rowsOut");
        RunResultSummary.PublicationKind kind = RunWire.publicationKind(
                in.readUnsignedByte("snapshot.result.publicationKind"));
        if (kind == RunResultSummary.PublicationKind.DISCARD) {
            return RunResultSummary.discard(rowsOut);
        }
        long generation = in.readLong("snapshot.result.manifestGeneration");
        byte[] sha256 = in.readFixed(32, "snapshot.result.manifestSha256");
        byte[] sinkIdentity = version >= VERSION
                && in.readBoolean("snapshot.result.hasSinkIdentity")
                ? in.readFixed(32, "snapshot.result.sinkIdentity") : null;
        return sinkIdentity == null
                ? RunResultSummary.manifest(rowsOut, generation, sha256)
                : RunResultSummary.manifest(rowsOut, generation, sha256, sinkIdentity);
    }

    private static byte[] writeBody(BodyWriter writer) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writer.write(out);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    private static void writeBlob(DataOutputStream out, byte[] value) throws IOException {
        if (value.length > MAX_FRAME_BYTES) throw new IllegalArgumentException("blob exceeds limit");
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBlob(RunWire.Reader in, String path, int maxBytes) {
        int length = in.readInt(path + ".length");
        if (length < 0 || length > maxBytes) {
            throw new IllegalArgumentException(path + " length out of bounds: " + length);
        }
        return in.readFixed(length, path);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private interface BodyWriter {
        void write(DataOutputStream out) throws IOException;
    }

    private static final class FrameBody {
        private final int version;
        private final RunWire.Reader reader;

        private FrameBody(int version, RunWire.Reader reader) {
            this.version = version;
            this.reader = reader;
        }
    }
}
