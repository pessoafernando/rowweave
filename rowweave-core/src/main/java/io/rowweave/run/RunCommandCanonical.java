package io.rowweave.run;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Canonical command body without the commandId, so an idempotent digest and a later codec share one source. */
final class RunCommandCanonical {
    private static final int LEGACY_MAGIC = 0x52574331; // RWC1
    private static final int V2_MAGIC = 0x52574332; // RWC2
    private static final int MAGIC = 0x52574333; // RWC3

    private RunCommandCanonical() {
    }

    static byte[] encodeBody(RunCommand command) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(command.canonicalVersion == 1
                    ? LEGACY_MAGIC
                    : command.canonicalVersion == 2 ? V2_MAGIC : MAGIC);
            out.writeByte(RunWire.kindId(command.kind));
            RunWire.writeText(out, command.runId);
            RunWire.writeNullableText(out, command.definitionId);
            RunWire.writeNullableText(out, command.artifactId);
            out.writeInt(command.maxAttempts);
            out.writeLong(command.deadlineEpochMillis);
            out.writeLong(command.expectedRevision);
            RunWire.writeNullableText(out, command.attemptId);
            RunWire.writeNullableText(out, command.publicationId);
            out.writeLong(command.fenceToken);
            out.writeByte(RunWire.attemptStatusId(command.targetAttemptStatus));
            out.writeBoolean(command.retryable);
            out.writeByte(RunWire.cancellationCauseId(command.cancellationCause));
            out.writeLong(command.eventTimeMillis);
            RunWire.writeNullableText(out, command.reasonCode);
            RunWire.writeNullableText(out, command.message);
            if (command.canonicalVersion >= 2) {
                writeResultSummary(out, command.resultSummary,
                        command.canonicalVersion);
            }
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    static byte[] digest(RunCommand command) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(encodeBody(command));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static RunCommand decodeBody(String commandId, byte[] body) {
        RunWire.Reader in = new RunWire.Reader(body);
        int magic = in.readInt("command.magic");
        if (magic != LEGACY_MAGIC && magic != V2_MAGIC && magic != MAGIC) {
            throw new IllegalArgumentException("invalid canonical command magic");
        }
        int version = magic == LEGACY_MAGIC ? 1 : magic == V2_MAGIC ? 2 : 3;
        RunCommand.Kind kind = RunWire.commandKind(in.readUnsignedByte("command.kind"));
        String runId = in.readText("command.runId", RunText.MAX_ID_BYTES);
        String definitionId = in.readNullableText("command.definitionId", RunText.MAX_ID_BYTES);
        String artifactId = in.readNullableText("command.artifactId", RunText.MAX_ID_BYTES);
        int maxAttempts = in.readInt("command.maxAttempts");
        long deadline = in.readLong("command.deadline");
        long expectedRevision = in.readLong("command.expectedRevision");
        String attemptId = in.readNullableText("command.attemptId", RunText.MAX_ID_BYTES);
        String publicationId = in.readNullableText("command.publicationId", RunText.MAX_ID_BYTES);
        long fenceToken = in.readLong("command.fenceToken");
        AttemptStatus target = RunWire.attemptStatus(
                in.readUnsignedByte("command.targetStatus"), true);
        boolean retryable = in.readBoolean("command.retryable");
        CancellationCause cause = RunWire.cancellationCause(
                in.readUnsignedByte("command.cancellationCause"), true);
        long eventTime = in.readLong("command.eventTime");
        String reason = in.readNullableText("command.reasonCode", RunText.MAX_REASON_CODE_BYTES);
        String message = in.readNullableText("command.message", RunText.MAX_MESSAGE_BYTES);
        RunResultSummary resultSummary = version == 1
                ? null : readResultSummary(in, version);
        in.finish("command");
        RunCommand decoded = RunCommand.decodedCanonical(version, commandId, kind, runId,
                definitionId, artifactId, maxAttempts, deadline, expectedRevision,
                attemptId, publicationId, fenceToken, target, retryable, cause,
                eventTime, reason, message, resultSummary);
        if (!MessageDigest.isEqual(body, encodeBody(decoded))) {
            throw new IllegalArgumentException("non-canonical command body");
        }
        return decoded;
    }

    private static void writeResultSummary(
            DataOutputStream out, RunResultSummary summary, int version)
            throws IOException {
        out.writeBoolean(summary != null);
        if (summary == null) return;
        out.writeLong(summary.rowsOut());
        out.writeByte(RunWire.publicationKindId(summary.publicationKind()));
        if (summary.publicationKind() == RunResultSummary.PublicationKind.MANIFEST) {
            out.writeLong(summary.manifestGeneration());
            out.write(summary.manifestSha256Unsafe());
            if (version >= 3) {
                out.writeBoolean(summary.sinkIdentityDigestUnsafe() != null);
                if (summary.sinkIdentityDigestUnsafe() != null) {
                    out.write(summary.sinkIdentityDigestUnsafe());
                }
            }
        }
    }

    private static RunResultSummary readResultSummary(RunWire.Reader in, int version) {
        if (!in.readBoolean("command.hasResultSummary")) return null;
        long rowsOut = in.readLong("command.result.rowsOut");
        RunResultSummary.PublicationKind kind = RunWire.publicationKind(
                in.readUnsignedByte("command.result.publicationKind"));
        if (kind == RunResultSummary.PublicationKind.DISCARD) {
            return RunResultSummary.discard(rowsOut);
        }
        long generation = in.readLong("command.result.manifestGeneration");
        byte[] manifestSha = in.readFixed(32, "command.result.manifestSha256");
        byte[] sinkIdentity = version >= 3
                && in.readBoolean("command.result.hasSinkIdentity")
                ? in.readFixed(32, "command.result.sinkIdentity") : null;
        return sinkIdentity == null
                ? RunResultSummary.manifest(rowsOut, generation, manifestSha)
                : RunResultSummary.manifest(
                        rowsOut, generation, manifestSha, sinkIdentity);
    }

    static String hex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        final char[] digits = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int value = bytes[i] & 0xff;
            out[i * 2] = digits[value >>> 4];
            out[i * 2 + 1] = digits[value & 0xf];
        }
        return new String(out);
    }

}
