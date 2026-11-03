package io.rowweave.run;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

/** Bounded checksum envelope for a stable RunSubmission plus the original PlanArtifact bytes. */
public final class RunSubmissionCodec {
    private static final int MAGIC = 0x52575255; // RWRU
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 12;
    private static final int CHECKSUM_BYTES = 32;
    private static final int MAX_ARTIFACT_BYTES = io.rowweave.artifact.PlanArtifactCodec.MAX_FRAME_BYTES;
    private static final int MAX_BODY_BYTES = MAX_ARTIFACT_BYTES + 1024;
    public static final int MAX_FRAME_BYTES = HEADER_BYTES + MAX_BODY_BYTES + CHECKSUM_BYTES;

    private RunSubmissionCodec() {
    }

    public static byte[] encode(RunSubmission submission, byte[] artifactBytes) {
        return encode(submission, artifactBytes, value -> { });
    }

    public static byte[] encode(RunSubmission submission, byte[] artifactBytes,
                                java.util.function.LongConsumer reservation) {
        if (submission == null) {
            throw new IllegalArgumentException("submission is required");
        }
        if (artifactBytes == null || artifactBytes.length > MAX_ARTIFACT_BYTES) {
            throw new IllegalArgumentException("artifact bytes length out of bounds");
        }
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        long bodyUpper = 64L + artifactBytes.length
                + textUpper(submission.runId()) + textUpper(submission.attemptId())
                + textUpper(submission.publicationId());
        reservation.accept(multiplyExact(4L, bodyUpper));
        byte[] body = body(submission, artifactBytes);
        if (body.length > MAX_BODY_BYTES) {
            throw new IllegalArgumentException("run submission body exceeds limit");
        }
        long frameBytes = HEADER_BYTES + (long) body.length + CHECKSUM_BYTES;
        // framed backing array + checksum toByteArray + returned toByteArray + digest.
        reservation.accept(addExact(multiplyExact(3L, frameBytes), CHECKSUM_BYTES));
        ByteArrayOutputStream framed = new ByteArrayOutputStream(
                HEADER_BYTES + body.length + CHECKSUM_BYTES);
        try (DataOutputStream out = new DataOutputStream(framed)) {
            out.writeInt(MAGIC);
            out.writeShort(VERSION);
            out.writeShort(0);
            out.writeInt(body.length);
            out.write(body);
            out.flush();
            out.write(sha256(framed.toByteArray()));
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return framed.toByteArray();
    }

    private static long textUpper(String value) {
        return 4L + multiplyExact(3L, value.length());
    }

    private static long addExact(long left, long right) {
        if (right < 0L || left > Long.MAX_VALUE - right) {
            throw new IllegalArgumentException("run submission resident estimate overflow");
        }
        return left + right;
    }

    private static long multiplyExact(long left, long right) {
        if (left < 0L || right < 0L || (left != 0L && right > Long.MAX_VALUE / left)) {
            throw new IllegalArgumentException("run submission resident estimate overflow");
        }
        return left * right;
    }

    public static Decoded decode(byte[] encoded) {
        return decode(encoded, value -> { });
    }

    public static Decoded decode(byte[] encoded, java.util.function.LongConsumer reservation) {
        if (encoded == null || encoded.length < HEADER_BYTES + CHECKSUM_BYTES
                || encoded.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("run submission frame length out of bounds");
        }
        if (reservation == null) throw new IllegalArgumentException("reservation is required");
        RunWire.Reader frame = new RunWire.Reader(encoded, reservation);
        if (frame.readInt("frame.magic") != MAGIC) {
            throw new IllegalArgumentException("invalid run submission magic");
        }
        int version = (frame.readUnsignedByte("frame.version.high") << 8)
                | frame.readUnsignedByte("frame.version.low");
        if (version != VERSION) {
            throw new IllegalArgumentException("unsupported run submission version: " + version);
        }
        if (frame.readUnsignedByte("frame.reserved.high") != 0
                || frame.readUnsignedByte("frame.reserved.low") != 0) {
            throw new IllegalArgumentException("non-zero run submission reserved bytes");
        }
        int bodyLength = frame.readInt("frame.bodyLength");
        if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                || encoded.length != HEADER_BYTES + bodyLength + CHECKSUM_BYTES) {
            throw new IllegalArgumentException(
                    "run submission body length mismatch: " + bodyLength);
        }
        byte[] body = frame.readFixed(bodyLength, "frame.body");
        byte[] checksum = frame.readFixed(CHECKSUM_BYTES, "frame.checksum");
        frame.finish("run submission frame");
        reservation.accept(encoded.length - CHECKSUM_BYTES);
        byte[] expected = sha256(Arrays.copyOf(encoded, encoded.length - CHECKSUM_BYTES));
        if (!MessageDigest.isEqual(expected, checksum)) {
            throw new IllegalArgumentException("run submission checksum mismatch");
        }

        RunWire.Reader in = new RunWire.Reader(body, reservation);
        String runId = in.readText("submission.runId", RunText.MAX_ID_BYTES);
        String attemptId = in.readText("submission.attemptId", RunText.MAX_ID_BYTES);
        String publicationId = in.readText(
                "submission.publicationId", RunText.MAX_ID_BYTES);
        int maxAttempts = in.readInt("submission.maxAttempts");
        long deadline = in.readLong("submission.deadline");
        long submittedAt = in.readLong("submission.submittedAt");
        int artifactLength = in.readInt("submission.artifact.length");
        if (artifactLength < 0 || artifactLength > MAX_ARTIFACT_BYTES) {
            throw new IllegalArgumentException(
                    "submission artifact length out of bounds: " + artifactLength);
        }
        byte[] artifactBytes = in.readFixed(artifactLength, "submission.artifact");
        in.finish("run submission body");
        reservation.accept(512L);
        return new Decoded(new RunSubmission(runId, attemptId, publicationId,
                maxAttempts, deadline, submittedAt), artifactBytes, reservation);
    }

    private static byte[] body(RunSubmission submission, byte[] artifactBytes) {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(1024 + artifactBytes.length);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            RunWire.writeText(out, submission.runId());
            RunWire.writeText(out, submission.attemptId());
            RunWire.writeText(out, submission.publicationId());
            out.writeInt(submission.maxAttempts());
            out.writeLong(submission.deadlineEpochMillis());
            out.writeLong(submission.submittedAtMillis());
            out.writeInt(artifactBytes.length);
            out.write(artifactBytes);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        return bytes.toByteArray();
    }

    private static byte[] sha256(byte[] bytes) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    public static final class Decoded {
        private final RunSubmission submission;
        private final byte[] artifactBytes;
        private final java.util.function.LongConsumer reservation;

        private Decoded(RunSubmission submission, byte[] artifactBytes,
                        java.util.function.LongConsumer reservation) {
            this.submission = submission;
            this.artifactBytes = artifactBytes;
            this.reservation = reservation;
        }

        public RunSubmission submission() { return submission; }
        public byte[] artifactBytes() {
            reservation.accept(artifactBytes.length);
            return artifactBytes.clone();
        }
    }
}
