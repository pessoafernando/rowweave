package io.rowweave.incremental;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.function.LongConsumer;

/** Canonical bounded large-lane wire for one incremental submission intent. */
public final class IncrementalRunSubmissionCodec {
    private static final int MAGIC = 0x52574953; // RWIS
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 4 + 4 + 4 + 1;
    private static final int DIGEST_BYTES = 32;
    private static final int TEXT_BYTES = IncrementalAuthorityValue.MAX_ID_BYTES;
    private static final int STRUCTURAL_BYTES = 16 * 1024;
    private static final int MAX_BODY_BYTES = STRUCTURAL_BYTES
            + IncrementalDefinitionCodec.MAX_FRAME_BYTES
            + PlanArtifactCodec.MAX_FRAME_BYTES
            + IncrementalSourceProviderContractCodec.MAX_FRAME_BYTES
            + IncrementalSourceConfigCodec.MAX_FRAME_BYTES;
    public static final int MAX_FRAME_BYTES =
            HEADER_BYTES + MAX_BODY_BYTES + DIGEST_BYTES;

    private IncrementalRunSubmissionCodec() { }

    public static byte[] encode(IncrementalRunSubmission submission) {
        return encode(submission, ignored -> { });
    }

    public static byte[] encode(
            IncrementalRunSubmission submission, LongConsumer reservation) {
        if (submission == null) {
            throw new IllegalArgumentException(
                    "incremental submission is required");
        }
        if (reservation == null) {
            throw new IllegalArgumentException("reservation is required");
        }
        byte[] definition = IncrementalDefinitionCodec.encode(
                submission.definition());
        reservation.accept(Math.multiplyExact(3L, definition.length));
        byte[] template = PlanArtifactCodec.encode(
                submission.templateArtifact(), reservation);
        byte[] contract = IncrementalSourceProviderContractCodec.encode(
                submission.sourceContract());
        byte[] configuration = IncrementalSourceConfigCodec.encode(
                submission.sourceConfiguration());
        reservation.accept(Math.multiplyExact(3L,
                (long) contract.length + configuration.length));
        try {
            int capacity = checkedBodyCapacity(
                    definition, template, contract, configuration);
            reservation.accept(Math.multiplyExact(3L,
                    (long) capacity + HEADER_BYTES + DIGEST_BYTES));
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream(
                    capacity);
            DataOutputStream body = new DataOutputStream(bodyBytes);
            writeText(body, submission.feedId());
            writeText(body, submission.requestIdentity());
            writeText(body, submission.attemptId());
            writeText(body, submission.publicationId());
            body.writeInt(submission.maxAttempts());
            body.writeLong(submission.deadlineEpochMillis());
            body.writeLong(submission.submittedAtMillis());
            body.writeLong(submission.expectedBackfillOrdinal());
            writeOptionalFrontier(body, submission.backfillFrom());
            writeOptionalFrontier(body, submission.backfillTarget());
            writeBytes(body, definition);
            writeBytes(body, template);
            writeBytes(body, contract);
            writeBytes(body, configuration);
            body.flush();
            byte[] payload = bodyBytes.toByteArray();
            int unsignedBytes = Math.addExact(HEADER_BYTES, payload.length);
            int total = Math.addExact(unsignedBytes, DIGEST_BYTES);
            if (total > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "incremental submission frame exceeds bound");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(total);
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.writeInt(MAGIC);
            frame.writeInt(VERSION);
            frame.writeInt(payload.length);
            frame.writeByte(submission.mode().wireId());
            frame.write(payload);
            frame.flush();
            byte[] unsigned = frameBytes.toByteArray();
            frame.write(sha256(unsigned));
            frame.flush();
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory incremental submission encoding failed",
                    impossible);
        }
    }

    public static IncrementalRunSubmission decode(byte[] frame) {
        return decode(frame, ignored -> { });
    }

    public static IncrementalRunSubmission decode(
            byte[] frame, LongConsumer reservation) {
        if (frame == null || reservation == null
                || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental submission frame length is invalid");
        }
        int unsignedLength = frame.length - DIGEST_BYTES;
        byte[] actualDigest = Arrays.copyOfRange(
                frame, unsignedLength, frame.length);
        if (!MessageDigest.isEqual(actualDigest,
                sha256(frame, 0, unsignedLength))) {
            throw new IllegalArgumentException(
                    "incremental submission checksum is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame, 0, unsignedLength));
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "incremental submission header is invalid");
            }
            int bodyLength = input.readInt();
            IncrementalRunMode mode = IncrementalRunMode.fromWire(
                    input.readUnsignedByte());
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                    || bodyLength != input.available()) {
                throw new IllegalArgumentException(
                        "incremental submission body length is invalid");
            }
            String feedId = readText(input, "feed id");
            String requestIdentity = readText(input, "request identity");
            String attemptId = readText(input, "attempt id");
            String publicationId = readText(input, "publication id");
            int maxAttempts = input.readInt();
            long deadline = input.readLong();
            long submittedAt = input.readLong();
            long ordinal = input.readLong();
            IncrementalFrontier from = readOptionalFrontier(
                    input, reservation, "backfill from");
            IncrementalFrontier target = readOptionalFrontier(
                    input, reservation, "backfill target");
            byte[] definitionFrame = readBytes(
                    input, IncrementalDefinitionCodec.MAX_FRAME_BYTES,
                    reservation, "definition");
            byte[] templateFrame = readBytes(
                    input, PlanArtifactCodec.MAX_FRAME_BYTES,
                    reservation, "template Artifact");
            byte[] contractFrame = readBytes(
                    input, IncrementalSourceProviderContractCodec.MAX_FRAME_BYTES,
                    reservation, "source contract");
            byte[] configurationFrame = readBytes(
                    input, IncrementalSourceConfigCodec.MAX_FRAME_BYTES,
                    reservation, "source configuration");
            if (input.read() != -1) {
                throw new IllegalArgumentException(
                        "incremental submission has trailing body bytes");
            }
            reservation.accept(Math.multiplyExact(
                    3L, definitionFrame.length));
            IncrementalDefinition definition =
                    IncrementalDefinitionCodec.decode(definitionFrame);
            PlanArtifact template = PlanArtifactCodec.decode(
                    templateFrame, reservation);
            reservation.accept(Math.multiplyExact(3L,
                    (long) contractFrame.length + configurationFrame.length));
            IncrementalSourceProviderContract contract =
                    IncrementalSourceProviderContractCodec.decode(contractFrame);
            IncrementalSourceConfig configuration =
                    IncrementalSourceConfigCodec.decode(configurationFrame);
            return mode == IncrementalRunMode.BACKFILL_V1
                    ? IncrementalRunSubmission.backfill(
                    feedId, ordinal, from, target, requestIdentity,
                    attemptId, publicationId, maxAttempts, deadline,
                    submittedAt, definition, template, contract, configuration)
                    : IncrementalRunSubmission.normal(
                    feedId, requestIdentity, mode, attemptId, publicationId,
                    maxAttempts, deadline, submittedAt, definition, template,
                    contract, configuration);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "incremental submission is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental submission is malformed", malformed);
        }
    }

    public static byte[] digest(IncrementalRunSubmission submission) {
        return sha256(encode(submission));
    }

    private static int checkedBodyCapacity(
            byte[] definition, byte[] template, byte[] contract,
            byte[] configuration) {
        long total = STRUCTURAL_BYTES;
        total = Math.addExact(total, definition.length);
        total = Math.addExact(total, template.length);
        total = Math.addExact(total, contract.length);
        total = Math.addExact(total, configuration.length);
        if (total > MAX_BODY_BYTES) {
            throw new IllegalArgumentException(
                    "incremental submission body exceeds bound");
        }
        return (int) total;
    }

    private static void writeText(DataOutputStream out, String value)
            throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String readText(DataInputStream in, String field)
            throws IOException {
        byte[] encoded = readBytes(
                in, TEXT_BYTES, ignored -> { }, field);
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded));
            return decoded.toString();
        } catch (CharacterCodingException invalid) {
            throw new IllegalArgumentException(
                    "incremental submission " + field + " is invalid UTF-8",
                    invalid);
        }
    }

    private static void writeOptionalFrontier(
            DataOutputStream out, IncrementalFrontier frontier)
            throws IOException {
        out.writeBoolean(frontier != null);
        if (frontier != null) {
            writeBytes(out, IncrementalFrontierCodec.encode(frontier));
        }
    }

    private static IncrementalFrontier readOptionalFrontier(
            DataInputStream in, LongConsumer reservation, String field)
            throws IOException {
        if (!in.readBoolean()) return null;
        byte[] encoded = readBytes(
                in, IncrementalFrontierCodec.MAX_FRAME_BYTES,
                reservation, field);
        reservation.accept(Math.multiplyExact(2L, encoded.length));
        return IncrementalFrontierCodec.decode(encoded);
    }

    private static void writeBytes(DataOutputStream out, byte[] value)
            throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(
            DataInputStream in, int limit, LongConsumer reservation,
            String field) throws IOException {
        if (in.available() < Integer.BYTES) {
            throw new IllegalArgumentException(field + " length is missing");
        }
        int length = in.readInt();
        if (length <= 0 || length > limit || length > in.available()) {
            throw new IllegalArgumentException(field + " length is invalid");
        }
        reservation.accept(length);
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    private static byte[] sha256(byte[] value) {
        return sha256(value, 0, value.length);
    }

    private static byte[] sha256(byte[] value, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(value, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
