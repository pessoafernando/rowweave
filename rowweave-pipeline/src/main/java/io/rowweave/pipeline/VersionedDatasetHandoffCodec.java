package io.rowweave.pipeline;

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

/** Canonical bounded wire codec for VERSIONED_DATASET handoff records. */
public final class VersionedDatasetHandoffCodec {
    private static final byte[] MAGIC = "RWXXVDH1".getBytes(StandardCharsets.US_ASCII);
    public static final int ENVELOPE_VERSION = 1;
    private static final int TYPE_KEY = 1;
    private static final int TYPE_ATTEMPT = 2;
    private static final int TYPE_EVIDENCE = 3;
    private static final int TYPE_WINNER = 4;
    private static final int TYPE_CONSUMER_REQUEST = 5;
    private static final int TYPE_CONSUMER_BINDING = 6;
    private static final int HEADER_BYTES = MAGIC.length + 4 + 4 + 4;
    private static final int CHECKSUM_BYTES = VersionedDatasetHandoffLimits.SHA256_BYTES;
    private static final int MAX_BODY_BYTES =
            VersionedDatasetHandoffLimits.MAX_RECORD_BYTES - HEADER_BYTES - CHECKSUM_BYTES;

    private VersionedDatasetHandoffCodec() { }

    public static byte[] encodeKey(VersionedDatasetHandoffKey value) {
        require(value, "key");
        return encode(TYPE_KEY, out -> writeKey(out, value));
    }

    public static VersionedDatasetHandoffKey decodeKey(byte[] frame) {
        VersionedDatasetHandoffKey value = decode(frame, TYPE_KEY,
                VersionedDatasetHandoffCodec::readKey);
        requireCanonical(frame, encodeKey(value));
        return value;
    }

    public static byte[] encodeAttempt(VersionedDatasetProducerAttempt value) {
        require(value, "attempt");
        return encode(TYPE_ATTEMPT, out -> writeAttempt(out, value));
    }

    public static VersionedDatasetProducerAttempt decodeAttempt(byte[] frame) {
        VersionedDatasetProducerAttempt value = decode(frame, TYPE_ATTEMPT,
                VersionedDatasetHandoffCodec::readAttempt);
        requireCanonical(frame, encodeAttempt(value));
        return value;
    }

    public static byte[] encodeEvidence(VersionedDatasetCommitEvidence value) {
        require(value, "evidence");
        return encode(TYPE_EVIDENCE, out -> writeEvidence(out, value));
    }

    public static VersionedDatasetCommitEvidence decodeEvidence(byte[] frame) {
        VersionedDatasetCommitEvidence value = decode(frame, TYPE_EVIDENCE,
                VersionedDatasetHandoffCodec::readEvidence);
        requireCanonical(frame, encodeEvidence(value));
        return value;
    }

    public static byte[] encodeWinner(VersionedDatasetVisibleWinner value) {
        require(value, "winner");
        return encode(TYPE_WINNER, out -> writeWinner(out, value));
    }

    public static VersionedDatasetVisibleWinner decodeWinner(byte[] frame) {
        VersionedDatasetVisibleWinner value = decode(frame, TYPE_WINNER,
                VersionedDatasetHandoffCodec::readWinner);
        requireCanonical(frame, encodeWinner(value));
        return value;
    }

    public static byte[] encodeConsumerRequest(VersionedDatasetConsumerRequest value) {
        require(value, "consumerRequest");
        return encode(TYPE_CONSUMER_REQUEST, out -> writeConsumerRequest(out, value));
    }

    public static VersionedDatasetConsumerRequest decodeConsumerRequest(byte[] frame) {
        VersionedDatasetConsumerRequest value = decode(frame, TYPE_CONSUMER_REQUEST,
                VersionedDatasetHandoffCodec::readConsumerRequest);
        requireCanonical(frame, encodeConsumerRequest(value));
        return value;
    }

    public static byte[] encodeConsumerBinding(VersionedDatasetConsumerBinding value) {
        require(value, "consumerBinding");
        return encode(TYPE_CONSUMER_BINDING, out -> writeConsumerBinding(out, value));
    }

    public static VersionedDatasetConsumerBinding decodeConsumerBinding(byte[] frame) {
        VersionedDatasetConsumerBinding value = decode(frame, TYPE_CONSUMER_BINDING,
                VersionedDatasetHandoffCodec::readConsumerBinding);
        requireCanonical(frame, encodeConsumerBinding(value));
        return value;
    }

    static String sha256Hex(byte[] value) {
        byte[] digest = sha256(value, 0, value.length);
        StringBuilder text = new StringBuilder(digest.length * 2);
        for (byte item : digest) text.append(String.format("%02x", item & 0xff));
        return text.toString();
    }

    private static void require(Object value, String path) {
        if (value == null) {
            throw PipelineText.failure("INVALID_RECORD", path, "record is required");
        }
    }

    private static byte[] encode(int type, Writer writer) {
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                writer.write(body);
            }
            byte[] payload = bodyBytes.toByteArray();
            if (payload.length > MAX_BODY_BYTES) {
                throw PipelineText.failure("LIMIT_EXCEEDED", "envelope.bodyLength",
                        "handoff record exceeds limit");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(
                    HEADER_BYTES + payload.length + CHECKSUM_BYTES);
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(ENVELOPE_VERSION);
                frame.writeInt(type);
                frame.writeInt(payload.length);
                frame.write(payload);
                frame.write(sha256(payload, 0, payload.length));
            }
            return frameBytes.toByteArray();
        } catch (PipelineValidationException failure) {
            throw failure;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory handoff encoding failed", impossible);
        }
    }

    private static <T> T decode(byte[] frame, int expectedType, Reader<T> reader) {
        if (frame == null || frame.length < HEADER_BYTES + CHECKSUM_BYTES) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope",
                    "handoff frame is shorter than header/checksum");
        }
        if (frame.length > VersionedDatasetHandoffLimits.MAX_RECORD_BYTES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", "envelope",
                    "handoff frame exceeds limit");
        }
        try (DataInputStream header = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            header.readFully(magic);
            if (!MessageDigest.isEqual(MAGIC, magic)) {
                throw PipelineText.failure("INVALID_MAGIC", "envelope.magic",
                        "invalid handoff magic");
            }
            int version = header.readInt();
            if (version != ENVELOPE_VERSION) {
                throw PipelineText.failure("UNSUPPORTED_ENVELOPE_VERSION",
                        "envelope.version", "unsupported handoff envelope version");
            }
            int type = header.readInt();
            if (type != expectedType) {
                throw PipelineText.failure("UNEXPECTED_RECORD_TYPE", "envelope.type",
                        "handoff record type does not match decoder");
            }
            int bodyLength = header.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES) {
                throw PipelineText.failure("LIMIT_EXCEEDED", "envelope.bodyLength",
                        "invalid handoff body length");
            }
            long expectedLength = (long) HEADER_BYTES + bodyLength + CHECKSUM_BYTES;
            if (expectedLength != frame.length) {
                throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope.bodyLength",
                        "frame length does not match body length");
            }
            byte[] expectedDigest = Arrays.copyOfRange(
                    frame, HEADER_BYTES + bodyLength, frame.length);
            byte[] actualDigest = sha256(frame, HEADER_BYTES, bodyLength);
            if (!MessageDigest.isEqual(expectedDigest, actualDigest)) {
                throw PipelineText.failure("CHECKSUM_MISMATCH", "envelope.checksum",
                        "handoff body checksum mismatch");
            }
            try (DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(frame, HEADER_BYTES, bodyLength))) {
                T value = reader.read(body);
                if (body.available() != 0) {
                    throw PipelineText.failure("NON_CANONICAL_FRAME", "envelope.body",
                            "handoff body contains trailing fields");
                }
                return value;
            }
        } catch (PipelineValidationException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope",
                    "truncated handoff frame");
        } catch (IOException failure) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "envelope",
                    "failed to decode handoff frame");
        }
    }

    private static void requireCanonical(byte[] supplied, byte[] canonical) {
        if (!Arrays.equals(supplied, canonical)) {
            throw PipelineText.failure("NON_CANONICAL_FRAME", "envelope",
                    "handoff frame is not canonical");
        }
    }

    private static void writeKey(DataOutputStream out, VersionedDatasetHandoffKey value)
            throws IOException {
        writeText(out, value.pipelineId(), "key.pipelineId");
        writeText(out, value.revisionId(), "key.revisionId");
        writeText(out, value.definitionDigest(), "key.definitionDigest");
        writeText(out, value.edgeId(), "key.edgeId");
        writeText(out, value.materializationId(), "key.materializationId");
        writeText(out, value.schemaFingerprint(), "key.schemaFingerprint");
        writeText(out, value.producerStepId(), "key.producerStepId");
        writeText(out, value.producerSinkOperatorId(), "key.producerSinkOperatorId");
        writeText(out, value.producerArtifactId(), "key.producerArtifactId");
        writeText(out, value.logicalRunId(), "key.logicalRunId");
    }

    private static VersionedDatasetHandoffKey readKey(DataInputStream in)
            throws IOException {
        return new VersionedDatasetHandoffKey(
                readText(in, "key.pipelineId"), readText(in, "key.revisionId"),
                readText(in, "key.definitionDigest"), readText(in, "key.edgeId"),
                readText(in, "key.materializationId"),
                readText(in, "key.schemaFingerprint"),
                readText(in, "key.producerStepId"),
                readText(in, "key.producerSinkOperatorId"),
                readText(in, "key.producerArtifactId"),
                readText(in, "key.logicalRunId"));
    }

    private static void writeAttempt(
            DataOutputStream out, VersionedDatasetProducerAttempt value)
            throws IOException {
        writeKey(out, value.key());
        writeText(out, value.producerRunId(), "attempt.producerRunId");
        writeText(out, value.attemptId(), "attempt.attemptId");
        out.writeLong(value.attemptNumber());
        out.writeLong(value.fenceToken());
    }

    private static VersionedDatasetProducerAttempt readAttempt(DataInputStream in)
            throws IOException {
        return new VersionedDatasetProducerAttempt(readKey(in),
                readText(in, "attempt.producerRunId"),
                readText(in, "attempt.attemptId"), in.readLong(), in.readLong());
    }

    private static void writeEvidence(
            DataOutputStream out, VersionedDatasetCommitEvidence value)
            throws IOException {
        writeAttempt(out, value.attempt());
        writeText(out, value.storageProviderId(), "evidence.storageProviderId");
        out.writeInt(value.storageContractVersion());
        writeText(out, value.versionId(), "evidence.versionId");
        writeText(out, value.readerBindingCodecId(), "evidence.readerBindingCodecId");
        out.writeInt(value.readerBindingCodecVersion());
        writeBytes(out, value.readerBinding());
        writeText(out, value.contentDigest(), "evidence.contentDigest");
        out.writeLong(value.rowCount());
        out.writeLong(value.byteCount());
        out.writeLong(value.committedAtEpochMillis());
    }

    private static VersionedDatasetCommitEvidence readEvidence(DataInputStream in)
            throws IOException {
        return new VersionedDatasetCommitEvidence(readAttempt(in),
                readText(in, "evidence.storageProviderId"), in.readInt(),
                readText(in, "evidence.versionId"),
                readText(in, "evidence.readerBindingCodecId"), in.readInt(),
                readBytes(in, "evidence.readerBinding",
                        VersionedDatasetHandoffLimits.MAX_READER_BINDING_BYTES),
                readText(in, "evidence.contentDigest"), in.readLong(),
                in.readLong(), in.readLong());
    }

    private static void writeWinner(
            DataOutputStream out, VersionedDatasetVisibleWinner value)
            throws IOException {
        writeEvidence(out, value.evidence());
        out.writeLong(value.visibleAtEpochMillis());
    }

    private static VersionedDatasetVisibleWinner readWinner(DataInputStream in)
            throws IOException {
        return new VersionedDatasetVisibleWinner(readEvidence(in), in.readLong());
    }

    private static void writeConsumerRequest(
            DataOutputStream out, VersionedDatasetConsumerRequest value)
            throws IOException {
        writeKey(out, value.key());
        writeText(out, value.expectedWinnerDigest(), "consumer.expectedWinnerDigest");
        writeText(out, value.consumerStepId(), "consumer.consumerStepId");
        writeText(out, value.consumerSourceOperatorId(),
                "consumer.consumerSourceOperatorId");
        writeText(out, value.consumerArtifactId(), "consumer.consumerArtifactId");
        writeText(out, value.consumerRunId(), "consumer.consumerRunId");
        writeText(out, value.consumerAttemptId(), "consumer.consumerAttemptId");
    }

    private static VersionedDatasetConsumerRequest readConsumerRequest(
            DataInputStream in) throws IOException {
        return new VersionedDatasetConsumerRequest(readKey(in),
                readText(in, "consumer.expectedWinnerDigest"),
                readText(in, "consumer.consumerStepId"),
                readText(in, "consumer.consumerSourceOperatorId"),
                readText(in, "consumer.consumerArtifactId"),
                readText(in, "consumer.consumerRunId"),
                readText(in, "consumer.consumerAttemptId"));
    }

    private static void writeConsumerBinding(
            DataOutputStream out, VersionedDatasetConsumerBinding value)
            throws IOException {
        writeConsumerRequest(out, value.request());
        writeWinner(out, value.winner());
        out.writeLong(value.boundAtEpochMillis());
    }

    private static VersionedDatasetConsumerBinding readConsumerBinding(
            DataInputStream in) throws IOException {
        return new VersionedDatasetConsumerBinding(
                readConsumerRequest(in), readWinner(in), in.readLong());
    }

    private static void writeText(DataOutputStream out, String value, String path)
            throws IOException {
        String checked = PipelineText.required(value, path,
                VersionedDatasetHandoffLimits.MAX_ID_BYTES);
        byte[] bytes = checked.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static String readText(DataInputStream in, String path) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > VersionedDatasetHandoffLimits.MAX_ID_BYTES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", path,
                    "text length exceeds limit");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        try {
            CharBuffer decoded = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes));
            return PipelineText.required(decoded.toString(), path,
                    VersionedDatasetHandoffLimits.MAX_ID_BYTES);
        } catch (CharacterCodingException failure) {
            throw PipelineText.failure("INVALID_UTF8", path,
                    "text is not canonical UTF-8");
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value)
            throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in, String path, int maximum)
            throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maximum) {
            throw PipelineText.failure("LIMIT_EXCEEDED", path,
                    "byte value exceeds limit");
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return bytes;
    }

    private static byte[] sha256(byte[] value, int offset, int length) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            digest.update(value, offset, length);
            return digest.digest();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private interface Writer {
        void write(DataOutputStream out) throws IOException;
    }

    private interface Reader<T> {
        T read(DataInputStream in) throws IOException;
    }
}
