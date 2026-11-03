package io.rowweave.pipeline;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

final class VersionedDatasetAuthorityRecordCodec {
    private static final byte[] MAGIC = "RWXXVDAR".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = MAGIC.length + 4 + 4;
    private static final int CHECKSUM_BYTES = VersionedDatasetHandoffLimits.SHA256_BYTES;
    private static final int MAX_BODY_BYTES =
            VersionedDatasetHandoffLimits.MAX_RECORD_BYTES - HEADER_BYTES - CHECKSUM_BYTES;

    private VersionedDatasetAuthorityRecordCodec() { }

    static byte[] encode(Slot value) {
        try {
            byte[] attempt = VersionedDatasetHandoffCodec.encodeAttempt(value.attempt);
            byte[] winner = value.winner == null ? null
                    : VersionedDatasetHandoffCodec.encodeWinner(value.winner);
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                writeBytes(body, attempt);
                body.writeBoolean(winner != null);
                if (winner != null) writeBytes(body, winner);
            }
            byte[] payload = bodyBytes.toByteArray();
            if (payload.length > MAX_BODY_BYTES) {
                throw PipelineText.failure("LIMIT_EXCEEDED", "authorityRecord.body",
                        "authority record exceeds limit");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(
                    HEADER_BYTES + payload.length + CHECKSUM_BYTES);
            try (DataOutputStream frame = new DataOutputStream(frameBytes)) {
                frame.write(MAGIC);
                frame.writeInt(VERSION);
                frame.writeInt(payload.length);
                frame.write(payload);
                frame.write(hexToBytes(
                        VersionedDatasetHandoffCodec.sha256Hex(payload)));
            }
            return frameBytes.toByteArray();
        } catch (PipelineValidationException failure) {
            throw failure;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory authority encoding failed", impossible);
        }
    }

    static Slot decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + CHECKSUM_BYTES) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "authorityRecord",
                    "authority record is shorter than header/checksum");
        }
        if (frame.length > VersionedDatasetHandoffLimits.MAX_RECORD_BYTES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", "authorityRecord",
                    "authority record exceeds limit");
        }
        try (DataInputStream header = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            header.readFully(magic);
            if (!MessageDigest.isEqual(MAGIC, magic)) {
                throw PipelineText.failure("INVALID_MAGIC", "authorityRecord.magic",
                        "invalid authority record magic");
            }
            if (header.readInt() != VERSION) {
                throw PipelineText.failure("UNSUPPORTED_ENVELOPE_VERSION",
                        "authorityRecord.version", "unsupported authority record version");
            }
            int bodyLength = header.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES) {
                throw PipelineText.failure("LIMIT_EXCEEDED",
                        "authorityRecord.bodyLength", "invalid authority body length");
            }
            if ((long) HEADER_BYTES + bodyLength + CHECKSUM_BYTES != frame.length) {
                throw PipelineText.failure("FRAME_LENGTH_MISMATCH",
                        "authorityRecord.bodyLength",
                        "authority frame length does not match body length");
            }
            byte[] expected = Arrays.copyOfRange(
                    frame, HEADER_BYTES + bodyLength, frame.length);
            byte[] payload = Arrays.copyOfRange(
                    frame, HEADER_BYTES, HEADER_BYTES + bodyLength);
            byte[] actual = hexToBytes(
                    VersionedDatasetHandoffCodec.sha256Hex(payload));
            if (!MessageDigest.isEqual(expected, actual)) {
                throw PipelineText.failure("CHECKSUM_MISMATCH",
                        "authorityRecord.checksum", "authority checksum mismatch");
            }
            Slot value;
            try (DataInputStream body = new DataInputStream(
                    new ByteArrayInputStream(payload))) {
                VersionedDatasetProducerAttempt attempt =
                        VersionedDatasetHandoffCodec.decodeAttempt(
                                readBytes(body, "authorityRecord.attempt"));
                VersionedDatasetVisibleWinner winner = body.readBoolean()
                        ? VersionedDatasetHandoffCodec.decodeWinner(
                                readBytes(body, "authorityRecord.winner"))
                        : null;
                if (body.available() != 0) {
                    throw PipelineText.failure("NON_CANONICAL_FRAME",
                            "authorityRecord.body", "record has trailing fields");
                }
                if (winner != null
                        && !winner.evidence().attempt().key().equals(attempt.key())) {
                    throw PipelineText.failure("AUTHORITY_RECORD_MISMATCH",
                            "authorityRecord.winner", "winner key differs from claim");
                }
                value = new Slot(attempt, winner);
            }
            if (!Arrays.equals(frame, encode(value))) {
                throw PipelineText.failure("NON_CANONICAL_FRAME", "authorityRecord",
                        "authority record is not canonical");
            }
            return value;
        } catch (PipelineValidationException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "authorityRecord",
                    "truncated authority record");
        } catch (IOException failure) {
            throw PipelineText.failure("FRAME_LENGTH_MISMATCH", "authorityRecord",
                    "failed to decode authority record");
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value)
            throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in, String path) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > VersionedDatasetHandoffLimits.MAX_RECORD_BYTES) {
            throw PipelineText.failure("LIMIT_EXCEEDED", path,
                    "nested frame exceeds limit");
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    private static byte[] hexToBytes(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    static final class Slot {
        final VersionedDatasetProducerAttempt attempt;
        final VersionedDatasetVisibleWinner winner;

        Slot(VersionedDatasetProducerAttempt attempt,
             VersionedDatasetVisibleWinner winner) {
            this.attempt = attempt;
            this.winner = winner;
        }
    }
}
