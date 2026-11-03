package io.rowweave.stream.contract;

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
import java.util.Arrays;
import java.util.List;

/** Canonical, checksummed and bounded Stream checkpoint manifest wire. */
public final class CheckpointManifestCodec {
    private static final byte[] MAGIC =
            "RWXXSCM1".getBytes(StandardCharsets.US_ASCII);
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_BODY_BYTES = 16 * 1024 * 1024;
    // A receipt is at least 107 wire bytes (two non-empty task/worker
    // identities and one digest evidence); the existing frame bound is the
    // authoritative physical-task bound, independent of logical node count.
    public static final int MAX_TASK_RECEIPTS = MAX_BODY_BYTES / 107;
    public static final int MAX_FRAME_BYTES =
            HEADER_BYTES + MAX_BODY_BYTES + DIGEST_BYTES;

    private CheckpointManifestCodec() { }

    public static byte[] encode(CheckpointManifest manifest) {
        if (manifest == null) throw new IllegalArgumentException("manifest is required");
        return frame(MAGIC, body(manifest));
    }

    public static CheckpointManifest decode(byte[] frame) {
        byte[] body = bodyFromFrame(frame, MAGIC, MAX_BODY_BYTES);
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(body))) {
            CheckpointIdentity checkpoint = readIdentity(input);
            int taskCount = readCount(input, 1, MAX_TASK_RECEIPTS,
                    "checkpoint.manifest.receipts");
            List<CheckpointTaskReceipt> receipts = new ArrayList<>(taskCount);
            for (int taskIndex = 0; taskIndex < taskCount; taskIndex++) {
                String taskId = readText(input, 512,
                        "checkpoint.manifest.receipt.taskId");
                long assignmentGeneration = input.readLong();
                String workerIncarnation = readText(input, 256,
                        "checkpoint.manifest.receipt.workerIncarnation");
                int evidenceCount = readCount(input, 1, 256,
                        "checkpoint.manifest.receipt.evidence");
                List<CheckpointEvidence> evidence = new ArrayList<>(evidenceCount);
                for (int member = 0; member < evidenceCount; member++) {
                    evidence.add(new CheckpointEvidence(
                            CheckpointEvidence.Kind.fromWire(input.readInt()),
                            readText(input, 512,
                                    "checkpoint.manifest.evidence.memberId"),
                            readText(input, 64,
                                    "checkpoint.manifest.evidence.digest"),
                            input.readLong()));
                }
                receipts.add(new CheckpointTaskReceipt(
                        checkpoint, taskId, assignmentGeneration,
                        workerIncarnation, evidence));
            }
            if (input.available() != 0) {
                throw failure("FRAME_LENGTH_MISMATCH", "checkpoint.manifest.body",
                        "manifest body has trailing bytes");
            }
            return new CheckpointManifest(checkpoint, receipts);
        } catch (StreamContractException failure) {
            throw failure;
        } catch (EOFException truncated) {
            throw failure("FRAME_LENGTH_MISMATCH", "checkpoint.manifest.body",
                    "manifest body is truncated");
        } catch (IOException malformed) {
            throw failure("FRAME_LENGTH_MISMATCH", "checkpoint.manifest.body",
                    "manifest body is malformed");
        }
    }

    static String digestOf(CheckpointManifest manifest) {
        return hex(sha256(body(manifest)));
    }

    private static byte[] body(CheckpointManifest manifest) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream() {
                @Override public synchronized void write(int value) {
                    requireRoom(1);
                    super.write(value);
                }

                @Override public synchronized void write(byte[] value,
                        int offset, int length) {
                    requireRoom(length);
                    super.write(value, offset, length);
                }

                private void requireRoom(int length) {
                    if (length < 0
                            || count + (long) length > MAX_BODY_BYTES) {
                        throw failure("LIMIT_EXCEEDED",
                                "checkpoint.manifest.body",
                                "manifest body exceeds its limit");
                    }
                }
            };
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeIdentity(output, manifest.checkpointIdentity());
                output.writeInt(manifest.taskReceipts().size());
                for (CheckpointTaskReceipt receipt : manifest.taskReceipts()) {
                    writeText(output, receipt.taskId());
                    output.writeLong(receipt.assignmentGeneration());
                    writeText(output, receipt.workerIncarnation());
                    output.writeInt(receipt.evidence().size());
                    for (CheckpointEvidence evidence : receipt.evidence()) {
                        output.writeInt(evidence.kind().wireId());
                        writeText(output, evidence.memberId());
                        writeText(output, evidence.contentDigest());
                        output.writeLong(evidence.contentBytes());
                    }
                }
            }
            byte[] body = bytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw failure("LIMIT_EXCEEDED", "checkpoint.manifest.body",
                        "manifest body exceeds its limit");
            }
            return body;
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory manifest encode failed", impossible);
        }
    }

    static void writeIdentity(DataOutputStream output, CheckpointIdentity checkpoint)
            throws IOException {
        writeText(output, checkpoint.definitionId());
        writeText(output, checkpoint.artifactId());
        writeText(output, checkpoint.runId());
        writeText(output, checkpoint.attemptId());
        output.writeLong(checkpoint.fenceToken());
        output.writeLong(checkpoint.coordinatorTerm());
        writeText(output, checkpoint.coordinatorIncarnation());
        output.writeLong(checkpoint.checkpointSequence());
        writeText(output, checkpoint.planDigest());
        writeText(output, checkpoint.deploymentDigest());
        output.writeInt(checkpoint.dataProtocolVersion());
        output.writeInt(checkpoint.stateFormatVersion());
    }

    static CheckpointIdentity readIdentity(DataInputStream input) throws IOException {
        return new CheckpointIdentity(
                readText(input, 256, "checkpoint.definitionId"),
                readText(input, 64, "checkpoint.artifactId"),
                readText(input, 256, "checkpoint.runId"),
                readText(input, 256, "checkpoint.attemptId"),
                input.readLong(), input.readLong(),
                readText(input, 256, "checkpoint.coordinatorIncarnation"),
                input.readLong(),
                readText(input, 64, "checkpoint.planDigest"),
                readText(input, 64, "checkpoint.deploymentDigest"),
                input.readInt(), input.readInt());
    }

    static void writeText(DataOutputStream output, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    static String readText(DataInputStream input, int maxBytes, String path)
            throws IOException {
        int length = input.readInt();
        if (length <= 0 || length > maxBytes || length > input.available()) {
            throw failure("LIMIT_EXCEEDED", path, "text exceeds its limit");
        }
        byte[] encoded = new byte[length];
        input.readFully(encoded);
        try {
            return StreamContractSupport.text(StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(encoded)).toString(), path, maxBytes);
        } catch (CharacterCodingException malformed) {
            throw failure("MALFORMED_UTF8", path, "text is not valid UTF-8");
        }
    }

    static byte[] frame(byte[] magic, byte[] body) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    HEADER_BYTES + body.length + DIGEST_BYTES);
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                output.write(magic);
                output.writeInt(1);
                output.writeInt(body.length);
                output.write(body);
                output.write(sha256(body));
            }
            return bytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory checkpoint frame failed", impossible);
        }
    }

    static byte[] bodyFromFrame(byte[] frame, byte[] magic, int maxBodyBytes) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES) {
            throw failure("FRAME_LENGTH_MISMATCH", "checkpoint.frame",
                    "checkpoint frame is too short");
        }
        if (frame.length > HEADER_BYTES + maxBodyBytes + DIGEST_BYTES) {
            throw failure("LIMIT_EXCEEDED", "checkpoint.frame",
                    "checkpoint frame exceeds its limit");
        }
        if (!MessageDigest.isEqual(magic, Arrays.copyOfRange(frame, 0, 8))) {
            throw failure("INVALID_MAGIC", "checkpoint.frame.magic", "invalid magic");
        }
        int version = ByteBuffer.wrap(frame, 8, 4).getInt();
        if (version != 1) {
            throw failure("UNSUPPORTED_ENVELOPE_VERSION", "checkpoint.frame.version",
                    "unsupported checkpoint frame version");
        }
        int bodyLength = ByteBuffer.wrap(frame, 12, 4).getInt();
        if (bodyLength < 0 || bodyLength > maxBodyBytes
                || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES != frame.length) {
            throw failure("FRAME_LENGTH_MISMATCH", "checkpoint.frame.body",
                    "checkpoint frame length does not match body length");
        }
        byte[] body = Arrays.copyOfRange(frame, HEADER_BYTES,
                HEADER_BYTES + bodyLength);
        byte[] expected = Arrays.copyOfRange(frame,
                HEADER_BYTES + bodyLength, frame.length);
        if (!MessageDigest.isEqual(sha256(body), expected)) {
            throw failure("CHECKSUM_MISMATCH", "checkpoint.frame.digest",
                    "checkpoint frame checksum mismatch");
        }
        return body;
    }

    private static int readCount(
            DataInputStream input, int min, int max, String path) throws IOException {
        int count = input.readInt();
        if (count < min || count > max || count > input.available() / 4 + 1) {
            throw failure("LIMIT_EXCEEDED", path, "count exceeds its limit");
        }
        return count;
    }

    static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }

    private static StreamContractException failure(
            String code, String path, String message) {
        return StreamContractSupport.failure(code, path, message);
    }
}
