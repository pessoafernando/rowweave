package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamSourceAssignment;
import io.rowweave.stream.contract.StreamSourceSplit;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/** Bounded task-local deployment proof for one discovered source split. */
final class StreamSourceTaskAssignmentCodec {
    static final int MAX_FRAME_BYTES = 4 * 1024 * 1024 + 8 * 1024;
    private static final byte[] MAGIC =
            "RWXXSTA2".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 2;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_TEXT_BYTES = 1_024;

    static final class Value {
        private final StreamSourceAssignment assignment;
        private final StreamSourceSplit split;

        private Value(StreamSourceAssignment assignment,
                StreamSourceSplit split) {
            this.assignment = assignment;
            this.split = split;
        }

        StreamSourceAssignment assignment() { return assignment; }
        StreamSourceSplit split() { return split; }
    }

    private StreamSourceTaskAssignmentCodec() { }

    static byte[] encode(StreamSourceAssignment assignment,
            StreamSourceSplit split) {
        if (assignment == null || split == null
                || !assignment.splitId().equals(split.splitId())) {
            throw new IllegalArgumentException(
                    "source task assignment and split must agree");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream body = new DataOutputStream(bodyBytes)) {
                writeText(body, assignment.jobId());
                writeText(body, assignment.attemptId());
                writeText(body, assignment.sourceOperatorUid());
                writeText(body, assignment.splitId());
                body.writeLong(assignment.splitGeneration());
                body.writeInt(assignment.subtaskIndex());
                writeText(body, assignment.workerIncarnation());
                body.writeLong(assignment.assignmentGeneration());
                writeText(body, split.positionCodecId());
                byte[] descriptor = split.descriptorFrame();
                body.writeInt(descriptor.length);
                body.write(descriptor);
            }
            byte[] body = bodyBytes.toByteArray();
            if (MAGIC.length + 8L + body.length + DIGEST_BYTES
                    > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "source task assignment exceeds its bound");
            }
            ByteArrayOutputStream frame = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(frame)) {
                output.write(MAGIC);
                output.writeInt(VERSION);
                output.writeInt(body.length);
                output.write(body);
                output.write(sha256(body));
            }
            return frame.toByteArray();
        } catch (IOException impossible) {
            throw new AssertionError(impossible);
        }
    }

    static Value decode(byte[] frame) {
        if (frame == null || frame.length < MAGIC.length + 8 + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "source task assignment frame is invalid");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            int version = input.readInt();
            int bodyLength = input.readInt();
            if (!MessageDigest.isEqual(MAGIC, magic) || version != VERSION
                    || bodyLength < 0
                    || MAGIC.length + 8L + bodyLength + DIGEST_BYTES
                    != frame.length) {
                throw new IllegalArgumentException(
                        "source task assignment header is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(expected, sha256(body))) {
                throw new IllegalArgumentException(
                        "source task assignment checksum differs");
            }
            return decodeBody(body);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "source task assignment is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "source task assignment is malformed", malformed);
        }
    }

    private static Value decodeBody(byte[] bodyBytes) throws IOException {
        try (DataInputStream body = new DataInputStream(
                new ByteArrayInputStream(bodyBytes))) {
            String jobId = readText(body);
            String attemptId = readText(body);
            String operatorUid = readText(body);
            String splitId = readText(body);
            long splitGeneration = body.readLong();
            int subtaskIndex = body.readInt();
            String workerIncarnation = readText(body);
            long assignmentGeneration = body.readLong();
            String positionCodecId = readText(body);
            int descriptorLength = body.readInt();
            if (descriptorLength < 0
                    || descriptorLength > 4 * 1024 * 1024
                    || descriptorLength > body.available()) {
                throw new IllegalArgumentException(
                        "source split descriptor exceeds its bound");
            }
            byte[] descriptor = new byte[descriptorLength];
            body.readFully(descriptor);
            if (body.available() != 0) {
                throw new IllegalArgumentException(
                        "source task assignment has trailing bytes");
            }
            StreamSourceAssignment assignment = new StreamSourceAssignment(
                    jobId, attemptId, operatorUid, splitId, splitGeneration,
                    subtaskIndex, workerIncarnation, assignmentGeneration);
            return new Value(assignment, new StreamSourceSplit(
                    splitId, positionCodecId, descriptor));
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "source task assignment text exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > MAX_TEXT_BYTES
                || length > input.available()) {
            throw new IllegalArgumentException(
                    "source task assignment text is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
