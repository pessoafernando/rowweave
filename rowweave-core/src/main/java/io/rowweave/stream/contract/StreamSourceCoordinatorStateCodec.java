package io.rowweave.stream.contract;

import io.rowweave.stream.spi.StreamDiscoveryCursor;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Checksummed, allocation-bounded checkpoint codec for Source coordination. */
public final class StreamSourceCoordinatorStateCodec {
    private static final byte[] MAGIC =
            "RWXXSSC2".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 2;
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_BODY_BYTES = 32 * 1024 * 1024;
    private static final int MAX_TEXT_BYTES = 1_024;
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private StreamSourceCoordinatorStateCodec() { }

    public static byte[] encode(StreamSourceCoordinatorState state) {
        if (state == null) {
            throw new IllegalArgumentException("coordinator state is required");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bodyBytes)) {
                writeText(output, state.jobId());
                writeText(output, state.attemptId());
                writeText(output, state.sourceOperatorUid());
                output.writeLong(state.discoveryEpoch());
                writeFrame(output, state.cursor().frame());
                output.writeInt(state.splits().size());
                for (StreamSourceCoordinatorSplit split : state.splits()) {
                    writeSplit(output, split);
                }
            }
            byte[] body = bodyBytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw new IllegalArgumentException(
                        "source coordinator checkpoint exceeds its bound");
            }
            ByteArrayOutputStream frame = new ByteArrayOutputStream(
                    HEADER_BYTES + body.length + DIGEST_BYTES);
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

    public static StreamSourceCoordinatorState decode(byte[] frame) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > HEADER_BYTES + MAX_BODY_BYTES
                + DIGEST_BYTES) {
            throw new IllegalArgumentException(
                    "source coordinator checkpoint length is invalid");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!MessageDigest.isEqual(magic, MAGIC)
                    || input.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "source coordinator checkpoint version is invalid");
            }
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                    || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES
                    != frame.length) {
                throw new IllegalArgumentException(
                        "source coordinator checkpoint body is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(sha256(body), expected)) {
                throw new IllegalArgumentException(
                        "source coordinator checkpoint checksum mismatch");
            }
            return decodeBody(body);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "source coordinator checkpoint is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "source coordinator checkpoint is malformed", malformed);
        }
    }

    private static StreamSourceCoordinatorState decodeBody(byte[] bodyBytes)
            throws IOException {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(bodyBytes))) {
            String jobId = readText(input);
            String attemptId = readText(input);
            String operatorUid = readText(input);
            long epoch = input.readLong();
            StreamDiscoveryCursor cursor = StreamDiscoveryCursor.of(
                    readFrame(input));
            int count = readCount(input, 1_000_000);
            List<StreamSourceCoordinatorSplit> splits =
                    new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                splits.add(readSplit(input));
            }
            if (input.available() != 0) {
                throw new IllegalArgumentException(
                        "source coordinator checkpoint has trailing bytes");
            }
            return new StreamSourceCoordinatorState(jobId, attemptId,
                    operatorUid, epoch, cursor, splits);
        }
    }

    private static void writeSplit(DataOutputStream output,
            StreamSourceCoordinatorSplit value) throws IOException {
        writeText(output, value.splitId());
        output.writeLong(value.splitGeneration());
        output.writeInt(value.status().wireId());
        output.writeBoolean(value.hasSplitDescriptor());
        if (value.hasSplitDescriptor()) {
            writeText(output, value.split().positionCodecId());
            writeFrame(output, value.split().descriptorFrame());
        }
        output.writeLong(value.lastAssignmentGeneration());
        output.writeBoolean(value.hasAssignment());
        if (value.hasAssignment()) writeAssignment(output, value.assignment());
        output.writeBoolean(value.hasPosition());
        if (value.hasPosition()) {
            output.writeLong(value.logicalPosition());
            writeFrame(output, value.positionFrame());
        }
    }

    private static StreamSourceCoordinatorSplit readSplit(
            DataInputStream input) throws IOException {
        String splitId = readText(input);
        long splitGeneration = input.readLong();
        StreamSourceCoordinatorSplit.Status status =
                StreamSourceCoordinatorSplit.Status.fromWire(input.readInt());
        StreamSourceSplit split = input.readBoolean()
                ? new StreamSourceSplit(splitId, readText(input),
                readFrame(input)) : null;
        long lastAssignmentGeneration = input.readLong();
        StreamSourceAssignment assignment = input.readBoolean()
                ? readAssignment(input) : null;
        long logicalPosition = -1L;
        byte[] position = null;
        if (input.readBoolean()) {
            logicalPosition = input.readLong();
            position = readFrame(input);
        }
        return new StreamSourceCoordinatorSplit(splitId, splitGeneration,
                split, status, assignment, lastAssignmentGeneration,
                logicalPosition, position);
    }

    private static void writeAssignment(DataOutputStream output,
            StreamSourceAssignment value) throws IOException {
        writeText(output, value.jobId());
        writeText(output, value.attemptId());
        writeText(output, value.sourceOperatorUid());
        writeText(output, value.splitId());
        output.writeLong(value.splitGeneration());
        output.writeInt(value.subtaskIndex());
        writeText(output, value.workerIncarnation());
        output.writeLong(value.assignmentGeneration());
    }

    private static StreamSourceAssignment readAssignment(
            DataInputStream input) throws IOException {
        return new StreamSourceAssignment(readText(input), readText(input),
                readText(input), readText(input), input.readLong(),
                input.readInt(), readText(input), input.readLong());
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "source coordinator text exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = readCount(input, MAX_TEXT_BYTES);
        if (length == 0) {
            throw new IllegalArgumentException(
                    "source coordinator text is empty");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeFrame(DataOutputStream output, byte[] value)
            throws IOException {
        if (value == null || value.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "source coordinator frame exceeds its bound");
        }
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readFrame(DataInputStream input) throws IOException {
        int length = readCount(input, MAX_FRAME_BYTES);
        byte[] value = new byte[length];
        input.readFully(value);
        return value;
    }

    private static int readCount(DataInputStream input, int maximum)
            throws IOException {
        int value = input.readInt();
        if (value < 0 || value > maximum) {
            throw new IllegalArgumentException(
                    "source coordinator count exceeds its bound");
        }
        return value;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }
}
