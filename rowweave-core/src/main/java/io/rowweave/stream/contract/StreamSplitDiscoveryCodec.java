package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.spi.StreamDiscoveryCursor;
import io.rowweave.stream.spi.StreamSplitChange;
import io.rowweave.stream.spi.StreamSplitDiscovery;

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
import java.util.List;

/** Checksummed and caller-bounded Worker frame for one discovery response. */
public final class StreamSplitDiscoveryCodec {
    private static final byte[] MAGIC =
            "RWXXSSD2".getBytes(StandardCharsets.US_ASCII);
    private static final int VERSION = 2;
    private static final int HEADER_BYTES = 16;
    private static final int DIGEST_BYTES = 32;
    private static final int MAX_BODY_BYTES = 32 * 1024 * 1024;
    public static final int MAX_FRAME_BYTES = HEADER_BYTES + MAX_BODY_BYTES
            + DIGEST_BYTES;
    private static final int MAX_TEXT_BYTES = 1_024;

    private StreamSplitDiscoveryCodec() { }

    public static byte[] encode(StreamSplitDiscovery discovery,
            int maximumChanges, int maximumCursorBytes,
            int maximumDescriptorBytes) {
        validateLimits(maximumChanges, maximumCursorBytes,
                maximumDescriptorBytes);
        if (discovery == null
                || discovery.changes().size() > maximumChanges
                || discovery.nextCursor().frame().length
                > maximumCursorBytes) {
            throw new IllegalArgumentException(
                    "split discovery exceeds its negotiated limits");
        }
        try {
            ByteArrayOutputStream bodyBytes = new ByteArrayOutputStream();
            try (DataOutputStream output = new DataOutputStream(bodyBytes)) {
                output.writeLong(discovery.discoveryEpoch());
                writeFrame(output, discovery.nextCursor().frame(),
                        maximumCursorBytes);
                output.writeInt(discovery.changes().size());
                for (StreamSplitChange change : discovery.changes()) {
                    output.writeInt(change.kind() == StreamSplitChange.Kind.ADD
                            ? 1 : 2);
                    writeText(output, change.splitId());
                    output.writeLong(change.generation());
                    if (change.kind() == StreamSplitChange.Kind.ADD) {
                        StreamSourceSplit checked = new StreamSourceSplit(
                                change.split().splitId(),
                                change.split().positionCodecId(),
                                change.split().descriptorFrame());
                        writeText(output, checked.positionCodecId());
                        writeFrame(output, checked.descriptorFrame(),
                                maximumDescriptorBytes);
                    }
                }
            }
            byte[] body = bodyBytes.toByteArray();
            if (body.length > MAX_BODY_BYTES) {
                throw new IllegalArgumentException(
                        "split discovery frame exceeds its hard bound");
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

    public static StreamSplitDiscovery decode(byte[] frame,
            int maximumChanges, int maximumCursorBytes,
            int maximumDescriptorBytes) {
        validateLimits(maximumChanges, maximumCursorBytes,
                maximumDescriptorBytes);
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "split discovery frame length is invalid");
        }
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(frame))) {
            byte[] magic = new byte[MAGIC.length];
            input.readFully(magic);
            if (!MessageDigest.isEqual(magic, MAGIC)
                    || input.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "split discovery frame version is invalid");
            }
            int bodyLength = input.readInt();
            if (bodyLength < 0 || bodyLength > MAX_BODY_BYTES
                    || HEADER_BYTES + (long) bodyLength + DIGEST_BYTES
                    != frame.length) {
                throw new IllegalArgumentException(
                        "split discovery frame body is invalid");
            }
            byte[] body = new byte[bodyLength];
            input.readFully(body);
            byte[] expected = new byte[DIGEST_BYTES];
            input.readFully(expected);
            if (!MessageDigest.isEqual(sha256(body), expected)) {
                throw new IllegalArgumentException(
                        "split discovery frame checksum mismatch");
            }
            return decodeBody(body, maximumChanges, maximumCursorBytes,
                    maximumDescriptorBytes);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "split discovery frame is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "split discovery frame is malformed", malformed);
        }
    }

    private static StreamSplitDiscovery decodeBody(byte[] bodyBytes,
            int maximumChanges, int maximumCursorBytes,
            int maximumDescriptorBytes) throws IOException {
        try (DataInputStream input = new DataInputStream(
                new ByteArrayInputStream(bodyBytes))) {
            long epoch = input.readLong();
            StreamDiscoveryCursor cursor = StreamDiscoveryCursor.of(
                    readFrame(input, maximumCursorBytes),
                    maximumCursorBytes);
            int count = input.readInt();
            if (count < 0 || count > maximumChanges) {
                throw new IllegalArgumentException(
                        "split discovery change count exceeds its limit");
            }
            List<StreamSplitChange> changes = new ArrayList<>(count);
            for (int index = 0; index < count; index++) {
                int kind = input.readInt();
                String splitId = readText(input);
                long generation = input.readLong();
                if (kind == 1) {
                    changes.add(StreamSplitChange.add(new StreamSplitSpec(
                            splitId, readText(input),
                            readFrame(input, maximumDescriptorBytes)),
                            generation));
                } else if (kind == 2) {
                    changes.add(StreamSplitChange.remove(
                            splitId, generation));
                } else {
                    throw new IllegalArgumentException(
                            "split discovery change kind is invalid");
                }
            }
            if (input.available() != 0) {
                throw new IllegalArgumentException(
                        "split discovery frame has trailing bytes");
            }
            return new StreamSplitDiscovery(epoch, cursor, changes);
        }
    }

    private static void validateLimits(int maximumChanges,
            int maximumCursorBytes, int maximumDescriptorBytes) {
        if (maximumChanges < 1
                || maximumChanges
                > StreamSourceDiscoverySpec.HARD_MAX_CHANGES
                || maximumCursorBytes < 1
                || maximumCursorBytes
                > StreamSourceDiscoverySpec.HARD_MAX_FRAME_BYTES
                || maximumDescriptorBytes < 1
                || maximumDescriptorBytes
                > StreamSourceDiscoverySpec.HARD_MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "split discovery codec limits are invalid");
        }
    }

    private static void writeText(DataOutputStream output, String value)
            throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length < 1 || bytes.length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "split discovery text exceeds its bound");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readText(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 1 || length > MAX_TEXT_BYTES) {
            throw new IllegalArgumentException(
                    "split discovery text length is invalid");
        }
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static void writeFrame(DataOutputStream output, byte[] value,
            int maximumBytes) throws IOException {
        if (value == null || value.length > maximumBytes) {
            throw new IllegalArgumentException(
                    "split discovery field exceeds its bound");
        }
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readFrame(DataInputStream input, int maximumBytes)
            throws IOException {
        int length = input.readInt();
        if (length < 0 || length > maximumBytes) {
            throw new IllegalArgumentException(
                    "split discovery field length is invalid");
        }
        byte[] value = new byte[length];
        input.readFully(value);
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
