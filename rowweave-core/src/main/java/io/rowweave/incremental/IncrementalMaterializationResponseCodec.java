package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.BatchSourceSplitDescriptorCodec;

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

/** Canonical bounded wire for provider materialization metadata. */
public final class IncrementalMaterializationResponseCodec {
    public static final int MAX_FRAME_BYTES = 8 * 1024 * 1024;
    private static final int MAGIC = 0x5257494d; // RWIM
    private static final int FRAME_VERSION = 1;
    private static final int HEADER_BYTES = 12;
    private static final int DIGEST_BYTES = 32;

    private IncrementalMaterializationResponseCodec() { }

    public static byte[] encode(IncrementalMaterializationResponse response) {
        if (response == null) {
            throw new IllegalArgumentException(
                    "incremental materialization response is required");
        }
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream();
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            writeText(payload, response.providerKey().connectorType());
            payload.writeInt(response.providerKey().connectorVersion());
            writeBytes(payload, CapturedTargetReceiptCodec.encode(
                    response.capturedTarget()));
            payload.write(response.partitionSetDigest());
            payload.writeInt(response.splits().size());
            for (IncrementalMaterializedSplit split : response.splits()) {
                writeBytes(payload, BatchSourceSplitDescriptorCodec.encode(
                        split.descriptor()));
                writeBytes(payload, split.openRequest());
            }
            payload.flush();
            byte[] body = payloadBytes.toByteArray();
            long total = (long) HEADER_BYTES + body.length + DIGEST_BYTES;
            if (total > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "incremental materialization frame exceeds bound");
            }
            ByteArrayOutputStream frameBytes =
                    new ByteArrayOutputStream((int) total);
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.writeInt(MAGIC);
            frame.writeInt(FRAME_VERSION);
            frame.writeInt(body.length);
            frame.write(body);
            frame.write(sha256(body));
            frame.flush();
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory incremental materialization encoding failed",
                    impossible);
        }
    }

    public static IncrementalMaterializationResponse decode(byte[] frame) {
        return decode(frame, ignored -> { });
    }

    /** Reserves the large-control 3x envelope before any payload allocation. */
    public static IncrementalMaterializationResponse decode(
            byte[] frame, java.util.function.LongConsumer reservation) {
        if (frame == null || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental materialization frame length is invalid");
        }
        if (reservation == null) {
            throw new IllegalArgumentException(
                    "incremental materialization reservation is required");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame));
            if (input.readInt() != MAGIC
                    || input.readInt() != FRAME_VERSION) {
                throw new IllegalArgumentException(
                        "incremental materialization frame header is invalid");
            }
            int payloadLength = input.readInt();
            long expected = (long) HEADER_BYTES + payloadLength + DIGEST_BYTES;
            if (payloadLength < 0 || expected != frame.length) {
                throw new IllegalArgumentException(
                        "incremental materialization payload length is invalid");
            }
            reservation.accept(Math.multiplyExact(3L, frame.length));
            byte[] body = new byte[payloadLength];
            input.readFully(body);
            byte[] checksum = new byte[DIGEST_BYTES];
            input.readFully(checksum);
            if (input.read() != -1 || !MessageDigest.isEqual(
                    checksum, sha256(body))) {
                throw new IllegalArgumentException(
                        "incremental materialization checksum is invalid");
            }
            DataInputStream payload = new DataInputStream(
                    new ByteArrayInputStream(body));
            IncrementalSourceProviderKey key =
                    new IncrementalSourceProviderKey(
                            readText(payload,
                                    IncrementalSourceProviderKey
                                            .MAX_CONNECTOR_TYPE_BYTES),
                            payload.readInt());
            CapturedTargetReceipt receipt = CapturedTargetReceiptCodec.decode(
                    readBytes(payload, CapturedTargetReceiptCodec.MAX_FRAME_BYTES,
                            "captured target"));
            byte[] partitions = readExact(payload, DIGEST_BYTES,
                    "partition set digest");
            if (payload.available() < Integer.BYTES) {
                throw new IllegalArgumentException(
                        "incremental materialization split count is missing");
            }
            int count = payload.readInt();
            if (count <= 0 || count > IncrementalMaterializationResponse.MAX_SPLITS
                    || (long) count * (2L * Integer.BYTES + 2L)
                    > payload.available()) {
                throw new IllegalArgumentException(
                        "incremental materialization split count is invalid");
            }
            List<IncrementalMaterializedSplit> splits =
                    new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                BatchSourceSplitDescriptor descriptor =
                        BatchSourceSplitDescriptorCodec.decode(readBytes(
                                payload,
                                BatchSourceSplitDescriptorCodec.MAX_FRAME_BYTES,
                                "split descriptor"));
                byte[] request = readBytes(payload,
                        IncrementalMaterializedSplit.MAX_OPEN_REQUEST_BYTES,
                        "split open request");
                splits.add(new IncrementalMaterializedSplit(
                        descriptor, request));
            }
            if (payload.read() != -1) {
                throw new IllegalArgumentException(
                        "incremental materialization payload has trailing bytes");
            }
            IncrementalMaterializationResponse decoded =
                    new IncrementalMaterializationResponse(
                            key, receipt, partitions, splits);
            if (!MessageDigest.isEqual(frame, encode(decoded))) {
                throw new IllegalArgumentException(
                        "incremental materialization frame is not canonical");
            }
            return decoded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "incremental materialization frame is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental materialization frame is malformed", malformed);
        }
    }

    public static byte[] digest(IncrementalMaterializationResponse response) {
        return sha256(encode(response));
    }

    private static void writeText(DataOutputStream out, String value)
            throws IOException {
        writeBytes(out, value.getBytes(StandardCharsets.UTF_8));
    }

    private static String readText(DataInputStream in, int limit)
            throws IOException {
        byte[] encoded = readBytes(in, limit, "provider type");
        String value = new String(encoded, StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(encoded,
                value.getBytes(StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException(
                    "incremental materialization text is invalid UTF-8");
        }
        return value;
    }

    private static void writeBytes(DataOutputStream out, byte[] value)
            throws IOException {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(
            DataInputStream in, int limit, String field) throws IOException {
        if (in.available() < Integer.BYTES) {
            throw new IllegalArgumentException(field + " length is missing");
        }
        int length = in.readInt();
        if (length <= 0 || length > limit || length > in.available()) {
            throw new IllegalArgumentException(field + " length is invalid");
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    private static byte[] readExact(
            DataInputStream in, int length, String field) throws IOException {
        if (in.available() < length) {
            throw new IllegalArgumentException(field + " is truncated");
        }
        byte[] value = new byte[length];
        in.readFully(value);
        return value;
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
