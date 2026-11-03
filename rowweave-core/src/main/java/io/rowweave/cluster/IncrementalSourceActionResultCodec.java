package io.rowweave.cluster;

import io.rowweave.incremental.CapturedTargetReceiptCodec;
import io.rowweave.incremental.IncrementalMaterializationResponseCodec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.function.LongConsumer;

/** Canonical bounded Worker-to-Master wire for source action results. */
public final class IncrementalSourceActionResultCodec {
    public static final int MAX_FRAME_BYTES =
            IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES + 64;
    private static final int MAGIC = 0x52575352; // RWSR
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 13;
    private static final int DIGEST_BYTES = 32;

    private IncrementalSourceActionResultCodec() { }

    static byte[] encode(IncrementalSourceActionResult result) {
        if (result == null) {
            throw new IllegalArgumentException(
                    "incremental source action result is required");
        }
        byte[] payload = result.kind()
                == IncrementalSourceActionResult.Kind.CAPTURED
                ? CapturedTargetReceiptCodec.encode(result.capturedTarget())
                : IncrementalMaterializationResponseCodec.encode(
                result.materialization());
        int total = Math.addExact(Math.addExact(
                HEADER_BYTES, payload.length), DIGEST_BYTES);
        if (total > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source action result exceeds bound");
        }
        try {
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(total);
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.writeInt(MAGIC);
            frame.writeInt(VERSION);
            frame.writeByte(result.kind().ordinal());
            frame.writeInt(payload.length);
            frame.write(payload);
            frame.write(sha256(payload));
            frame.flush();
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory source action result encoding failed", impossible);
        }
    }

    static IncrementalSourceActionResult decode(
            byte[] frame, LongConsumer reservation) {
        if (frame == null || reservation == null
                || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source action result frame is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame));
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "incremental source action result header is invalid");
            }
            int kindWire = input.readUnsignedByte();
            IncrementalSourceActionResult.Kind[] kinds =
                    IncrementalSourceActionResult.Kind.values();
            if (kindWire >= kinds.length) {
                throw new IllegalArgumentException(
                        "incremental source action result kind is invalid");
            }
            IncrementalSourceActionResult.Kind kind = kinds[kindWire];
            int payloadLength = input.readInt();
            int maximum = kind == IncrementalSourceActionResult.Kind.CAPTURED
                    ? CapturedTargetReceiptCodec.MAX_FRAME_BYTES
                    : IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES;
            long expected = (long) HEADER_BYTES + payloadLength + DIGEST_BYTES;
            if (payloadLength <= 0 || payloadLength > maximum
                    || expected != frame.length) {
                throw new IllegalArgumentException(
                        "incremental source action result length is invalid");
            }
            byte[] checksum = Arrays.copyOfRange(
                    frame, frame.length - DIGEST_BYTES, frame.length);
            byte[] payloadDigest = sha256(Arrays.copyOfRange(
                    frame, HEADER_BYTES, frame.length - DIGEST_BYTES));
            if (!MessageDigest.isEqual(checksum, payloadDigest)) {
                throw new IllegalArgumentException(
                        "incremental source action result checksum is invalid");
            }
            reservation.accept(Math.multiplyExact(3L, frame.length));
            byte[] payload = new byte[payloadLength];
            input.readFully(payload);
            byte[] ignoredChecksum = new byte[DIGEST_BYTES];
            input.readFully(ignoredChecksum);
            if (input.read() != -1) {
                throw new IllegalArgumentException(
                        "incremental source action result has trailing bytes");
            }
            IncrementalSourceActionResult decoded = kind
                    == IncrementalSourceActionResult.Kind.CAPTURED
                    ? IncrementalSourceActionResult.captured(
                    CapturedTargetReceiptCodec.decode(payload))
                    : IncrementalSourceActionResult.materialized(
                    IncrementalMaterializationResponseCodec.decode(
                            payload, ignored -> { }));
            if (!MessageDigest.isEqual(frame, encode(decoded))) {
                throw new IllegalArgumentException(
                        "incremental source action result is not canonical");
            }
            return decoded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "incremental source action result is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental source action result is malformed", malformed);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }
}
