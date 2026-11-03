package io.rowweave.cluster;

import io.rowweave.incremental.CapturedTargetReceiptCodec;
import io.rowweave.incremental.IncrementalCaptureRequestCodec;
import io.rowweave.incremental.IncrementalSourceConfigCodec;
import io.rowweave.incremental.IncrementalSourceProviderContractCodec;

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

/** Canonical bounded Master-to-Worker wire for source control actions. */
public final class IncrementalSourceActionCommandCodec {
    public static final int MAX_FRAME_BYTES = 96 * 1024;
    private static final int MAGIC = 0x52575341; // RWSA
    private static final int VERSION = 1;
    private static final int HEADER_BYTES = 13;
    private static final int DIGEST_BYTES = 32;

    private IncrementalSourceActionCommandCodec() { }

    static byte[] encode(IncrementalSourceActionCommand command) {
        if (command == null) {
            throw new IllegalArgumentException(
                    "incremental source action command is required");
        }
        byte[] contract = IncrementalSourceProviderContractCodec.encode(
                command.contract());
        byte[] config = IncrementalSourceConfigCodec.encode(
                command.configuration());
        byte[] operand = command.action()
                == IncrementalSourceActionCommand.Action.CAPTURE
                ? IncrementalCaptureRequestCodec.encode(command.captureRequest())
                : CapturedTargetReceiptCodec.encode(command.capturedTarget());
        try {
            ByteArrayOutputStream payloadBytes = new ByteArrayOutputStream(
                    contract.length + config.length + operand.length + 12);
            DataOutputStream payload = new DataOutputStream(payloadBytes);
            writeBytes(payload, contract);
            writeBytes(payload, config);
            writeBytes(payload, operand);
            payload.flush();
            byte[] body = payloadBytes.toByteArray();
            int total = Math.addExact(
                    Math.addExact(HEADER_BYTES, body.length), DIGEST_BYTES);
            if (total > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "incremental source action command exceeds bound");
            }
            ByteArrayOutputStream frameBytes = new ByteArrayOutputStream(total);
            DataOutputStream frame = new DataOutputStream(frameBytes);
            frame.writeInt(MAGIC);
            frame.writeInt(VERSION);
            frame.writeByte(command.action().ordinal());
            frame.writeInt(body.length);
            frame.write(body);
            frame.write(sha256(body));
            frame.flush();
            return frameBytes.toByteArray();
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory source action command encoding failed", impossible);
        }
    }

    static IncrementalSourceActionCommand decode(
            byte[] frame, LongConsumer reservation) {
        if (frame == null || reservation == null
                || frame.length < HEADER_BYTES + DIGEST_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source action command frame is invalid");
        }
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(frame));
            if (input.readInt() != MAGIC || input.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "incremental source action command header is invalid");
            }
            int actionWire = input.readUnsignedByte();
            IncrementalSourceActionCommand.Action[] actions =
                    IncrementalSourceActionCommand.Action.values();
            if (actionWire >= actions.length) {
                throw new IllegalArgumentException(
                        "incremental source action command kind is invalid");
            }
            IncrementalSourceActionCommand.Action action = actions[actionWire];
            int payloadLength = input.readInt();
            long expected = (long) HEADER_BYTES + payloadLength + DIGEST_BYTES;
            if (payloadLength < 0 || expected != frame.length) {
                throw new IllegalArgumentException(
                        "incremental source action command length is invalid");
            }
            byte[] checksum = Arrays.copyOfRange(
                    frame, frame.length - DIGEST_BYTES, frame.length);
            byte[] bodyDigest = sha256(Arrays.copyOfRange(
                    frame, HEADER_BYTES, frame.length - DIGEST_BYTES));
            if (!MessageDigest.isEqual(checksum, bodyDigest)) {
                throw new IllegalArgumentException(
                        "incremental source action command checksum is invalid");
            }
            reservation.accept(Math.multiplyExact(3L, frame.length));
            byte[] contractBytes = readBytes(input,
                    IncrementalSourceProviderContractCodec.MAX_FRAME_BYTES,
                    "contract");
            byte[] configBytes = readBytes(input,
                    IncrementalSourceConfigCodec.MAX_FRAME_BYTES,
                    "configuration");
            int operandLimit = action
                    == IncrementalSourceActionCommand.Action.CAPTURE
                    ? IncrementalCaptureRequestCodec.MAX_FRAME_BYTES
                    : CapturedTargetReceiptCodec.MAX_FRAME_BYTES;
            byte[] operand = readBytes(input, operandLimit, "operand");
            byte[] ignoredChecksum = new byte[DIGEST_BYTES];
            input.readFully(ignoredChecksum);
            if (input.read() != -1) {
                throw new IllegalArgumentException(
                        "incremental source action command has trailing bytes");
            }
            io.rowweave.incremental.IncrementalSourceProviderContract contract =
                    IncrementalSourceProviderContractCodec.decode(contractBytes);
            io.rowweave.incremental.IncrementalSourceConfig config =
                    IncrementalSourceConfigCodec.decode(configBytes);
            IncrementalSourceActionCommand decoded = action
                    == IncrementalSourceActionCommand.Action.CAPTURE
                    ? IncrementalSourceActionCommand.capture(contract, config,
                    IncrementalCaptureRequestCodec.decode(operand))
                    : IncrementalSourceActionCommand.materialize(contract, config,
                    CapturedTargetReceiptCodec.decode(operand));
            if (!MessageDigest.isEqual(frame, encode(decoded))) {
                throw new IllegalArgumentException(
                        "incremental source action command is not canonical");
            }
            return decoded;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "incremental source action command is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental source action command is malformed", malformed);
        }
    }

    private static void writeBytes(DataOutputStream output, byte[] value)
            throws IOException {
        output.writeInt(value.length);
        output.write(value);
    }

    private static byte[] readBytes(
            DataInputStream input, int maximum, String field)
            throws IOException {
        if (input.available() < Integer.BYTES) {
            throw new IllegalArgumentException(
                    "incremental source action " + field + " length is missing");
        }
        int length = input.readInt();
        if (length <= 0 || length > maximum
                || length > input.available() - DIGEST_BYTES) {
            throw new IllegalArgumentException(
                    "incremental source action " + field + " length is invalid");
        }
        byte[] value = new byte[length];
        input.readFully(value);
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
