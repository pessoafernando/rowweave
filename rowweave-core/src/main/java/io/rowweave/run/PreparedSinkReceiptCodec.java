package io.rowweave.run;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical bounded outer field for prepared Sink evidence. */
public final class PreparedSinkReceiptCodec {
    public static final int MAX_FRAME_BYTES = 20 * 1024;
    private static final int MAGIC = 0x52575052; // RWPR
    private static final int VERSION = 1;
    private static final int DIGEST_BYTES = 32;

    private PreparedSinkReceiptCodec() { }

    public static byte[] encode(PreparedSinkReceipt receipt) {
        if (receipt == null) throw new IllegalArgumentException("receipt is required");
        byte[] token = StageAssignmentManifestCodec.encodeToken(receipt.token());
        byte[] locator = PreparedSinkLocatorCodec.encode(receipt.locator());
        ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                token.length + locator.length + 96);
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(token.length);
            out.write(token);
            out.write(receipt.commandDigest());
            out.writeInt(locator.length);
            out.write(locator);
        } catch (IOException impossible) {
            throw new UncheckedIOException(impossible);
        }
        byte[] body = bytes.toByteArray();
        if (body.length > MAX_FRAME_BYTES - DIGEST_BYTES) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt frame exceeds limit");
        }
        byte[] checksum = SinkAttemptIdentityCodec.sha256(body, 0, body.length);
        byte[] frame = Arrays.copyOf(body, body.length + checksum.length);
        System.arraycopy(checksum, 0, frame, body.length, checksum.length);
        return frame;
    }

    public static PreparedSinkReceipt decode(byte[] frame) {
        if (frame == null || frame.length < 80 || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt frame length out of bounds");
        }
        int bodyLength = frame.length - DIGEST_BYTES;
        if (!MessageDigest.isEqual(
                SinkAttemptIdentityCodec.sha256(frame, 0, bodyLength),
                Arrays.copyOfRange(frame, bodyLength, frame.length))) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt checksum mismatch");
        }
        try (DataInputStream in = new DataInputStream(
                new ByteArrayInputStream(frame, 0, bodyLength))) {
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "prepared Sink receipt codec unsupported");
            }
            int tokenLength = in.readInt();
            if (tokenLength < 1
                    || tokenLength > StageAssignmentManifestCodec.MAX_TOKEN_FRAME_BYTES) {
                throw new IllegalArgumentException("prepared Sink token length out of bounds");
            }
            byte[] tokenBytes = new byte[tokenLength];
            in.readFully(tokenBytes);
            StageAssignmentToken token =
                    StageAssignmentManifestCodec.decodeToken(tokenBytes);
            byte[] commandDigest = new byte[DIGEST_BYTES];
            in.readFully(commandDigest);
            int locatorLength = in.readInt();
            if (locatorLength < 1
                    || locatorLength > PreparedSinkLocatorCodec.MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "prepared Sink locator length out of bounds");
            }
            byte[] locatorBytes = new byte[locatorLength];
            in.readFully(locatorBytes);
            if (in.available() != 0) {
                throw new IllegalArgumentException(
                        "prepared Sink receipt has trailing bytes");
            }
            PreparedSinkReceipt receipt = new PreparedSinkReceipt(
                    token, commandDigest,
                    PreparedSinkLocatorCodec.decode(locatorBytes));
            if (!MessageDigest.isEqual(frame, encode(receipt))) {
                throw new IllegalArgumentException(
                        "prepared Sink receipt is not canonical");
            }
            return receipt;
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "prepared Sink receipt is malformed", malformed);
        }
    }
}
