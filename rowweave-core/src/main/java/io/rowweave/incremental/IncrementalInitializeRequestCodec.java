package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical checksum-protected wire for one bounded feed bootstrap request. */
public final class IncrementalInitializeRequestCodec {
    public static final int MAX_FRAME_BYTES = 512;
    private static final int MAGIC = 0x52574649; // RWFI
    private static final int VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;

    private IncrementalInitializeRequestCodec() { }

    public static byte[] encode(IncrementalInitializeRequest request) {
        if (request == null) {
            throw new IllegalArgumentException(
                    "incremental initialize request is required");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(256);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            IncrementalAuthorityCodecSupport.writeText(out, request.feedId());
            out.writeLong(request.feedIncarnation());
            out.write(request.definitionDigest());
            out.write(request.sourceContractDigest());
            out.write(request.frontierContractDigest());
            out.writeByte(request.frontierKind().wireId());
            out.flush();
            byte[] body = bytes.toByteArray();
            if (body.length + CHECKSUM_BYTES > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "incremental initialize request exceeds frame limit");
            }
            byte[] frame = Arrays.copyOf(body, body.length + CHECKSUM_BYTES);
            System.arraycopy(IncrementalAuthorityValue.sha256(body), 0,
                    frame, body.length, CHECKSUM_BYTES);
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "encode incremental initialize request failed", impossible);
        }
    }

    public static IncrementalInitializeRequest decode(byte[] frame) {
        if (frame == null || frame.length <= CHECKSUM_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental initialize request frame length is invalid");
        }
        byte[] body = Arrays.copyOf(frame, frame.length - CHECKSUM_BYTES);
        byte[] checksum = Arrays.copyOfRange(frame, body.length, frame.length);
        if (!MessageDigest.isEqual(
                IncrementalAuthorityValue.sha256(body), checksum)) {
            throw new IllegalArgumentException(
                    "incremental initialize request checksum mismatch");
        }
        try {
            DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(body));
            if (in.readInt() != MAGIC || in.readUnsignedByte() != VERSION) {
                throw new IllegalArgumentException(
                        "incremental initialize request header is invalid");
            }
            IncrementalInitializeRequest decoded =
                    new IncrementalInitializeRequest(
                            IncrementalAuthorityCodecSupport.readText(
                                    in, "initialize feed id"),
                            in.readLong(),
                            IncrementalAuthorityCodecSupport.readDigest(
                                    in, "initialize definition digest"),
                            IncrementalAuthorityCodecSupport.readDigest(
                                    in, "initialize source contract digest"),
                            IncrementalAuthorityCodecSupport.readDigest(
                                    in, "initialize frontier contract digest"),
                            IncrementalFrontier.Kind.fromWire(
                                    in.readUnsignedByte()));
            if (in.read() != -1) {
                throw new IllegalArgumentException(
                        "incremental initialize request has trailing bytes");
            }
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException(
                        "incremental initialize request is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental initialize request is malformed", malformed);
        }
    }
}
