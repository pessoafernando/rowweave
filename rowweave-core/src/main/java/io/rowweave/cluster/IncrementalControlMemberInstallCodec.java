package io.rowweave.cluster;

import io.rowweave.incremental.IncrementalControlMemberCodec;

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
import java.util.Arrays;

/** Bounded canonical Worker command for installing the engine-owned CDC member. */
public final class IncrementalControlMemberInstallCodec {
    public static final int MAX_FRAME_BYTES = IncrementalControlMemberCodec.MAX_FRAME_BYTES
            + io.rowweave.commit.CommitManifest.MAX_ENCODED_BYTES + 4096;
    private static final int MAGIC = 0x5257494d; // RWIM
    private static final int VERSION = 1;

    private IncrementalControlMemberInstallCodec() { }

    static byte[] encode(
            String domainPath, byte[] controlFrame, byte[] manifestFrame) {
        if (domainPath == null || domainPath.isEmpty()
                || controlFrame == null || controlFrame.length == 0
                || controlFrame.length
                > IncrementalControlMemberCodec.MAX_FRAME_BYTES
                || manifestFrame == null || manifestFrame.length == 0
                || manifestFrame.length
                > io.rowweave.commit.CommitManifest.MAX_ENCODED_BYTES) {
            throw new IllegalArgumentException(
                    "incremental control install fields are invalid");
        }
        byte[] path = domainPath.getBytes(StandardCharsets.UTF_8);
        if (path.length == 0 || path.length > 2048) {
            throw new IllegalArgumentException(
                    "incremental control domain path exceeds bound");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(
                    16 + path.length + controlFrame.length + 32);
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            out.writeInt(path.length);
            out.write(path);
            out.writeInt(controlFrame.length);
            out.write(controlFrame);
            out.writeInt(manifestFrame.length);
            out.write(manifestFrame);
            out.flush();
            byte[] body = bytes.toByteArray();
            byte[] frame = Arrays.copyOf(body, body.length + 32);
            System.arraycopy(sha256(body), 0, frame, body.length, 32);
            if (frame.length > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException(
                        "incremental control install frame exceeds bound");
            }
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException(
                    "in-memory incremental control encoding failed", impossible);
        }
    }

    static Decoded decode(byte[] frame) {
        if (frame == null || frame.length < 48
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "incremental control install frame length is invalid");
        }
        int bodyLength = frame.length - 32;
        byte[] body = Arrays.copyOf(frame, bodyLength);
        if (!MessageDigest.isEqual(
                sha256(body), Arrays.copyOfRange(frame, bodyLength, frame.length))) {
            throw new IllegalArgumentException(
                    "incremental control install checksum differs");
        }
        try {
            DataInputStream in = new DataInputStream(
                    new ByteArrayInputStream(body));
            if (in.readInt() != MAGIC || in.readInt() != VERSION) {
                throw new IllegalArgumentException(
                        "incremental control install header is invalid");
            }
            int pathLength = in.readInt();
            if (pathLength < 1 || pathLength > 2048
                    || pathLength > in.available()) {
                throw new IllegalArgumentException(
                        "incremental control domain path is invalid");
            }
            byte[] path = new byte[pathLength];
            in.readFully(path);
            int controlLength = in.readInt();
            if (controlLength < 1
                    || controlLength > IncrementalControlMemberCodec.MAX_FRAME_BYTES
                    || controlLength + 4 > in.available()) {
                throw new IllegalArgumentException(
                        "incremental control member is invalid");
            }
            byte[] control = new byte[controlLength];
            in.readFully(control);
            int manifestLength = in.readInt();
            if (manifestLength < 1
                    || manifestLength
                    > io.rowweave.commit.CommitManifest.MAX_ENCODED_BYTES
                    || manifestLength != in.available()) {
                throw new IllegalArgumentException(
                        "incremental publication manifest is invalid");
            }
            byte[] manifest = new byte[manifestLength];
            in.readFully(manifest);
            final String domainPath;
            try {
                domainPath = StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(path)).toString();
            } catch (CharacterCodingException malformed) {
                throw new IllegalArgumentException(
                        "incremental control domain path is not UTF-8",
                        malformed);
            }
            byte[] canonical = encode(domainPath, control, manifest);
            if (!MessageDigest.isEqual(frame, canonical)) {
                throw new IllegalArgumentException(
                        "incremental control install frame is noncanonical");
            }
            return new Decoded(domainPath, control, manifest);
        } catch (EOFException truncated) {
            throw new IllegalArgumentException(
                    "incremental control install frame is truncated", truncated);
        } catch (IOException malformed) {
            throw new IllegalArgumentException(
                    "incremental control install frame is malformed", malformed);
        }
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    static final class Decoded {
        final String domainPath;
        final byte[] controlFrame;
        final byte[] manifestFrame;

        private Decoded(
                String domainPath, byte[] controlFrame, byte[] manifestFrame) {
            this.domainPath = domainPath;
            this.controlFrame = controlFrame;
            this.manifestFrame = manifestFrame;
        }
    }
}
