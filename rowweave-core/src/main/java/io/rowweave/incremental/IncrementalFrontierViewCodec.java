package io.rowweave.incremental;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical checksum-protected wire for the redacted public frontier view. */
public final class IncrementalFrontierViewCodec {
    public static final int MAX_FRAME_BYTES = 4096;
    private static final int MAGIC = 0x52574656; // RWFV
    private static final int VERSION = 1;
    private static final int CHECKSUM_BYTES = 32;

    private IncrementalFrontierViewCodec() {}

    public static byte[] encode(IncrementalFrontierView view) {
        if (view == null) throw new IllegalArgumentException("frontier view is required");
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(MAGIC);
            out.writeByte(VERSION);
            IncrementalAuthorityCodecSupport.writeText(out, view.feedId());
            out.writeLong(view.feedIncarnation());
            out.writeLong(view.revision());
            out.writeByte(view.frontierKind().wireId());
            IncrementalAuthorityCodecSupport.writeText(
                    out, view.definitionDigestPrefix());
            IncrementalAuthorityCodecSupport.writeText(
                    out, view.committedFrontierDigestPrefix());
            out.writeBoolean(view.activeRunId() != null);
            if (view.activeRunId() != null) {
                IncrementalAuthorityCodecSupport.writeText(out, view.activeRunId());
            }
            IncrementalAuthorityCodecSupport.writeText(out, view.state());
            out.flush();
            byte[] body = bytes.toByteArray();
            if (body.length + CHECKSUM_BYTES > MAX_FRAME_BYTES) {
                throw new IllegalArgumentException("frontier view exceeds frame limit");
            }
            byte[] frame = Arrays.copyOf(body, body.length + CHECKSUM_BYTES);
            System.arraycopy(IncrementalAuthorityValue.sha256(body), 0,
                    frame, body.length, CHECKSUM_BYTES);
            return frame;
        } catch (IOException impossible) {
            throw new IllegalStateException("encode frontier view failed", impossible);
        }
    }

    public static IncrementalFrontierView decode(byte[] frame) {
        if (frame == null || frame.length <= CHECKSUM_BYTES
                || frame.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("frontier view frame length is invalid");
        }
        byte[] body = Arrays.copyOf(frame, frame.length - CHECKSUM_BYTES);
        byte[] checksum = Arrays.copyOfRange(frame, body.length, frame.length);
        if (!MessageDigest.isEqual(
                IncrementalAuthorityValue.sha256(body), checksum)) {
            throw new IllegalArgumentException("frontier view checksum mismatch");
        }
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(body));
            if (in.readInt() != MAGIC || in.readUnsignedByte() != VERSION) {
                throw new IllegalArgumentException("frontier view header is invalid");
            }
            String feedId = IncrementalAuthorityCodecSupport.readText(in, "view feed id");
            long incarnation = in.readLong();
            long revision = in.readLong();
            IncrementalFrontier.Kind kind =
                    IncrementalFrontier.Kind.fromWire(in.readUnsignedByte());
            String definition = IncrementalAuthorityCodecSupport.readText(
                    in, "view definition digest prefix");
            String committed = IncrementalAuthorityCodecSupport.readText(
                    in, "view committed digest prefix");
            String active = in.readBoolean()
                    ? IncrementalAuthorityCodecSupport.readText(in, "view active run")
                    : null;
            String state = IncrementalAuthorityCodecSupport.readText(in, "view state");
            if (in.read() != -1) {
                throw new IllegalArgumentException("frontier view trailing bytes");
            }
            IncrementalFrontierView decoded = new IncrementalFrontierView(
                    feedId, incarnation, revision, kind,
                    definition, committed, active, state);
            if (!Arrays.equals(frame, encode(decoded))) {
                throw new IllegalArgumentException("frontier view is not canonical");
            }
            return decoded;
        } catch (IOException malformed) {
            throw new IllegalArgumentException("frontier view is malformed", malformed);
        }
    }
}
