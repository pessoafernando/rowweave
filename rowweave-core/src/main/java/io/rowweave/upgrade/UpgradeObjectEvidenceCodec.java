package io.rowweave.upgrade;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Strict bounded internal frame for one shadow-copy object receipt. */
public final class UpgradeObjectEvidenceCodec {
    public static final int MAX_FRAME_BYTES = 4096;
    private static final int CODEC_VERSION = 1;
    private static final byte[] MAGIC = "RWXXOE01".getBytes(StandardCharsets.US_ASCII);

    private UpgradeObjectEvidenceCodec() { }

    public static byte[] encode(UpgradeObjectEvidence evidence) {
        if (evidence == null) {
            throw new IllegalArgumentException("object evidence is required");
        }
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bytes);
            out.writeInt(evidence.version());
            out.writeLong(evidence.generation());
            UpgradeCodecSupport.writeString(out, evidence.relativePath());
            UpgradeCodecSupport.writeDigest(out, evidence.sourceDigest());
            UpgradeCodecSupport.writeDigest(out, evidence.targetDigest());
            UpgradeCodecSupport.writeString(out, evidence.commandId());
            UpgradeCodecSupport.writeDigest(out, evidence.commandDigest());
            out.flush();
            return UpgradeCodecSupport.frame(
                    MAGIC, CODEC_VERSION, bytes.toByteArray(), MAX_FRAME_BYTES);
        } catch (IOException impossible) {
            throw new IllegalStateException("object evidence encoding failed", impossible);
        }
    }

    public static UpgradeObjectEvidence decode(byte[] frame) {
        byte[] body = UpgradeCodecSupport.body(
                frame, MAGIC, CODEC_VERSION, MAX_FRAME_BYTES);
        try {
            DataInputStream input = new DataInputStream(
                    new ByteArrayInputStream(body));
            UpgradeObjectEvidence evidence = new UpgradeObjectEvidence(
                    input.readInt(), input.readLong(),
                    UpgradeCodecSupport.readString(input),
                    UpgradeCodecSupport.readDigest(input),
                    UpgradeCodecSupport.readDigest(input),
                    UpgradeCodecSupport.readString(input),
                    UpgradeCodecSupport.readDigest(input));
            UpgradeCodecSupport.requireConsumed(input);
            if (!java.util.Arrays.equals(frame, encode(evidence))) {
                throw new IllegalArgumentException(
                        "object evidence frame is not canonical");
            }
            return evidence;
        } catch (IOException malformed) {
            throw UpgradeCodecSupport.malformed("object evidence frame", malformed);
        }
    }
}
