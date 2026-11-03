package io.rowweave.upgrade;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/** Canonical, checksummed and bounded StoreFormatHeader frame. */
public final class StoreFormatHeaderCodec {
    public static final int MAX_FRAME_BYTES = 4096;

    private static final byte[] MAGIC =
            "RWXXSTH1".getBytes(StandardCharsets.US_ASCII);
    private static final int CODEC_VERSION = 1;

    private StoreFormatHeaderCodec() {
    }

    public static byte[] encode(StoreFormatHeader header) {
        if (header == null) throw new IllegalArgumentException("store header is required");
        try {
            return UpgradeCodecSupport.frame(MAGIC, CODEC_VERSION,
                    UpgradeCodecSupport.encodeHeaderBody(header), MAX_FRAME_BYTES);
        } catch (IOException impossible) {
            throw new IllegalStateException("in-memory store header encoding failed", impossible);
        }
    }

    public static StoreFormatHeader decode(byte[] frame) {
        byte[] body = UpgradeCodecSupport.body(
                frame, MAGIC, CODEC_VERSION, MAX_FRAME_BYTES);
        try {
            DataInputStream input = new DataInputStream(new ByteArrayInputStream(body));
            StoreFormatHeader header = UpgradeCodecSupport.decodeHeaderBody(input);
            UpgradeCodecSupport.requireConsumed(input);
            if (!Arrays.equals(frame, encode(header))) {
                throw new IllegalArgumentException("store header frame is not canonical");
            }
            return header;
        } catch (IOException malformed) {
            throw UpgradeCodecSupport.malformed("store header", malformed);
        }
    }
}
