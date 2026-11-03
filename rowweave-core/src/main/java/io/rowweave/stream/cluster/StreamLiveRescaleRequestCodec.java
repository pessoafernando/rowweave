package io.rowweave.stream.cluster;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Bounded canonical payload for one live-rescale control command. */
final class StreamLiveRescaleRequestCodec {
    private static final byte[] MAGIC =
            "RWXXRSV1".getBytes(StandardCharsets.US_ASCII);
    static final int FRAME_BYTES = MAGIC.length + Integer.BYTES;

    private StreamLiveRescaleRequestCodec() { }

    static byte[] encode(int targetParallelism) {
        if (targetParallelism < 1) {
            throw new IllegalArgumentException(
                    "live rescale target parallelism must be positive");
        }
        return ByteBuffer.allocate(FRAME_BYTES)
                .put(MAGIC).putInt(targetParallelism).array();
    }

    static int decode(byte[] frame) {
        if (frame == null || frame.length != FRAME_BYTES) {
            throw invalid();
        }
        ByteBuffer input = ByteBuffer.wrap(frame);
        byte[] magic = new byte[MAGIC.length];
        input.get(magic);
        int targetParallelism = input.getInt();
        if (!MessageDigest.isEqual(MAGIC, magic)
                || targetParallelism < 1 || input.hasRemaining()) {
            throw invalid();
        }
        return targetParallelism;
    }

    private static StreamProductControlException invalid() {
        return new StreamProductControlException(
                StreamProductControlException.Code.INVALID_FRAME,
                "Stream live rescale request is invalid");
    }
}
