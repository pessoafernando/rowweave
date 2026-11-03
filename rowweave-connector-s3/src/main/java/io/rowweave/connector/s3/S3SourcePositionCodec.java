package io.rowweave.connector.s3;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Canonical checksummed S3 split position codec. */
public final class S3SourcePositionCodec {
    public static final String CODEC_ID = "rowweave.s3.position.v1";
    public static final int MAX_FRAME_BYTES = 128;
    private static final String CODE = "S3_POSITION_INVALID";
    private static final byte[] MAGIC =
            "RWXS3PO1".getBytes(StandardCharsets.US_ASCII);

    private S3SourcePositionCodec() { }

    public static byte[] encode(S3SourcePosition position) {
        if (position == null) throw invalid();
        return S3CodecSupport.encode(MAGIC, MAX_FRAME_BYTES, output -> {
            output.write(S3CodecSupport.parseSha256(position.splitId(), CODE));
            output.writeLong(position.completedRecords());
        }, CODE);
    }

    public static S3SourcePosition decode(byte[] frame) {
        S3SourcePosition result = S3CodecSupport.decode(
                frame, MAGIC, MAX_FRAME_BYTES, input -> {
                    byte[] split = new byte[S3CodecSupport.DIGEST_BYTES];
                    input.readFully(split);
                    return new S3SourcePosition(S3CodecSupport.hex(split),
                            input.readLong());
                }, CODE);
        if (!MessageDigest.isEqual(frame, encode(result))) throw invalid();
        return result;
    }

    private static IllegalArgumentException invalid() {
        return S3CodecSupport.invalid(CODE);
    }
}
