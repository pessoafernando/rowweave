package io.rowweave.stream.cluster;

/** Stable bounded frame codec for shared Stream run authority backends. */
public final class StreamClusterRunStateCodec {
    private StreamClusterRunStateCodec() {}

    public static byte[] encode(StreamClusterRunState state) {
        if (state == null) {
            throw new IllegalArgumentException("Stream run state is required");
        }
        return FileStreamClusterRunAuthority.encode(state);
    }

    public static StreamClusterRunState decode(byte[] frame) {
        if (frame == null) {
            throw new IllegalArgumentException("Stream run frame is required");
        }
        return FileStreamClusterRunAuthority.decode(frame.clone());
    }
}
