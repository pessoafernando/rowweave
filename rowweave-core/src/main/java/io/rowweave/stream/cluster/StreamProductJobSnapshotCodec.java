package io.rowweave.stream.cluster;

/** Stable checksummed frame shared by File and external authority stores. */
public final class StreamProductJobSnapshotCodec {
    private StreamProductJobSnapshotCodec() { }

    public static byte[] encode(StreamProductJobSnapshot snapshot) {
        if (snapshot == null) {
            throw new IllegalArgumentException(
                    "Stream product job snapshot is required");
        }
        return FileStreamProductJobAuthority.encode(snapshot);
    }

    public static StreamProductJobSnapshot decode(byte[] frame) {
        if (frame == null) {
            throw new IllegalArgumentException(
                    "Stream product job frame is required");
        }
        return FileStreamProductJobAuthority.decode(frame.clone());
    }
}
