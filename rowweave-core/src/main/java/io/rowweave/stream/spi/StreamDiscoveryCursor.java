package io.rowweave.stream.spi;

import java.util.Arrays;

/** Opaque, immutable connector cursor persisted by the Source coordinator. */
public final class StreamDiscoveryCursor {
    private static final int HARD_MAX_BYTES = 4 * 1024 * 1024;
    private static final StreamDiscoveryCursor INITIAL =
            new StreamDiscoveryCursor(new byte[0]);
    private final byte[] frame;

    private StreamDiscoveryCursor(byte[] frame) {
        this.frame = frame.clone();
    }

    public static StreamDiscoveryCursor initial() { return INITIAL; }

    public static StreamDiscoveryCursor of(byte[] frame) {
        return of(frame, HARD_MAX_BYTES);
    }

    public static StreamDiscoveryCursor of(byte[] frame, int maximumBytes) {
        if (frame == null || maximumBytes < 1
                || maximumBytes > HARD_MAX_BYTES
                || frame.length > maximumBytes) {
            throw new IllegalArgumentException("discovery cursor is invalid");
        }
        return frame.length == 0 ? INITIAL : new StreamDiscoveryCursor(frame);
    }

    public byte[] frame() { return frame.clone(); }

    @Override public boolean equals(Object other) {
        return other instanceof StreamDiscoveryCursor
                && Arrays.equals(frame, ((StreamDiscoveryCursor) other).frame);
    }

    @Override public int hashCode() { return Arrays.hashCode(frame); }
}
