package io.rowweave.stream.api;

import java.util.Objects;

/** Explicit state, merge and output ceilings for a generic window. */
public final class StreamWindowResourceSpec {
    public static final int MAX_ACTIVE_WINDOWS_PER_KEY = 1_000_000;
    public static final long MAX_ROWS_PER_WINDOW = 1_000_000_000L;
    public static final long MAX_BYTES = 1024L * 1024L * 1024L * 1024L;
    public static final int MAX_SESSION_MERGE_FAN_IN = 1_000_000;

    private final int maximumActiveWindowsPerKey;
    private final long maximumRowsPerWindow;
    private final long maximumWindowBytes;
    private final long maximumTotalBytes;
    private final int maximumSessionMergeFanIn;
    private final StreamEmissionLimit triggerEmissionLimit;

    private StreamWindowResourceSpec(int maximumActiveWindowsPerKey,
            long maximumRowsPerWindow, long maximumWindowBytes,
            long maximumTotalBytes, int maximumSessionMergeFanIn,
            StreamEmissionLimit triggerEmissionLimit) {
        if (maximumActiveWindowsPerKey <= 0
                || maximumActiveWindowsPerKey > MAX_ACTIVE_WINDOWS_PER_KEY
                || maximumRowsPerWindow <= 0L
                || maximumRowsPerWindow > MAX_ROWS_PER_WINDOW
                || maximumWindowBytes <= 0L
                || maximumWindowBytes > MAX_BYTES
                || maximumTotalBytes < maximumWindowBytes
                || maximumTotalBytes > MAX_BYTES
                || maximumSessionMergeFanIn <= 0
                || maximumSessionMergeFanIn > MAX_SESSION_MERGE_FAN_IN
                || triggerEmissionLimit == null) {
            throw new IllegalArgumentException("invalid window resource bounds");
        }
        this.maximumActiveWindowsPerKey = maximumActiveWindowsPerKey;
        this.maximumRowsPerWindow = maximumRowsPerWindow;
        this.maximumWindowBytes = maximumWindowBytes;
        this.maximumTotalBytes = maximumTotalBytes;
        this.maximumSessionMergeFanIn = maximumSessionMergeFanIn;
        this.triggerEmissionLimit = triggerEmissionLimit;
    }

    public static StreamWindowResourceSpec of(int maximumActiveWindowsPerKey,
            long maximumRowsPerWindow, long maximumWindowBytes,
            long maximumTotalBytes, int maximumSessionMergeFanIn,
            StreamEmissionLimit triggerEmissionLimit) {
        return new StreamWindowResourceSpec(maximumActiveWindowsPerKey,
                maximumRowsPerWindow, maximumWindowBytes, maximumTotalBytes,
                maximumSessionMergeFanIn, triggerEmissionLimit);
    }

    public int maximumActiveWindowsPerKey() {
        return maximumActiveWindowsPerKey;
    }
    public long maximumRowsPerWindow() { return maximumRowsPerWindow; }
    public long maximumWindowBytes() { return maximumWindowBytes; }
    public long maximumTotalBytes() { return maximumTotalBytes; }
    public int maximumSessionMergeFanIn() {
        return maximumSessionMergeFanIn;
    }
    public StreamEmissionLimit triggerEmissionLimit() {
        return triggerEmissionLimit;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamWindowResourceSpec)) return false;
        StreamWindowResourceSpec that = (StreamWindowResourceSpec) other;
        return maximumActiveWindowsPerKey == that.maximumActiveWindowsPerKey
                && maximumRowsPerWindow == that.maximumRowsPerWindow
                && maximumWindowBytes == that.maximumWindowBytes
                && maximumTotalBytes == that.maximumTotalBytes
                && maximumSessionMergeFanIn == that.maximumSessionMergeFanIn
                && triggerEmissionLimit.equals(that.triggerEmissionLimit);
    }

    @Override public int hashCode() {
        return Objects.hash(maximumActiveWindowsPerKey, maximumRowsPerWindow,
                maximumWindowBytes, maximumTotalBytes,
                maximumSessionMergeFanIn, triggerEmissionLimit);
    }
}
