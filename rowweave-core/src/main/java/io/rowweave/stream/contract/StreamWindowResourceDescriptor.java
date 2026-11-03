package io.rowweave.stream.contract;

import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamWindowResourceSpec;

import java.util.Objects;

/** Portable bounded state, cursor, merge and emission profile for a window. */
public final class StreamWindowResourceDescriptor {
    private final int maximumActiveWindowsPerKey;
    private final long maximumRowsPerWindow;
    private final long maximumWindowBytes;
    private final long maximumTotalBytes;
    private final int maximumSessionMergeFanIn;
    private final int maximumTriggerOutputs;
    private final long maximumTriggerOutputBytes;

    public StreamWindowResourceDescriptor(int maximumActiveWindowsPerKey,
            long maximumRowsPerWindow, long maximumWindowBytes,
            long maximumTotalBytes, int maximumSessionMergeFanIn,
            int maximumTriggerOutputs, long maximumTriggerOutputBytes) {
        if (maximumActiveWindowsPerKey <= 0
                || maximumActiveWindowsPerKey
                > StreamWindowResourceSpec.MAX_ACTIVE_WINDOWS_PER_KEY
                || maximumRowsPerWindow <= 0L
                || maximumRowsPerWindow
                > StreamWindowResourceSpec.MAX_ROWS_PER_WINDOW
                || maximumWindowBytes <= 0L
                || maximumWindowBytes > StreamWindowResourceSpec.MAX_BYTES
                || maximumTotalBytes < maximumWindowBytes
                || maximumTotalBytes > StreamWindowResourceSpec.MAX_BYTES
                || maximumSessionMergeFanIn <= 0
                || maximumSessionMergeFanIn
                > StreamWindowResourceSpec.MAX_SESSION_MERGE_FAN_IN
                || maximumTriggerOutputs <= 0
                || maximumTriggerOutputs
                > StreamEmissionLimit.MAX_OUTPUTS_PER_INPUT
                || maximumTriggerOutputBytes <= 0L
                || maximumTriggerOutputBytes
                > StreamEmissionLimit.MAX_OUTPUT_BYTES_PER_INPUT) {
            throw StreamContractSupport.failure(
                    "INVALID_WINDOW_RESOURCE_LIMIT", "nodes.window.resources",
                    "window resource declaration is invalid");
        }
        this.maximumActiveWindowsPerKey = maximumActiveWindowsPerKey;
        this.maximumRowsPerWindow = maximumRowsPerWindow;
        this.maximumWindowBytes = maximumWindowBytes;
        this.maximumTotalBytes = maximumTotalBytes;
        this.maximumSessionMergeFanIn = maximumSessionMergeFanIn;
        this.maximumTriggerOutputs = maximumTriggerOutputs;
        this.maximumTriggerOutputBytes = maximumTriggerOutputBytes;
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
    public int maximumTriggerOutputs() { return maximumTriggerOutputs; }
    public long maximumTriggerOutputBytes() {
        return maximumTriggerOutputBytes;
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamWindowResourceDescriptor)) return false;
        StreamWindowResourceDescriptor that =
                (StreamWindowResourceDescriptor) other;
        return maximumActiveWindowsPerKey == that.maximumActiveWindowsPerKey
                && maximumRowsPerWindow == that.maximumRowsPerWindow
                && maximumWindowBytes == that.maximumWindowBytes
                && maximumTotalBytes == that.maximumTotalBytes
                && maximumSessionMergeFanIn == that.maximumSessionMergeFanIn
                && maximumTriggerOutputs == that.maximumTriggerOutputs
                && maximumTriggerOutputBytes
                == that.maximumTriggerOutputBytes;
    }

    @Override public int hashCode() {
        return Objects.hash(maximumActiveWindowsPerKey, maximumRowsPerWindow,
                maximumWindowBytes, maximumTotalBytes,
                maximumSessionMergeFanIn, maximumTriggerOutputs,
                maximumTriggerOutputBytes);
    }
}
