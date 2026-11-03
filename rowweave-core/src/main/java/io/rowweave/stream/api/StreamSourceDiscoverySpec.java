package io.rowweave.stream.api;

import java.util.Objects;

/** Public inert resource and latency limits for dynamic split discovery. */
public final class StreamSourceDiscoverySpec {
    public static final int HARD_MAX_SPLITS = 1_000_000;
    public static final int HARD_MAX_CHANGES = 4_096;
    public static final int HARD_MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private final long discoveryIntervalMillis;
    private final long discoveryTimeoutMillis;
    private final int maximumKnownSplits;
    private final int maximumPendingSplits;
    private final int maximumChangesPerDiscovery;
    private final int maximumCursorBytes;
    private final int maximumDescriptorBytes;

    public StreamSourceDiscoverySpec(long discoveryIntervalMillis,
            long discoveryTimeoutMillis, int maximumKnownSplits,
            int maximumPendingSplits, int maximumChangesPerDiscovery,
            int maximumCursorBytes, int maximumDescriptorBytes) {
        if (discoveryIntervalMillis < 1L
                || discoveryTimeoutMillis < 1L
                || discoveryTimeoutMillis < discoveryIntervalMillis
                || maximumKnownSplits < 1
                || maximumKnownSplits > HARD_MAX_SPLITS
                || maximumPendingSplits < 1
                || maximumPendingSplits > maximumKnownSplits
                || maximumChangesPerDiscovery < 1
                || maximumChangesPerDiscovery > maximumPendingSplits
                || maximumChangesPerDiscovery > HARD_MAX_CHANGES
                || maximumCursorBytes < 1
                || maximumCursorBytes > HARD_MAX_FRAME_BYTES
                || maximumDescriptorBytes < 1
                || maximumDescriptorBytes > HARD_MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "dynamic source discovery limits are invalid");
        }
        this.discoveryIntervalMillis = discoveryIntervalMillis;
        this.discoveryTimeoutMillis = discoveryTimeoutMillis;
        this.maximumKnownSplits = maximumKnownSplits;
        this.maximumPendingSplits = maximumPendingSplits;
        this.maximumChangesPerDiscovery = maximumChangesPerDiscovery;
        this.maximumCursorBytes = maximumCursorBytes;
        this.maximumDescriptorBytes = maximumDescriptorBytes;
    }

    public long discoveryIntervalMillis() { return discoveryIntervalMillis; }
    public long discoveryTimeoutMillis() { return discoveryTimeoutMillis; }
    public int maximumKnownSplits() { return maximumKnownSplits; }
    public int maximumPendingSplits() { return maximumPendingSplits; }
    public int maximumChangesPerDiscovery() {
        return maximumChangesPerDiscovery;
    }
    public int maximumCursorBytes() { return maximumCursorBytes; }
    public int maximumDescriptorBytes() { return maximumDescriptorBytes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamSourceDiscoverySpec)) return false;
        StreamSourceDiscoverySpec that = (StreamSourceDiscoverySpec) other;
        return discoveryIntervalMillis == that.discoveryIntervalMillis
                && discoveryTimeoutMillis == that.discoveryTimeoutMillis
                && maximumKnownSplits == that.maximumKnownSplits
                && maximumPendingSplits == that.maximumPendingSplits
                && maximumChangesPerDiscovery
                == that.maximumChangesPerDiscovery
                && maximumCursorBytes == that.maximumCursorBytes
                && maximumDescriptorBytes == that.maximumDescriptorBytes;
    }

    @Override public int hashCode() {
        return Objects.hash(discoveryIntervalMillis, discoveryTimeoutMillis,
                maximumKnownSplits, maximumPendingSplits,
                maximumChangesPerDiscovery, maximumCursorBytes,
                maximumDescriptorBytes);
    }
}
