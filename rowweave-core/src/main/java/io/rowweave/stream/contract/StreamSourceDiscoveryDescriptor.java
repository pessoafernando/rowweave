package io.rowweave.stream.contract;

import java.util.Objects;

/** Portable resource/latency contract for a dynamic Source enumerator. */
public final class StreamSourceDiscoveryDescriptor {
    private final long discoveryIntervalMillis;
    private final long discoveryTimeoutMillis;
    private final int maximumKnownSplits;
    private final int maximumPendingSplits;
    private final int maximumChangesPerDiscovery;
    private final int maximumCursorBytes;
    private final int maximumDescriptorBytes;

    public StreamSourceDiscoveryDescriptor(long discoveryIntervalMillis,
            long discoveryTimeoutMillis, int maximumKnownSplits,
            int maximumPendingSplits, int maximumChangesPerDiscovery,
            int maximumCursorBytes, int maximumDescriptorBytes) {
        io.rowweave.stream.api.StreamSourceDiscoverySpec checked =
                new io.rowweave.stream.api.StreamSourceDiscoverySpec(
                        discoveryIntervalMillis, discoveryTimeoutMillis,
                        maximumKnownSplits, maximumPendingSplits,
                        maximumChangesPerDiscovery, maximumCursorBytes,
                        maximumDescriptorBytes);
        this.discoveryIntervalMillis = checked.discoveryIntervalMillis();
        this.discoveryTimeoutMillis = checked.discoveryTimeoutMillis();
        this.maximumKnownSplits = checked.maximumKnownSplits();
        this.maximumPendingSplits = checked.maximumPendingSplits();
        this.maximumChangesPerDiscovery =
                checked.maximumChangesPerDiscovery();
        this.maximumCursorBytes = checked.maximumCursorBytes();
        this.maximumDescriptorBytes = checked.maximumDescriptorBytes();
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
        if (!(other instanceof StreamSourceDiscoveryDescriptor)) return false;
        StreamSourceDiscoveryDescriptor that =
                (StreamSourceDiscoveryDescriptor) other;
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
