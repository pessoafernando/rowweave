package io.rowweave.stream.cluster;

import java.util.Collection;
import java.util.List;

/** Node-local resource and capability request for one operator subtask. */
public final class StreamTaskPlacementRequest {
    private final StreamTaskIdentity identity;
    private final int controlProtocolVersion;
    private final int dataProtocolVersion;
    private final int maximumFrameBytes;
    private final long residentBytes;
    private final int threads;
    private final int channels;
    private final int fileDescriptorCount;
    private final int timerCount;
    private final int windowCount;
    private final long checkpointBytes;
    private final long spillDiskBytes;
    private final List<String> requiredCapabilities;

    public StreamTaskPlacementRequest(StreamTaskIdentity identity,
            int controlProtocolVersion, int dataProtocolVersion,
            int maximumFrameBytes, long residentBytes, int threads,
            int channels, int timerCount, int windowCount,
            long checkpointBytes, long spillDiskBytes,
            Collection<String> requiredCapabilities) {
        this(identity, controlProtocolVersion, dataProtocolVersion,
                maximumFrameBytes, residentBytes, threads, channels,
                channels, timerCount, windowCount, checkpointBytes,
                spillDiskBytes, requiredCapabilities);
    }

    public StreamTaskPlacementRequest(StreamTaskIdentity identity,
            int controlProtocolVersion, int dataProtocolVersion,
            int maximumFrameBytes, long residentBytes, int threads,
            int channels, int fileDescriptorCount, int timerCount,
            int windowCount, long checkpointBytes, long spillDiskBytes,
            Collection<String> requiredCapabilities) {
        if (identity == null || controlProtocolVersion <= 0
                || dataProtocolVersion <= 0 || maximumFrameBytes <= 0
                || residentBytes <= 0L || threads <= 0
                || threads > 65_536 || channels <= 0 || channels > 4_096
                || fileDescriptorCount <= 0
                || timerCount < 0 || windowCount < 0
                || checkpointBytes < 0L || spillDiskBytes < 0L) {
            throw new IllegalArgumentException(
                    "Stream task placement request is invalid");
        }
        this.identity = identity;
        this.controlProtocolVersion = controlProtocolVersion;
        this.dataProtocolVersion = dataProtocolVersion;
        this.maximumFrameBytes = maximumFrameBytes;
        this.residentBytes = residentBytes;
        this.threads = threads;
        this.channels = channels;
        this.fileDescriptorCount = fileDescriptorCount;
        this.timerCount = timerCount;
        this.windowCount = windowCount;
        this.checkpointBytes = checkpointBytes;
        this.spillDiskBytes = spillDiskBytes;
        this.requiredCapabilities =
                StreamWorkerCapabilityProfile.capabilities(
                        requiredCapabilities);
    }

    public StreamTaskIdentity identity() { return identity; }
    public int controlProtocolVersion() { return controlProtocolVersion; }
    public int dataProtocolVersion() { return dataProtocolVersion; }
    public int maximumFrameBytes() { return maximumFrameBytes; }
    public long residentBytes() { return residentBytes; }
    public int threads() { return threads; }
    public int channels() { return channels; }
    public int fileDescriptorCount() { return fileDescriptorCount; }
    public int timerCount() { return timerCount; }
    public int windowCount() { return windowCount; }
    public long checkpointBytes() { return checkpointBytes; }
    public long spillDiskBytes() { return spillDiskBytes; }
    public List<String> requiredCapabilities() { return requiredCapabilities; }
}
