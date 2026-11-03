package io.rowweave.stream.api;

import java.util.Objects;

/** Explicit finite resource ceilings carried with every product Stream job. */
public final class StreamResourceProfile {
    private final int mailboxEvents;
    private final int maximumFrameBytes;
    private final int channelCreditBytes;
    private final long managedStateBytes;
    private final int timerCount;
    private final int windowCount;
    private final long checkpointBytes;
    private final int threadCount;
    private final int fileDescriptorCount;
    private final long spillDiskBytes;

    public StreamResourceProfile(int mailboxEvents, int maximumFrameBytes,
            int channelCreditBytes, long managedStateBytes, int timerCount,
            int windowCount, long checkpointBytes, int threadCount,
            int fileDescriptorCount, long spillDiskBytes) {
        if (mailboxEvents < 1 || maximumFrameBytes < 1
                || channelCreditBytes < maximumFrameBytes
                || managedStateBytes < 0L || timerCount < 0 || windowCount < 0
                || checkpointBytes < 0L || threadCount < 1
                || fileDescriptorCount < 1 || spillDiskBytes < 0L) {
            throw new IllegalArgumentException("resource ceilings must be finite and valid");
        }
        this.mailboxEvents = mailboxEvents;
        this.maximumFrameBytes = maximumFrameBytes;
        this.channelCreditBytes = channelCreditBytes;
        this.managedStateBytes = managedStateBytes;
        this.timerCount = timerCount;
        this.windowCount = windowCount;
        this.checkpointBytes = checkpointBytes;
        this.threadCount = threadCount;
        this.fileDescriptorCount = fileDescriptorCount;
        this.spillDiskBytes = spillDiskBytes;
    }

    public static StreamResourceProfile defaults() {
        return new StreamResourceProfile(4_096, 4 * 1024 * 1024,
                16 * 1024 * 1024, 256L * 1024L * 1024L,
                1_000_000, 100_000, 512L * 1024L * 1024L,
                64, 1_024, 10L * 1024L * 1024L * 1024L);
    }

    public int mailboxEvents() { return mailboxEvents; }
    public int maximumFrameBytes() { return maximumFrameBytes; }
    public int channelCreditBytes() { return channelCreditBytes; }
    public long managedStateBytes() { return managedStateBytes; }
    public int timerCount() { return timerCount; }
    public int windowCount() { return windowCount; }
    public long checkpointBytes() { return checkpointBytes; }
    public int threadCount() { return threadCount; }
    public int fileDescriptorCount() { return fileDescriptorCount; }
    public long spillDiskBytes() { return spillDiskBytes; }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamResourceProfile)) return false;
        StreamResourceProfile that = (StreamResourceProfile) other;
        return mailboxEvents == that.mailboxEvents
                && maximumFrameBytes == that.maximumFrameBytes
                && channelCreditBytes == that.channelCreditBytes
                && managedStateBytes == that.managedStateBytes
                && timerCount == that.timerCount && windowCount == that.windowCount
                && checkpointBytes == that.checkpointBytes
                && threadCount == that.threadCount
                && fileDescriptorCount == that.fileDescriptorCount
                && spillDiskBytes == that.spillDiskBytes;
    }

    @Override public int hashCode() {
        return Objects.hash(mailboxEvents, maximumFrameBytes, channelCreditBytes,
                managedStateBytes, timerCount, windowCount, checkpointBytes,
                threadCount, fileDescriptorCount, spillDiskBytes);
    }
}
