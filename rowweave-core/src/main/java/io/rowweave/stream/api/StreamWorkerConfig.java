package io.rowweave.stream.api;

import io.rowweave.cluster.rpc.ControlSecurityConfig;
import java.nio.file.Path;
import java.util.Properties;

/** Immutable resource and durable-state configuration for one Stream Worker. */
public final class StreamWorkerConfig {
    private static final int DEFAULT_TASK_SLOTS = 64;
    private static final int DEFAULT_CHANNEL_CAPACITY = 64;
    private static final int DEFAULT_MAXIMUM_FRAME_BYTES = 4 * 1024 * 1024;
    private static final long DEFAULT_MAXIMUM_RESIDENT_BYTES =
            1024L * 1024L * 1024L;
    private static final int DEFAULT_MAXIMUM_THREADS = 128;
    private static final int DEFAULT_MAXIMUM_TIMER_COUNT = 1_000_000;
    private static final int DEFAULT_MAXIMUM_WINDOW_COUNT = 1_000_000;
    private static final long DEFAULT_MAXIMUM_CHECKPOINT_BYTES =
            1024L * 1024L * 1024L;
    private static final long DEFAULT_MAXIMUM_SPILL_DISK_BYTES =
            8L * 1024L * 1024L * 1024L;
    private static final int DEFAULT_MAXIMUM_FILE_DESCRIPTOR_COUNT = 4_096;

    private final String workerId;
    private final Path clusterStateRoot;
    private final int taskSlots;
    private final int channelCapacity;
    private final int maximumFrameBytes;
    private final long maximumResidentBytes;
    private final int maximumThreads;
    private final int maximumTimerCount;
    private final int maximumWindowCount;
    private final long maximumCheckpointBytes;
    private final long maximumSpillDiskBytes;
    private final int maximumFileDescriptorCount;
    private final ControlSecurityConfig controlSecurity;
    private final int controlPort;

    public StreamWorkerConfig(String workerId, Path clusterStateRoot,
            int taskSlots, int channelCapacity, int maximumFrameBytes,
            long maximumResidentBytes, int maximumThreads) {
        this(workerId, clusterStateRoot, taskSlots, channelCapacity,
                maximumFrameBytes, maximumResidentBytes, maximumThreads,
                DEFAULT_MAXIMUM_TIMER_COUNT, DEFAULT_MAXIMUM_WINDOW_COUNT,
                DEFAULT_MAXIMUM_CHECKPOINT_BYTES,
                DEFAULT_MAXIMUM_SPILL_DISK_BYTES,
                DEFAULT_MAXIMUM_FILE_DESCRIPTOR_COUNT,
                loopbackSecurity(), 0);
    }

    public StreamWorkerConfig(String workerId, Path clusterStateRoot,
            int taskSlots, int channelCapacity, int maximumFrameBytes,
            long maximumResidentBytes, int maximumThreads,
            int maximumTimerCount, int maximumWindowCount,
            long maximumCheckpointBytes, long maximumSpillDiskBytes,
            int maximumFileDescriptorCount) {
        this(workerId, clusterStateRoot, taskSlots, channelCapacity,
                maximumFrameBytes, maximumResidentBytes, maximumThreads,
                maximumTimerCount, maximumWindowCount,
                maximumCheckpointBytes, maximumSpillDiskBytes,
                maximumFileDescriptorCount, loopbackSecurity(), 0);
    }

    private StreamWorkerConfig(String workerId, Path clusterStateRoot,
            int taskSlots, int channelCapacity, int maximumFrameBytes,
            long maximumResidentBytes, int maximumThreads,
            int maximumTimerCount, int maximumWindowCount,
            long maximumCheckpointBytes, long maximumSpillDiskBytes,
            int maximumFileDescriptorCount,
            ControlSecurityConfig controlSecurity, int controlPort) {
        if (workerId == null || workerId.isEmpty() || workerId.length() > 256
                || clusterStateRoot == null
                || taskSlots < 1 || taskSlots > 4_096
                || channelCapacity < 1 || channelCapacity > 4_096
                || maximumFrameBytes < 1
                || maximumFrameBytes > 4 * 1024 * 1024
                || maximumResidentBytes < 1L
                || maximumThreads < 4 || maximumThreads > 65_536
                || maximumTimerCount < 0 || maximumWindowCount < 0
                || maximumCheckpointBytes < 0L
                || maximumSpillDiskBytes < 0L
                || maximumFileDescriptorCount < 1
                || controlSecurity == null
                || controlPort < 0 || controlPort > 65_535) {
            throw new IllegalArgumentException(
                    "Stream Worker configuration is invalid");
        }
        this.workerId = workerId;
        this.clusterStateRoot = clusterStateRoot.toAbsolutePath().normalize();
        this.taskSlots = taskSlots;
        this.channelCapacity = channelCapacity;
        this.maximumFrameBytes = maximumFrameBytes;
        this.maximumResidentBytes = maximumResidentBytes;
        this.maximumThreads = maximumThreads;
        this.maximumTimerCount = maximumTimerCount;
        this.maximumWindowCount = maximumWindowCount;
        this.maximumCheckpointBytes = maximumCheckpointBytes;
        this.maximumSpillDiskBytes = maximumSpillDiskBytes;
        this.maximumFileDescriptorCount = maximumFileDescriptorCount;
        this.controlSecurity = controlSecurity;
        this.controlPort = controlPort;
    }

    /** Returns a copy bound to the explicit LOOPBACK or mTLS endpoint. */
    public StreamWorkerConfig withControlEndpoint(
            ControlSecurityConfig security, int port) {
        return new StreamWorkerConfig(workerId, clusterStateRoot, taskSlots,
                channelCapacity, maximumFrameBytes, maximumResidentBytes,
                maximumThreads, maximumTimerCount, maximumWindowCount,
                maximumCheckpointBytes, maximumSpillDiskBytes,
                maximumFileDescriptorCount, security, port);
    }

    public static StreamWorkerConfig defaults(
            String workerId, Path clusterStateRoot) {
        return new StreamWorkerConfig(workerId, clusterStateRoot,
                DEFAULT_TASK_SLOTS,
                DEFAULT_CHANNEL_CAPACITY, DEFAULT_MAXIMUM_FRAME_BYTES,
                DEFAULT_MAXIMUM_RESIDENT_BYTES, DEFAULT_MAXIMUM_THREADS);
    }

    static StreamWorkerConfig fromProperties(String workerId,
            Path clusterStateRoot, Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException(
                    "Stream Worker properties are required");
        }
        return new StreamWorkerConfig(workerId, clusterStateRoot,
                integer(properties, "rowweave.stream.worker.taskSlots",
                        DEFAULT_TASK_SLOTS),
                integer(properties, "rowweave.stream.worker.channelCapacity",
                        DEFAULT_CHANNEL_CAPACITY),
                integer(properties, "rowweave.stream.worker.maximumFrameBytes",
                        DEFAULT_MAXIMUM_FRAME_BYTES),
                number(properties,
                        "rowweave.stream.worker.maximumResidentBytes",
                        DEFAULT_MAXIMUM_RESIDENT_BYTES),
                integer(properties, "rowweave.stream.worker.maximumThreads",
                        DEFAULT_MAXIMUM_THREADS),
                integer(properties, "rowweave.stream.worker.maximumTimerCount",
                        DEFAULT_MAXIMUM_TIMER_COUNT),
                integer(properties, "rowweave.stream.worker.maximumWindowCount",
                        DEFAULT_MAXIMUM_WINDOW_COUNT),
                number(properties,
                        "rowweave.stream.worker.maximumCheckpointBytes",
                        DEFAULT_MAXIMUM_CHECKPOINT_BYTES),
                number(properties,
                        "rowweave.stream.worker.maximumSpillDiskBytes",
                        DEFAULT_MAXIMUM_SPILL_DISK_BYTES),
                integer(properties,
                        "rowweave.stream.worker.maximumFileDescriptorCount",
                        DEFAULT_MAXIMUM_FILE_DESCRIPTOR_COUNT));
    }

    public String workerId() { return workerId; }
    public Path clusterStateRoot() { return clusterStateRoot; }
    public int taskSlots() { return taskSlots; }
    public int channelCapacity() { return channelCapacity; }
    public int maximumFrameBytes() { return maximumFrameBytes; }
    public long maximumResidentBytes() { return maximumResidentBytes; }
    public int maximumThreads() { return maximumThreads; }
    public int maximumTimerCount() { return maximumTimerCount; }
    public int maximumWindowCount() { return maximumWindowCount; }
    public long maximumCheckpointBytes() { return maximumCheckpointBytes; }
    public long maximumSpillDiskBytes() { return maximumSpillDiskBytes; }
    public int maximumFileDescriptorCount() {
        return maximumFileDescriptorCount;
    }
    public ControlSecurityConfig controlSecurity() { return controlSecurity; }
    public int controlPort() { return controlPort; }

    private static ControlSecurityConfig loopbackSecurity() {
        return ControlSecurityConfig.fromProperties(new Properties());
    }

    private static int integer(
            Properties properties, String key, int fallback) {
        long value = number(properties, key, fallback);
        if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(key + " is out of range");
        }
        return (int) value;
    }

    private static long number(
            Properties properties, String key, long fallback) {
        String text = properties.getProperty(key);
        if (text == null || text.trim().isEmpty()) return fallback;
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key + " is not an integer");
        }
    }
}
