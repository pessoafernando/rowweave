package io.rowweave.stream.cluster;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;

/** Immutable worker-incarnation capability and resource envelope for Stream tasks. */
public final class StreamWorkerCapabilityProfile {
    public static final String CONTROL_CAPABILITY = "rowweave.stream.control.v1";
    public static final String DATA_CAPABILITY = "rowweave.stream.data.v1";
    public static final String CHECKPOINT_TRANSFER_CAPABILITY =
            "rowweave.stream.checkpoint-transfer.v2";
    public static final String PROGRESS_CAPABILITY =
            "rowweave.stream.task-progress.v3";

    private final String workerId;
    private final String workerIncarnation;
    private final int controlProtocolVersion;
    private final int dataProtocolVersion;
    private final List<String> capabilities;
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

    public StreamWorkerCapabilityProfile(
            String workerId, String workerIncarnation,
            int controlProtocolVersion, int dataProtocolVersion,
            Collection<String> capabilities, int taskSlots,
            int channelCapacity, int maximumFrameBytes,
            long maximumResidentBytes, int maximumThreads) {
        this(workerId, workerIncarnation, controlProtocolVersion,
                dataProtocolVersion, capabilities, taskSlots,
                channelCapacity, maximumFrameBytes, maximumResidentBytes,
                maximumThreads, Integer.MAX_VALUE, Integer.MAX_VALUE,
                Long.MAX_VALUE, Long.MAX_VALUE,
                defaultFileDescriptorCount(channelCapacity));
    }

    public StreamWorkerCapabilityProfile(
            String workerId, String workerIncarnation,
            int controlProtocolVersion, int dataProtocolVersion,
            Collection<String> capabilities, int taskSlots,
            int channelCapacity, int maximumFrameBytes,
            long maximumResidentBytes, int maximumThreads,
            int maximumTimerCount, int maximumWindowCount,
            long maximumCheckpointBytes, long maximumSpillDiskBytes) {
        this(workerId, workerIncarnation, controlProtocolVersion,
                dataProtocolVersion, capabilities, taskSlots,
                channelCapacity, maximumFrameBytes, maximumResidentBytes,
                maximumThreads, maximumTimerCount, maximumWindowCount,
                maximumCheckpointBytes, maximumSpillDiskBytes,
                defaultFileDescriptorCount(channelCapacity));
    }

    public StreamWorkerCapabilityProfile(
            String workerId, String workerIncarnation,
            int controlProtocolVersion, int dataProtocolVersion,
            Collection<String> capabilities, int taskSlots,
            int channelCapacity, int maximumFrameBytes,
            long maximumResidentBytes, int maximumThreads,
            int maximumTimerCount, int maximumWindowCount,
            long maximumCheckpointBytes, long maximumSpillDiskBytes,
            int maximumFileDescriptorCount) {
        if (controlProtocolVersion <= 0 || dataProtocolVersion <= 0
                || taskSlots <= 0 || taskSlots > 4096
                || channelCapacity <= 0 || channelCapacity > 4096
                || maximumFrameBytes <= 0 || maximumResidentBytes <= 0L
                || maximumThreads <= 0 || maximumThreads > 65_536
                || maximumTimerCount < 0 || maximumWindowCount < 0
                || maximumCheckpointBytes < 0L
                || maximumSpillDiskBytes < 0L
                || maximumFileDescriptorCount <= 0) {
            throw new IllegalArgumentException(
                    "Stream worker capability resource envelope is invalid");
        }
        this.workerId = StreamControlMessage.text(workerId, "worker ID");
        this.workerIncarnation = StreamControlMessage.text(
                workerIncarnation, "worker incarnation");
        this.controlProtocolVersion = controlProtocolVersion;
        this.dataProtocolVersion = dataProtocolVersion;
        this.capabilities = capabilities(capabilities);
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
    }

    public String workerId() { return workerId; }
    public String workerIncarnation() { return workerIncarnation; }
    public int controlProtocolVersion() { return controlProtocolVersion; }
    public int dataProtocolVersion() { return dataProtocolVersion; }
    public List<String> capabilities() { return capabilities; }
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

    boolean supportsProgressObservation() {
        return controlProtocolVersion >= 3
                && capabilities.contains(PROGRESS_CAPABILITY);
    }

    private static int defaultFileDescriptorCount(int channelCapacity) {
        return Math.multiplyExact(channelCapacity, 2);
    }

    static List<String> capabilities(Collection<String> values) {
        if (values == null || values.isEmpty() || values.size() > 256) {
            throw new IllegalArgumentException(
                    "Stream worker capabilities are required and bounded");
        }
        TreeSet<String> canonical = new TreeSet<>();
        for (String value : values) {
            try {
                StreamControlMessage.text(value, "Stream worker capability");
            } catch (IllegalArgumentException invalid) {
                throw new IllegalArgumentException(
                        "Stream worker capability is invalid", invalid);
            }
            if (!canonical.add(value)) {
                throw new IllegalArgumentException(
                        "Stream worker capability is duplicated");
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(canonical));
    }
}
