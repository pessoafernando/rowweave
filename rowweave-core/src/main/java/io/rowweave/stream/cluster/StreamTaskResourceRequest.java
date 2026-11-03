package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.PortableStreamPlan;

import java.util.Collection;
import java.util.List;

/** Whole-placement request; the pool either places every task or returns no result. */
public final class StreamTaskResourceRequest {
    private final int taskCount;
    private final int controlProtocolVersion;
    private final int dataProtocolVersion;
    private final int maximumFrameBytes;
    private final long residentBytesPerTask;
    private final int threadsPerTask;
    private final int channelsPerTask;
    private final List<String> requiredCapabilities;

    public StreamTaskResourceRequest(
            int taskCount, int controlProtocolVersion,
            int dataProtocolVersion, int maximumFrameBytes,
            long residentBytesPerTask, int threadsPerTask,
            Collection<String> requiredCapabilities) {
        this(taskCount, controlProtocolVersion, dataProtocolVersion,
                maximumFrameBytes, residentBytesPerTask, threadsPerTask, 1,
                requiredCapabilities);
    }

    public StreamTaskResourceRequest(
            int taskCount, int controlProtocolVersion,
            int dataProtocolVersion, int maximumFrameBytes,
            long residentBytesPerTask, int threadsPerTask,
            int channelsPerTask,
            Collection<String> requiredCapabilities) {
        if (taskCount <= 0 || taskCount > PortableStreamPlan.MAX_NODES
                || controlProtocolVersion <= 0 || dataProtocolVersion <= 0
                || maximumFrameBytes <= 0 || residentBytesPerTask <= 0L
                || threadsPerTask <= 0 || threadsPerTask > 65_536
                || channelsPerTask <= 0 || channelsPerTask > 64) {
            throw new IllegalArgumentException(
                    "Stream task resource request is invalid");
        }
        this.requiredCapabilities =
                StreamWorkerCapabilityProfile.capabilities(requiredCapabilities);
        if (!this.requiredCapabilities.contains(
                PortableStreamPlan.STREAM_CAPABILITY)
                || !this.requiredCapabilities.contains(
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY)
                || !this.requiredCapabilities.contains(
                StreamWorkerCapabilityProfile.DATA_CAPABILITY)) {
            throw new IllegalArgumentException(
                    "Stream task request lacks required cluster capabilities");
        }
        this.taskCount = taskCount;
        this.controlProtocolVersion = controlProtocolVersion;
        this.dataProtocolVersion = dataProtocolVersion;
        this.maximumFrameBytes = maximumFrameBytes;
        this.residentBytesPerTask = residentBytesPerTask;
        this.threadsPerTask = threadsPerTask;
        this.channelsPerTask = channelsPerTask;
    }

    public int taskCount() { return taskCount; }
    public int controlProtocolVersion() { return controlProtocolVersion; }
    public int dataProtocolVersion() { return dataProtocolVersion; }
    public int maximumFrameBytes() { return maximumFrameBytes; }
    public long residentBytesPerTask() { return residentBytesPerTask; }
    public int threadsPerTask() { return threadsPerTask; }
    public int channelsPerTask() { return channelsPerTask; }
    public List<String> requiredCapabilities() { return requiredCapabilities; }
}
