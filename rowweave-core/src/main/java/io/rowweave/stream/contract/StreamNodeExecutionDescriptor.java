package io.rowweave.stream.contract;

import java.util.Objects;

/** Versioned node-local semantics and finite task resource request. */
public final class StreamNodeExecutionDescriptor {
    private final PortableStreamPlan.ChangelogMode changelogMode;
    private final PortableStreamPlan.TimeMode timeMode;
    private final int parallelism;
    private final int maximumParallelism;
    private final long maximumResidentBytes;
    private final int maximumTimerCount;
    private final int maximumWindowCount;
    private final long maximumCheckpointBytes;
    private final long maximumSpillDiskBytes;
    private final int maximumThreads;

    public StreamNodeExecutionDescriptor(
            PortableStreamPlan.ChangelogMode changelogMode,
            PortableStreamPlan.TimeMode timeMode,
            int parallelism, int maximumParallelism,
            long maximumResidentBytes, long maximumCheckpointBytes,
            long maximumSpillDiskBytes, int maximumThreads) {
        this(changelogMode, timeMode, parallelism, maximumParallelism,
                maximumResidentBytes, 0, 0, maximumCheckpointBytes,
                maximumSpillDiskBytes, maximumThreads);
    }

    public StreamNodeExecutionDescriptor(
            PortableStreamPlan.ChangelogMode changelogMode,
            PortableStreamPlan.TimeMode timeMode,
            int parallelism, int maximumParallelism,
            long maximumResidentBytes, int maximumTimerCount,
            int maximumWindowCount, long maximumCheckpointBytes,
            long maximumSpillDiskBytes, int maximumThreads) {
        if (changelogMode == null || timeMode == null
                || parallelism < 1
                || maximumParallelism < parallelism
                || maximumParallelism > PortableStreamPlan.MAX_PARALLELISM
                || maximumResidentBytes <= 0L
                || maximumTimerCount < 0 || maximumWindowCount < 0
                || maximumCheckpointBytes < 0L
                || maximumSpillDiskBytes < 0L
                || maximumThreads < 1 || maximumThreads > 65_536) {
            throw StreamContractSupport.failure(
                    "INVALID_NODE_EXECUTION_CONTRACT",
                    "nodes.execution",
                    "node execution semantics and resources are invalid");
        }
        this.changelogMode = changelogMode;
        this.timeMode = timeMode;
        this.parallelism = parallelism;
        this.maximumParallelism = maximumParallelism;
        this.maximumResidentBytes = maximumResidentBytes;
        this.maximumTimerCount = maximumTimerCount;
        this.maximumWindowCount = maximumWindowCount;
        this.maximumCheckpointBytes = maximumCheckpointBytes;
        this.maximumSpillDiskBytes = maximumSpillDiskBytes;
        this.maximumThreads = maximumThreads;
    }

    public PortableStreamPlan.ChangelogMode changelogMode() {
        return changelogMode;
    }
    public PortableStreamPlan.TimeMode timeMode() { return timeMode; }
    public int parallelism() { return parallelism; }
    public int maximumParallelism() { return maximumParallelism; }
    public long maximumResidentBytes() { return maximumResidentBytes; }
    public int maximumTimerCount() { return maximumTimerCount; }
    public int maximumWindowCount() { return maximumWindowCount; }
    public long maximumCheckpointBytes() { return maximumCheckpointBytes; }
    public long maximumSpillDiskBytes() { return maximumSpillDiskBytes; }
    public int maximumThreads() { return maximumThreads; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamNodeExecutionDescriptor)) return false;
        StreamNodeExecutionDescriptor that =
                (StreamNodeExecutionDescriptor) other;
        return changelogMode == that.changelogMode
                && timeMode == that.timeMode
                && parallelism == that.parallelism
                && maximumParallelism == that.maximumParallelism
                && maximumResidentBytes == that.maximumResidentBytes
                && maximumTimerCount == that.maximumTimerCount
                && maximumWindowCount == that.maximumWindowCount
                && maximumCheckpointBytes == that.maximumCheckpointBytes
                && maximumSpillDiskBytes == that.maximumSpillDiskBytes
                && maximumThreads == that.maximumThreads;
    }

    @Override
    public int hashCode() {
        return Objects.hash(changelogMode, timeMode, parallelism,
                maximumParallelism, maximumResidentBytes, maximumTimerCount,
                maximumWindowCount, maximumCheckpointBytes,
                maximumSpillDiskBytes, maximumThreads);
    }
}
