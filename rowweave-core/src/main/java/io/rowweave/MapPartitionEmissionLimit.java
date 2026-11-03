package io.rowweave;

import java.io.Serializable;
import java.util.Objects;

/** Hard output quota for one physical map-partition invocation. */
public final class MapPartitionEmissionLimit implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final long MAX_OUTPUT_ROWS_PER_PARTITION = 1_000_000_000L;
    public static final long MAX_OUTPUT_BYTES_PER_PARTITION =
            1024L * 1024L * 1024L * 1024L;

    private final long maxOutputRowsPerPartition;
    private final long maxOutputBytesPerPartition;

    public MapPartitionEmissionLimit(
            long maxOutputRowsPerPartition,
            long maxOutputBytesPerPartition) {
        if (maxOutputRowsPerPartition <= 0L
                || maxOutputRowsPerPartition > MAX_OUTPUT_ROWS_PER_PARTITION
                || maxOutputBytesPerPartition <= 0L
                || maxOutputBytesPerPartition > MAX_OUTPUT_BYTES_PER_PARTITION) {
            throw new IllegalArgumentException(
                    "map-partition emission limits are outside their bounds");
        }
        this.maxOutputRowsPerPartition = maxOutputRowsPerPartition;
        this.maxOutputBytesPerPartition = maxOutputBytesPerPartition;
    }

    public long maxOutputRowsPerPartition() {
        return maxOutputRowsPerPartition;
    }

    public long maxOutputBytesPerPartition() {
        return maxOutputBytesPerPartition;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof MapPartitionEmissionLimit)) return false;
        MapPartitionEmissionLimit that = (MapPartitionEmissionLimit) other;
        return maxOutputRowsPerPartition == that.maxOutputRowsPerPartition
                && maxOutputBytesPerPartition
                == that.maxOutputBytesPerPartition;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxOutputRowsPerPartition,
                maxOutputBytesPerPartition);
    }
}
