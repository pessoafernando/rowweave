package io.rowweave.stream.contract;

import java.util.Objects;

/** Stable logical channel and bounded credit contract for one Stream DAG input. */
public final class StreamEdge {
    public static final int MAX_FRAME_BYTES = 16 * 1024 * 1024;
    public static final int MAX_CREDIT_BYTES = 1024 * 1024 * 1024;

    public enum Partitioning {
        FORWARD(1), HASH(2), REBALANCE(3), BROADCAST(4);

        private final int wireId;

        Partitioning(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static Partitioning fromWire(int wireId) {
            for (Partitioning value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw StreamContractSupport.failure(
                    "UNKNOWN_PARTITIONING", "plan.edges.partitioning",
                    "unknown Stream edge partitioning");
        }
    }

    private final String sourceOperatorUid;
    private final String targetOperatorUid;
    private final int targetInputIndex;
    private final String logicalChannelId;
    private final Partitioning partitioning;
    private final int maximumFrameBytes;
    private final int initialCreditBytes;
    private final String sourceOutputTagId;

    public StreamEdge(
            String sourceOperatorUid, String targetOperatorUid,
            int targetInputIndex, String logicalChannelId,
            Partitioning partitioning, int maximumFrameBytes,
            int initialCreditBytes) {
        this(sourceOperatorUid, targetOperatorUid, targetInputIndex,
                logicalChannelId, partitioning, maximumFrameBytes,
                initialCreditBytes, null);
    }

    public StreamEdge(
            String sourceOperatorUid, String targetOperatorUid,
            int targetInputIndex, String logicalChannelId,
            Partitioning partitioning, int maximumFrameBytes,
            int initialCreditBytes, String sourceOutputTagId) {
        this.sourceOperatorUid = StreamContractSupport.text(
                sourceOperatorUid, "plan.edges.sourceOperatorUid", 256);
        this.targetOperatorUid = StreamContractSupport.text(
                targetOperatorUid, "plan.edges.targetOperatorUid", 256);
        if (targetInputIndex < 0 || targetInputIndex > 1) {
            throw StreamContractSupport.failure(
                    "INVALID_EDGE_INPUT", "plan.edges.targetInputIndex",
                    "target input index is outside the Stream arity");
        }
        this.targetInputIndex = targetInputIndex;
        this.logicalChannelId = StreamContractSupport.text(
                logicalChannelId, "plan.edges.logicalChannelId", 256);
        if (partitioning == null) {
            throw StreamContractSupport.failure(
                    "UNKNOWN_PARTITIONING", "plan.edges.partitioning",
                    "edge partitioning is required");
        }
        this.partitioning = partitioning;
        if (maximumFrameBytes <= 0 || maximumFrameBytes > MAX_FRAME_BYTES
                || initialCreditBytes < maximumFrameBytes
                || initialCreditBytes > MAX_CREDIT_BYTES) {
            throw StreamContractSupport.failure(
                    "INVALID_EDGE_CREDIT", "plan.edges.credit",
                    "edge credit must cover one bounded frame");
        }
        this.maximumFrameBytes = maximumFrameBytes;
        this.initialCreditBytes = initialCreditBytes;
        this.sourceOutputTagId = sourceOutputTagId == null ? null
                : StreamContractSupport.text(sourceOutputTagId,
                "plan.edges.sourceOutputTagId", 256);
    }

    public String sourceOperatorUid() { return sourceOperatorUid; }
    public String targetOperatorUid() { return targetOperatorUid; }
    public int targetInputIndex() { return targetInputIndex; }
    public String logicalChannelId() { return logicalChannelId; }
    public Partitioning partitioning() { return partitioning; }
    public int maximumFrameBytes() { return maximumFrameBytes; }
    public int initialCreditBytes() { return initialCreditBytes; }
    public String sourceOutputTagId() { return sourceOutputTagId; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamEdge)) return false;
        StreamEdge that = (StreamEdge) other;
        return targetInputIndex == that.targetInputIndex
                && maximumFrameBytes == that.maximumFrameBytes
                && initialCreditBytes == that.initialCreditBytes
                && sourceOperatorUid.equals(that.sourceOperatorUid)
                && targetOperatorUid.equals(that.targetOperatorUid)
                && logicalChannelId.equals(that.logicalChannelId)
                && partitioning == that.partitioning
                && Objects.equals(sourceOutputTagId,
                that.sourceOutputTagId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceOperatorUid, targetOperatorUid, targetInputIndex,
                logicalChannelId, partitioning, maximumFrameBytes,
                initialCreditBytes, sourceOutputTagId);
    }
}
