package io.rowweave.stream.contract;

import java.util.Arrays;
import java.util.Objects;

/** Fixed, inert source split descriptor carried by PortableStreamPlanV1. */
public final class StreamSourceSplit {
    private final String splitId;
    private final String positionCodecId;
    private final byte[] descriptorFrame;

    public StreamSourceSplit(
            String splitId, String positionCodecId, byte[] descriptorFrame) {
        this.splitId = StreamContractSupport.digest(splitId, "source.splits.splitId");
        this.positionCodecId = StreamContractSupport.text(
                positionCodecId, "source.splits.positionCodecId", 256);
        this.descriptorFrame = StreamContractSupport.frame(
                descriptorFrame, "source.splits.descriptor");
    }

    public String splitId() { return splitId; }
    public String positionCodecId() { return positionCodecId; }
    public byte[] descriptorFrame() { return descriptorFrame.clone(); }
    byte[] descriptorFrameUnsafe() { return descriptorFrame; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamSourceSplit)) return false;
        StreamSourceSplit that = (StreamSourceSplit) other;
        return splitId.equals(that.splitId)
                && positionCodecId.equals(that.positionCodecId)
                && Arrays.equals(descriptorFrame, that.descriptorFrame);
    }

    @Override
    public int hashCode() {
        return Objects.hash(splitId, positionCodecId, Arrays.hashCode(descriptorFrame));
    }
}
