package io.rowweave.stream.api;

import java.util.Arrays;
import java.util.Objects;

/** Inert fixed split description; never holds a live source reader. */
public final class StreamSplitSpec {
    private final String splitId;
    private final String positionCodecId;
    private final byte[] descriptorFrame;

    public StreamSplitSpec(String splitId, String positionCodecId,
                           byte[] descriptorFrame) {
        if (splitId == null || positionCodecId == null || descriptorFrame == null) {
            throw new IllegalArgumentException("split fields are required");
        }
        this.splitId = splitId;
        this.positionCodecId = positionCodecId;
        this.descriptorFrame = descriptorFrame.clone();
    }

    public String splitId() { return splitId; }
    public String positionCodecId() { return positionCodecId; }
    public byte[] descriptorFrame() { return descriptorFrame.clone(); }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamSplitSpec)) return false;
        StreamSplitSpec that = (StreamSplitSpec) other;
        return splitId.equals(that.splitId)
                && positionCodecId.equals(that.positionCodecId)
                && Arrays.equals(descriptorFrame, that.descriptorFrame);
    }

    @Override public int hashCode() {
        return Objects.hash(splitId, positionCodecId, Arrays.hashCode(descriptorFrame));
    }
}
