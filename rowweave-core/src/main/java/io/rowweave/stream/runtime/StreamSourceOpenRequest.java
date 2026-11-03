package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;

/** Immutable, credential-free source open request assembled after provider preflight. */
public final class StreamSourceOpenRequest {
    private final String sourceOperatorUid;
    private final StreamSourceBinding binding;
    private final StreamSourceSplit split;
    private final StreamChannelIdentity outputChannelIdentity;
    private final byte[] restoredPositionFrame;
    private final long restoredLogicalPosition;

    StreamSourceOpenRequest(
            String sourceOperatorUid, StreamSourceBinding binding,
            StreamSourceSplit split,
            StreamChannelIdentity outputChannelIdentity) {
        this(sourceOperatorUid, binding, split, outputChannelIdentity,
                null, -1L);
    }

    StreamSourceOpenRequest(
            String sourceOperatorUid, StreamSourceBinding binding,
            StreamSourceSplit split,
            StreamChannelIdentity outputChannelIdentity,
            byte[] restoredPositionFrame, long restoredLogicalPosition) {
        this.sourceOperatorUid = sourceOperatorUid;
        this.binding = binding;
        this.split = split;
        this.outputChannelIdentity = outputChannelIdentity;
        if ((restoredPositionFrame == null) != (restoredLogicalPosition < 0L)) {
            throw new IllegalArgumentException(
                    "restored source position and ordinal must appear together");
        }
        this.restoredPositionFrame = restoredPositionFrame == null ? null
                : restoredPositionFrame.clone();
        this.restoredLogicalPosition = restoredLogicalPosition;
    }

    public String sourceOperatorUid() { return sourceOperatorUid; }
    public StreamSourceBinding binding() { return binding; }
    public StreamSourceSplit split() { return split; }
    public StreamChannelIdentity outputChannelIdentity() {
        return outputChannelIdentity;
    }
    public boolean hasRestoredPosition() {
        return restoredPositionFrame != null;
    }
    public byte[] restoredPositionFrame() {
        if (restoredPositionFrame == null) {
            throw new IllegalStateException("source open has no restored position");
        }
        return restoredPositionFrame.clone();
    }
    public long restoredLogicalPosition() {
        if (restoredPositionFrame == null) {
            throw new IllegalStateException("source open has no restored position");
        }
        return restoredLogicalPosition;
    }
}
