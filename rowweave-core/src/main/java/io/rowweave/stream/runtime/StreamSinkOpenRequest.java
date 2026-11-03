package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamSinkBinding;

/** Immutable, credential-free eager Sink open request assembled after preflight. */
public final class StreamSinkOpenRequest {
    private final String sinkOperatorUid;
    private final int subtaskIndex;
    private final StreamSinkBinding binding;
    private final StreamChannelIdentity inputChannelIdentity;

    StreamSinkOpenRequest(
            String sinkOperatorUid, StreamSinkBinding binding,
            StreamChannelIdentity inputChannelIdentity) {
        this(sinkOperatorUid, 0, binding, inputChannelIdentity);
    }

    StreamSinkOpenRequest(
            String sinkOperatorUid, int subtaskIndex,
            StreamSinkBinding binding,
            StreamChannelIdentity inputChannelIdentity) {
        if (subtaskIndex < 0) {
            throw new IllegalArgumentException(
                    "Stream Sink subtask index must not be negative");
        }
        this.sinkOperatorUid = sinkOperatorUid;
        this.subtaskIndex = subtaskIndex;
        this.binding = binding;
        this.inputChannelIdentity = inputChannelIdentity;
    }

    public String sinkOperatorUid() { return sinkOperatorUid; }
    public int subtaskIndex() { return subtaskIndex; }
    public StreamSinkBinding binding() { return binding; }
    public StreamChannelIdentity inputChannelIdentity() {
        return inputChannelIdentity;
    }
}
