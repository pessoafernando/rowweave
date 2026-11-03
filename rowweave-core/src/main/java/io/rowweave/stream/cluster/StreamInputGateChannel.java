package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.StreamChannelInput;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * One logical input assembled from all physical lanes of an incoming edge.
 * The gate keeps ordering and barrier alignment bounded by the upstream
 * channels; it does not materialize records.
 */
final class StreamInputGateChannel implements StreamChannelInput {
    private final List<StreamChannelInput> lanes;
    private final StreamMultiInputGate gate;

    StreamInputGateChannel(List<? extends StreamChannelInput> lanes) {
        if (lanes == null || lanes.size() < 2) {
            throw new IllegalArgumentException(
                    "logical input gate requires multiple physical lanes");
        }
        this.lanes = Collections.unmodifiableList(
                new ArrayList<>(lanes));
        this.gate = new StreamMultiInputGate(this.lanes);
    }

    @Override public StreamChannelIdentity channelIdentity() {
        return lanes.get(0).channelIdentity();
    }

    @Override public StreamEnvelope receive() throws InterruptedException {
        StreamMultiInputGate.Event event = gate.take();
        return event == null ? null : event.envelope();
    }

    @Override public StreamEnvelope poll(long timeoutMillis)
            throws InterruptedException {
        StreamMultiInputGate.Event event = gate.poll(timeoutMillis);
        return event == null ? null : event.envelope();
    }

    @Override public void cancel() { gate.cancel(); }

    @Override public void fail(Throwable cause) { gate.fail(cause); }
}
