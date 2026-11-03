package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;

/** Producer endpoint shared by local and socket Stream data lanes. */
public interface StreamChannelOutput {
    StreamChannelIdentity channelIdentity();
    void send(StreamEnvelope envelope);
    void close();
    void cancel();
    void fail(Throwable cause);

    /** Actual output-lane counters; unavailable means coverage is incomplete. */
    default StreamChannelMetrics metrics() {
        return StreamChannelMetrics.unavailable();
    }
}
