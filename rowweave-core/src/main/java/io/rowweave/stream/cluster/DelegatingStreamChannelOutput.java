package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.runtime.StreamChannelOutput;
import io.rowweave.stream.runtime.StreamChannelMetrics;

/** Single-assignment bridge used while a cyclic runtime object graph is wired. */
final class DelegatingStreamChannelOutput implements StreamChannelOutput {
    private final StreamChannelIdentity identity;
    private StreamChannelOutput delegate;

    DelegatingStreamChannelOutput(StreamChannelIdentity identity) {
        if (identity == null) {
            throw new IllegalArgumentException(
                    "Stream bridge identity is required");
        }
        this.identity = identity;
    }

    synchronized void bind(StreamChannelOutput value) {
        if (value == null || delegate != null
                || !identity.equals(value.channelIdentity())) {
            throw new IllegalStateException(
                    "Stream bridge can be bound exactly once");
        }
        delegate = value;
    }

    @Override public StreamChannelIdentity channelIdentity() {
        return identity;
    }

    @Override public void send(StreamEnvelope envelope) {
        target().send(envelope);
    }

    @Override public void close() { target().close(); }

    @Override public void cancel() { target().cancel(); }

    @Override public void fail(Throwable cause) { target().fail(cause); }

    @Override public StreamChannelMetrics metrics() {
        return target().metrics();
    }

    private synchronized StreamChannelOutput target() {
        if (delegate == null) {
            throw new IllegalStateException(
                    "Stream bridge is not bound");
        }
        return delegate;
    }
}
