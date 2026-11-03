package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;

/** Consumer endpoint shared by local and socket Stream data lanes. */
public interface StreamChannelInput {
    StreamChannelIdentity channelIdentity();
    StreamEnvelope receive() throws InterruptedException;

    /** Bounded mailbox poll; remote and local lanes share the same contract. */
    default StreamEnvelope poll(long timeoutMillis) throws InterruptedException {
        if (timeoutMillis < 0L) {
            throw new IllegalArgumentException(
                    "Stream channel poll timeout must not be negative");
        }
        return receive();
    }

    void cancel();
    void fail(Throwable cause);
}
