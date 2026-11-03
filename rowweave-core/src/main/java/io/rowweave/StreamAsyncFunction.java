package io.rowweave;

import java.io.Serializable;

/**
 * Portable supervised asynchronous lookup function.
 *
 * <p>{@link #createRequest(DataRow)} and
 * {@link #complete(DataRow, byte[], StreamCollector)} run in the operator
 * mailbox. {@link #invoke(StreamAsyncRequest)} runs on an engine-managed
 * executor; implementations must not create threads or emit rows from that
 * callback. A retry receives the same operation identity and request frame
 * with an incremented attempt number.
 */
public interface StreamAsyncFunction extends Serializable {

    /** Creates the canonical, replayable external request for one input. */
    byte[] createRequest(DataRow input);

    /** Performs one external read attempt and returns its canonical response. */
    byte[] invoke(StreamAsyncRequest request) throws Exception;

    /** Projects one successful response through the callback-scoped output. */
    void complete(DataRow input, byte[] responseFrame, StreamCollector output);
}
