package io.rowweave.stream.cluster;

/** Transport adapter beneath the common reconnectable Stream product handles. */
interface StreamProductControlTransport extends AutoCloseable {
    StreamProductJobView call(StreamProductControlWire.Request request);

    @Override
    default void close() { }
}
