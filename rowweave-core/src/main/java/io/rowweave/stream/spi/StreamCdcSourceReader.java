package io.rowweave.stream.spi;

/** Single-owner resident CDC reader; the engine owns enumerator state. */
public interface StreamCdcSourceReader extends AutoCloseable {
    StreamCdcSourceEvent next() throws Exception;
    default void notifyCheckpointComplete(
            StreamCdcCheckpointCompletion completion) throws Exception { }
    default void wakeUp() { }
    void cancel();
    @Override void close() throws Exception;
}
