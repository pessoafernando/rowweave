package io.rowweave.stream.spi;

/** Single-owner eager writer; record frames follow the declared changelog mode. */
public interface StreamSinkWriter extends AutoCloseable {
    void write(byte[] recordFrame) throws Exception;
    default void cancel() { }
    @Override default void close() throws Exception { }
}
