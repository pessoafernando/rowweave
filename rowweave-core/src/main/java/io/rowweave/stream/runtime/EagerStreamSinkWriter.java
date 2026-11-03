package io.rowweave.stream.runtime;

/** Eager Sink writer; writes may be externally visible before cancellation. */
public interface EagerStreamSinkWriter extends AutoCloseable {
    void write(byte[] recordFrame) throws Exception;
    void cancel();
    @Override void close() throws Exception;
}
