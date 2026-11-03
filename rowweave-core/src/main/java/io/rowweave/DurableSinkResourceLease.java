package io.rowweave;

/** Engine-accounted resource claim supplied to durable Sink provider code. */
public interface DurableSinkResourceLease extends AutoCloseable {

    long residentBytes();

    int openHandles();

    boolean isOpen();

    @Override
    void close();
}
