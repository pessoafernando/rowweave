package io.rowweave.stream.spi;

/** Lazy connector enumerator. Calls are externally time- and resource-bounded. */
public interface StreamSourceEnumerator extends AutoCloseable {
    StreamSplitDiscovery discover(StreamDiscoveryCursor cursor, int limit)
            throws Exception;

    @Override
    void close() throws Exception;
}
