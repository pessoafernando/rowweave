package io.rowweave.stream.runtime;

/** A single-split reader owned exclusively by its source task thread. */
public interface StreamSourceReader extends AutoCloseable {
    StreamSourceRecord next() throws Exception;
    default StreamSourceEvent nextEvent() throws Exception {
        StreamSourceRecord record = next();
        return record == null ? null : StreamSourceEvent.record(record);
    }
    default byte[] checkpointPositionFrame() {
        throw new UnsupportedOperationException(
                "checkpointable source must expose its current position");
    }
    default void wakeUp() { }
    void cancel();
    @Override void close() throws Exception;
}
