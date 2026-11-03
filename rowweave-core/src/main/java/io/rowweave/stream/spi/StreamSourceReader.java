package io.rowweave.stream.spi;

/** Single-owner replayable reader used by the public connector SDK. */
public interface StreamSourceReader extends AutoCloseable {
    StreamSourceRecord next() throws Exception;
    default StreamSourceEvent nextEvent() throws Exception {
        StreamSourceRecord record = next();
        return record == null ? null : StreamSourceEvent.record(record);
    }
    byte[] checkpointPositionFrame();
    default void wakeUp() { }
    void cancel();
    @Override void close() throws Exception;
}
