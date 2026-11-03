package io.rowweave.stream.spi;

/** Bounded encoded row and the exact replay position after that row. */
public final class StreamSourceRecord {
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;
    private final byte[] rowFrame;
    private final byte[] positionFrame;
    private final Long eventTimestamp;

    private StreamSourceRecord(byte[] row, byte[] position, Long timestamp) {
        this.rowFrame = frame(row, "row");
        this.positionFrame = frame(position, "position");
        this.eventTimestamp = timestamp;
    }
    public static StreamSourceRecord of(byte[] row, byte[] position) {
        return new StreamSourceRecord(row, position, null);
    }
    public static StreamSourceRecord of(
            byte[] row, byte[] position, long eventTimestamp) {
        return new StreamSourceRecord(row, position, eventTimestamp);
    }
    public byte[] rowFrame() { return rowFrame.clone(); }
    public byte[] positionFrame() { return positionFrame.clone(); }
    public boolean hasEventTimestamp() { return eventTimestamp != null; }
    public long eventTimestamp() {
        if (eventTimestamp == null) throw new IllegalStateException("no event timestamp");
        return eventTimestamp;
    }
    private static byte[] frame(byte[] value, String field) {
        if (value == null || value.length == 0 || value.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException("Stream source " + field + " frame is invalid");
        }
        return value.clone();
    }
}
