package io.rowweave.stream.runtime;

/** Immutable provider record plus its exact replay position. */
public final class StreamSourceRecord {
    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;

    private final byte[] recordFrame;
    private final byte[] positionFrame;
    private final Long eventTimestamp;

    private StreamSourceRecord(
            byte[] recordFrame, byte[] positionFrame, Long eventTimestamp) {
        this.recordFrame = frame(recordFrame, "record");
        this.positionFrame = frame(positionFrame, "position");
        this.eventTimestamp = eventTimestamp;
    }

    public static StreamSourceRecord of(byte[] recordFrame, byte[] positionFrame) {
        return new StreamSourceRecord(recordFrame, positionFrame, null);
    }

    public static StreamSourceRecord of(
            byte[] recordFrame, byte[] positionFrame, long eventTimestamp) {
        return new StreamSourceRecord(
                recordFrame, positionFrame, Long.valueOf(eventTimestamp));
    }

    public byte[] recordFrame() { return recordFrame.clone(); }
    public byte[] positionFrame() { return positionFrame.clone(); }
    public boolean hasEventTimestamp() { return eventTimestamp != null; }

    public long eventTimestamp() {
        if (eventTimestamp == null) {
            throw new IllegalStateException("source record has no event timestamp");
        }
        return eventTimestamp.longValue();
    }

    private static byte[] frame(byte[] value, String name) {
        if (value == null || value.length == 0 || value.length > MAX_FRAME_BYTES) {
            throw new IllegalArgumentException(
                    "Stream source " + name + " frame is invalid");
        }
        return value.clone();
    }
}
