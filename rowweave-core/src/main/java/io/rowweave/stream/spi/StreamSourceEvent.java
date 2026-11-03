package io.rowweave.stream.spi;

/** Provider-side record/watermark/idle event before runtime fencing. */
public final class StreamSourceEvent {
    public enum Kind { RECORD, WATERMARK, WATERMARK_STATUS, END_OF_INPUT }
    public enum WatermarkStatus { ACTIVE, IDLE }
    private final Kind kind;
    private final StreamSourceRecord record;
    private final long value;
    private final WatermarkStatus status;
    private StreamSourceEvent(Kind kind, StreamSourceRecord record,
            long value, WatermarkStatus status) {
        this.kind = kind; this.record = record; this.value = value; this.status = status;
    }
    public static StreamSourceEvent record(StreamSourceRecord value) {
        if (value == null) throw new IllegalArgumentException("record is required");
        return new StreamSourceEvent(Kind.RECORD, value, 0L, null);
    }
    public static StreamSourceEvent watermark(long value) {
        return new StreamSourceEvent(Kind.WATERMARK, null, value, null);
    }
    public static StreamSourceEvent watermarkStatus(
            WatermarkStatus status, long generation) {
        if (status == null || generation <= 0L) throw new IllegalArgumentException("status is invalid");
        return new StreamSourceEvent(Kind.WATERMARK_STATUS, null, generation, status);
    }
    /** Normal completion of a source explicitly declared bounded in its plan. */
    public static StreamSourceEvent endOfInput() {
        return new StreamSourceEvent(Kind.END_OF_INPUT, null, 0L, null);
    }
    public Kind kind() { return kind; }
    public StreamSourceRecord record() {
        require(Kind.RECORD); return record;
    }
    public long watermark() {
        require(Kind.WATERMARK); return value;
    }
    public long watermarkStatusGeneration() {
        require(Kind.WATERMARK_STATUS); return value;
    }
    public WatermarkStatus watermarkStatus() {
        require(Kind.WATERMARK_STATUS); return status;
    }
    private void require(Kind expected) {
        if (kind != expected) throw new IllegalStateException(
                "Stream source event is not " + expected.name());
    }
}
