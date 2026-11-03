package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamEnvelope;

/** Immutable provider-side source event before channel identity/sequence binding. */
public final class StreamSourceEvent {
    public enum Kind { RECORD, WATERMARK, WATERMARK_STATUS, END_OF_INPUT }

    private final Kind kind;
    private final StreamSourceRecord record;
    private final long watermark;
    private final StreamEnvelope.WatermarkStatus watermarkStatus;
    private final long watermarkStatusGeneration;

    private StreamSourceEvent(
            Kind kind, StreamSourceRecord record, long watermark,
            StreamEnvelope.WatermarkStatus watermarkStatus,
            long watermarkStatusGeneration) {
        this.kind = kind;
        this.record = record;
        this.watermark = watermark;
        this.watermarkStatus = watermarkStatus;
        this.watermarkStatusGeneration = watermarkStatusGeneration;
    }

    public static StreamSourceEvent record(StreamSourceRecord record) {
        if (record == null) {
            throw new IllegalArgumentException("Stream source record is required");
        }
        return new StreamSourceEvent(
                Kind.RECORD, record, 0L, null, 0L);
    }

    public static StreamSourceEvent watermark(long watermark) {
        return new StreamSourceEvent(
                Kind.WATERMARK, null, watermark, null, 0L);
    }

    public static StreamSourceEvent watermarkStatus(
            StreamEnvelope.WatermarkStatus status, long generation) {
        if (status == null || generation <= 0L) {
            throw new IllegalArgumentException(
                    "Stream source watermark status and generation are required");
        }
        return new StreamSourceEvent(
                Kind.WATERMARK_STATUS, null, 0L, status, generation);
    }

    /** Declares normal completion of a bounded/static source lane. */
    public static StreamSourceEvent endOfInput() {
        return new StreamSourceEvent(
                Kind.END_OF_INPUT, null, 0L, null, 0L);
    }

    public Kind kind() { return kind; }

    public StreamSourceRecord record() {
        if (kind != Kind.RECORD) {
            throw new IllegalStateException("source event is not a record");
        }
        return record;
    }

    public long watermark() {
        if (kind != Kind.WATERMARK) {
            throw new IllegalStateException("source event is not a watermark");
        }
        return watermark;
    }

    public StreamEnvelope.WatermarkStatus watermarkStatus() {
        if (kind != Kind.WATERMARK_STATUS) {
            throw new IllegalStateException(
                    "source event is not a watermark status");
        }
        return watermarkStatus;
    }

    public long watermarkStatusGeneration() {
        if (kind != Kind.WATERMARK_STATUS) {
            throw new IllegalStateException(
                    "source event is not a watermark status");
        }
        return watermarkStatusGeneration;
    }
}
