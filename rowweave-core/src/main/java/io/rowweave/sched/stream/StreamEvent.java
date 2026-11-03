package io.rowweave.sched.stream;

import io.rowweave.DataRow;

/**
 * The element carried on the internal stream. It follows the shape of a Flink {@code StreamElement} but drops streaming semantics.
 * What flows between operators and between workers is a StreamEvent, never a bare {@link DataRow}. The kinds are:
 * <ul>
 * <li>{@link #record(DataRow)} - a data row (the payload).</li>
 * <li>{@link #endOfStream()} - the upstream is fully consumed, replacing a channel sentinel, a len=-1 network frame, or counting rows.</li>
 * <li>{@link #barrier(long)} - a batch boundary, aligning push batches with the credit backpressure granularity.</li>
 * <li>{@link #error(Throwable)} and {@link #cancel()} - failure and cancellation carried explicitly, instead of being guessed from socket exceptions.</li>
 * </ul>
 *
 * <p><b>No streaming-only semantics</b>: no watermark, no event-time, no WatermarkStatus (each runtime keeps its own narrow closed set).
 *
 * <p>Scope: a zero-serialization binary RECORD ({@code binaryRecord} or {@code ByteRow}) depends on {@code mem.RowSerializer},
 * which is not enabled today, so this element does not carry one yet. The shuffle receive path that consumes bytes directly returns with that route.
 */
public final class StreamEvent {

    public enum Kind { RECORD, END_OF_STREAM, BARRIER, ERROR, CANCEL }

    private static final StreamEvent EOS =
            new StreamEvent(Kind.END_OF_STREAM, null, 0L, null, null);
    private static final StreamEvent CANCEL =
            new StreamEvent(Kind.CANCEL, null, 0L, null, null);

    private final Kind kind;
    private final DataRow row;        // RECORD only
    private final long batchId;    // BARRIER only
    private final Throwable cause; // ERROR only
    private final SourceRecordPosition sourcePosition; // positioned source RECORD only

    private StreamEvent(
            Kind kind, DataRow row, long batchId, Throwable cause,
            SourceRecordPosition sourcePosition) {
        this.kind = kind;
        this.row = row;
        this.batchId = batchId;
        this.cause = cause;
        this.sourcePosition = sourcePosition;
    }

    public static StreamEvent record(DataRow row) {
        if (row == null) {
            throw new IllegalArgumentException("record row must not be null");
        }
        return new StreamEvent(Kind.RECORD, row, 0L, null, null);
    }

    /** internal source-bound RECORD; position is stripped by positioned validation. */
    public static StreamEvent record(DataRow row, SourceRecordPosition sourcePosition) {
        if (row == null) {
            throw new IllegalArgumentException("record row must not be null");
        }
        if (sourcePosition == null) {
            throw new IllegalArgumentException("record source position must not be null");
        }
        return new StreamEvent(Kind.RECORD, row, 0L, null, sourcePosition);
    }

    public static StreamEvent endOfStream() {
        return EOS;
    }

    public static StreamEvent barrier(long batchId) {
        return new StreamEvent(Kind.BARRIER, null, batchId, null, null);
    }

    public static StreamEvent error(Throwable cause) {
        return new StreamEvent(Kind.ERROR, null, 0L, cause, null);
    }

    public static StreamEvent cancel() {
        return CANCEL;
    }

    public Kind kind() {
        return kind;
    }

    public boolean isRecord() {
        return kind == Kind.RECORD;
    }

    public boolean isEndOfStream() {
        return kind == Kind.END_OF_STREAM;
    }

    public boolean isBarrier() {
        return kind == Kind.BARRIER;
    }

    public boolean isError() {
        return kind == Kind.ERROR;
    }

    public boolean isCancel() {
        return kind == Kind.CANCEL;
    }

    /** The data row of a RECORD; throws IllegalStateException for any other kind. */
    public DataRow row() {
        if (kind != Kind.RECORD) {
            throw new IllegalStateException("not a RECORD event: " + kind);
        }
        return row;
    }

    public boolean hasSourcePosition() {
        return kind == Kind.RECORD && sourcePosition != null;
    }

    /** Positioned source RECORD's internal diagnostic position; absent/non-RECORD fails closed. */
    public SourceRecordPosition sourcePosition() {
        if (!hasSourcePosition()) {
            throw new IllegalStateException("RECORD event has no source position: " + kind);
        }
        return sourcePosition;
    }

    /** The batch number of a BARRIER; throws IllegalStateException for any other kind. */
    public long batchId() {
        if (kind != Kind.BARRIER) {
            throw new IllegalStateException("not a BARRIER event: " + kind);
        }
        return batchId;
    }

    /** The cause of an ERROR; throws IllegalStateException for any other kind. */
    public Throwable cause() {
        if (kind != Kind.ERROR) {
            throw new IllegalStateException("not an ERROR event: " + kind);
        }
        return cause;
    }

    @Override
    public String toString() {
        switch (kind) {
            case RECORD: return "Record(" + row + ")";
            case BARRIER: return "Barrier(" + batchId + ")";
            case ERROR: return "Error(" + cause + ")";
            default: return kind.name();
        }
    }
}
