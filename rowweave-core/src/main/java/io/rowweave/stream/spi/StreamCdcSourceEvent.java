package io.rowweave.stream.spi;

/** Bounded high-level CDC event validated by the engine-owned enumerator. */
public final class StreamCdcSourceEvent {
    public enum Kind {
        SNAPSHOT_AFTER_IMAGE,
        SNAPSHOT_SPLIT_COMPLETE,
        SNAPSHOT_HANDOFF_COMPLETE,
        HANDOFF_COMPLETE,
        LOG_RECORD,
        LOG_POSITION
    }

    private static final int MAX_FRAME_BYTES = 4 * 1024 * 1024;
    private final Kind kind;
    private final String splitId;
    private final long logSequence;
    private final byte[] upsertFrame;
    private final byte[] completedExternalPosition;

    private StreamCdcSourceEvent(Kind kind, String splitId,
            long logSequence, byte[] upsertFrame,
            byte[] completedExternalPosition) {
        if (kind == null
                || ((kind == Kind.SNAPSHOT_AFTER_IMAGE
                || kind == Kind.SNAPSHOT_SPLIT_COMPLETE
                || kind == Kind.SNAPSHOT_HANDOFF_COMPLETE)
                != (splitId != null))
                || ((kind == Kind.SNAPSHOT_AFTER_IMAGE
                || kind == Kind.LOG_RECORD)
                != (upsertFrame != null))
                || (upsertFrame != null && (upsertFrame.length == 0
                || upsertFrame.length > MAX_FRAME_BYTES))
                || (kind == Kind.LOG_RECORD) != (logSequence >= 0L)
                || (completedExternalPosition != null
                && (completedExternalPosition.length == 0
                || completedExternalPosition.length
                > io.rowweave.stream.contract.StreamCdcEnumeratorState
                .MAX_EXTERNAL_POSITION_BYTES))
                || ((kind == Kind.SNAPSHOT_HANDOFF_COMPLETE
                || kind == Kind.LOG_POSITION)
                != (completedExternalPosition != null
                && kind != Kind.LOG_RECORD))) {
            throw new IllegalArgumentException("invalid resident CDC event");
        }
        this.kind = kind;
        this.splitId = splitId;
        this.logSequence = logSequence;
        this.upsertFrame = upsertFrame == null ? null : upsertFrame.clone();
        this.completedExternalPosition = completedExternalPosition == null
                ? null : completedExternalPosition.clone();
    }

    public static StreamCdcSourceEvent snapshotAfterImage(
            String splitId, byte[] canonicalUpsertFrame) {
        return new StreamCdcSourceEvent(Kind.SNAPSHOT_AFTER_IMAGE,
                splitId, -1L, canonicalUpsertFrame, null);
    }

    public static StreamCdcSourceEvent snapshotSplitComplete(String splitId) {
        return new StreamCdcSourceEvent(Kind.SNAPSHOT_SPLIT_COMPLETE,
                splitId, -1L, null, null);
    }

    public static StreamCdcSourceEvent snapshotHandoffComplete(
            String splitId, byte[] completedExternalPositionFrame) {
        return new StreamCdcSourceEvent(Kind.SNAPSHOT_HANDOFF_COMPLETE,
                splitId, -1L, null, completedExternalPositionFrame);
    }

    public static StreamCdcSourceEvent handoffComplete() {
        return new StreamCdcSourceEvent(
                Kind.HANDOFF_COMPLETE, null, -1L, null, null);
    }

    public static StreamCdcSourceEvent logRecord(
            long sequence, byte[] canonicalUpsertFrame) {
        return new StreamCdcSourceEvent(
                Kind.LOG_RECORD, null, sequence, canonicalUpsertFrame, null);
    }

    public static StreamCdcSourceEvent logRecord(
            long sequence, byte[] canonicalUpsertFrame,
            byte[] completedExternalPositionFrame) {
        return new StreamCdcSourceEvent(
                Kind.LOG_RECORD, null, sequence, canonicalUpsertFrame,
                completedExternalPositionFrame);
    }

    public static StreamCdcSourceEvent logPosition(
            byte[] completedExternalPositionFrame) {
        return new StreamCdcSourceEvent(
                Kind.LOG_POSITION, null, -1L, null,
                completedExternalPositionFrame);
    }

    public Kind kind() { return kind; }
    public String splitId() { return splitId; }
    public long logSequence() { return logSequence; }
    public byte[] upsertFrame() {
        return upsertFrame == null ? null : upsertFrame.clone();
    }
    public boolean hasCompletedExternalPosition() {
        return completedExternalPosition != null;
    }
    public byte[] completedExternalPositionFrame() {
        if (completedExternalPosition == null) {
            throw new IllegalStateException(
                    "resident CDC event has no completed external position");
        }
        return completedExternalPosition.clone();
    }
}
