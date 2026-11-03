package io.rowweave.stream.contract;

import java.util.Arrays;

/** Immutable Stream after-image or tombstone with a stable non-empty key. */
public final class StreamUpsertRecord {
    public enum Kind {
        AFTER_IMAGE(1), TOMBSTONE(2);

        private final int wireId;

        Kind(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static Kind fromWire(int wireId) {
            for (Kind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure(StreamUpsertException.Code.MALFORMED_FRAME,
                    "unknown Stream UPSERT record kind");
        }
    }

    private static final int MAX_COMPONENT_BYTES = 4 * 1024 * 1024;

    private final Kind kind;
    private final byte[] stableKeyFrame;
    private final byte[] afterImageFrame;

    private StreamUpsertRecord(
            Kind kind, byte[] stableKeyFrame, byte[] afterImageFrame) {
        if (kind == null || stableKeyFrame == null
                || stableKeyFrame.length == 0
                || stableKeyFrame.length > MAX_COMPONENT_BYTES
                || (kind == Kind.AFTER_IMAGE
                && (afterImageFrame == null || afterImageFrame.length == 0
                || afterImageFrame.length > MAX_COMPONENT_BYTES))
                || (kind == Kind.TOMBSTONE && afterImageFrame != null)) {
            throw failure(StreamUpsertException.Code.INVALID_RECORD,
                    "UPSERT requires a stable key and exactly one after-image or tombstone shape");
        }
        this.kind = kind;
        this.stableKeyFrame = stableKeyFrame.clone();
        this.afterImageFrame = afterImageFrame == null
                ? null : afterImageFrame.clone();
    }

    public static StreamUpsertRecord afterImage(
            byte[] stableKeyFrame, byte[] afterImageFrame) {
        return new StreamUpsertRecord(
                Kind.AFTER_IMAGE, stableKeyFrame, afterImageFrame);
    }

    public static StreamUpsertRecord tombstone(byte[] stableKeyFrame) {
        return new StreamUpsertRecord(Kind.TOMBSTONE, stableKeyFrame, null);
    }

    public Kind kind() { return kind; }
    public byte[] stableKeyFrame() { return stableKeyFrame.clone(); }
    public boolean hasAfterImage() { return kind == Kind.AFTER_IMAGE; }

    public byte[] afterImageFrame() {
        if (!hasAfterImage()) {
            throw new IllegalStateException("UPSERT tombstone has no after-image");
        }
        return afterImageFrame.clone();
    }

    byte[] stableKeyFrameUnsafe() { return stableKeyFrame; }
    byte[] afterImageFrameUnsafe() { return afterImageFrame; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamUpsertRecord)) return false;
        StreamUpsertRecord that = (StreamUpsertRecord) other;
        return kind == that.kind
                && Arrays.equals(stableKeyFrame, that.stableKeyFrame)
                && Arrays.equals(afterImageFrame, that.afterImageFrame);
    }

    @Override
    public int hashCode() {
        return 31 * (31 * kind.hashCode() + Arrays.hashCode(stableKeyFrame))
                + Arrays.hashCode(afterImageFrame);
    }

    private static StreamUpsertException failure(
            StreamUpsertException.Code code, String message) {
        return new StreamUpsertException(code, message);
    }
}
