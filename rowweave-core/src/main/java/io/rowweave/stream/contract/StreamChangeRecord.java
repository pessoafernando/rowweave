package io.rowweave.stream.contract;

import java.util.Arrays;

/** Immutable Stream INSERT/UPDATE_BEFORE/UPDATE_AFTER/DELETE record. */
public final class StreamChangeRecord {
    public enum Kind {
        INSERT(1), UPDATE_BEFORE(2), UPDATE_AFTER(3), DELETE(4);

        private final int wireId;

        Kind(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }

        static Kind fromWire(int wireId) {
            for (Kind value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure(StreamChangeException.Code.MALFORMED_FRAME,
                    "unknown Stream RETRACT record kind");
        }
    }

    private static final int MAX_COMPONENT_BYTES = 4 * 1024 * 1024;

    private final Kind kind;
    private final byte[] stableKeyFrame;
    private final byte[] beforeImageFrame;
    private final byte[] afterImageFrame;

    private StreamChangeRecord(Kind kind, byte[] stableKeyFrame,
            byte[] beforeImageFrame, byte[] afterImageFrame) {
        if (kind == null || invalidComponent(stableKeyFrame)
                || invalidComponent(beforeImageFrame)
                || invalidComponent(afterImageFrame)
                || !validShape(kind, stableKeyFrame,
                beforeImageFrame, afterImageFrame)) {
            throw failure(StreamChangeException.Code.INVALID_RECORD,
                    "RETRACT record images do not match its change kind");
        }
        this.kind = kind;
        this.stableKeyFrame = copy(stableKeyFrame);
        this.beforeImageFrame = copy(beforeImageFrame);
        this.afterImageFrame = copy(afterImageFrame);
    }

    public static StreamChangeRecord insert(byte[] afterImageFrame) {
        return new StreamChangeRecord(
                Kind.INSERT, null, null, afterImageFrame);
    }

    public static StreamChangeRecord updateBefore(
            byte[] stableKeyFrame, byte[] beforeImageFrame) {
        return new StreamChangeRecord(Kind.UPDATE_BEFORE,
                stableKeyFrame, beforeImageFrame, null);
    }

    public static StreamChangeRecord updateAfter(
            byte[] stableKeyFrame, byte[] afterImageFrame) {
        return new StreamChangeRecord(Kind.UPDATE_AFTER,
                stableKeyFrame, null, afterImageFrame);
    }

    public static StreamChangeRecord delete(
            byte[] stableKeyFrame, byte[] beforeImageFrame) {
        return new StreamChangeRecord(Kind.DELETE,
                stableKeyFrame, beforeImageFrame, null);
    }

    public static StreamChangeRecord keyOnlyDelete(byte[] stableKeyFrame) {
        return new StreamChangeRecord(
                Kind.DELETE, stableKeyFrame, null, null);
    }

    public Kind kind() { return kind; }
    public boolean hasStableKey() { return stableKeyFrame != null; }
    public boolean hasBeforeImage() { return beforeImageFrame != null; }
    public boolean hasAfterImage() { return afterImageFrame != null; }

    public byte[] stableKeyFrame() {
        if (!hasStableKey()) {
            throw new IllegalStateException(
                    "Stream INSERT has no encoded stable key");
        }
        return stableKeyFrame.clone();
    }

    public byte[] beforeImageFrame() {
        if (!hasBeforeImage()) {
            throw new IllegalStateException(
                    "Stream change has no before-image");
        }
        return beforeImageFrame.clone();
    }

    public byte[] afterImageFrame() {
        if (!hasAfterImage()) {
            throw new IllegalStateException(
                    "Stream change has no after-image");
        }
        return afterImageFrame.clone();
    }

    byte[] stableKeyFrameUnsafe() { return stableKeyFrame; }
    byte[] beforeImageFrameUnsafe() { return beforeImageFrame; }
    byte[] afterImageFrameUnsafe() { return afterImageFrame; }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof StreamChangeRecord)) return false;
        StreamChangeRecord that = (StreamChangeRecord) other;
        return kind == that.kind
                && Arrays.equals(stableKeyFrame, that.stableKeyFrame)
                && Arrays.equals(beforeImageFrame, that.beforeImageFrame)
                && Arrays.equals(afterImageFrame, that.afterImageFrame);
    }

    @Override
    public int hashCode() {
        int result = 31 * kind.hashCode()
                + Arrays.hashCode(stableKeyFrame);
        result = 31 * result + Arrays.hashCode(beforeImageFrame);
        return 31 * result + Arrays.hashCode(afterImageFrame);
    }

    private static boolean invalidComponent(byte[] value) {
        return value != null && (value.length == 0
                || value.length > MAX_COMPONENT_BYTES);
    }

    private static boolean validShape(Kind kind, byte[] key,
            byte[] before, byte[] after) {
        switch (kind) {
            case INSERT:
                return key == null && before == null && after != null;
            case UPDATE_BEFORE:
                return key != null && before != null && after == null;
            case UPDATE_AFTER:
                return key != null && before == null && after != null;
            case DELETE:
                return key != null && after == null;
            default:
                return false;
        }
    }

    private static byte[] copy(byte[] value) {
        return value == null ? null : value.clone();
    }

    private static StreamChangeException failure(
            StreamChangeException.Code code, String message) {
        return new StreamChangeException(code, message);
    }
}
