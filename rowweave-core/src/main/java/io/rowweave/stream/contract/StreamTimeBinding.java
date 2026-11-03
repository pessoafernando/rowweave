package io.rowweave.stream.contract;

import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamEventTimeSpec;
import io.rowweave.stream.api.StreamExpression;

import java.util.Objects;

/** Portable timestamp extraction and bounded watermark generator contract. */
public final class StreamTimeBinding implements StreamNodeBinding {
    public enum WatermarkMode {
        MONOTONIC(1), BOUNDED_OUT_OF_ORDERNESS(2);
        private final int wireId;
        WatermarkMode(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static WatermarkMode fromWireId(int wireId) {
            for (WatermarkMode value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_WATERMARK_MODE",
                    "unknown watermark mode");
        }
    }

    public enum InvalidTimestampPolicy {
        FAIL(1), DROP(2);
        private final int wireId;
        InvalidTimestampPolicy(int wireId) { this.wireId = wireId; }
        int wireId() { return wireId; }
        static InvalidTimestampPolicy fromWireId(int wireId) {
            for (InvalidTimestampPolicy value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw failure("UNKNOWN_TIMESTAMP_POLICY",
                    "unknown invalid timestamp policy");
        }
    }

    private final StreamExpression timestampExpression;
    private final WatermarkMode watermarkMode;
    private final long maximumOutOfOrderMillis;
    private final long idleTimeoutMillis;
    private final InvalidTimestampPolicy invalidTimestampPolicy;

    public StreamTimeBinding(StreamExpression timestampExpression,
            WatermarkMode watermarkMode, long maximumOutOfOrderMillis,
            long idleTimeoutMillis,
            InvalidTimestampPolicy invalidTimestampPolicy) {
        if (timestampExpression == null
                || timestampExpression.dataType()
                != PortableDataType.TIMESTAMP) {
            throw failure("EXPRESSION_TYPE_MISMATCH",
                    "timestamp expression must produce TIMESTAMP");
        }
        if (watermarkMode == null || invalidTimestampPolicy == null
                || maximumOutOfOrderMillis < 0L
                || maximumOutOfOrderMillis
                > StreamEventTimeSpec.MAX_OUT_OF_ORDER_MILLIS
                || idleTimeoutMillis <= 0L
                || idleTimeoutMillis
                > StreamEventTimeSpec.MAX_IDLE_TIMEOUT_MILLIS
                || (watermarkMode == WatermarkMode.MONOTONIC
                && maximumOutOfOrderMillis != 0L)) {
            throw failure("INVALID_TIME_ASSIGNMENT",
                    "timestamp/watermark bounds are invalid");
        }
        this.timestampExpression = timestampExpression;
        this.watermarkMode = watermarkMode;
        this.maximumOutOfOrderMillis = maximumOutOfOrderMillis;
        this.idleTimeoutMillis = idleTimeoutMillis;
        this.invalidTimestampPolicy = invalidTimestampPolicy;
    }

    @Override public StreamOperatorCode.BindingKind kind() {
        return StreamOperatorCode.BindingKind.TIME;
    }

    public StreamExpression timestampExpression() {
        return timestampExpression;
    }
    public WatermarkMode watermarkMode() { return watermarkMode; }
    public long maximumOutOfOrderMillis() {
        return maximumOutOfOrderMillis;
    }
    public long idleTimeoutMillis() { return idleTimeoutMillis; }
    public InvalidTimestampPolicy invalidTimestampPolicy() {
        return invalidTimestampPolicy;
    }

    public void validateInputSchema(PortableRowSchema schema) {
        StreamCalcBinding.validateExpressionInput(timestampExpression, schema);
    }

    private static StreamContractException failure(String code,
            String message) {
        return StreamContractSupport.failure(code, "nodes.time", message);
    }

    @Override public boolean equals(Object other) {
        if (!(other instanceof StreamTimeBinding)) return false;
        StreamTimeBinding that = (StreamTimeBinding) other;
        return maximumOutOfOrderMillis == that.maximumOutOfOrderMillis
                && idleTimeoutMillis == that.idleTimeoutMillis
                && timestampExpression.equals(that.timestampExpression)
                && watermarkMode == that.watermarkMode
                && invalidTimestampPolicy == that.invalidTimestampPolicy;
    }

    @Override public int hashCode() {
        return Objects.hash(timestampExpression, watermarkMode,
                maximumOutOfOrderMillis, idleTimeoutMillis,
                invalidTimestampPolicy);
    }
}
