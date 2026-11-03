package io.rowweave;

import java.util.Objects;
import java.util.regex.Pattern;

/** Closed, canonical identity of one rejected-row quality reason. */
public final class QualityReasonKey implements Comparable<QualityReasonKey> {
    private static final Pattern CODE = Pattern.compile("[A-Z0-9_]{1,64}");
    private static final Pattern RULE = Pattern.compile("[A-Za-z][A-Za-z0-9._-]{0,63}");

    private final String errorCode;
    private final String ruleId;
    private final int fieldIndex;
    private final String detail;

    private QualityReasonKey(String errorCode, String ruleId, int fieldIndex, String detail) {
        if (errorCode == null || !CODE.matcher(errorCode).matches()) {
            throw new IllegalArgumentException("quality error code is outside the closed grammar");
        }
        if (ruleId == null || (!ruleId.isEmpty() && !RULE.matcher(ruleId).matches())) {
            throw new IllegalArgumentException("quality rule id is outside the closed grammar");
        }
        if (fieldIndex < -1 || fieldIndex > 4095) {
            throw new IllegalArgumentException("quality field index is outside [-1, 4095]");
        }
        if (!isAdmittedDetail(detail)) {
            throw new IllegalArgumentException("quality detail is outside the closed token set");
        }
        this.errorCode = errorCode;
        this.ruleId = ruleId;
        this.fieldIndex = fieldIndex;
        this.detail = detail;
    }

    private static boolean isAdmittedDetail(String detail) {
        if (detail == null) return false;
        switch (detail) {
            case "ROW_WIDTH":
            case "BooleanType":
            case "StringType":
            case "IntegerType":
            case "LongType":
            case "BigDecimalType":
            case "DateType":
            case "TimestampType":
            case "NOT_NULL":
            case "INTEGER_RANGE_CLOSED":
            case "LONG_RANGE_CLOSED":
            case "DECIMAL_RANGE_CLOSED":
            case "STRING_UTF8_BYTES_RANGE_CLOSED":
            case "UTF8":
            case "CR":
            case "ROW_BYTES":
            case "ROWWEAVE_ROW_TEXT_V1":
                return true;
            default:
                return false;
        }
    }

    public static QualityReasonKey of(
            String errorCode, String ruleId, int fieldIndex, String detail) {
        return new QualityReasonKey(errorCode, ruleId, fieldIndex, detail);
    }

    public String errorCode() {
        return errorCode;
    }

    public String ruleId() {
        return ruleId;
    }

    public int fieldIndex() {
        return fieldIndex;
    }

    public String detail() {
        return detail;
    }

    @Override
    public int compareTo(QualityReasonKey other) {
        int compared = errorCode.compareTo(other.errorCode);
        if (compared != 0) return compared;
        compared = ruleId.compareTo(other.ruleId);
        if (compared != 0) return compared;
        compared = Integer.compare(fieldIndex, other.fieldIndex);
        if (compared != 0) return compared;
        return detail.compareTo(other.detail);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof QualityReasonKey)) return false;
        QualityReasonKey that = (QualityReasonKey) other;
        return fieldIndex == that.fieldIndex
                && errorCode.equals(that.errorCode)
                && ruleId.equals(that.ruleId)
                && detail.equals(that.detail);
    }

    @Override
    public int hashCode() {
        return Objects.hash(errorCode, ruleId, fieldIndex, detail);
    }
}
