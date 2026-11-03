package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** one structured evaluation, never exception-text parsing. */
class RowValidationEvaluationTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));

    private static RowValidation validation() {
        return RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .stringUtf8BytesRange("adaptor.length", 0, 1, 8)
                .integerRange("period.first", 1, 1, 12)
                .integerRange("period.second", 1, 2, 11)
                .decimalRange("amount.range", 2,
                        new BigDecimal("-10.00"), new BigDecimal("10.00"))
                .build();
    }

    @Test
    void validEvaluationUsesCanonicalEmptyDiagnostic() {
        RowValidation.Evaluation result = validation().evaluate(
                row("A001", 2, new BigDecimal("10.000")));

        assertTrue(result.isValid());
        assertEquals(RowValidation.Evaluation.Status.VALID, result.status());
        assertEquals("", result.errorCode());
        assertEquals("", result.ruleId());
        assertEquals(-1, result.fieldIndex());
        assertEquals("", result.detail());
    }

    @Test
    void widthAndTypeAreStructuredRejectionsWithoutValuesOrActualClass() {
        RowValidation.Evaluation width = validation().evaluate(row("only-one"));
        assertRejected(width, RowValidation.Evaluation.Status.ROW_WIDTH_MISMATCH,
                "QUALITY_ROW_WIDTH_MISMATCH", "", -1, "ROW_WIDTH");

        RowValidation.Evaluation type = validation().evaluate(
                row("A001", "secret-period", BigDecimal.ZERO));
        assertRejected(type, RowValidation.Evaluation.Status.VALUE_TYPE_MISMATCH,
                "QUALITY_VALUE_TYPE_MISMATCH", "", 1, "IntegerType");
        assertFalse(type.toString().contains("secret-period"));
        assertFalse(type.toString().contains("java.lang.String"));
    }

    @Test
    void firstDeclaredRuleWinsAndMalformedUtf16IsAClosedRuleViolation() {
        RowValidation.Evaluation required = validation().evaluate(
                row(null, 99, BigDecimal.ZERO));
        assertRejected(required, RowValidation.Evaluation.Status.RULE_VIOLATION,
                "QUALITY_RULE_VIOLATION", "adaptor.required", 0, "NOT_NULL");

        RowValidation.Evaluation firstRange = validation().evaluate(
                row("A001", 13, BigDecimal.ZERO));
        assertRejected(firstRange, RowValidation.Evaluation.Status.RULE_VIOLATION,
                "QUALITY_RULE_VIOLATION", "period.first", 1,
                "INTEGER_RANGE_CLOSED");

        RowValidation.Evaluation malformed = validation().evaluate(
                row("A\ud800", 2, BigDecimal.ZERO));
        assertRejected(malformed, RowValidation.Evaluation.Status.RULE_VIOLATION,
                "QUALITY_RULE_VIOLATION", "adaptor.length", 0,
                "STRING_UTF8_BYTES_RANGE_CLOSED");
    }

    @Test
    void failFastValidateKeepsItsExactPublicMessages() {
        IllegalStateException width = assertThrows(IllegalStateException.class,
                () -> validation().validate(row("one")));
        assertEquals("QUALITY_ROW_WIDTH_MISMATCH expected=3 actual=1", width.getMessage());

        IllegalStateException type = assertThrows(IllegalStateException.class,
                () -> validation().validate(row("A", "secret", BigDecimal.ZERO)));
        assertEquals("QUALITY_VALUE_TYPE_MISMATCH rule=period.first field=1 type=IntegerType",
                type.getMessage());

        IllegalStateException rule = assertThrows(IllegalStateException.class,
                () -> validation().validate(row("A", 13, BigDecimal.ZERO)));
        assertEquals("QUALITY_RULE_VIOLATION rule=period.first field=1 kind=INTEGER_RANGE_CLOSED",
                rule.getMessage());
    }

    private static void assertRejected(
            RowValidation.Evaluation result, RowValidation.Evaluation.Status status,
            String errorCode, String ruleId, int fieldIndex, String detail) {
        assertFalse(result.isValid());
        assertEquals(status, result.status());
        assertEquals(errorCode, result.errorCode());
        assertEquals(ruleId, result.ruleId());
        assertEquals(fieldIndex, result.fieldIndex());
        assertEquals(detail, result.detail());
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }
}
