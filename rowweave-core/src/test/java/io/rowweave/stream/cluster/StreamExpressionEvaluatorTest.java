package io.rowweave.stream.cluster;

import io.rowweave.DataRow;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.api.StreamExpression;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** executable oracle for the complete CALC function registry. */
class StreamExpressionEvaluatorTest {
    private final StreamExpressionEvaluator evaluator =
            new StreamExpressionEvaluator(schema());
    private final DataRow row = row();

    @Test
    void evaluatesBooleanComparisonAndNullSemantics() {
        StreamExpression nil = StreamExpression.nullLiteral(
                PortableDataType.BOOLEAN);
        assertNull(eval(call(StreamExpression.Function.AND, bool(true), nil)));
        assertEquals(false,
                eval(call(StreamExpression.Function.AND, bool(false), nil)));
        assertEquals(true,
                eval(call(StreamExpression.Function.OR, bool(true), nil)));
        assertNull(eval(call(StreamExpression.Function.OR, bool(false), nil)));
        assertNull(eval(call(StreamExpression.Function.NOT, nil)));
        assertEquals(true, eval(call(StreamExpression.Function.IS_NULL, nil)));
        assertEquals(false,
                eval(call(StreamExpression.Function.IS_NOT_NULL, nil)));

        StreamExpression three = StreamExpression.integerLiteral(3);
        StreamExpression four = StreamExpression.integerLiteral(4);
        assertEquals(true, eval(call(StreamExpression.Function.EQUAL,
                three, StreamExpression.integerLiteral(3))));
        assertEquals(true, eval(call(StreamExpression.Function.NOT_EQUAL,
                three, four)));
        assertEquals(true, eval(call(StreamExpression.Function.LESS_THAN,
                three, four)));
        assertEquals(true, eval(call(StreamExpression.Function.LESS_OR_EQUAL,
                three, three)));
        assertEquals(true, eval(call(StreamExpression.Function.GREATER_THAN,
                four, three)));
        assertEquals(true, eval(call(StreamExpression.Function.GREATER_OR_EQUAL,
                four, four)));
        assertNull(eval(call(StreamExpression.Function.EQUAL,
                StreamExpression.nullLiteral(PortableDataType.INTEGER), three)));
        assertEquals(7, eval(StreamExpression.input(
                "number", PortableDataType.INTEGER, false)));
    }

    @Test
    void evaluatesNumericFunctionsWithoutPrematureDecimalRounding() {
        assertEquals(7, eval(call(StreamExpression.Function.ADD,
                StreamExpression.integerLiteral(3),
                StreamExpression.integerLiteral(4))));
        assertEquals(3L, eval(call(StreamExpression.Function.SUBTRACT,
                StreamExpression.longLiteral(7L),
                StreamExpression.integerLiteral(4))));
        assertEquals(12, eval(call(StreamExpression.Function.MULTIPLY,
                StreamExpression.integerLiteral(3),
                StreamExpression.integerLiteral(4))));
        assertEquals(-3, eval(call(StreamExpression.Function.NEGATE,
                StreamExpression.integerLiteral(3))));
        assertEquals(0, ((BigDecimal) eval(call(
                StreamExpression.Function.ADD,
                StreamExpression.decimalLiteral(new BigDecimal("1.000000000009")),
                StreamExpression.decimalLiteral(new BigDecimal("0.000000000002")))))
                .compareTo(new BigDecimal("1.000000000011")));
        assertEquals(0, ((BigDecimal) eval(call(
                StreamExpression.Function.DIVIDE,
                StreamExpression.decimalLiteral(BigDecimal.ONE),
                StreamExpression.decimalLiteral(new BigDecimal("3")))))
                .compareTo(new BigDecimal(
                        "0.3333333333333333333333333333333333")));
        assertThrows(ArithmeticException.class, () -> eval(call(
                StreamExpression.Function.ADD,
                StreamExpression.integerLiteral(Integer.MAX_VALUE),
                StreamExpression.integerLiteral(1))));
        assertThrows(ArithmeticException.class, () -> eval(call(
                StreamExpression.Function.NEGATE,
                StreamExpression.longLiteral(Long.MIN_VALUE))));
        assertThrows(ArithmeticException.class, () -> eval(call(
                StreamExpression.Function.DIVIDE,
                StreamExpression.decimalLiteral(BigDecimal.ONE),
                StreamExpression.decimalLiteral(BigDecimal.ZERO))));
    }

    @Test
    void evaluatesConditionalStringAndCastFunctions() {
        StreamExpression nilText = StreamExpression.nullLiteral(
                PortableDataType.STRING);
        assertEquals("fallback", eval(call(StreamExpression.Function.COALESCE,
                nilText, StreamExpression.stringLiteral("fallback"))));
        assertEquals("chosen", eval(call(StreamExpression.Function.CASE_WHEN,
                bool(true), StreamExpression.stringLiteral("chosen"),
                StreamExpression.stringLiteral("other"))));
        assertNull(eval(call(StreamExpression.Function.CONCAT,
                StreamExpression.stringLiteral("a"), nilText)));
        assertEquals("abc", eval(call(StreamExpression.Function.CONCAT,
                StreamExpression.stringLiteral("a"),
                StreamExpression.stringLiteral("b"),
                StreamExpression.stringLiteral("c"))));
        assertEquals("i", eval(call(StreamExpression.Function.LOWER,
                StreamExpression.stringLiteral("I"))));
        assertEquals("SS", eval(call(StreamExpression.Function.UPPER,
                StreamExpression.stringLiteral("ss"))));
        assertEquals("text", eval(call(StreamExpression.Function.TRIM,
                StreamExpression.stringLiteral("  text\t"))));
        assertEquals("bcd", eval(call(StreamExpression.Function.SUBSTRING,
                StreamExpression.stringLiteral("abcdef"),
                StreamExpression.integerLiteral(2),
                StreamExpression.integerLiteral(3))));
        assertEquals(6, eval(call(StreamExpression.Function.LENGTH,
                StreamExpression.stringLiteral("abcdef"))));

        assertEquals(true, eval(call(StreamExpression.Function.CAST_BOOLEAN,
                StreamExpression.stringLiteral(" TRUE "))));
        assertEquals("12.3", eval(call(StreamExpression.Function.CAST_STRING,
                StreamExpression.decimalLiteral(new BigDecimal("12.30")))));
        assertEquals(12, eval(call(StreamExpression.Function.CAST_INTEGER,
                StreamExpression.stringLiteral(" 12 "))));
        assertEquals(12L, eval(call(StreamExpression.Function.CAST_LONG,
                StreamExpression.integerLiteral(12))));
        assertEquals(0, ((BigDecimal) eval(call(
                StreamExpression.Function.CAST_DECIMAL,
                StreamExpression.stringLiteral(" 12.30 "))))
                .compareTo(new BigDecimal("12.30")));
        assertThrows(IllegalArgumentException.class, () -> eval(call(
                StreamExpression.Function.CAST_BOOLEAN,
                StreamExpression.stringLiteral("yes"))));
        assertThrows(ArithmeticException.class, () -> eval(call(
                StreamExpression.Function.CAST_INTEGER,
                StreamExpression.longLiteral(((long) Integer.MAX_VALUE) + 1L))));
    }

    @Test
    void evaluatesDateAndTimestampFunctionsInUtcRegardlessOfDefaults() {
        Locale originalLocale = Locale.getDefault();
        TimeZone originalZone = TimeZone.getDefault();
        try {
            Locale.setDefault(new Locale("tr", "TR"));
            TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Honolulu"));
            long leapDay = Instant.parse("2024-02-29T23:30:00Z").toEpochMilli();
            StreamExpression timestamp = StreamExpression.timestampLiteral(
                    leapDay);
            assertEquals(2024, eval(call(StreamExpression.Function.YEAR,
                    timestamp)));
            assertEquals(2, eval(call(StreamExpression.Function.MONTH,
                    timestamp)));
            assertEquals(29, eval(call(StreamExpression.Function.DAY,
                    timestamp)));
            assertEquals("i", eval(call(StreamExpression.Function.LOWER,
                    StreamExpression.stringLiteral("I"))));

            assertEquals(leapDay, eval(call(
                    StreamExpression.Function.TIMESTAMP_TO_EPOCH_MILLIS,
                    timestamp)));
            assertEquals(new Timestamp(leapDay), eval(call(
                    StreamExpression.Function.EPOCH_MILLIS_TO_TIMESTAMP,
                    StreamExpression.longLiteral(leapDay))));
            assertEquals(new Timestamp(leapDay), eval(call(
                    StreamExpression.Function.CAST_TIMESTAMP,
                    StreamExpression.longLiteral(leapDay))));

            StreamExpression day = StreamExpression.dateLiteral(19_782L);
            assertEquals(new java.sql.Date(19_783L * 86_400_000L), eval(call(
                    StreamExpression.Function.DATE_ADD_DAYS, day,
                    StreamExpression.integerLiteral(1))));
            assertEquals(2L, eval(call(StreamExpression.Function.DATE_DIFF_DAYS,
                    StreamExpression.dateLiteral(19_784L), day)));
            assertEquals(new java.sql.Date(leapDay), eval(call(
                    StreamExpression.Function.CAST_DATE, timestamp)));
        } finally {
            TimeZone.setDefault(originalZone);
            Locale.setDefault(originalLocale);
        }
    }

    private Object eval(StreamExpression expression) {
        return evaluator.evaluate(expression, row);
    }

    private static StreamExpression call(StreamExpression.Function function,
            StreamExpression... arguments) {
        return StreamExpression.call(function, arguments);
    }

    private static StreamExpression bool(boolean value) {
        return StreamExpression.booleanLiteral(value);
    }

    private static PortableRowSchema schema() {
        return new PortableRowSchema(Arrays.asList(
                new PortableField("number", "number", "number",
                        PortableDataType.INTEGER, false),
                new PortableField("nullable", "nullable", "nullable",
                        PortableDataType.STRING, true)));
    }

    private static DataRow row() {
        DataRow row = new DataRow(2);
        row.set(0, 7);
        row.set(1, null);
        return row;
    }
}
