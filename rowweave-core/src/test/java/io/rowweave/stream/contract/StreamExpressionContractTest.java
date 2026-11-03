package io.rowweave.stream.contract;

import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamExpressionException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded typed expression IR for the CALC primitive. */
class StreamExpressionContractTest {

    @Test
    void typedExpressionsRoundTripCanonicallyWithoutNamesOrLocale() {
        StreamExpression amount = StreamExpression.input(
                "amount-id", PortableDataType.BIG_DECIMAL, true);
        StreamExpression zero = StreamExpression.decimalLiteral(
                new BigDecimal("0.00"));
        StreamExpression normalized = StreamExpression.call(
                StreamExpression.Function.COALESCE, amount, zero);
        StreamExpression doubled = StreamExpression.call(
                StreamExpression.Function.MULTIPLY, normalized,
                StreamExpression.decimalLiteral(new BigDecimal("2.0")));
        StreamExpression predicate = StreamExpression.call(
                StreamExpression.Function.GREATER_THAN, amount, zero);

        assertEquals(PortableDataType.BIG_DECIMAL, doubled.dataType());
        assertFalse(doubled.nullable());
        assertEquals(PortableDataType.BOOLEAN, predicate.dataType());
        assertTrue(predicate.nullable());
        assertEquals("0", zero.literalValue());

        byte[] encoded = StreamExpressionCodec.encode(doubled);
        StreamExpression decoded = StreamExpressionCodec.decode(encoded);
        assertEquals(doubled, decoded);
        assertArrayEquals(encoded, StreamExpressionCodec.encode(decoded));
    }

    @Test
    void functionRegistryHasPermanentIdsAndInfersStringDateAndCaseTypes() {
        assertEquals(1, StreamExpression.Function.AND.wireId());
        assertEquals(10, StreamExpression.Function.MULTIPLY.wireId());
        assertEquals(20, StreamExpression.Function.CASE_WHEN.wireId());
        assertEquals(30, StreamExpression.Function.DATE_DIFF_DAYS.wireId());
        assertEquals(StreamExpression.Function.DATE_DIFF_DAYS,
                StreamExpression.Function.fromWireId(30));

        StreamExpression upper = StreamExpression.call(
                StreamExpression.Function.UPPER,
                StreamExpression.input("name-id", PortableDataType.STRING, false));
        StreamExpression selected = StreamExpression.call(
                StreamExpression.Function.CASE_WHEN,
                StreamExpression.booleanLiteral(true), upper,
                StreamExpression.stringLiteral("fallback"));
        assertEquals(PortableDataType.STRING, selected.dataType());
        assertFalse(selected.nullable());

        StreamExpression day = StreamExpression.call(
                StreamExpression.Function.DATE_ADD_DAYS,
                StreamExpression.dateLiteral(20_000L),
                StreamExpression.integerLiteral(2));
        assertEquals(PortableDataType.DATE, day.dataType());
        assertFalse(day.nullable());
    }

    @Test
    void invalidArityTypesAndNonCanonicalInputFailClosed() {
        assertFailure("EXPRESSION_ARITY_MISMATCH", () ->
                StreamExpression.call(StreamExpression.Function.NOT,
                        StreamExpression.booleanLiteral(true),
                        StreamExpression.booleanLiteral(false)));
        assertFailure("EXPRESSION_TYPE_MISMATCH", () ->
                StreamExpression.call(StreamExpression.Function.ADD,
                        StreamExpression.stringLiteral("1"),
                        StreamExpression.integerLiteral(1)));
        assertFailure("NON_CANONICAL_TEXT", () -> StreamExpression.input(
                "e\u0301", PortableDataType.STRING, false));
        assertFailure("UNKNOWN_EXPRESSION_FUNCTION", () ->
                StreamExpression.Function.fromWireId(999));
    }

    @Test
    void depthNodeAndLiteralBoundsAreHardLimits() {
        StreamExpression expression = StreamExpression.booleanLiteral(true);
        for (int index = 1; index < StreamExpression.MAX_DEPTH; index++) {
            expression = StreamExpression.call(
                    StreamExpression.Function.NOT, expression);
        }
        assertEquals(StreamExpression.MAX_DEPTH, expression.depth());
        final StreamExpression atLimit = expression;
        assertFailure("EXPRESSION_LIMIT_EXCEEDED", () ->
                StreamExpression.call(StreamExpression.Function.NOT, atLimit));

        char[] oversized = new char[StreamExpression.MAX_LITERAL_BYTES + 1];
        Arrays.fill(oversized, 'x');
        assertFailure("EXPRESSION_LIMIT_EXCEEDED", () ->
                StreamExpression.stringLiteral(new String(oversized)));
    }

    @Test
    void checksumAndTrailingBytesAreRejected() {
        byte[] encoded = StreamExpressionCodec.encode(
                StreamExpression.longLiteral(7L));
        encoded[encoded.length - 1] ^= 1;
        assertContractFailure("CHECKSUM_MISMATCH", () ->
                StreamExpressionCodec.decode(encoded));

        assertThrows(StreamContractException.class,
                () -> StreamExpressionCodec.decode(new byte[] {1, 2, 3}));
    }

    private static void assertFailure(String code, Runnable action) {
        StreamExpressionException failure = assertThrows(
                StreamExpressionException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static void assertContractFailure(String code, Runnable action) {
        StreamContractException failure = assertThrows(
                StreamContractException.class, action::run);
        assertEquals(code, failure.code());
    }
}
