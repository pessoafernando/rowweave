package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** one-row-at-a-time fail-fast validation semantics. */
class RowValidationStreamingTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("sequence", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));

    private static RowValidation validation() {
        return RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .stringUtf8BytesRange("adaptor.length", 0, 1, 8)
                .integerRange("period.range", 1, 1, 12)
                .longRange("sequence.range", 2, 0L, 100L)
                .decimalRange("amount.range", 3,
                        new BigDecimal("-10.00"), new BigDecimal("10.00"))
                .build();
    }

    @Test
    void validRowsPassWithoutCopyingOrMutatingEventsAndValues() {
        DataRow first = row("A001", 1, 0L, new BigDecimal("10.0000"));
        DataRow second = row("C003", 12, 100L, new BigDecimal("-10.000"));
        StreamEvent firstEvent = StreamEvent.record(first);
        StreamEvent secondEvent = StreamEvent.record(second);
        List<StreamEvent> output = apply(validation(), Arrays.asList(firstEvent, secondEvent));

        assertEquals(3, output.size());
        assertSame(firstEvent, output.get(0));
        assertSame(secondEvent, output.get(1));
        assertSame(first, output.get(0).row());
        assertSame(second.getBigDecimal(3), output.get(1).row().getBigDecimal(3));
        assertEquals(new BigDecimal("-10.000"), output.get(1).row().getBigDecimal(3));
        assertTrue(output.get(2).isEndOfStream());
    }

    @Test
    void nullSkipsRangesAndOrderedRulesChooseTheFirstStableFailure() {
        RowValidation nullPolicy = RowValidation.builder(META)
                .stringUtf8BytesRange("adaptor.range", 0, 1, 8)
                .notNull("adaptor.required", 0)
                .integerRange("period.first", 1, 1, 12)
                .integerRange("period.second", 1, 2, 11)
                .build();

        assertViolation(nullPolicy, row(null, 1, null, null),
                "adaptor.required", "NOT_NULL", null);
        assertViolation(nullPolicy, row("A", 13, null, null),
                "period.first", "INTEGER_RANGE_CLOSED", null);
    }

    @Test
    void everyRangeKindUsesClosedBoundsAndUtf8ByteLength() {
        RowValidation validation = validation();

        assertViolation(validation, row("A", 0, 1L, BigDecimal.ZERO),
                "period.range", "INTEGER_RANGE_CLOSED", null);
        assertViolation(validation, row("A", 1, 101L, BigDecimal.ZERO),
                "sequence.range", "LONG_RANGE_CLOSED", null);
        assertViolation(validation, row("A", 1, 1L, new BigDecimal("10.0001")),
                "amount.range", "DECIMAL_RANGE_CLOSED", null);
        assertViolation(validation, row("unicode😀ABC", 1, 1L, BigDecimal.ZERO),
                "adaptor.length", "STRING_UTF8_BYTES_RANGE_CLOSED", null);
    }

    @Test
    void widthTypeAndMalformedUtf16FailBeforeEmitWithoutLeakingValues() {
        assertFailure(validation(), row("only-one-field"),
                "QUALITY_ROW_WIDTH_MISMATCH", "only-one-field");
        assertFailure(validation(), row("A", "secret-period-13", 1L, BigDecimal.ZERO),
                "QUALITY_VALUE_TYPE_MISMATCH", "secret-period-13");
        assertFailure(validation(), row("A\ud800", 1, 1L, BigDecimal.ZERO),
                "QUALITY_RULE_VIOLATION", "A\ud800");
    }

    @Test
    void emitsBeforeNextPullAndForwardsTerminalControlExactly() {
        RowValidation validation = validation();
        int[] pulled = {0};
        int[] emitted = {0};
        RowStream stream = () -> {
            if (pulled[0] == 2) return null;
            assertEquals(pulled[0], emitted[0], "validation pulled ahead of downstream emit");
            pulled[0]++;
            return StreamEvent.record(row("A" + pulled[0], 1, 1L, BigDecimal.ZERO));
        };
        validation.apply(stream, event -> {
            if (event.isRecord()) emitted[0]++;
        });
        assertEquals(2, emitted[0]);

        StreamEvent barrier = StreamEvent.barrier(9L);
        RuntimeException upstream = new RuntimeException("upstream-secret");
        List<StreamEvent> errorOutput = apply(validation, Arrays.asList(
                StreamEvent.record(row("A", 1, 1L, BigDecimal.ZERO)),
                barrier, StreamEvent.error(upstream),
                StreamEvent.record(row("B", 1, 1L, BigDecimal.ZERO))));
        assertEquals(Arrays.asList(StreamEvent.Kind.RECORD, StreamEvent.Kind.BARRIER,
                StreamEvent.Kind.ERROR), kinds(errorOutput));
        assertSame(barrier, errorOutput.get(1));
        assertSame(upstream, errorOutput.get(2).cause());

        List<StreamEvent> cancelOutput = apply(validation, Arrays.asList(
                StreamEvent.cancel(), StreamEvent.record(row("B", 1, 1L, BigDecimal.ZERO))));
        assertEquals(Arrays.asList(StreamEvent.Kind.CANCEL), kinds(cancelOutput));

        StreamEvent eos = StreamEvent.endOfStream();
        List<StreamEvent> eosOutput = apply(validation, Arrays.asList(
                eos, StreamEvent.record(row("B", 1, 1L, BigDecimal.ZERO))));
        assertEquals(Arrays.asList(StreamEvent.Kind.END_OF_STREAM), kinds(eosOutput));
        assertSame(eos, eosOutput.get(0));
    }

    private static void assertViolation(
            RowValidation validation, DataRow row, String ruleId, String kind, String forbidden) {
        IllegalStateException failure = assertFailure(
                validation, row, "QUALITY_RULE_VIOLATION", forbidden);
        assertTrue(failure.getMessage().contains("rule=" + ruleId), failure::getMessage);
        assertTrue(failure.getMessage().contains("kind=" + kind), failure::getMessage);
    }

    private static IllegalStateException assertFailure(
            RowValidation validation, DataRow row, String prefix, String forbidden) {
        List<StreamEvent> output = new ArrayList<>();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> validation.apply(RowStream.fromIterable(Arrays.asList(row)), output::add));
        assertTrue(failure.getMessage().startsWith(prefix), failure::getMessage);
        if (forbidden != null) assertFalse(failure.getMessage().contains(forbidden));
        assertTrue(output.isEmpty(), "failing row must not be emitted");
        return failure;
    }

    private static List<StreamEvent> apply(
            RowValidation validation, List<StreamEvent> input) {
        Iterator<StreamEvent> iterator = input.iterator();
        List<StreamEvent> output = new ArrayList<>();
        validation.apply(() -> iterator.hasNext() ? iterator.next() : null, output::add);
        return output;
    }

    private static List<StreamEvent.Kind> kinds(List<StreamEvent> events) {
        List<StreamEvent.Kind> kinds = new ArrayList<>();
        for (StreamEvent event : events) kinds.add(event.kind());
        return kinds;
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }
}
