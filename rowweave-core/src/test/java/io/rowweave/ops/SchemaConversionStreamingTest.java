package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** one-row-at-a-time conversion and fail-closed row semantics. */
class SchemaConversionStreamingTest {

    @Test
    void convertsTheClosedMatrixWithoutRoundingAndCreatesFreshRows() {
        DataSchema source = new DataSchema(
                new Field("i", DataType.IntegerType),
                new Field("l", DataType.LongType),
                new Field("amount", DataType.BigDecimalType),
                new Field("memo", DataType.StringType),
                new Field("date", DataType.DateType),
                new Field("timestamp", DataType.TimestampType));
        SchemaConversion conversion = SchemaConversion.builder(source)
                .required(0, new Field("i64", DataType.LongType), SchemaConversion.Cast.INTEGER_TO_LONG)
                .required(0, new Field("idec", DataType.BigDecimalType), SchemaConversion.Cast.INTEGER_TO_DECIMAL)
                .required(1, new Field("ldec", DataType.BigDecimalType), SchemaConversion.Cast.LONG_TO_DECIMAL)
                .required(2, new Field("amount", DataType.BigDecimalType), SchemaConversion.Cast.IDENTITY)
                .nullable(3, new Field("memo", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                .required(4, new Field("date", DataType.DateType), SchemaConversion.Cast.IDENTITY)
                .required(5, new Field("timestamp", DataType.TimestampType), SchemaConversion.Cast.IDENTITY)
                .build();
        BigDecimal amount = new BigDecimal("123.45000000000000000001");
        Date date = new Date(1_700_000_000_123L);
        Timestamp timestamp = new Timestamp(1_700_000_000_456L);
        timestamp.setNanos(456_789_123);
        DataRow input = row(7, Long.MIN_VALUE, amount, "memo", date, timestamp);
        CollectingRowSink sink = new CollectingRowSink();

        conversion.apply(RowStream.fromIterable(Arrays.asList(input)), sink);

        assertEquals(1, sink.rows().size());
        DataRow output = sink.rows().get(0);
        assertNotSame(input, output);
        assertEquals(Long.valueOf(7L), output.getLong(0));
        assertEquals(new BigDecimal("7"), output.getBigDecimal(1));
        assertEquals(new BigDecimal(Long.toString(Long.MIN_VALUE)), output.getBigDecimal(2));
        assertSame(amount, output.getBigDecimal(3), "schema conversion must not round BigDecimal");
        assertEquals("memo", output.getString(4));
        assertSame(date, output.getDate(5));
        assertSame(timestamp, output.getTimestamp(6));
        assertEquals(456_789_123, output.getTimestamp(6).getNanos());
        assertTrue(sink.sawEndOfStream());
    }

    @Test
    void preservesRuntimeNullAndCreatesIndependentMutableDefaultsPerRow() {
        DataSchema source = new DataSchema(new Field("memo", DataType.StringType));
        Date date = new Date(123L);
        Timestamp timestamp = new Timestamp(456L);
        timestamp.setNanos(456_123_789);
        SchemaConversion conversion = SchemaConversion.builder(source)
                .nullable(0, new Field("memo", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                .defaulted(new Field("date", DataType.DateType), date)
                .defaulted(new Field("timestamp", DataType.TimestampType), timestamp)
                .defaulted(new Field("missing", DataType.StringType), null)
                .build();
        CollectingRowSink sink = new CollectingRowSink();

        conversion.apply(RowStream.fromIterable(Arrays.asList(row((Object) null), row("present"))), sink);

        DataRow first = sink.rows().get(0);
        DataRow second = sink.rows().get(1);
        assertNull(first.get(0), "default must not replace a present source field's null");
        assertEquals("present", second.getString(0));
        assertNull(first.get(3));
        assertNotSame(first.getDate(1), second.getDate(1));
        assertNotSame(first.getTimestamp(2), second.getTimestamp(2));
        first.getDate(1).setTime(999L);
        first.getTimestamp(2).setNanos(1);
        assertEquals(123L, second.getDate(1).getTime());
        assertEquals(456_123_789, second.getTimestamp(2).getNanos());
    }

    @Test
    void rejectsBadRowsBeforeEmitAndDoesNotLeakValues() {
        DataSchema source = new DataSchema(
                new Field("id", DataType.IntegerType),
                new Field("memo", DataType.StringType));
        SchemaConversion conversion = SchemaConversion.builder(source)
                .required(0, new Field("id", DataType.LongType), SchemaConversion.Cast.INTEGER_TO_LONG)
                .nullable(1, new Field("memo", DataType.StringType), SchemaConversion.Cast.IDENTITY)
                .build();

        assertRowFailure(conversion, row(1), "SCHEMA_ROW_WIDTH_MISMATCH", null);
        assertRowFailure(conversion, row(null, "memo"), "SCHEMA_NULL_VIOLATION", "memo");
        assertRowFailure(conversion, row("secret-123", "memo"),
                "SCHEMA_VALUE_TYPE_MISMATCH", "secret-123");
    }

    @Test
    void emitsEachRecordBeforePullingTheNextAndNeverBuffersTheInput() {
        DataSchema source = new DataSchema(new Field("id", DataType.IntegerType));
        SchemaConversion conversion = SchemaConversion.builder(source)
                .required(0, new Field("id", DataType.IntegerType), SchemaConversion.Cast.IDENTITY)
                .build();
        int[] pulled = {0};
        int[] emitted = {0};
        RowStream stream = () -> {
            if (pulled[0] == 2) {
                return null;
            }
            assertEquals(pulled[0], emitted[0], "operator pulled ahead of downstream emission");
            pulled[0]++;
            return StreamEvent.record(row(pulled[0]));
        };
        List<DataRow> outputs = new ArrayList<>();

        conversion.apply(stream, event -> {
            if (event.isRecord()) {
                outputs.add(event.row());
                emitted[0]++;
            }
        });

        assertEquals(2, outputs.size());
        assertNotSame(outputs.get(0), outputs.get(1));
        assertEquals(2, pulled[0]);
    }

    @Test
    void forwardsBarrierAndStopsExactlyAtErrorOrCancelWithoutEos() {
        DataSchema source = new DataSchema(new Field("id", DataType.IntegerType));
        SchemaConversion conversion = SchemaConversion.builder(source)
                .required(0, new Field("id", DataType.IntegerType), SchemaConversion.Cast.IDENTITY)
                .build();
        RuntimeException boom = new RuntimeException("upstream-secret");
        List<StreamEvent> errorInput = Arrays.asList(
                StreamEvent.record(row(1)), StreamEvent.barrier(7L),
                StreamEvent.error(boom), StreamEvent.record(row(2)));
        List<StreamEvent> errorOutput = apply(conversion, errorInput);
        assertEquals(Arrays.asList(StreamEvent.Kind.RECORD, StreamEvent.Kind.BARRIER, StreamEvent.Kind.ERROR),
                kinds(errorOutput));
        assertSame(boom, errorOutput.get(2).cause());

        List<StreamEvent> cancelOutput = apply(conversion, Arrays.asList(
                StreamEvent.record(row(1)), StreamEvent.cancel(), StreamEvent.record(row(2))));
        assertEquals(Arrays.asList(StreamEvent.Kind.RECORD, StreamEvent.Kind.CANCEL), kinds(cancelOutput));
        assertFalse(kinds(cancelOutput).contains(StreamEvent.Kind.END_OF_STREAM));

        List<StreamEvent> eosOutput = apply(conversion, Arrays.asList(
                StreamEvent.record(row(1)), StreamEvent.endOfStream(), StreamEvent.record(row(2))));
        assertEquals(Arrays.asList(StreamEvent.Kind.RECORD, StreamEvent.Kind.END_OF_STREAM), kinds(eosOutput));
    }

    private static void assertRowFailure(
            SchemaConversion conversion, DataRow row, String code, String forbidden) {
        CollectingRowSink sink = new CollectingRowSink();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> conversion.apply(RowStream.fromIterable(Arrays.asList(row)), sink));
        assertTrue(failure.getMessage().startsWith(code), failure::getMessage);
        if (forbidden != null) {
            assertFalse(failure.getMessage().contains(forbidden));
        }
        assertTrue(sink.rows().isEmpty(), "bad row must not emit a partial output");
        assertFalse(sink.sawEndOfStream(), "failed conversion must not claim success");
    }

    private static List<StreamEvent> apply(SchemaConversion conversion, List<StreamEvent> input) {
        Iterator<StreamEvent> iterator = input.iterator();
        List<StreamEvent> output = new ArrayList<>();
        conversion.apply(() -> iterator.hasNext() ? iterator.next() : null, output::add);
        return output;
    }

    private static List<StreamEvent.Kind> kinds(List<StreamEvent> events) {
        List<StreamEvent.Kind> kinds = new ArrayList<>();
        for (StreamEvent event : events) {
            kinds.add(event.kind());
        }
        return kinds;
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }
}
