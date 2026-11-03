package io.rowweave.ops;

import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.SourceRecordPosition;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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

/** bounded classifier/carrier/routes runtime. */
class TypedRowClassificationRuntimeTest {

    private static final String SOURCE_UID = "ledger.source";
    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType));
    private static final BatchSourceSplitDescriptor SPLIT = descriptor(META);

    @Test
    void classifiesEveryPositionedRecordIntoExactlyOneFinalRoute() {
        TypedRowClassification runtime = runtime(META, validation());
        List<StreamEvent> carriers = run(runtime.classifierStage(), Arrays.asList(
                positioned(row("A", 1, BigDecimal.ZERO), 0L),
                positioned(row(null, 1, BigDecimal.ZERO), 1L),
                positioned(row("B", "secret-period", BigDecimal.ZERO), 2L),
                positioned(row("C", 13, BigDecimal.ZERO), 3L),
                positioned(row("short"), 4L),
                StreamEvent.endOfStream()));

        assertEquals(5, records(carriers).size(), "classifier must emit one carrier per input");
        List<DataRow> valid = records(run(runtime.validRouteStage(), carriers));
        List<DataRow> rejected = records(run(runtime.rejectedRouteStage(), carriers));
        assertEquals(1, valid.size());
        assertEquals(4, rejected.size());
        assertEquals(5, valid.size() + rejected.size());
        assertEquals("A", valid.get(0).get(0));

        assertRejected(rejected.get(0), 1L, "QUALITY_RULE_VIOLATION",
                "adaptor.required", 0, "NOT_NULL");
        assertRejected(rejected.get(1), 2L, "QUALITY_VALUE_TYPE_MISMATCH",
                "", 1, "IntegerType");
        assertRejected(rejected.get(2), 3L, "QUALITY_RULE_VIOLATION",
                "period.range", 1, "INTEGER_RANGE_CLOSED");
        assertRejected(rejected.get(3), 4L, "QUALITY_ROW_WIDTH_MISMATCH",
                "", -1, "ROW_WIDTH");
    }

    @Test
    void validatesUnruledFieldsAndNeverSerializesRejectedInputValues() {
        DataSchema meta = new DataSchema(
                new Field("id", DataType.IntegerType),
                new Field("unruled_secret", DataType.StringType));
        TypedRowClassification runtime = runtime(meta,
                RowValidation.builder(meta).notNull("id.required", 0).build());
        DataRow invalid = row(7, new Object());

        List<DataRow> carriers = records(run(runtime.classifierStage(), Arrays.asList(
                positioned(meta, invalid, 0L), StreamEvent.endOfStream())));
        assertEquals(1, carriers.size());
        DataRow carrier = carriers.get(0);
        assertNull(carrier.get(0));
        assertNull(carrier.get(1));
        assertFalse(Arrays.asList(carrier.values()).contains(invalid.get(1)));

        RowSerializer serializer = new RowSerializer(runtime.carrierSchema());
        DataRow decoded = serializer.deserialize(serializer.serialize(carrier));
        List<DataRow> rejected = records(run(runtime.rejectedRouteStage(), Arrays.asList(
                StreamEvent.record(decoded), StreamEvent.endOfStream())));
        assertEquals(1, rejected.size());
        assertRejected(rejected.get(0), digest(meta), 0L, "QUALITY_VALUE_TYPE_MISMATCH",
                "", 1, "StringType");
    }

    @Test
    void validCarrierFreezesMutableTypedValuesBeforeUpstreamReuse() {
        DataSchema meta = new DataSchema(
                new Field("date", DataType.DateType),
                new Field("timestamp", DataType.TimestampType));
        RowValidation validation = RowValidation.builder(meta)
                .notNull("date.required", 0)
                .notNull("timestamp.required", 1)
                .build();
        TypedRowClassification runtime = runtime(meta, validation);
        Date date = new Date(1_700_000_000_000L);
        Timestamp timestamp = new Timestamp(1_700_000_000_123L);
        timestamp.setNanos(987_654_321);
        long frozenTimestampMillis = timestamp.getTime();
        DataRow input = row(date, timestamp);

        DataRow carrier = records(run(runtime.classifierStage(), Arrays.asList(
                positioned(meta, input, 0L), StreamEvent.endOfStream()))).get(0);
        date.setTime(1L);
        timestamp.setTime(2L);
        timestamp.setNanos(3);

        DataRow valid = records(run(runtime.validRouteStage(), Arrays.asList(
                StreamEvent.record(carrier), StreamEvent.endOfStream()))).get(0);
        assertNotSame(input, valid);
        assertNotSame(date, valid.get(0));
        assertNotSame(timestamp, valid.get(1));
        assertEquals(1_700_000_000_000L, ((Date) valid.get(0)).getTime());
        assertEquals(frozenTimestampMillis, ((Timestamp) valid.get(1)).getTime());
        assertEquals(987_654_321, ((Timestamp) valid.get(1)).getNanos());
    }

    @Test
    void routesRejectMalformedOrNoncanonicalCarriersWithoutEmitting() {
        TypedRowClassification runtime = runtime(META, validation());
        DataRow valid = validCarrier(row("A", 1, BigDecimal.ZERO));
        DataRow rejected = rejectedCarrier(0L, "QUALITY_RULE_VIOLATION",
                "adaptor.required", 0, "NOT_NULL");

        assertRouteFailure(runtime.validRouteStage(), row("short"));
        assertRouteFailure(runtime.validRouteStage(), mutate(valid, META.getFieldCount(), "VALID"));
        assertRouteFailure(runtime.validRouteStage(), mutate(valid, META.getFieldCount(), 2));
        assertRouteFailure(runtime.validRouteStage(), mutate(valid, META.getFieldCount() + 4, "not-empty"));
        assertRouteFailure(runtime.validRouteStage(), mutate(valid, 1, "wrong-type"));
        assertRouteFailure(runtime.rejectedRouteStage(), mutate(rejected, 0, "leaked"));
        assertRouteFailure(runtime.rejectedRouteStage(), mutate(rejected,
                META.getFieldCount() + 2, splitDigest().toUpperCase()));
        assertRouteFailure(runtime.rejectedRouteStage(), mutate(rejected,
                META.getFieldCount() + 6, -1));
    }

    @Test
    void missingWrongAndNoncontiguousPositionsFailAsSystemErrorsNotRejects() {
        TypedRowClassification runtime = runtime(META, validation());
        assertClassifierFailure(runtime, StreamEvent.record(row("A", 1, BigDecimal.ZERO)));
        assertClassifierFailure(runtime, StreamEvent.record(row("A", 1, BigDecimal.ZERO),
                SourceRecordPosition.of("other.source", SPLIT, 0L)));
        assertClassifierFailure(runtime, positioned(row("A", 1, BigDecimal.ZERO), 1L));

        List<StreamEvent> emitted = new ArrayList<>();
        IllegalStateException duplicate = assertThrows(IllegalStateException.class,
                () -> runtime.classifierStage().run(stream(Arrays.asList(
                        positioned(row("A", 1, BigDecimal.ZERO), 0L),
                        positioned(row("B", 2, BigDecimal.ZERO), 0L))), emitted::add));
        assertTrue(duplicate.getMessage().startsWith("QUALITY_SOURCE_POSITION_INVALID:"),
                duplicate::getMessage);
        assertEquals(1, records(emitted).size());
    }

    @Test
    void classifierAndRoutesForwardControlEventsAndStopAtTerminal() {
        TypedRowClassification runtime = runtime(META, validation());
        StreamEvent barrier = StreamEvent.barrier(9L);
        RuntimeException cause = new RuntimeException("upstream-secret");
        StreamEvent error = StreamEvent.error(cause);
        List<StreamEvent> classifier = run(runtime.classifierStage(), Arrays.asList(
                barrier, error, positioned(row("unread", 1, BigDecimal.ZERO), 0L)));
        assertEquals(Arrays.asList(StreamEvent.Kind.BARRIER, StreamEvent.Kind.ERROR),
                kinds(classifier));
        assertSame(barrier, classifier.get(0));
        assertSame(cause, classifier.get(1).cause());

        for (PipelineExecutor.Stage route : Arrays.asList(
                runtime.validRouteStage(), runtime.rejectedRouteStage())) {
            StreamEvent cancel = StreamEvent.cancel();
            List<StreamEvent> output = run(route, Arrays.asList(
                    barrier, cancel, StreamEvent.record(validCarrier(
                            row("unread", 1, BigDecimal.ZERO)))));
            assertEquals(Arrays.asList(StreamEvent.Kind.BARRIER, StreamEvent.Kind.CANCEL),
                    kinds(output));
            assertSame(cancel, output.get(1));
        }
    }

    @Test
    void classifierAndRoutesEmitBeforePullingNextRecord() {
        TypedRowClassification runtime = runtime(META, validation());
        assertNoPrefetch(runtime.classifierStage(), ordinal ->
                positioned(row("A" + ordinal, 1, BigDecimal.ZERO), ordinal));
        assertNoPrefetch(runtime.validRouteStage(), ordinal ->
                StreamEvent.record(validCarrier(row("A" + ordinal, 1, BigDecimal.ZERO))));
        assertNoPrefetch(runtime.rejectedRouteStage(), ordinal ->
                StreamEvent.record(rejectedCarrier(ordinal, "QUALITY_RULE_VIOLATION",
                        "adaptor.required", 0, "NOT_NULL")));
    }

    private static void assertNoPrefetch(
            PipelineExecutor.Stage stage, EventFactory events) {
        int[] pulled = {0};
        int[] emitted = {0};
        RowStream input = () -> {
            if (pulled[0] == 2) return null;
            assertEquals(pulled[0], emitted[0], "stage pulled ahead of its emit");
            return events.event(pulled[0]++);
        };
        stage.run(input, event -> {
            if (event.isRecord()) emitted[0]++;
        });
        assertEquals(2, emitted[0]);
    }

    private static void assertClassifierFailure(
            TypedRowClassification runtime, StreamEvent input) {
        List<StreamEvent> output = new ArrayList<>();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> runtime.classifierStage().run(stream(Collections.singletonList(input)),
                        output::add));
        assertTrue(failure.getMessage().startsWith("QUALITY_SOURCE_POSITION_INVALID:"),
                failure::getMessage);
        assertTrue(output.isEmpty());
    }

    private static void assertRouteFailure(PipelineExecutor.Stage route, DataRow carrier) {
        List<StreamEvent> output = new ArrayList<>();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> route.run(stream(Collections.singletonList(StreamEvent.record(carrier))),
                        output::add));
        assertTrue(failure.getMessage().startsWith("QUALITY_CLASSIFICATION_RUNTIME_INVALID:"),
                failure::getMessage);
        assertTrue(output.isEmpty());
    }

    private static TypedRowClassification runtime(DataSchema meta, RowValidation validation) {
        return TypedRowClassification.of(meta, validation, SOURCE_UID,
                Collections.singletonList(descriptor(meta)));
    }

    private static RowValidation validation() {
        return RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .decimalRange("amount.range", 2,
                        new BigDecimal("-10"), new BigDecimal("10"))
                .build();
    }

    private static List<StreamEvent> run(
            PipelineExecutor.Stage stage, List<StreamEvent> input) {
        List<StreamEvent> output = new ArrayList<>();
        stage.run(stream(input), output::add);
        return output;
    }

    private static RowStream stream(List<StreamEvent> input) {
        Iterator<StreamEvent> iterator = input.iterator();
        return () -> iterator.hasNext() ? iterator.next() : null;
    }

    private static List<DataRow> records(List<StreamEvent> events) {
        List<DataRow> rows = new ArrayList<>();
        for (StreamEvent event : events) if (event.isRecord()) rows.add(event.row());
        return rows;
    }

    private static List<StreamEvent.Kind> kinds(List<StreamEvent> events) {
        List<StreamEvent.Kind> kinds = new ArrayList<>();
        for (StreamEvent event : events) kinds.add(event.kind());
        return kinds;
    }

    private static void assertRejected(
            DataRow row, long ordinal, String code, String ruleId, int fieldIndex, String detail) {
        assertRejected(row, splitDigest(), ordinal, code, ruleId, fieldIndex, detail);
    }

    private static void assertRejected(
            DataRow row, String digest, long ordinal, String code,
            String ruleId, int fieldIndex, String detail) {
        assertEquals(SOURCE_UID, row.get(0));
        assertEquals(digest, row.get(1));
        assertEquals(ordinal, row.get(2));
        assertEquals(code, row.get(3));
        assertEquals(ruleId, row.get(4));
        assertEquals(fieldIndex, row.get(5));
        assertEquals(detail, row.get(6));
    }

    private static DataRow validCarrier(DataRow original) {
        DataRow carrier = new DataRow(META.getFieldCount() + 8);
        for (int i = 0; i < META.getFieldCount(); i++) carrier.set(i, original.get(i));
        int offset = META.getFieldCount();
        carrier.set(offset, 0);
        carrier.set(offset + 1, "");
        carrier.set(offset + 2, "");
        carrier.set(offset + 3, 0L);
        carrier.set(offset + 4, "");
        carrier.set(offset + 5, "");
        carrier.set(offset + 6, -1);
        carrier.set(offset + 7, "");
        return carrier;
    }

    private static DataRow rejectedCarrier(
            long ordinal, String code, String ruleId, int fieldIndex, String detail) {
        DataRow carrier = new DataRow(META.getFieldCount() + 8);
        int offset = META.getFieldCount();
        carrier.set(offset, 1);
        carrier.set(offset + 1, SOURCE_UID);
        carrier.set(offset + 2, splitDigest());
        carrier.set(offset + 3, ordinal);
        carrier.set(offset + 4, code);
        carrier.set(offset + 5, ruleId);
        carrier.set(offset + 6, fieldIndex);
        carrier.set(offset + 7, detail);
        return carrier;
    }

    private static DataRow mutate(DataRow source, int index, Object value) {
        DataRow changed = source.copy();
        changed.set(index, value);
        return changed;
    }

    private static StreamEvent positioned(DataRow row, long ordinal) {
        return positioned(META, row, ordinal);
    }

    private static StreamEvent positioned(DataSchema meta, DataRow row, long ordinal) {
        BatchSourceSplitDescriptor descriptor = descriptor(meta);
        return StreamEvent.record(row,
                SourceRecordPosition.of(SOURCE_UID, descriptor, ordinal));
    }

    private static String splitDigest() {
        return digest(META);
    }

    private static String digest(DataSchema meta) {
        return SourceRecordPosition.of(SOURCE_UID, descriptor(meta), 0L)
                .splitIdentitySha256();
    }

    private static BatchSourceSplitDescriptor descriptor(DataSchema meta) {
        return new BatchSourceSplitDescriptor(
                1, "probe", 1, "domain/private-ledger-object",
                SchemaFingerprint.of(meta), "private-snapshot-token",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    @FunctionalInterface
    private interface EventFactory {
        StreamEvent event(int ordinal);
    }
}
