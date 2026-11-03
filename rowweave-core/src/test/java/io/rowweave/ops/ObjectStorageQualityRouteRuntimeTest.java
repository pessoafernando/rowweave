package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.PipelineExecutor;
import io.rowweave.sched.stream.RowStream;
import io.rowweave.sched.stream.StreamEvent;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Object routes admit only the decoder's closed carrier language. */
class ObjectStorageQualityRouteRuntimeTest {

    private static final DataSchema INPUT = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema REJECTED = ObjectStorageQualityRoute.rejectedMeta();
    private static final DataSchema CARRIER = carrierMeta();

    @Test
    void projectsCanonicalValidAndAllFourRejectedTuples() {
        List<DataRow> valid = records(run(
                ObjectStorageQualityRoute.routeStage(
                        CARRIER, INPUT, ObjectStorageQualityRoute.VALID_ROUTE),
                validCarrier()));
        assertEquals(1, valid.size());
        assertEquals(7L, valid.get(0).getLong(0));
        assertEquals(new BigDecimal("1.00"), valid.get(0).getBigDecimal(1));

        for (String[] diagnostic : new String[][]{
                {"QUALITY_RAW_UTF8_INVALID", "UTF8"},
                {"QUALITY_RAW_CR_INVALID", "CR"},
                {"QUALITY_RAW_ROW_LIMIT_EXCEEDED", "ROW_BYTES"},
                {"QUALITY_RAW_FORMAT_INVALID", "ROWWEAVE_ROW_TEXT_V1"}}) {
            List<DataRow> rows = records(run(
                    ObjectStorageQualityRoute.routeStage(
                            CARRIER, REJECTED, ObjectStorageQualityRoute.REJECTED_ROUTE),
                    rejectedCarrier(diagnostic[0], diagnostic[1])));
            assertEquals(1, rows.size());
            assertEquals(diagnostic[0], rows.get(0).getString(3));
            assertEquals(diagnostic[1], rows.get(0).getString(6));
        }
    }

    @Test
    void rejectsPayloadRetentionAndEveryNoncanonicalDiagnosticShape() {
        PipelineExecutor.Stage route = ObjectStorageQualityRoute.routeStage(
                CARRIER, REJECTED, ObjectStorageQualityRoute.REJECTED_ROUTE);
        assertRouteFailure(route, mutate(rejectedCarrier(
                "QUALITY_RAW_UTF8_INVALID", "UTF8"), 0, 99L));
        assertRouteFailure(route, mutate(rejectedCarrier(
                "QUALITY_RAW_UTF8_INVALID", "UTF8"), 4, 2));
        assertRouteFailure(route, mutate(rejectedCarrier(
                "QUALITY_RAW_UTF8_INVALID", "UTF8"), 5, "ABCDEF" + repeat('0', 58)));
        assertRouteFailure(route, mutate(rejectedCarrier(
                "QUALITY_RAW_UTF8_INVALID", "UTF8"), 7, "OTHER_RULE"));
        assertRouteFailure(route, mutate(rejectedCarrier(
                "QUALITY_RAW_UTF8_INVALID", "UTF8"), 9, "CR"));
        assertRouteFailure(route, mutate(rejectedCarrier(
                "QUALITY_RAW_UNKNOWN", "UTF8"), 6, "QUALITY_RAW_UNKNOWN"));
    }

    private static DataRow validCarrier() {
        DataRow row = new DataRow(CARRIER.getFieldCount());
        row.set(0, 7L);
        row.set(1, new BigDecimal("1.00"));
        int offset = INPUT.getFieldCount();
        row.set(offset, 0);
        row.set(offset + 1, "");
        row.set(offset + 2, "");
        row.set(offset + 3, 0L);
        row.set(offset + 4, "");
        row.set(offset + 5, "");
        row.set(offset + 6, -1);
        row.set(offset + 7, "");
        return row;
    }

    private static DataRow rejectedCarrier(String code, String detail) {
        DataRow row = new DataRow(CARRIER.getFieldCount());
        int offset = INPUT.getFieldCount();
        row.set(offset, 1);
        row.set(offset + 1, "ledger.object.source");
        row.set(offset + 2, repeat('0', 64));
        row.set(offset + 3, 1L);
        row.set(offset + 4, code);
        row.set(offset + 5, "OBJECT_ROW_DECODE_V1");
        row.set(offset + 6, -1);
        row.set(offset + 7, detail);
        return row;
    }

    private static DataRow mutate(DataRow source, int index, Object value) {
        DataRow copy = new DataRow(source.size());
        copy.setValues(source.values());
        copy.set(index, value);
        return copy;
    }

    private static void assertRouteFailure(PipelineExecutor.Stage route, DataRow carrier) {
        List<StreamEvent> output = new ArrayList<>();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> route.run(stream(carrier), output::add));
        assertTrue(failure.getMessage().startsWith("OBJECT_QUALITY_RUNTIME_INVALID:"),
                failure::getMessage);
        assertTrue(output.isEmpty());
    }

    private static List<StreamEvent> run(PipelineExecutor.Stage route, DataRow carrier) {
        List<StreamEvent> output = new ArrayList<>();
        route.run(stream(carrier), output::add);
        return output;
    }

    private static RowStream stream(DataRow carrier) {
        Iterator<StreamEvent> events = Collections.singletonList(
                StreamEvent.record(carrier)).iterator();
        return () -> events.hasNext() ? events.next() : null;
    }

    private static List<DataRow> records(List<StreamEvent> events) {
        List<DataRow> rows = new ArrayList<>();
        for (StreamEvent event : events) if (event.isRecord()) rows.add(event.row());
        return rows;
    }

    private static DataSchema carrierMeta() {
        Field[] fields = new Field[INPUT.getFieldCount() + 8];
        System.arraycopy(INPUT.getFields(), 0, fields, 0, INPUT.getFieldCount());
        fields[INPUT.getFieldCount()] = new Field("quality_route_tag", DataType.IntegerType);
        System.arraycopy(REJECTED.getFields(), 0, fields, INPUT.getFieldCount() + 1, 7);
        return new DataSchema(fields);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
