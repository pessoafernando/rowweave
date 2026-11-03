package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.MemoryManager;
import io.rowweave.mem.SpillingSegmentGroupAggBuffer;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

    /** The direct and legacy key encodings of GroupBy and Distinct must agree on every sensitive point. */
class ProjectedKeyOperatorTest {

    private static final DataSchema META = new DataSchema(
            new Field("amount", DataType.BigDecimalType),
            new Field("decimalKey", DataType.BigDecimalType),
            new Field("timestampKey", DataType.TimestampType),
            new Field("dynamicKey", DataType.NullType));

    @Test
    void propertySwitchControlsGroupByAndDistinctWithoutChangingResults() {
        String previous = System.getProperty(KeyProjectionMode.PROPERTY);
        try {
            System.setProperty(KeyProjectionMode.PROPERTY, "false");
            assertFalse(KeyProjectionMode.enabled());
            List<String> legacyGroup = groupFingerprints(rows());
            List<String> legacyDistinct = distinctFingerprints(rows());

            System.setProperty(KeyProjectionMode.PROPERTY, "true");
            assertTrue(KeyProjectionMode.enabled());
            List<String> directGroup = groupFingerprints(rows());
            List<String> directDistinct = distinctFingerprints(rows());

            assertEquals(legacyGroup, directGroup);
            assertEquals(legacyDistinct, directDistinct);
            assertEquals(43, directGroup.size(), "1.0 and 1.00, null and the filler keys stay in separate groups");
            assertEquals(43, directDistinct.size(), "after spilling only the first row of each exact key is kept");
        } finally {
            if (previous == null) {
                System.clearProperty(KeyProjectionMode.PROPERTY);
            } else {
                System.setProperty(KeyProjectionMode.PROPERTY, previous);
            }
        }
    }

    private static List<DataRow> rows() {
        List<DataRow> rows = new ArrayList<>();
        Timestamp ts = timestamp(1_650_000_000_321L, 987_654_321);
        rows.add(row("1", "1.0", ts, "\u52A8\u6001\uD83D\uDE00"));
        rows.add(row("2", "1.00", ts, "\u52A8\u6001\uD83D\uDE00"));
        rows.add(row("3", "1.0", ts, "\u52A8\u6001\uD83D\uDE00"));
        rows.add(row("4", null, null, null));
        rows.add(row("5", null, null, null));
        for (int i = 0; i < 40; i++) {
            Timestamp fillerTs = timestamp(1_700_000_000_000L + i, i * 1_000_003);
            rows.add(row(Integer.toString(i), Integer.toString(i), fillerTs, (i & 1) == 0 ? i : "k" + i));
            rows.add(row("999", Integer.toString(i), fillerTs, (i & 1) == 0 ? i : "k" + i));
        }
        return rows;
    }

    private static DataRow row(String amount, String decimalKey, Timestamp timestampKey, Object dynamicKey) {
        DataRow row = new DataRow(4);
        row.set(0, new BigDecimal(amount));
        row.set(1, decimalKey == null ? null : new BigDecimal(decimalKey));
        row.set(2, timestampKey);
        row.set(3, dynamicKey);
        return row;
    }

    private static Timestamp timestamp(long millis, int nanos) {
        Timestamp timestamp = new Timestamp(millis);
        timestamp.setNanos(nanos);
        return timestamp;
    }

    private static List<String> groupFingerprints(List<DataRow> rows) {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(
                META, "decimalKey", "timestampKey", "dynamicKey").sum("amount");
        CollectingRowSink sink = new CollectingRowSink();
        agg.aggregate(RowStream.fromIterable(rows), sink);
        List<String> result = new ArrayList<>();
        for (DataRow row : sink.rows()) {
            BigDecimal decimal = (BigDecimal) row.get(0);
            Timestamp timestamp = (Timestamp) row.get(1);
            result.add(decimalFingerprint(decimal) + "|" + timestampFingerprint(timestamp) + "|"
                    + dynamicFingerprint(row.get(2)) + "=" + row.getBigDecimal(3).toPlainString());
        }
        result.sort(String::compareTo);
        return result;
    }

    private static List<String> distinctFingerprints(List<DataRow> rows) {
        Distinct distinct = Distinct.byColumns(META, "decimalKey", "timestampKey", "dynamicKey")
                .withBufferFactory(factory -> new SpillingSegmentGroupAggBuffer(
                        META, new String[]{"decimalKey", "timestampKey", "dynamicKey"},
                        factory, new MemoryManager(128), new HeapPool()));
        CollectingRowSink sink = new CollectingRowSink();
        distinct.dedup(RowStream.fromIterable(rows), sink);
        List<String> result = new ArrayList<>();
        for (DataRow row : sink.rows()) {
            result.add(decimalFingerprint((BigDecimal) row.get(1)) + "|"
                    + timestampFingerprint((Timestamp) row.get(2)) + "|"
                    + dynamicFingerprint(row.get(3)) + "=" + row.getBigDecimal(0).toPlainString());
        }
        result.sort(String::compareTo);
        return result;
    }

    private static String decimalFingerprint(BigDecimal value) {
        return value == null ? "null" : value.toPlainString() + "@" + value.scale();
    }

    private static String timestampFingerprint(Timestamp value) {
        return value == null ? "null" : value.getTime() + ":" + value.getNanos();
    }

    private static String dynamicFingerprint(Object value) {
        return value == null ? "null" : value.getClass().getSimpleName() + ":" + value;
    }
}
