package io.rowweave;

import io.rowweave.ops.Filter;
import io.rowweave.ops.Select;
import io.rowweave.ops.join.HashJoin;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Regression contracts distilled from the third-party ScenarioLab review. */
class ScenarioLabFindingsContractTest {

    private static final DataSchema KEY_DECIMAL = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("v", DataType.BigDecimalType));

    @BeforeEach
    @AfterEach
    void resetProcessRuntime() {
        InProcessBatchRuntime.resetProcessWideForTesting();
    }

    @Test
    void unionRejectsWidthOrTypeDifferenceAtGraphConstruction() {
        BatchSession session = RowWeave.createBatchSession("union-schema-contract");
        DataSet left = session.fromSource(new CollectionSource(
                new DataSchema(new Field("id", DataType.LongType)),
                Arrays.asList(row(1L))));
        DataSet differentType = session.fromSource(new CollectionSource(
                new DataSchema(new Field("id", DataType.StringType)),
                Arrays.asList(row("1"))));
        DataSet differentName = session.fromSource(new CollectionSource(
                new DataSchema(new Field("other", DataType.LongType)),
                Arrays.asList(row(1L))));
        DataSet differentWidth = session.fromSource(new CollectionSource(
                new DataSchema(new Field("id", DataType.LongType),
                        new Field("extra", DataType.LongType)),
                Arrays.asList(row(1L, 2L))));

        IllegalArgumentException typeFailure = assertThrows(
                IllegalArgumentException.class, () -> left.union(differentType));
        assertTrue(typeFailure.getMessage().contains("union schema mismatch"),
                typeFailure::getMessage);
        assertThrows(IllegalArgumentException.class, () -> left.union(differentWidth));
        left.union(differentName); // v1 compatibility: union is positional; names may differ.
    }

    @Test
    void binaryKeyedOperatorsRejectDifferentKeyTypesAtGraphConstruction() {
        DataSchema left = new DataSchema(new Field("k", DataType.LongType));
        DataSchema right = new DataSchema(new Field("k", DataType.StringType));

        assertKeyTypeFailure(() -> SortMergeJoin.of(
                left, right, keys("k"), keys("k"), JoinType.INNER));
        assertKeyTypeFailure(() -> HashJoin.of(
                left, right, keys("k"), keys("k"), JoinType.INNER));
    }

    @Test
    void expressionCompilationRejectsUnknownFieldsEvenWithoutRows() {
        IllegalArgumentException filterFailure = assertThrows(
                IllegalArgumentException.class,
                () -> Filter.fromExpression(KEY_DECIMAL, "missing > 0"));
        assertTrue(filterFailure.getMessage().contains("unknown field: missing"),
                filterFailure::getMessage);

        IllegalArgumentException selectFailure = assertThrows(
                IllegalArgumentException.class,
                () -> Select.of(KEY_DECIMAL, "missing + 1 as value"));
        assertTrue(selectFailure.getMessage().contains("unknown field: missing"),
                selectFailure::getMessage);

        BatchSession session = RowWeave.createBatchSession("unknown-whole-agg-field");
        DataSet empty = session.fromSource(new CollectionSource(
                KEY_DECIMAL, java.util.Collections.emptyList()));
        IllegalArgumentException maxFailure = assertThrows(
                IllegalArgumentException.class, () -> empty.max("missing"));
        assertTrue(maxFailure.getMessage().contains("unknown field for max: missing"),
                maxFailure::getMessage);
    }

    @Test
    void namedParametersRemainValidButDoNotHideUnknownFields() {
        java.util.Map<String, Object> params = new java.util.HashMap<>();
        params.put("threshold", BigDecimal.ONE);
        Filter.fromExpression(KEY_DECIMAL, "v > threshold", params);

        assertThrows(IllegalArgumentException.class,
                () -> Filter.fromExpression(KEY_DECIMAL, "v > typo", params));
    }

    @Test
    void selectInfersSerializableTypesForComputedExpressions(@TempDir Path tmp) {
        DataSchema sourceMeta = new DataSchema(
                new Field("id", DataType.LongType),
                new Field("name", DataType.StringType));
        BatchSession session = RowWeave.createBatchSession("select-schema-contract");
        DataSet selected = session.fromSource(new CollectionSource(
                        sourceMeta, Arrays.asList(row(7L, "Ada"))))
                .select("id + 1 as next_id", "upper(name) as upper_name",
                        "len(name) as name_length", "contains(name, 'd') as has_d");

        assertTypes(selected.getSchema(), DataType.BigDecimalType, DataType.StringType,
                DataType.BigDecimalType, DataType.BooleanType);

        Path output = tmp.resolve("computed.tsv");
        selected.sinkTo(new FileSink(selected.getSchema(), output.toString()));
        session.execute();

        List<DataRow> rows = FileSink.read(selected.getSchema(), output.toString());
        assertEquals(1, rows.size());
        assertEquals(new BigDecimal("8"), rows.get(0).get(0));
        assertEquals("ADA", rows.get(0).get(1));
        assertEquals(new BigDecimal("3"), rows.get(0).get(2));
        assertEquals(Boolean.TRUE, rows.get(0).get(3));
    }

    @Test
    void temporalExpressionsMatchDeclaredTypesAndFileCodec(@TempDir Path tmp) {
        DataSchema sourceMeta = new DataSchema(
                new Field("part", DataType.StringType),
                new Field("business_date", DataType.DateType),
                new Field("event_time", DataType.TimestampType));
        java.sql.Date businessDate = java.sql.Date.valueOf("2026-09-22");
        java.sql.Timestamp eventTime = java.sql.Timestamp.valueOf("2026-09-22 10:11:12.123456789");
        BatchSession session = RowWeave.createBatchSession("select-temporal-schema-contract");
        DataSet selected = session.fromSource(new CollectionSource(
                        sourceMeta, Arrays.asList(row("hour", businessDate, eventTime))))
                .select("dateadd(part, 1, business_date) as dynamic_shift",
                        "dateadd('day', 1, business_date) as date_shift",
                        "dateadd('day', 1, event_time) as timestamp_shift",
                        "year(event_time) as event_year",
                        "to_char(event_time) as rendered_time",
                        "cast(event_time as timestamp) as recast_time",
                        "cast(event_time as date) as recast_date",
                        "cast(business_date as timestamp) as date_at_midnight",
                        "hour(event_time) as event_hour",
                        "minute(event_time) as event_minute",
                        "second(event_time) as event_second");

        assertTypes(selected.getSchema(), DataType.TimestampType, DataType.DateType,
                DataType.TimestampType, DataType.BigDecimalType, DataType.StringType,
                DataType.TimestampType, DataType.DateType, DataType.TimestampType,
                DataType.BigDecimalType, DataType.BigDecimalType, DataType.BigDecimalType);

        Path output = tmp.resolve("computed-temporal.tsv");
        selected.sinkTo(new FileSink(selected.getSchema(), output.toString()));
        session.execute();

        List<DataRow> rows = FileSink.read(selected.getSchema(), output.toString());
        assertEquals(1, rows.size());
        assertEquals(java.sql.Timestamp.valueOf("2026-09-22 01:00:00"), rows.get(0).get(0));
        assertEquals(java.sql.Date.valueOf("2026-09-23"), rows.get(0).get(1));
        assertEquals(java.sql.Timestamp.valueOf("2026-09-23 10:11:12.123456789"),
                rows.get(0).get(2));
        assertEquals(new BigDecimal("2026"), rows.get(0).get(3));
        assertEquals("2026-09-22 10:11:12", rows.get(0).get(4));
        assertEquals(eventTime, rows.get(0).get(5));
        assertEquals(businessDate, rows.get(0).get(6));
        assertEquals(java.sql.Timestamp.valueOf("2026-09-22 00:00:00"), rows.get(0).get(7));
        assertEquals(new BigDecimal("10"), rows.get(0).get(8));
        assertEquals(new BigDecimal("11"), rows.get(0).get(9));
        assertEquals(new BigDecimal("12"), rows.get(0).get(10));
    }

    @Test
    void distinctByKeyKeepsTheDocumentedCanonicalRepresentative(@TempDir Path tmp) {
        List<DataRow> input = Arrays.asList(
                row("g1", decimal("10.5")), row("g1", decimal("20.25")),
                row("g1", decimal("3.125")), row("g2", decimal("100")),
                row("g2", decimal("0.001")), row("g2", decimal("7")),
                row("g3", decimal("42")), row("g3", decimal("-8.4")),
                row("g3", decimal("0")), row("g4", decimal("999.999999")),
                row("g4", decimal("0.000001")), row("g4", null));
        BatchSession session = RowWeave.createBatchSession("distinct-first-contract");
        Path output = tmp.resolve("distinct.tsv");
        session.fromSource(new CollectionSource(KEY_DECIMAL, input))
                .distinct("k")
                .sinkTo(new FileSink(KEY_DECIMAL, output.toString()));

        session.execute();

        Map<String, BigDecimal> actual = new LinkedHashMap<>();
        for (DataRow row : FileSink.read(KEY_DECIMAL, output.toString())) {
            actual.put((String) row.get(0), (BigDecimal) row.get(1));
        }
        Map<String, BigDecimal> expected = new LinkedHashMap<>();
        expected.put("g1", decimal("10.5"));
        expected.put("g2", decimal("7"));
        expected.put("g3", decimal("0"));
        expected.put("g4", decimal("0.000001"));
        assertEquals(expected, actual);
    }

    private static void assertKeyTypeFailure(org.junit.jupiter.api.function.Executable action) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, action);
        assertTrue(failure.getMessage().contains("key type mismatch"), failure::getMessage);
    }

    private static void assertTypes(DataSchema meta, DataType... expected) {
        assertEquals(expected.length, meta.getFieldCount());
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], meta.getField(i).getDataType(), "field " + i);
        }
    }

    private static String[] keys(String... names) {
        return names;
    }

    private static BigDecimal decimal(String value) {
        return new BigDecimal(value);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

}
