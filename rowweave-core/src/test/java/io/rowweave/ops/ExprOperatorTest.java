package io.rowweave.ops;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.sched.stream.CollectingRowSink;
import io.rowweave.sched.stream.RowStream;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ExprOperatorTest {

    private static final DataSchema META = new DataSchema(
            new Field("a", DataType.IntegerType),
            new Field("b", DataType.IntegerType),
            new Field("amt", DataType.BigDecimalType));

    private static DataRow row(Integer a, Integer b, BigDecimal amt) {
        DataRow r = new DataRow(3);
        r.set(0, a); r.set(1, b); r.set(2, amt);
        return r;
    }

    private static List<DataRow> filterRows(Filter f, List<DataRow> in) {
        CollectingRowSink sink = new CollectingRowSink();
        f.apply(RowStream.fromIterable(in), sink);
        return sink.rows();
    }

    @Test
    void filterFromExpressionBasic() {
        Filter f = Filter.fromExpression(META, "a > b");
        List<DataRow> out = filterRows(f, Arrays.asList(
                row(10, 3, null), row(1, 5, null), row(7, 7, null)));
        assertEquals(1, out.size()); // only 10>3
        assertEquals(10, out.get(0).get(0));
    }

    @Test
    void filterExpressionNullUnknownDoesNotPass() {
                // amt > 5 where amt is null -> unknown -> row NOT passed (filter three-valued logic)
        Filter f = Filter.fromExpression(META, "amt > 5");
        List<DataRow> out = filterRows(f, Arrays.asList(
                row(1, 1, new BigDecimal("10")),  // 10>5 pass
                row(2, 2, null),                  // null>5 unknown -> drop
                row(3, 3, new BigDecimal("2"))));  // 2>5 false -> drop
        assertEquals(1, out.size());
        assertEquals(new BigDecimal("10"), out.get(0).get(2));
    }

    @Test
    void filterExpressionLogicalAndWithNull() {
        // (a > 0 and amt > 5): row with null amt -> unknown -> drop
        Filter f = Filter.fromExpression(META, "a > 0 and amt > 5");
        List<DataRow> out = filterRows(f, Arrays.asList(
                row(1, 0, new BigDecimal("10")),  // true and true -> pass
                row(1, 0, null)));                 // true and unknown -> drop
        assertEquals(1, out.size());
    }

    @Test
    void filterEqualsNormalization() {
        // == normalized to =
        Filter f = Filter.fromExpression(META, "a == b");
        List<DataRow> out = filterRows(f, Arrays.asList(row(5, 5, null), row(1, 2, null)));
        assertEquals(1, out.size());
        assertEquals(5, out.get(0).get(0));
    }

    @Test
    void filterFromExpressionWithNamedParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("threshold", new BigDecimal("5"));
        Filter f = Filter.fromExpression(META, "amt > threshold", params);
        List<DataRow> out = filterRows(f, Arrays.asList(
                row(1, 1, new BigDecimal("10")), row(2, 2, new BigDecimal("3"))));
        assertEquals(1, out.size());
        assertEquals(new BigDecimal("10"), out.get(0).get(2));
    }

    @Test
    void selectExpressionComputedColumn() {
        Select s = Select.of(META, "a + b", "a * b");
        DataRow out = s.project(row(10, 3, null));
        assertEquals(new BigDecimal("13"), out.get(0));
        assertEquals(new BigDecimal("30"), out.get(1));
        assertEquals(2, s.getOutputSchema().getFieldCount());
    }

    @Test
    void selectExpressionAliasAndFieldName() {
        Select s = Select.of(META, "a + b as total", "amt");
        DataSchema m = s.getOutputSchema();
        assertEquals("total", m.getField(0).getName());        // alias
        assertEquals(DataType.BigDecimalType, m.getField(0).getDataType());
        assertEquals("amt", m.getField(1).getName());          // bare field ref name
        assertEquals(DataType.BigDecimalType, m.getField(1).getDataType()); // field type preserved
    }

    @Test
    void selectExpressionNullArithmeticYieldsNull() {
        Select s = Select.of(META, "amt + 1");
        DataRow out = s.project(row(1, 1, null)); // null + 1 = null
        assertNull(out.get(0));
    }

    @Test
    void dataSetFilterAndSelectExpressionEndToEnd() {
        final List<DataRow> srcRows = Arrays.asList(
                row(10, 3, new BigDecimal("100")),
                row(1, 5, new BigDecimal("50")),
                row(8, 2, new BigDecimal("80")));
        io.rowweave.BatchSession session = io.rowweave.RowWeave.createBatchSession("expr");
        io.rowweave.DataSet ds = session.fromSource(new io.rowweave.Source() {
                    @Override public DataSchema getSchema() { return META; }
                    @Override public java.util.Iterator<DataRow> iterator() { return srcRows.iterator(); }
                })
                .filter("a > b")             // 10>3, 8>2 pass (1>5 drop)
                .select("a + b as sum");     // computed
        io.rowweave.WorkerCollectingOutput out = new io.rowweave.WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        List<Object> sums = new ArrayList<>();
        for (DataRow r : out.getRows()) {
            sums.add(r.get(0));
        }
        assertEquals(Arrays.asList(new BigDecimal("13"), new BigDecimal("10")), sums); // 10+3, 8+2
        assertEquals("sum", ds.getSchema().getField(0).getName());
    }
}
