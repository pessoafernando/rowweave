package io.rowweave;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The convenience overloads are complete --
 * filter(expr,params), distinct(String...), union(DataSet), reduceGroup(fn), and the Grouper alias, combine and getFields forms.
 * Syntactic sugar at graph-build time that delegates to existing describable operators; a single-machine commit returns the right rows.
 */
class DataSetApiCompatTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("q", DataType.IntegerType));

    private static DataRow row(String k, String amt, Integer q) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, amt == null ? null : new BigDecimal(amt));
        r.set(2, q);
        return r;
    }

    private BatchSession session;

    private DataSet src(List<DataRow> rs) {
        session = RowWeave.createBatchSession("apicompat");
        return session.fromSource(new CollectionSource(META, rs));
    }

    private List<DataRow> run(DataSet ds) {
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        return out.getRows();
    }

    private static List<DataRow> data() {
        return new ArrayList<>(Arrays.asList(
                row("a", "10", 1), row("b", "20", 2), row("a", "5", 3), row("c", "7", 1), row("b", "20", 2)));
    }

    // filter(expr, params) with named parameters
    @Test void filterWithNamedParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("threshold", new BigDecimal("10"));
        List<DataRow> out = run(src(data()).filter("amt >= threshold", params));
        assertEquals(3, out.size()); // 10,20,20 >= 10
    }

    // the distinct(String...) overload
    @Test void distinctByFieldNames() {
        List<DataRow> out = run(src(data()).distinct("k", "amt", "q"));
        assertEquals(4, out.size());  // (b,20,2) repeated once
    }

    // the union(DataSet) overload
    @Test void unionConvenience() {
        BatchSession s = RowWeave.createBatchSession("u");
        DataSet left = s.fromSource(new CollectionSource(META, data()));
        DataSet right = s.fromSource(new CollectionSource(META, data()));
        DataSet u = left.union(right);
        WorkerCollectingOutput out = new WorkerCollectingOutput(u.getSchema());
        u.sinkTo(out);
        s.execute();
        assertEquals(10, out.getRows().size()); // 5 + 5
    }

    // the reduceGroup(fn) overload without outMeta
    @Test void reduceGroupConvenience() {
        DataSchema outMeta = new DataSchema(new Field("k", DataType.StringType), new Field("cnt", DataType.IntegerType));
        GroupReduceFunction fn = new GroupReduceFunction() {
            @Override public DataSchema getResultSchema() { return outMeta; }
            @Override public void reduce(Iterable<DataRow> rows, Collector c) {
                int n = 0; Object k = null;
                for (DataRow r : rows) { n++; k = r.get(0); }
                DataRow o = new DataRow(2); o.set(0, k); o.set(1, n); c.collect(o);
            }
        };
        List<DataRow> out = run(src(data()).reduceGroup(fn));
        assertEquals(1, out.size());  // the whole table as one group, with no key
    }

    // the Grouper alias form
    @Test void grouperSumWithAlias() {
        DataSet ds = src(data()).groupBy("k").sum("amt", "total");
        assertEquals("total", ds.getSchema().getField(1).getName());
        assertEquals(3, run(ds).size());
    }

    @Test void grouperCountMaxMinAlias() {
        assertEquals("c", src(data()).groupBy("k").count("amt", "c").getSchema().getField(1).getName());
        assertEquals("mx", src(data()).groupBy("k").max("amt", "mx").getSchema().getField(1).getName());
        assertEquals("mn", src(data()).groupBy("k").min("amt", "mn").getSchema().getField(1).getName());
    }

    // #5 Grouper.getFields()
    @Test void grouperGetFields() {
        assertArrayEquals(new String[]{"k", "q"}, src(data()).groupBy("k", "q").getFields());
    }

    // Grouper.combineGroup, delegating to DataSet.combineGroup
    @Test void grouperCombineGroup() {
        GroupCombineFunction fn = (rows, c) -> {
            BigDecimal s = BigDecimal.ZERO;
            for (DataRow r : rows) { BigDecimal v = (BigDecimal) r.get(1); if (v != null) s = s.add(v); }
            DataRow o = new DataRow(3); o.set(0, "x"); o.set(1, s); o.set(2, 0); c.collect(o);
        };
        List<DataRow> out = run(src(data()).groupBy("k").combineGroup(fn));
        assertTrue(out.size() > 0);
    }
}
