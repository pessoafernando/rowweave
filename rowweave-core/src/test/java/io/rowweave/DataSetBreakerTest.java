package io.rowweave;

import io.rowweave.ops.Distinct;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.ReduceGroup;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DataSetBreakerTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("q", DataType.IntegerType));

    private static DataRow row(String k, BigDecimal amt, Integer q) {
        DataRow r = new DataRow(3);
        r.set(0, k);
        r.set(1, amt);
        r.set(2, q);
        return r;
    }

    private static List<DataRow> rows(DataRow... rs) {
        return new ArrayList<>(Arrays.asList(rs));
    }

    private ExecutionEnvironment session;

    /** Builds a session-bound source for the commit path, the same route production takes. */
    private DataSet src(List<DataRow> rs) {
        session = RowWeave.createEnvironment("t");
        return session.fromSource(new Source() {
            @Override public DataSchema getSchema() { return META; }
            @Override public java.util.Iterator<DataRow> iterator() { return rs.iterator(); }
        });
    }

    private List<DataRow> run(DataSet ds) {
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        return out.getRows();
    }

    @Test
    void groupBySumCountEndToEndMatchesV1() {
        List<DataRow> in = rows(
                row("a", new BigDecimal("10"), 1), row("b", new BigDecimal("20"), 1),
                row("a", new BigDecimal("5"), 1), row("a", new BigDecimal("2"), 1));
        // v2 via DataSet
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt").count();
        DataSet ds = src(in).groupBy(agg);
        List<DataRow> v2 = run(ds);
        // expected: a -> sum 17.0000000000 count 3 ; b -> sum 20.0000000000 count 1
        Map<String, DataRow> m = new HashMap<>();
        for (DataRow r : v2) {
            m.put((String) r.get(0), r);
        }
        assertEquals(new BigDecimal("17.0000000000"), m.get("a").get(1));
        assertEquals(3L, m.get("a").get(2));
        assertEquals(new BigDecimal("20.0000000000"), m.get("b").get(1));
        assertEquals(1L, m.get("b").get(2));
        // output meta: [k(String), sum(BigDecimal), count(Long)]
        DataSchema om = ds.getSchema();
        assertEquals(3, om.getFieldCount());
        assertEquals("k", om.getField(0).getName());
        assertEquals(DataType.BigDecimalType, om.getField(1).getDataType());
        assertEquals(DataType.LongType, om.getField(2).getDataType());
    }

    @Test
    void orderByEndToEnd() {
        List<DataRow> in = rows(
                row("c", new BigDecimal("3"), 3), row("a", new BigDecimal("1"), 1),
                row("b", new BigDecimal("2"), 2));
        OrderBy ob = OrderBy.of(META,
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.DEFAULT));
        List<DataRow> out = run(src(in).orderBy(ob, 1, false));
        assertEquals(Arrays.asList("a", "b", "c"),
                Arrays.asList(out.get(0).get(0), out.get(1).get(0), out.get(2).get(0)));
    }

    @Test
    void distinctEndToEnd() {
        List<DataRow> in = rows(
                row("a", new BigDecimal("1"), 1), row("a", new BigDecimal("1"), 1),
                row("b", new BigDecimal("2"), 2));
        Distinct d = Distinct.allColumns(META);
        assertEquals(2, run(src(in).distinct(d)).size());
    }

    @Test
    void topEndToEnd() {
        List<DataRow> in = rows(
                row("a", new BigDecimal("5"), 5), row("b", new BigDecimal("1"), 1),
                row("c", new BigDecimal("9"), 9), row("d", new BigDecimal("3"), 3));
        Top t = Top.of(META, 2, new SortField(2, SortField.Direction.ASC, SortField.NullOrder.DEFAULT));
        List<DataRow> out = run(src(in).top(t, 1));
        assertEquals(2, out.size());
        assertEquals(1, out.get(0).get(2)); // smallest q
        assertEquals(3, out.get(1).get(2));
    }

    @Test
    void reduceGroupEndToEnd() {
        DataSchema outMeta = new DataSchema(new Field("k", DataType.StringType), new Field("n", DataType.IntegerType));
        ReduceGroup rg = ReduceGroup.of(META, "k", new GroupReduceFunction() {
            @Override
            public DataSchema getResultSchema() {
                return outMeta;
            }
            @Override
            public void reduce(Iterable<DataRow> iter, Collector collector) {
                String key = null;
                int n = 0;
                for (DataRow r : iter) {
                    key = (String) r.get(0);
                    n++;
                }
                DataRow o = new DataRow(2);
                o.set(0, key);
                o.set(1, n);
                collector.collect(o);
            }
        });
        List<DataRow> in = rows(
                row("a", null, 1), row("b", null, 1), row("a", null, 1));
        List<DataRow> out = run(src(in).reduceGroup(rg, outMeta));
        Map<String, Integer> m = new HashMap<>();
        for (DataRow r : out) {
            m.put((String) r.get(0), (Integer) r.get(1));
        }
        assertEquals(2, m.get("a"));
        assertEquals(1, m.get("b"));
    }

    @Test
    void mixedPassThroughAndBreakerChain() {
        // source -> filter -> groupBy -> orderBy(by sum)
        List<DataRow> in = rows(
                row("a", new BigDecimal("10"), 1), row("b", new BigDecimal("20"), 1),
                row("a", new BigDecimal("5"), 1), row("c", new BigDecimal("100"), 0),
                row("b", new BigDecimal("3"), 1));
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt");
        DataSet grouped = src(in)
                .filter(r -> (Integer) r.get(2) == 1)   // drop c (q=0)
                .groupBy(agg);                          // [k, sum]
        OrderBy ob = OrderBy.of(grouped.getSchema(),
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.DEFAULT)); // by sum asc
        List<DataRow> out = run(grouped.orderBy(ob, 1, false));
        // 15, b: 23 ; c filtered out -> ordered [a(15), b(23)]
        assertEquals(2, out.size());
        assertEquals("a", out.get(0).get(0));
        assertEquals(new BigDecimal("15.0000000000"), out.get(0).get(1));
        assertEquals("b", out.get(1).get(0));
        assertEquals(new BigDecimal("23.0000000000"), out.get(1).get(1));
    }
}
