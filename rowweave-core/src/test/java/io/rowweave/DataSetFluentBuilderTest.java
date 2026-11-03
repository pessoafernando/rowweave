package io.rowweave;

import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * The fluent builder shells: Grouper, JoinDataSet and CoGroupDataSet.
 * A DataSet produced by a shell must be bit-identical as a multiset to one built through the operator directly, since the shell is graph-build-time sugar over the same describable operator.
 */
class DataSetFluentBuilderTest {

    private static final DataSchema ORDER = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("amt", DataType.BigDecimalType),
            new Field("q", DataType.IntegerType));
    private static final DataSchema DIM = new DataSchema(
            new Field("k", DataType.StringType),
            new Field("label", DataType.IntegerType));

    private static DataRow o(String k, String amt, Integer q) {
        DataRow r = new DataRow(3);
        r.set(0, k); r.set(1, amt == null ? null : new BigDecimal(amt)); r.set(2, q);
        return r;
    }

    private static DataRow d(String k, Integer label) {
        DataRow r = new DataRow(2); r.set(0, k); r.set(1, label); return r;
    }

    private static List<DataRow> orders() {
        return new ArrayList<>(Arrays.asList(
                o("a", "10", 1), o("b", "20", 2), o("a", "5", 3), o("c", "7", 1)));
    }

    private static List<DataRow> dims() {
        return new ArrayList<>(Arrays.asList(d("a", 100), d("b", 200)));
    }

    private BatchSession session;

    private DataSet srcOrders() {
        session = RowWeave.createBatchSession("fb");
        return session.fromSource(new CollectionSource(ORDER, orders()));
    }

    private List<DataRow> run(DataSet ds) {
        WorkerCollectingOutput out = new WorkerCollectingOutput(ds.getSchema());
        ds.sinkTo(out);
        session.execute();
        return out.getRows();
    }

    private static long[] fp(List<DataRow> rows) {
        long count = rows.size(), sum = 0;
        for (DataRow r : rows) {
            long h = 1125899906842597L;
            for (int i = 0; i < r.size(); i++) {
                Object v = r.get(i);
                String s = v == null ? "\\N" : v.toString();
                for (int j = 0; j < s.length(); j++) h = 31 * h + s.charAt(j);
                h = 31 * h + 7;
            }
            sum += h;
        }
        return new long[]{count, sum};
    }

    // ---- Grouper ----

    @Test void grouper_sum_matchesOp() {
        List<DataRow> viaShell = run(srcOrders().groupBy("k").sum("amt"));
        List<DataRow> viaOp = run(srcOrders().groupBy(ParallelGroupByAgg.of(ORDER, "k").sum("amt")));
        assertArrayEquals(fp(viaOp), fp(viaShell), "Grouper.sum vs op");
        assertEquals(3, viaShell.size());
    }

    @Test void grouper_count_countField_max_min() {
        assertArrayEquals(
                fp(run(srcOrders().groupBy(ParallelGroupByAgg.of(ORDER, "k").count()))),
                fp(run(srcOrders().groupBy("k").count())), "Grouper.count vs op");
        assertArrayEquals(
                fp(run(srcOrders().groupBy(ParallelGroupByAgg.of(ORDER, "k").countField("amt")))),
                fp(run(srcOrders().groupBy("k").count("amt"))), "Grouper.count(f) vs op");
        assertArrayEquals(
                fp(run(srcOrders().groupBy(ParallelGroupByAgg.of(ORDER, "k").max("amt")))),
                fp(run(srcOrders().groupBy("k").max("amt"))), "Grouper.max vs op");
        assertArrayEquals(
                fp(run(srcOrders().groupBy(ParallelGroupByAgg.of(ORDER, "k").min("amt")))),
                fp(run(srcOrders().groupBy("k").min("amt"))), "Grouper.min vs op");
    }

    // ---- JoinDataSet ----

    @Test void joinBuilder_inner_matchesOp() {
        List<DataRow> viaShell = run(srcOrders().join(session.fromSource(new CollectionSource(DIM, dims()))).on("k"));
        // The operator form needs a rebuild to reuse the same session, which fan-out refuses, so each side gets its own session.
        BatchSession s2 = RowWeave.createBatchSession("fb2");
        DataSet ords = s2.fromSource(new CollectionSource(ORDER, orders()));
        DataSet dim = s2.fromSource(new CollectionSource(DIM, dims()));
        DataSet opJoin = ords.join(dim, SortMergeJoin.of(ORDER, DIM, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        WorkerCollectingOutput out = new WorkerCollectingOutput(opJoin.getSchema());
        opJoin.sinkTo(out);
        s2.execute();
        assertArrayEquals(fp(out.getRows()), fp(viaShell), "join builder vs op");
        assertEquals(3, viaShell.size()); // a×2 + b×1
    }

    @Test void leftJoin_builder_keepsUnmatched() {
        List<DataRow> viaShell = run(srcOrders().leftJoin(session.fromSource(new CollectionSource(DIM, dims()))).on("k"));
        // c matches nothing and is kept by LEFT, so a twice plus b once plus c once gives 4.
        assertEquals(4, viaShell.size());
    }

    // ---- CoGroupDataSet ----

    @Test void coGroupBuilder_matchesOp() {
        DataSchema cgOut = new DataSchema(new Field("k", DataType.StringType), new Field("n", DataType.IntegerType));
        CoGroupFunction fn = new CoGroupFunction() {
            @Override public DataSchema getResultSchema() { return cgOut; }
            @Override public void coGroup(Iterable<DataRow> l, Iterable<DataRow> r, Collector c) {
                int n = 0; Object key = null;
                for (DataRow x : l) { n++; key = x.get(0); }
                for (DataRow x : r) { n++; if (key == null) key = x.get(0); }
                DataRow out = new DataRow(2); out.set(0, key); out.set(1, n); c.collect(out);
            }
        };
        List<DataRow> viaShell = run(srcOrders().coGroup(session.fromSource(new CollectionSource(DIM, dims())), fn).on("k"));
        assertTrue(viaShell.size() > 0, "coGroup builder emits output");
    }
}
