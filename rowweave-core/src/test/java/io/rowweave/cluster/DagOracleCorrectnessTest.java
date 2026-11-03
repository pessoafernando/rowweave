package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.Distinct;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import io.rowweave.ops.Union;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Correctness edge: an **independent plain Java oracle**, to break the circular argument of grading the
 * engine against its own output.
 *
 * <p>Problem: {@link ComplexDagQuadrantTest} and {@link ComplexMultiIoDagQuadrantTest} use "single
 * machine P1" as the reference. If one shared operator has a bug, P1, P4 and the cluster are wrong
 * together and their comparisons still pass green (the engine compared only with itself).
 *
 * <p>This test computes the expected result of each DAG with {@link OracleOps} (hand-written HashMap and
 * List, sharing nothing with the engine's push machinery) and asserts **oracle == single machine P1 ==
 * single machine P4** (through {@link BatchSession#commit()} and {@link FileSink}, as multiset
 * fingerprints). The quadrant tests already show P1 == P4 == cluster, so oracle == P1 closes the loop and
 * implies oracle == every quadrant.
 *
 * <p>The cluster is not exercised here, to keep in-process cluster timing out of the correctness check
 * (cluster == P1 is covered by ComplexDag*QuadrantTest under @Tag multiprocess), so this test runs in the
 * **default suite** (no @Tag) and CI checks the correctness edge on every build.
 */
class DagOracleCorrectnessTest {

    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("grade", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType),
            new Field("qty", DataType.IntegerType));

    private static final int N = 100_000;

    private static List<DataRow> orders() {
        List<DataRow> rows = new ArrayList<>(N);
        for (int i = 1; i <= N; i++) {
            DataRow r = new DataRow(6);
            r.set(0, (long) i);
            r.set(1, i % 50);
            r.set(2, i % 10);
            r.set(3, i % 5);
            r.set(4, new BigDecimal(i % 1000));
            r.set(5, i % 20);
            rows.add(r);
        }
        return rows;
    }

    private static final DataSchema A = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("va", DataType.BigDecimalType));
    private static final DataSchema B = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("vb", DataType.BigDecimalType));

    private static List<DataRow> dataA() {
        List<DataRow> l = new ArrayList<>();
        for (int i = 0; i < 8000; i++) { DataRow r = new DataRow(2); r.set(0, i % 12); r.set(1, new BigDecimal(i % 50)); l.add(r); }
        return l;
    }

    private static List<DataRow> dataB() {
        List<DataRow> l = new ArrayList<>();
        for (int i = 0; i < 6000; i++) { DataRow r = new DataRow(2); r.set(0, i % 10); r.set(1, new BigDecimal(i % 40)); l.add(r); }
        return l;
    }

    @FunctionalInterface
    private interface Dag { DataSet build(DataSet src); }

    private long[] runEngine(String name, int parallelism, DataSchema srcMeta, List<DataRow> data, Dag dag, Path dir) {
        Path f = dir.resolve(name + "-p" + parallelism + ".tsv");
        BatchSession s = RowWeave.createBatchSession(name);
        s.setParallelism(parallelism);
        DataSet out = dag.build(s.fromSource(new CollectionSource(srcMeta, data)));
        out.sinkTo(new FileSink(out.getSchema(), f.toString()));
        s.execute();
        return fingerprint(f);
    }

    private long[] oracleFingerprint(String name, DataSchema outMeta, List<DataRow> rows, Path dir) {
        Path f = dir.resolve(name + "-oracle.tsv");
        Sink out = new FileSink(outMeta, f.toString());
        for (DataRow r : rows) out.collect(r);
        out.close();
        return fingerprint(f);
    }

    private static long[] fingerprint(Path p) {
        long count = 0, sum = 0;
        try (BufferedReader r = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                count++;
                long h = 1125899906842597L;
                for (int i = 0; i < line.length(); i++) h = 31 * h + line.charAt(i);
                sum += h;
            }
        } catch (Exception e) { throw new RuntimeException(e); }
        return new long[]{count, sum};
    }

    private void assertOracle(String name, DataSchema srcMeta, List<DataRow> data, Dag dag,
                              DataSchema oracleMeta, List<DataRow> oracleRows, Path dir) {
        long[] p1 = runEngine(name, 1, srcMeta, data, dag, dir);
        long[] p4 = runEngine(name, 4, srcMeta, data, dag, dir);
        long[] orc = oracleFingerprint(name, oracleMeta, oracleRows, dir);
        assertTrue(orc[0] > 0, name + " the reference should emit output rows");
        assertArrayEquals(orc, p1, name + " single-process P1 differs from the java-only reference multiset (correctness bug)");
        assertArrayEquals(orc, p4, name + " single-process P4 differs from the java-only reference multiset (correctness bug)");
    }

    private DataSchema gbMeta(String... keys) {
        return ParallelGroupByAgg.of(ORDER, keys).sum("amount").getOutputSchema();
    }

    private static SortField asc(int i) { return new SortField(i, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST); }
    private static SortField desc(int i) { return new SortField(i, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST); }

    private static int[] idx(int... a) { return a; }

    @Test void dag01(@TempDir Path dir) {
        List<DataRow> data = orders();
        Dag dag = src -> src.filter("amount >= 50").filter("qty >= 2")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept").sum("amount"))
                .filter("dept >= 5")
                .orderBy(OrderBy.of(gbMeta("dept"), desc(1)), 1, false)
                .top(Top.of(gbMeta("dept"), 20, desc(1)), 1)
                .filter("dept < 45");
        // oracle:filter amount>=50 & qty>=2 → groupBy dept sum(amount) → filter dept>=5 → top20 by sum desc → filter dept<45
        DataSchema g = gbMeta("dept");
        List<DataRow> o = OracleOps.filter(data, r -> ((BigDecimal) r.get(4)).compareTo(new BigDecimal(50)) >= 0 && (Integer) r.get(5) >= 2);
        o = OracleOps.groupBySum(o, idx(1), 4);
        o = OracleOps.filter(o, r -> (Integer) r.get(0) >= 5);
        o = OracleOps.top(o, g, 20, desc(1));
        o = OracleOps.filter(o, r -> (Integer) r.get(0) < 45);
        assertOracle("dag01", ORDER, data, dag, g, o, dir);
    }

    @Test void dag03(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema g = ParallelGroupByAgg.of(ORDER, "region", "grade").sum("amount").getOutputSchema();
        Dag dag = src -> src.filter("amount >= 20").filter("amount < 900").filter("qty >= 1")
                .filter("region >= 1").filter("grade <= 3")
                .groupBy(ParallelGroupByAgg.of(ORDER, "region", "grade").sum("amount"))
                .orderBy(OrderBy.of(g, asc(0), asc(1)), 1, false);
        List<DataRow> o = OracleOps.filter(data, r -> {
            BigDecimal a = (BigDecimal) r.get(4);
            return a.compareTo(new BigDecimal(20)) >= 0 && a.compareTo(new BigDecimal(900)) < 0
                    && (Integer) r.get(5) >= 1 && (Integer) r.get(2) >= 1 && (Integer) r.get(3) <= 3;
        });
        o = OracleOps.groupBySum(o, idx(2, 3), 4);
        assertOracle("dag03", ORDER, data, dag, g, o, dir);
    }

    @Test void dag05(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema selMeta = new DataSchema(ORDER.getField(1), ORDER.getField(2)); // select(1,2) → [dept,region]
        Dag dag = src -> src.filter("amount >= 5")
                .distinct(Distinct.byColumns(ORDER, "dept", "region", "grade"))
                .filter("region >= 2")
                .distinct(Distinct.byColumns(ORDER, "dept", "region"))
                .orderBy(OrderBy.of(ORDER, asc(1), asc(2)), 1, false)
                .select(1, 2)
                .filter("dept < 48");
        List<DataRow> o = OracleOps.filter(data, r -> ((BigDecimal) r.get(4)).compareTo(new BigDecimal(5)) >= 0);
        o = OracleOps.distinct(o, 1, 2, 3);
        o = OracleOps.filter(o, r -> (Integer) r.get(2) >= 2);
        o = OracleOps.distinct(o, 1, 2);
        o = OracleOps.select(o, 1, 2);
        o = OracleOps.filter(o, r -> (Integer) r.get(0) < 48);
        assertOracle("dag05", ORDER, data, dag, selMeta, o, dir);
    }

    @Test void dag06(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema mx = ParallelGroupByAgg.of(ORDER, "region").max("amount").getOutputSchema(); // [region, max0]
        DataSchema g2 = ParallelGroupByAgg.of(mx, mx.getField(0).getName()).min(mx.getField(1).getName()).getOutputSchema();
        Dag dag = src -> src.filter("qty >= 1").filter("amount >= 30")
                .groupBy(ParallelGroupByAgg.of(ORDER, "region").max("amount"))
                .filter("region >= 1")
                .select(0, 1)
                .groupBy(ParallelGroupByAgg.of(mx, mx.getField(0).getName()).min(mx.getField(1).getName()))
                .orderBy(OrderBy.of(g2, desc(0)), 1, false);
        List<DataRow> o = OracleOps.filter(data, r -> (Integer) r.get(5) >= 1 && ((BigDecimal) r.get(4)).compareTo(new BigDecimal(30)) >= 0);
        o = OracleOps.groupByExtreme(o, idx(2), 4, true); // max(amount) by region → [region,max]
        o = OracleOps.filter(o, r -> (Integer) r.get(0) >= 1);
        o = OracleOps.select(o, 0, 1);
        o = OracleOps.groupByExtreme(o, idx(0), 1, false); // min(max) by region
        assertOracle("dag06", ORDER, data, dag, g2, o, dir);
    }

    @Test void dag10(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema g = ParallelGroupByAgg.of(ORDER, "dept", "grade").sum("qty").getOutputSchema(); // [dept,grade,sum0]
        Dag dag = src -> src.filter("amount >= 10").filter("amount < 950").filter("qty >= 1")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept", "grade").sum("qty"))
                .distinct(Distinct.byColumns(g, "dept", "grade"))
                .orderBy(OrderBy.of(g, asc(0), asc(1)), 1, false)
                .top(Top.of(g, 40, desc(2)), 1);
        List<DataRow> o = OracleOps.filter(data, r -> {
            BigDecimal a = (BigDecimal) r.get(4);
            return a.compareTo(new BigDecimal(10)) >= 0 && a.compareTo(new BigDecimal(950)) < 0 && (Integer) r.get(5) >= 1;
        });
        o = OracleOps.groupBySum(o, idx(1, 3), 5); // sum(qty), qty idx=5
        o = OracleOps.distinct(o, 0, 1);
        o = OracleOps.orderBy(o, g, asc(0), asc(1));
        o = OracleOps.top(o, g, 40, desc(2));
        assertOracle("dag10", ORDER, data, dag, g, o, dir);
    }

    @Test void dag02(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema g1 = ParallelGroupByAgg.of(ORDER, "dept", "region").sum("amount").getOutputSchema(); // [dept,region,sum0]
        String sumCol = g1.getField(g1.getFieldCount() - 1).getName();
        DataSchema g2 = ParallelGroupByAgg.of(g1, "dept").sum(sumCol).getOutputSchema(); // [dept, sum0]
        Dag dag = src -> {
            DataSet a = src.filter("amount >= 10")
                    .groupBy(ParallelGroupByAgg.of(ORDER, "dept", "region").sum("amount"));
            return a.distinct(Distinct.byColumns(g1, "dept"))
                    .filter("dept >= 2")
                    .groupBy(ParallelGroupByAgg.of(g1, "dept").sum(sumCol))
                    .orderBy(OrderBy.of(g2, asc(0)), 1, false)
                    .select(0, 1);
        };
        List<DataRow> o = OracleOps.filter(data, r -> ((BigDecimal) r.get(4)).compareTo(new BigDecimal(10)) >= 0);
        o = OracleOps.groupBySum(o, idx(1, 2), 4);   // [dept,region,sum]
        o = OracleOps.distinct(o, 0);                // distinct by dept, first-seen row
        o = OracleOps.filter(o, r -> (Integer) r.get(0) >= 2);
        o = OracleOps.groupBySum(o, idx(0), 2);      // sum(sum0) by dept → [dept, sum]
        o = OracleOps.select(o, 0, 1);
        assertOracle("dag02", ORDER, data, dag, g2, o, dir);
    }

    @Test void dag04(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema cnt = ParallelGroupByAgg.of(ORDER, "dept").count().getOutputSchema(); // [dept, count0]
        DataSchema mxMeta = ParallelGroupByAgg.of(cnt, cnt.getField(0).getName()).max(cnt.getField(1).getName()).getOutputSchema();
        Dag dag = src -> src.filter("qty >= 3")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept").count())
                .filter("dept >= 1")
                .select(0, 1)
                .groupBy(ParallelGroupByAgg.of(cnt, cnt.getField(0).getName()).max(cnt.getField(1).getName()))
                .orderBy(OrderBy.of(mxMeta, asc(0)), 1, false)
                .top(Top.of(mxMeta, 30, asc(0)), 1);
        List<DataRow> o = OracleOps.filter(data, r -> (Integer) r.get(5) >= 3);
        o = OracleOps.groupByCount(o, idx(1));         // [dept, count]
        o = OracleOps.filter(o, r -> (Integer) r.get(0) >= 1);
        o = OracleOps.select(o, 0, 1);
        o = OracleOps.groupByExtreme(o, idx(0), 1, true); // max(count) by dept
        o = OracleOps.orderBy(o, mxMeta, asc(0));
        o = OracleOps.top(o, mxMeta, 30, asc(0));
        assertOracle("dag04", ORDER, data, dag, mxMeta, o, dir);
    }

    @Test void dag07(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema selMeta = new DataSchema(
                new Field("dept", DataType.IntegerType),
                new Field("region", DataType.IntegerType),
                new Field("amount", DataType.BigDecimalType));
        DataSchema g = ParallelGroupByAgg.of(selMeta, "region").sum("amount").getOutputSchema(); // [region, sum0]
        Dag dag = src -> src.filter("amount >= 15")
                .select(0, 1, 2, 4)   // [id,dept,region,amount]
                .filter("dept >= 3")
                .select(1, 2, 3)      // [dept,region,amount]
                .groupBy(ParallelGroupByAgg.of(selMeta, "region").sum("amount"))
                .orderBy(OrderBy.of(g, desc(1)), 1, false)
                .top(Top.of(g, 8, desc(1)), 1);
        List<DataRow> o = OracleOps.filter(data, r -> ((BigDecimal) r.get(4)).compareTo(new BigDecimal(15)) >= 0);
        o = OracleOps.select(o, 0, 1, 2, 4);          // [id,dept,region,amount]
        o = OracleOps.filter(o, r -> (Integer) r.get(1) >= 3);
        o = OracleOps.select(o, 1, 2, 3);             // [dept,region,amount]
        o = OracleOps.groupBySum(o, idx(1), 2);       // sum(amount) by region → [region, sum]
        o = OracleOps.orderBy(o, g, desc(1));
        o = OracleOps.top(o, g, 8, desc(1));
        assertOracle("dag07", ORDER, data, dag, g, o, dir);
    }

    @Test void dag08(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema cnt = ParallelGroupByAgg.of(ORDER, "dept", "region").countField("amount").getOutputSchema(); // [dept,region,count0]
        DataSchema selMeta = new DataSchema(cnt.getField(0), cnt.getField(1)); // select(0,1) → [dept,region]
        Dag dag = src -> src.filter("amount >= 0").filter("qty >= 2")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept", "region").countField("amount"))
                .filter("dept >= 1")
                .orderBy(OrderBy.of(cnt, asc(0), asc(1)), 1, false)
                .top(Top.of(cnt, 50, desc(2)), 1)
                .select(0, 1);
        List<DataRow> o = OracleOps.filter(data, r -> ((BigDecimal) r.get(4)).compareTo(BigDecimal.ZERO) >= 0 && (Integer) r.get(5) >= 2);
        o = OracleOps.groupByCountField(o, idx(1, 2), 4); // countField(amount) by (dept,region)
        o = OracleOps.filter(o, r -> (Integer) r.get(0) >= 1);
        o = OracleOps.orderBy(o, cnt, asc(0), asc(1));
        o = OracleOps.top(o, cnt, 50, desc(2));
        o = OracleOps.select(o, 0, 1);
        assertOracle("dag08", ORDER, data, dag, selMeta, o, dir);
    }

    @Test void dag09(@TempDir Path dir) {
        List<DataRow> data = orders();
        DataSchema g3 = ParallelGroupByAgg.of(ORDER, "dept", "region", "grade").sum("amount").getOutputSchema();
        String sumCol = g3.getField(3).getName(); // "sum0"
        DataSchema narrowed = new DataSchema(g3.getField(0), g3.getField(3)); // [dept, sum0]
        DataSchema g1 = ParallelGroupByAgg.of(narrowed, "dept").sum(sumCol).getOutputSchema();
        Dag dag = src -> src.filter("amount >= 25")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept", "region", "grade").sum("amount"))
                .filter("region >= 1")
                .select(0, 3)  // [dept, sum0]
                .groupBy(ParallelGroupByAgg.of(narrowed, "dept").sum(sumCol))
                .orderBy(OrderBy.of(g1, asc(0)), 1, false)
                .filter("dept < 49");
        List<DataRow> o = OracleOps.filter(data, r -> ((BigDecimal) r.get(4)).compareTo(new BigDecimal(25)) >= 0);
        o = OracleOps.groupBySum(o, idx(1, 2, 3), 4);  // [dept,region,grade,sum]
        o = OracleOps.filter(o, r -> (Integer) r.get(1) >= 1);
        o = OracleOps.select(o, 0, 3);                 // [dept, sum]
        o = OracleOps.groupBySum(o, idx(0), 1);        // sum(sum0) by dept → [dept, sum]
        o = OracleOps.filter(o, r -> (Integer) r.get(0) < 49);
        assertOracle("dag09", ORDER, data, dag, g1, o, dir);
    }

    @Test void mio4_diamondJoin(@TempDir Path dir) {
        List<DataRow> da = dataA();
        DataSchema joinMeta = SortMergeJoin.of(A, A, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
        Path f1 = dir.resolve("mio4-p1.tsv"), f4 = dir.resolve("mio4-p4.tsv");
        long[] p1 = runMio4("mio4-p1", 1, da, f1);
        long[] p4 = runMio4("mio4-p4", 4, da, f4);
        List<DataRow> left = OracleOps.filter(da, r -> ((BigDecimal) r.get(1)).compareTo(new BigDecimal(5)) >= 0 && (Integer) r.get(0) >= 2);
        List<DataRow> right = OracleOps.filter(da, r -> ((BigDecimal) r.get(1)).compareTo(new BigDecimal(5)) >= 0 && (Integer) r.get(0) <= 9);
        List<DataRow> joined = OracleOps.joinInner(left, idx(0), right, idx(0));
        long[] orc = oracleFingerprint("mio4", joinMeta, joined, dir);
        assertArrayEquals(orc, p1, "mio4 diamond join P1 versus the reference");
        assertArrayEquals(orc, p4, "mio4 diamond join P4 versus the reference");
    }

    private long[] runMio4(String name, int parallelism, List<DataRow> da, Path f) {
        BatchSession s = RowWeave.createBatchSession(name);
        s.setParallelism(parallelism);
        DataSet a = s.fromSource(new CollectionSource(A, da));
        DataSet mid = a.filter("va >= 5");
        DataSet b1 = mid.filter("k >= 2");
        DataSet joined = b1.join(a.filter("va >= 5").filter("k <= 9"),
                SortMergeJoin.of(A, A, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        joined.sinkTo(new FileSink(joined.getSchema(), f.toString()));
        s.execute();
        return fingerprint(f);
    }

    @Test void mio5_threeOutputs(@TempDir Path dir) {
        List<DataRow> da = dataA(), db = dataB();
        DataSchema joinAB = SortMergeJoin.of(A, B, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
        DataSchema jgb = ParallelGroupByAgg.of(joinAB, "k").sum("va").getOutputSchema();
        Path f1 = dir.resolve("mio5-o1.tsv"), f2 = dir.resolve("mio5-o2.tsv"), f3 = dir.resolve("mio5-o3.tsv");
        BatchSession s = RowWeave.createBatchSession("mio5");
        s.setParallelism(4);
        DataSet a = s.fromSource(new CollectionSource(A, da));
        DataSet b = s.fromSource(new CollectionSource(B, db));
        DataSet j = a.join(b, SortMergeJoin.of(A, B, new String[]{"k"}, new String[]{"k"}, JoinType.INNER))
                .groupBy(ParallelGroupByAgg.of(joinAB, "k").sum("va"));
        j.sinkTo(new FileSink(jgb, f1.toString()));
        DataSet o2 = a.filter("va >= 20");
        o2.sinkTo(new FileSink(A, f2.toString()));
        DataSet o3 = b.distinct(Distinct.byColumns(B, "k"));
        o3.sinkTo(new FileSink(B, f3.toString()));
        s.execute();
        // oracle
        List<DataRow> joined = OracleOps.joinInner(da, idx(0), db, idx(0)); // [k,va,k,vb]
        List<DataRow> exp1 = OracleOps.groupBySum(joined, idx(0), 1);       // sum(va) by k
        List<DataRow> exp2 = OracleOps.filter(da, r -> ((BigDecimal) r.get(1)).compareTo(new BigDecimal(20)) >= 0);
        List<DataRow> exp3 = OracleOps.distinct(db, 0);
        assertArrayEquals(oracleFingerprint("mio5-o1", jgb, exp1, dir), fingerprint(f1), "mio5 out1 join→groupBy vs oracle");
        assertArrayEquals(oracleFingerprint("mio5-o2", A, exp2, dir), fingerprint(f2), "mio5 out2 filter vs oracle");
        assertArrayEquals(oracleFingerprint("mio5-o3", B, exp3, dir), fingerprint(f3), "mio5 out3 distinct vs oracle");
    }

    private static final CoGroupFunction CG_FN = new CoGroupFunction() {
        private final DataSchema out = new DataSchema(
                new Field("k", DataType.IntegerType), new Field("ac", DataType.IntegerType), new Field("bc", DataType.IntegerType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void coGroup(Iterable<DataRow> it1, Iterable<DataRow> it2, Collector c) {
            int ac = 0; Object key = null;
            for (DataRow r : it1) { ac++; key = r.get(0); }
            int bc = 0;
            for (DataRow r : it2) { bc++; if (key == null) key = r.get(0); }
            DataRow o = new DataRow(3); o.set(0, key); o.set(1, ac); o.set(2, bc); c.collect(o);
        }
    };

    @Test void mio2_union(@TempDir Path dir) {
        List<DataRow> data = dataA();
        Path f1 = dir.resolve("mio2u-p1.tsv");
        Path f4 = dir.resolve("mio2u-p4.tsv");
        long[] p1 = runUnionFilter("mio2u-p1", 1, data, f1);
        long[] p4 = runUnionFilter("mio2u-p4", 4, data, f4);
        // oracle:union(A, A) → filter va>=8
        List<DataRow> u = OracleOps.union(data, data);
        List<DataRow> o = OracleOps.filter(u, r -> ((BigDecimal) r.get(1)).compareTo(new BigDecimal(8)) >= 0);
        long[] orc = oracleFingerprint("mio2u", A, o, dir);
        assertArrayEquals(orc, p1, "mio2 union P1 vs oracle");
        assertArrayEquals(orc, p4, "mio2 union P4 vs oracle");
    }

    private long[] runUnionFilter(String name, int parallelism, List<DataRow> data, Path f) {
        BatchSession s = RowWeave.createBatchSession(name);
        s.setParallelism(parallelism);
        DataSet a1 = s.fromSource(new CollectionSource(A, data));
        DataSet a2 = s.fromSource(new CollectionSource(A, data));
        DataSet u = a1.union(a2, Union.of(A)).filter("va >= 8");
        u.sinkTo(new FileSink(A, f.toString()));
        s.execute();
        return fingerprint(f);
    }

    @Test void mio3_coGroupFilter(@TempDir Path dir) {
        List<DataRow> da = dataA(), db = dataB();
        DataSchema cgOut = CG_FN.getResultSchema();
        Path f1 = dir.resolve("mio3-p1.tsv");
        Path f4 = dir.resolve("mio3-p4.tsv");
        long[] p1 = runCoGroupFilter("mio3-p1", 1, da, db, f1);
        long[] p4 = runCoGroupFilter("mio3-p4", 4, da, db, f4);
        List<DataRow> cg = oracleCoGroup(da, db);
        List<DataRow> o = OracleOps.filter(cg, r -> (Integer) r.get(1) >= 1);
        long[] orc = oracleFingerprint("mio3", cgOut, o, dir);
        assertArrayEquals(orc, p1, "mio3 coGroup P1 vs oracle");
        assertArrayEquals(orc, p4, "mio3 coGroup P4 vs oracle");
    }

    private long[] runCoGroupFilter(String name, int parallelism, List<DataRow> da, List<DataRow> db, Path f) {
        BatchSession s = RowWeave.createBatchSession(name);
        s.setParallelism(parallelism);
        DataSet a = s.fromSource(new CollectionSource(A, da));
        DataSet b = s.fromSource(new CollectionSource(B, db));
        DataSet cg = a.coGroup(b, CoGroup.of(A, B, new String[]{"k"}, new String[]{"k"}, CG_FN));
        DataSet o1 = cg.filter("ac >= 1");
        o1.sinkTo(new FileSink(CG_FN.getResultSchema(), f.toString()));
        s.execute();
        return fingerprint(f);
    }

    private List<DataRow> oracleCoGroup(List<DataRow> da, List<DataRow> db) {
        java.util.TreeSet<Integer> keys = new java.util.TreeSet<>();
        java.util.Map<Integer, Integer> ac = new java.util.HashMap<>(), bc = new java.util.HashMap<>();
        for (DataRow r : da) { int k = (Integer) r.get(0); keys.add(k); ac.merge(k, 1, Integer::sum); }
        for (DataRow r : db) { int k = (Integer) r.get(0); keys.add(k); bc.merge(k, 1, Integer::sum); }
        List<DataRow> out = new ArrayList<>();
        for (int k : keys) {
            DataRow o = new DataRow(3); o.set(0, k); o.set(1, ac.getOrDefault(k, 0)); o.set(2, bc.getOrDefault(k, 0));
            out.add(o);
        }
        return out;
    }

    @Test void mio1_joinFanout(@TempDir Path dir) {
        List<DataRow> da = dataA(), db = dataB();
        DataSchema joinMeta = SortMergeJoin.of(A, B, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
        Path f1a = dir.resolve("mio1-o1.tsv"), f1b = dir.resolve("mio1-o2.tsv");
        BatchSession s = RowWeave.createBatchSession("mio1");
        s.setParallelism(4);
        DataSet a = s.fromSource(new CollectionSource(A, da));
        DataSet b = s.fromSource(new CollectionSource(B, db));
        DataSet j = a.join(b, SortMergeJoin.of(A, B, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        DataSet o1 = j.filter("va >= 10");
        o1.sinkTo(new FileSink(joinMeta, f1a.toString()));
        DataSet o2 = j.filter("vb >= 5");
        o2.sinkTo(new FileSink(joinMeta, f1b.toString()));
        s.execute();
        List<DataRow> joined = OracleOps.joinInner(da, idx(0), db, idx(0));
        List<DataRow> exp1 = OracleOps.filter(joined, r -> ((BigDecimal) r.get(1)).compareTo(new BigDecimal(10)) >= 0);
        List<DataRow> exp2 = OracleOps.filter(joined, r -> ((BigDecimal) r.get(3)).compareTo(new BigDecimal(5)) >= 0);
        assertArrayEquals(oracleFingerprint("mio1-o1", joinMeta, exp1, dir), fingerprint(f1a), "mio1 out1(va>=10) vs oracle");
        assertArrayEquals(oracleFingerprint("mio1-o2", joinMeta, exp2, dir), fingerprint(f1b), "mio1 out2(vb>=5) vs oracle");
    }
}

