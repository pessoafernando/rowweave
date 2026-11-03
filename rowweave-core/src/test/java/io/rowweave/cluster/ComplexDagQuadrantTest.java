package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.FileSource;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.Distinct;
import io.rowweave.ops.OrderBy;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.ops.Top;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ComplexDagQuadrantTest {

    private static final int N = 100_000;
    private static final DataSchema ORDER = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("dept", DataType.IntegerType),
            new Field("region", DataType.IntegerType),
            new Field("grade", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType),
            new Field("qty", DataType.IntegerType));

    private FileSource sourceInput;
    private MasterServer master1, master4;
    private WorkerServer w1a, w1b, w4a, w4b, w4c, w4d;
    private Path tmpDir;

    @BeforeAll
    void setUp() throws Exception {
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
        tmpDir = Files.createTempDirectory("cdag");
        sourceInput = ClusterTestFileInput.write(tmpDir.resolve("orders-source.tsv"), ORDER, rows);
        master1 = new MasterServer(5000);
        w1a = worker("m1w0", master1); w1b = worker("m1w1", master1);
        master4 = new MasterServer(5000);
        w4a = worker("m4w0", master4); w4b = worker("m4w1", master4);
        w4c = worker("m4w2", master4); w4d = worker("m4w3", master4);
    }

    private WorkerServer worker(String id, MasterServer m) throws Exception {
        File d = new File(tmpDir.toFile(), id); d.mkdirs();
        return WorkerServer.newUnguarded(id, 4, "127.0.0.1", m.port(), d);
    }

    @AfterAll
    void tearDown() {
        for (WorkerServer w : new WorkerServer[]{w1a, w1b, w4a, w4b, w4c, w4d}) if (w != null) w.close();
        if (master1 != null) master1.close();
        if (master4 != null) master4.close();
    }

    @FunctionalInterface
    private interface Dag { DataSet build(DataSet src); }

    private DataSchema gbMeta(String... keys) {
        return ParallelGroupByAgg.of(ORDER, keys).sum("amount").getOutputSchema();
    }

    private long[] runQuad(String name, int parallelism, io.rowweave.cluster.JobClient client, Dag dag, DataSchema outMeta) {
        Path f;
        try { f = Files.createTempFile("cdag-" + name + "-", ".tsv"); } catch (Exception e) { throw new RuntimeException(e); }
        BatchSession s = client != null
                ? new BatchSession(name, name, client)
                : RowWeave.createBatchSession(name);
        s.setParallelism(parallelism);
        DataSet src = s.fromSource(sourceInput);
        DataSet out = dag.build(src);
        out.sinkTo(new FileSink(out.getSchema(), f.toString()));
        s.execute();
        long[] fp = fingerprint(f);
        try { Files.deleteIfExists(f); } catch (Exception ignored) { }
        return fp;
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

    private void assertQuadrantConsistent(String name, Dag dag) {
        long[] oracle = runQuad(name + "-s1", 1, null, dag, null);
        long[] sP4 = runQuad(name + "-s4", 4, null, dag, null);
        long[] cP1 = runQuad(name + "-c1", 1, new ClusterJobClient("127.0.0.1", master1.port()), dag, null);
        long[] cP4 = runQuad(name + "-c4", 4, new ClusterJobClient("127.0.0.1", master4.port()), dag, null);
        assertArrayEquals(oracle, sP4, name + " single-process P4 differs from the single-process P1 reference multiset");
        assertArrayEquals(oracle, cP1, name + " cluster P1 differs from the single-process P1 reference multiset");
        assertArrayEquals(oracle, cP4, name + " cluster P4 differs from the single-process P1 reference multiset");
        System.out.printf("[dag] %-28s all four quadrants agree outRows=%d%n", name, oracle[0]);
    }

    @Test void dag01_filterSelectGroupOrderTop() {
        assertQuadrantConsistent("dag01", src -> src
                .filter("amount >= 50")
                .filter("qty >= 2")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept").sum("amount"))
                .filter("dept >= 5")
                .orderBy(OrderBy.of(gbMeta("dept"), new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1, false)
                .top(Top.of(gbMeta("dept"), 20, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1)
                .filter("dept < 45"));
    }

    @Test void dag02_multiGroupDistinct() { // filter,groupBy,distinct,filter,groupBy,orderBy,select = 7
        DataSchema g1 = ParallelGroupByAgg.of(ORDER, "dept", "region").sum("amount").getOutputSchema();
        assertQuadrantConsistent("dag02", src -> {
            DataSet a = src.filter("amount >= 10")
                    .groupBy(ParallelGroupByAgg.of(ORDER, "dept", "region").sum("amount"));
            return a.distinct(Distinct.byColumns(g1, "dept"))
                    .filter("dept >= 2")
                    .groupBy(ParallelGroupByAgg.of(g1, "dept").sum(g1.getField(g1.getFieldCount() - 1).getName()))
                    .orderBy(OrderBy.of(ParallelGroupByAgg.of(g1, "dept").sum(g1.getField(g1.getFieldCount() - 1).getName()).getOutputSchema(),
                            new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), 1, false)
                    .select(0, 1);
        });
    }

    @Test void dag03_deepFilterChainGroup() { // filter×5 + groupBy + orderBy = 7
        assertQuadrantConsistent("dag03", src -> src
                .filter("amount >= 20").filter("amount < 900").filter("qty >= 1")
                .filter("region >= 1").filter("grade <= 3")
                .groupBy(ParallelGroupByAgg.of(ORDER, "region", "grade").sum("amount"))
                .orderBy(OrderBy.of(ParallelGroupByAgg.of(ORDER, "region", "grade").sum("amount").getOutputSchema(),
                        new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                        new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), 1, false));
    }

    @Test void dag04_groupCountThenGroup() { // filter,groupByCount,filter,select,groupBy,orderBy,top = 7
        DataSchema cnt = ParallelGroupByAgg.of(ORDER, "dept").count().getOutputSchema();
        assertQuadrantConsistent("dag04", src -> src
                .filter("qty >= 3")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept").count())
                .filter("dept >= 1")
                .select(0, 1)
                .groupBy(ParallelGroupByAgg.of(cnt, cnt.getField(0).getName()).max(cnt.getField(1).getName()))
                .orderBy(OrderBy.of(ParallelGroupByAgg.of(cnt, cnt.getField(0).getName()).max(cnt.getField(1).getName()).getOutputSchema(),
                        new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), 1, false)
                .top(Top.of(ParallelGroupByAgg.of(cnt, cnt.getField(0).getName()).max(cnt.getField(1).getName()).getOutputSchema(),
                        30, new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), 1));
    }

    @Test void dag05_distinctHeavy() { // filter,distinct,filter,distinct,orderBy,select,filter = 7
        assertQuadrantConsistent("dag05", src -> src
                .filter("amount >= 5")
                .distinct(Distinct.byColumns(ORDER, "dept", "region", "grade"))
                .filter("region >= 2")
                .distinct(Distinct.byColumns(ORDER, "dept", "region"))
                .orderBy(OrderBy.of(ORDER, new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                        new SortField(2, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), 1, false)
                .select(1, 2)
                .filter("dept < 48"));
    }

    @Test void dag06_maxMinChain() {
        DataSchema mx = ParallelGroupByAgg.of(ORDER, "region").max("amount").getOutputSchema();
        assertQuadrantConsistent("dag06", src -> src
                .filter("qty >= 1").filter("amount >= 30")
                .groupBy(ParallelGroupByAgg.of(ORDER, "region").max("amount"))
                .filter("region >= 1")
                .select(0, 1)
                .groupBy(ParallelGroupByAgg.of(mx, mx.getField(0).getName()).min(mx.getField(1).getName()))
                .orderBy(OrderBy.of(ParallelGroupByAgg.of(mx, mx.getField(0).getName()).min(mx.getField(1).getName()).getOutputSchema(),
                        new SortField(0, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1, false));
    }

    @Test void dag07_selectMapFilterGroup() { // filter,select,filter,select,groupBy,orderBy,top = 7
        assertQuadrantConsistent("dag07", src -> src
                .filter("amount >= 15")
                .select(0, 1, 2, 4)   // id,dept,region,amount
                .filter("dept >= 3")
                .select(1, 2, 3)      // dept,region,amount(new idx)
                .groupBy(ParallelGroupByAgg.of(new DataSchema(
                        new Field("dept", DataType.IntegerType),
                        new Field("region", DataType.IntegerType),
                        new Field("amount", DataType.BigDecimalType)), "region").sum("amount"))
                .orderBy(OrderBy.of(ParallelGroupByAgg.of(new DataSchema(
                        new Field("dept", DataType.IntegerType),
                        new Field("region", DataType.IntegerType),
                        new Field("amount", DataType.BigDecimalType)), "region").sum("amount").getOutputSchema(),
                        new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1, false)
                .top(Top.of(ParallelGroupByAgg.of(new DataSchema(
                        new Field("dept", DataType.IntegerType),
                        new Field("region", DataType.IntegerType),
                        new Field("amount", DataType.BigDecimalType)), "region").sum("amount").getOutputSchema(),
                        8, new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1));
    }

    @Test void dag08_countFieldChain() { // filter,filter,groupByCount,filter,orderBy,top,select = 7
        DataSchema cnt = ParallelGroupByAgg.of(ORDER, "dept", "region").countField("amount").getOutputSchema();
        assertQuadrantConsistent("dag08", src -> src
                .filter("amount >= 0").filter("qty >= 2")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept", "region").countField("amount"))
                .filter("dept >= 1")
                .orderBy(OrderBy.of(cnt, new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                        new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), 1, false)
                .top(Top.of(cnt, 50, new SortField(2, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1)
                .select(0, 1));
    }

    @Test void dag09_wideGroupThenNarrow() { // filter,groupBy(3key),filter,select,groupBy(1key),orderBy,filter = 7
        DataSchema g3 = ParallelGroupByAgg.of(ORDER, "dept", "region", "grade").sum("amount").getOutputSchema();
        String sumCol = g3.getField(3).getName(); // "sum0"
        DataSchema narrowed = new DataSchema(g3.getField(0), g3.getField(3)); // [dept, sum0]
        DataSchema g1 = ParallelGroupByAgg.of(narrowed, "dept").sum(sumCol).getOutputSchema();
        assertQuadrantConsistent("dag09", src -> src
                .filter("amount >= 25")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept", "region", "grade").sum("amount"))
                .filter("region >= 1")
                .select(0, 3)  // [dept, sum0]
                .groupBy(ParallelGroupByAgg.of(narrowed, "dept").sum(sumCol))
                .orderBy(OrderBy.of(g1, new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), 1, false)
                .filter("dept < 49"));
    }

    @Test void dag10_longFilterGroupDistinctOrderTop() { // filter,filter,filter,groupBy,distinct,orderBy,top = 7
        DataSchema g = ParallelGroupByAgg.of(ORDER, "dept", "grade").sum("qty").getOutputSchema();
        assertQuadrantConsistent("dag10", src -> src
                .filter("amount >= 10").filter("amount < 950").filter("qty >= 1")
                .groupBy(ParallelGroupByAgg.of(ORDER, "dept", "grade").sum("qty"))
                .distinct(Distinct.byColumns(g, "dept", "grade"))
                .orderBy(OrderBy.of(g, new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                        new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)), 1, false)
                .top(Top.of(g, 40, new SortField(2, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)), 1));
    }
}

