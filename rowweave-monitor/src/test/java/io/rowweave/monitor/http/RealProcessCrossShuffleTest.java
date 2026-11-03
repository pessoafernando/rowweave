package io.rowweave.monitor.http;

import io.rowweave.RowWeave;
import io.rowweave.CoGroupFunction;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.bench.CsvInput;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.TenDagData;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.Union;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Representative correctness tests for cross-worker shuffle on a real three-process cluster: one
 * DAG each for binary join, coGroup, union and a diamond, with parallelism at least two. They
 * prove the socket shuffle really crosses JVMs, since instances land on two workers, and that the
 * result equals the single-machine oracle as a multiset. A single {@link RealProcessCluster} is
 * shared, started once in @BeforeAll and reused by every @Test, which keeps the fork cost down.
 * Work runs through BatchSession.commit with CsvInput instead of an embedded list source.
 */
@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RealProcessCrossShuffleTest {

    private static final DataSchema ORDER = TenDagData.ORDER;
    private static final DataSchema DIM = TenDagData.DIM;

    // coGroup on dept -> [dept, lc, rc]: rows in the left group and rows in the right group.
    private static final CoGroupFunction CG_FN = new CoGroupFunction() {
        private final DataSchema out = new DataSchema(new Field("dept", DataType.IntegerType),
                new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));
        @Override public DataSchema getResultSchema() { return out; }
        @Override public void coGroup(Iterable<DataRow> l, Iterable<DataRow> r, Collector c) {
            int lc = 0; Object k = null;
            for (DataRow x : l) { lc++; k = x.get(1); }
            int rc = 0;
            for (DataRow x : r) { rc++; if (k == null) k = x.get(1); }
            DataRow o = new DataRow(3); o.set(0, k); o.set(1, lc); o.set(2, rc); c.collect(o);
        }
    };

    private Path dir;
    private String ordersA, ordersB, supp, dim;
    private RealProcessCluster cluster;

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("realproc-xshuffle");
        ordersA = TenDagData.orders(dir, "ordersA", 40_000);
        ordersB = TenDagData.orders(dir, "ordersB", 40_000);
        supp = TenDagData.supplement(dir, "supp", 20_000, 20_000);
        dim = TenDagData.dim(dir);
        cluster = new RealProcessCluster(2, 32);  // one master and two workers, as separate processes
    }

    @AfterAll
    void tearDown() {
        if (cluster != null) cluster.close();
        if (dir != null) deleteRec(dir.toFile());
    }

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }

    private CsvInput csv(String path, DataSchema meta) { return new CsvInput(path, meta); }

    /** Run one DAG on the single machine at parallelism one as the oracle, then on the real cluster at parallelism two; assert the multisets agree and cluster instances span two workers. */
    private void assertClusterMatchesSingle(String name, DataSchema outMeta, Function<BatchSession, DataSet> build) throws Exception {
        Path single = dir.resolve(name + "-single.tsv");
        Path clusterOut = dir.resolve(name + "-cluster.tsv");

        BatchSession s1 = RowWeave.createBatchSession(name + "-s1");
        s1.setParallelism(1);
        build.apply(s1).sinkTo(new FileSink(outMeta, single.toString()));
        s1.execute();
        List<String> oracle = fingerprint(outMeta, single);
        assertFalse(oracle.isEmpty(), name + " single-process oracle produces output");

        BatchSession sc = new BatchSession(name + "-c2", name, cluster.newClient());
        sc.setParallelism(2);
        build.apply(sc).sinkTo(new FileSink(outMeta, clusterOut.toString()));
        sc.execute();
        assertEquals(oracle, fingerprint(outMeta, clusterOut), name + " real cluster P2 multiset equals the single process");

        // Assert instances really span two workers, read through the monitor HTTP API.
        Set<String> workers = new LinkedHashSet<>();
        for (String jid : cluster.jobIds()) {
            workers.addAll(cluster.stageWorkerDistribution(jid));
        }
        assertTrue(workers.size() >= 2, name + " must really span 2 workers (cross-JVM shuffle), actual:" + workers);
    }

    private List<String> fingerprint(DataSchema meta, Path p) {
        List<String> rows = new ArrayList<>();
        for (DataRow r : FileSink.read(meta, p.toString())) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < meta.getFieldCount(); i++) {
                Object v = r.get(i);
                if (v instanceof BigDecimal) v = ((BigDecimal) v).setScale(10, RoundingMode.HALF_UP).toPlainString();
                sb.append(v).append('|');
            }
            rows.add(sb.toString());
        }
        rows.sort(null);
        return rows;
    }

    // ---- Case 1: inner join then groupBy, so a keyed binary shuffle has two inbound edges crossing workers ----
    private static final DataSchema JOIN_GB =
            ParallelGroupByAgg.of(TenDagData.ORDER, "dept").sum("amount").getOutputSchema();

    @Test
    void innerJoinGroupBy() throws Exception {
        assertClusterMatchesSingle("join", JOIN_GB, s ->
                s.fromSource(csv(ordersA, ORDER))
                        .join(s.fromSource(csv(ordersB, ORDER))).on("id")
                        .select("id", "dept", "region", "grade", "amount", "qty")
                        .groupBy(ParallelGroupByAgg.of(ORDER, "dept").sum("amount")));
    }

    // ---- Case 2: coGroup on dept ----
    private static final DataSchema CG_OUT = new DataSchema(new Field("dept", DataType.IntegerType),
            new Field("lc", DataType.IntegerType), new Field("rc", DataType.IntegerType));

    @Test
    void coGroupOnDept() throws Exception {
        assertClusterMatchesSingle("cogroup", CG_OUT, s ->
                s.fromSource(csv(ordersA, ORDER))
                        .coGroup(s.fromSource(csv(supp, ORDER)), CG_FN).on("dept")
                        .filter("lc >= 1"));
    }

    // ---- Case 3: union then groupBy ----
    private static final DataSchema UNION_GB =
            ParallelGroupByAgg.of(TenDagData.ORDER, "region").max("amount").min("amount").count().getOutputSchema();

    @Test
    void unionGroupBy() throws Exception {
        assertClusterMatchesSingle("union", UNION_GB, s ->
                s.fromSource(csv(ordersA, ORDER))
                        .union(s.fromSource(csv(supp, ORDER)), Union.of(ORDER))
                        .partitionByHash("region")
                        .groupBy(ParallelGroupByAgg.of(ORDER, "region").max("amount").min("amount").count()));
    }

    // ---- Case 4: a diamond: source, two join branches, union, groupBy ----
    @Test
    void diamondJoinUnion() throws Exception {
        assertClusterMatchesSingle("diamond", JOIN_GB, s -> {
            DataSet b1 = s.fromSource(csv(ordersA, ORDER))
                    .join(s.fromSource(csv(dim, DIM))).on("dept")
                    .select("id", "dept", "region", "grade", "amount", "qty")
                    .partitionByHash("dept");
            DataSet b2 = s.fromSource(csv(ordersB, ORDER))
                    .filter("amount >= 100")
                    .partitionByHash("dept");
            return b1.union(b2, Union.of(b1.getSchema()))
                    .groupBy(ParallelGroupByAgg.of(ORDER, "dept").sum("amount"));
        });
    }
}
