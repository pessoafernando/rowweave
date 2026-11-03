package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.Collector;
import io.rowweave.CoGroupFunction;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.Union;
import io.rowweave.ops.join.CoGroup;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
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
class ComplexMultiIoDagQuadrantTest {

    private static final int A_ROWS = Integer.getInteger("rowweave.mio.rows.a", 800);
    private static final int B_ROWS = Integer.getInteger("rowweave.mio.rows.b", 600);

    private static final DataSchema A = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("va", DataType.BigDecimalType));
    private static final DataSchema B = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("vb", DataType.BigDecimalType));
    private static final DataSchema JOIN_AB = SortMergeJoin.of(A, B, new String[]{"k"}, new String[]{"k"}, JoinType.INNER).getOutputSchema();
    private static final DataSchema CG_OUT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("ac", DataType.IntegerType), new Field("bc", DataType.IntegerType));

    private static final CoGroupFunction CG_FN = new CoGroupFunction() {
        @Override public DataSchema getResultSchema() { return CG_OUT; }
        @Override public void coGroup(Iterable<DataRow> it1, Iterable<DataRow> it2, Collector c) {
            int ac = 0; Object key = null;
            for (DataRow r : it1) { ac++; key = r.get(0); }
            int bc = 0;
            for (DataRow r : it2) { bc++; if (key == null) key = r.get(0); }
            DataRow o = new DataRow(3); o.set(0, key); o.set(1, ac); o.set(2, bc); c.collect(o);
        }
    };

    private List<DataRow> dataA, dataB;
    private MasterServer master1, master4;
    private WorkerServer[] workers;
    private Path tmpDir;

    @BeforeAll
    void setUp() throws Exception {
        dataA = new ArrayList<>(); dataB = new ArrayList<>();
        for (int i = 0; i < A_ROWS; i++) { DataRow r = new DataRow(2); r.set(0, i % 12); r.set(1, new BigDecimal(i % 50)); dataA.add(r); }
        for (int i = 0; i < B_ROWS; i++) { DataRow r = new DataRow(2); r.set(0, i % 10); r.set(1, new BigDecimal(i % 40)); dataB.add(r); }
        tmpDir = Files.createTempDirectory("mio");
        master1 = new MasterServer(5000);
        master4 = new MasterServer(5000);
        workers = new WorkerServer[6];
        workers[0] = worker("m1w0", master1); workers[1] = worker("m1w1", master1);
        workers[2] = worker("m4w0", master4); workers[3] = worker("m4w1", master4);
        workers[4] = worker("m4w2", master4); workers[5] = worker("m4w3", master4);
    }

    private WorkerServer worker(String id, MasterServer m) throws Exception {
        File d = new File(tmpDir.toFile(), id); d.mkdirs();
        return WorkerServer.newUnguarded(id, 4, "127.0.0.1", m.port(), d);
    }

    @AfterAll
    void tearDown() {
        if (workers != null) for (WorkerServer w : workers) if (w != null) w.close();
        if (master1 != null) master1.close();
        if (master4 != null) master4.close();
    }

    @FunctionalInterface
    private interface MultiIoDag { void build(ExecutionEnvironment s, DataSet srcA, DataSet srcB, List<FileSink> outs); }

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

    private List<long[]> runQuad(String name, int parallelism, io.rowweave.cluster.JobClient client, int nOut, MultiIoDag dag) {
        List<FileSink> outs = new ArrayList<>();
        List<Path> paths = new ArrayList<>();
        for (int i = 0; i < nOut; i++) {
            Path f = tmpDir.resolve(name + "-o" + i + ".tsv");
            paths.add(f);
        }
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(parallelism);
        DataSet srcA = s.fromSource(new CollectionSource(A, dataA));
        DataSet srcB = s.fromSource(new CollectionSource(B, dataB));
        MultiIoCollector collector = new MultiIoCollector(paths);
        dag.build(s, srcA, srcB, collector);
        s.execute();
        List<long[]> fps = new ArrayList<>();
        for (Path p : paths.subList(0, collector.used)) fps.add(fingerprint(p));
        return fps;
    }

    private static final class MultiIoCollector extends ArrayList<FileSink> {
        final List<Path> paths; int used = 0;
        MultiIoCollector(List<Path> paths) { this.paths = paths; }
        FileSink out(DataSchema meta) { FileSink f = new FileSink(meta, paths.get(used++).toString()); return f; }
    }

    private void assertQuadrant(String name, int nOut, MultiIoDag dag) {
        List<long[]> oracle = runQuad(name + "-s1", 1, null, nOut, dag);
        List<long[]> sP4 = runQuad(name + "-s4", 4, null, nOut, dag);
        List<long[]> cP1 = runQuad(name + "-c1", 1, new ClusterJobClient("127.0.0.1", master1.port()), nOut, dag);
        List<long[]> cP4 = runQuad(name + "-c4", 4, new ClusterJobClient("127.0.0.1", master4.port()), nOut, dag);
        assertEquals(nOut, oracle.size(), name + " output count");
        for (int i = 0; i < nOut; i++) {
            assertArrayEquals(oracle.get(i), sP4.get(i), name + " out" + i + " single-process P4 versus the reference");
            assertArrayEquals(oracle.get(i), cP1.get(i), name + " out" + i + " cluster P1 versus the reference");
            assertArrayEquals(oracle.get(i), cP4.get(i), name + " out" + i + " cluster P4 versus the reference");
        }
        System.out.printf("[mio] %-24s %d outputs all four quadrants agree%n", name, nOut);
    }

    @Test void mio1_joinFanoutTwoOut() {
        assertQuadrant("mio1", 2, (s, a, b, outs) -> {
            DataSet j = a.join(b, SortMergeJoin.of(A, B, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
            DataSet o1 = j.filter("va >= 10");
            o1.sinkTo(((MultiIoCollector) outs).out(o1.getSchema()));
            DataSet o2 = j.filter("vb >= 5");
            o2.sinkTo(((MultiIoCollector) outs).out(o2.getSchema()));
        });
    }

    @Test void mio2_unionAndGroupTwoOut() {
        assertQuadrant("mio2", 2, (s, a, b, outs) -> {
            DataSet a2 = s.fromSource(new CollectionSource(A, dataA));
            DataSet u = a.union(a2, Union.of(A)).filter("va >= 8");
            u.sinkTo(((MultiIoCollector) outs).out(u.getSchema()));
            DataSet g = b.groupBy(ParallelGroupByAgg.of(B, "k").sum("vb"));
            g.sinkTo(((MultiIoCollector) outs).out(g.getSchema()));
        });
    }

    @Test void mio3_coGroupFanout() {
        assertQuadrant("mio3", 2, (s, a, b, outs) -> {
            DataSet cg = a.coGroup(b, CoGroup.of(A, B, new String[]{"k"}, new String[]{"k"}, CG_FN));
            DataSet o1 = cg.filter("ac >= 1");
            o1.sinkTo(((MultiIoCollector) outs).out(o1.getSchema()));
            DataSet o2 = cg.groupBy(ParallelGroupByAgg.of(CG_OUT, "k").sum("bc"));
            o2.sinkTo(((MultiIoCollector) outs).out(o2.getSchema()));
        });
    }

    @Test void mio4_diamondPlusIndependent() {
        DataSchema gA = ParallelGroupByAgg.of(A, "k").sum("va").getOutputSchema();
        assertQuadrant("mio4", 2, (s, a, b, outs) -> {
            DataSet mid = a.filter("va >= 5");
            DataSet b1 = mid.filter("k >= 2");
            DataSet joined = b1.join(a.filter("va >= 5").filter("k <= 9"),
                    SortMergeJoin.of(A, A, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
            joined.sinkTo(((MultiIoCollector) outs).out(joined.getSchema()));
            DataSet g = b.groupBy(ParallelGroupByAgg.of(B, "k").sum("vb"));
            g.sinkTo(((MultiIoCollector) outs).out(g.getSchema()));
        });
    }

    @Test void mio5_threeOutputs() {
        DataSchema jgb = ParallelGroupByAgg.of(JOIN_AB, "k").sum("va").getOutputSchema();
        assertQuadrant("mio5", 3, (s, a, b, outs) -> {
            DataSet j = a.join(b, SortMergeJoin.of(A, B, new String[]{"k"}, new String[]{"k"}, JoinType.INNER))
                    .groupBy(ParallelGroupByAgg.of(JOIN_AB, "k").sum("va"));
            j.sinkTo(((MultiIoCollector) outs).out(j.getSchema()));
            DataSet o2 = a.filter("va >= 20");
            o2.sinkTo(((MultiIoCollector) outs).out(o2.getSchema()));
            DataSet o3 = b.distinct(io.rowweave.ops.Distinct.byColumns(B, "k"));
            o3.sinkTo(((MultiIoCollector) outs).out(o3.getSchema()));
        });
    }
}

