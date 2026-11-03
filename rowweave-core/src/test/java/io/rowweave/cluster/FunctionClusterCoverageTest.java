package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.Collector;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.FlatMapFunction;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.JoinFunction;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the **cluster path** of code-lambda Functions. The single-machine side has its own assertion
 * tests; this class adds the cluster path. It covers MapFunction, FlatMapFunction and JoinFunction
 * (through withFunc) — these are shipped as a serialized userFunction, which is a correctness-critical
 * path. Cluster P1 and P2 are compared against the single machine bit-identically (multiset
 * fingerprints). @Tag(multiprocess).
 */
@Tag("multiprocess")
class FunctionClusterCoverageTest {

    private static final DataSchema ORDER = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema DIM = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("label", DataType.IntegerType));

    private static List<DataRow> orders() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 3000; i++) { DataRow r = new DataRow(2); r.set(0, i % 12); r.set(1, new BigDecimal(i % 50)); rs.add(r); }
        return rs;
    }

    private static List<DataRow> dims() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 10; i++) { DataRow r = new DataRow(2); r.set(0, i); r.set(1, i * 100); rs.add(r); }
        return rs;
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

    private static final DataSchema MAP_OUT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt2", DataType.BigDecimalType));
    private static final MapFunction DOUBLE_AMT = in -> {
        DataRow o = new DataRow(2);
        o.set(0, in.get(0));
        o.set(1, ((BigDecimal) in.get(1)).multiply(new BigDecimal(2)));
        return o;
    };

    private static final FlatMapFunction EXPAND = (in, c) -> {
        int n = ((BigDecimal) in.get(1)).intValue() % 3 + 1;
        for (int i = 0; i < n; i++) {
            DataRow o = new DataRow(2); o.set(0, in.get(0)); o.set(1, in.get(1)); c.collect(o);
        }
    };

    private static final DataSchema JF_OUT = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("prod", DataType.BigDecimalType));
    private static final JoinFunction MUL = new JoinFunction() {
        @Override public DataSchema getResultSchema() { return JF_OUT; }
        @Override public DataRow join(DataRow left, DataRow right) {
            DataRow o = new DataRow(2);
            o.set(0, left.get(0));
            o.set(1, ((BigDecimal) left.get(1)).multiply(new BigDecimal((Integer) right.get(1))));
            return o;
        }
    };

    @FunctionalInterface
    private interface Scene { DataSet build(ExecutionEnvironment s); }

    private long[] run(String name, int parallelism, io.rowweave.cluster.JobClient client, DataSchema outMeta, Scene scene, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(parallelism);
        DataSet ds = scene.build(s);
        ds.sinkTo(new FileSink(outMeta, f.toString()));
        s.execute();
        return fingerprint(f);
    }

    private void assertClusterMatchesSingle(String name, DataSchema outMeta, Scene scene, Path tmp) throws Exception {
        long[] single = run(name + "-s", 1, null, outMeta, scene, tmp);
        assertTrue(single[0] > 0, name + " emits output");
        File w0 = new File(tmp.toFile(), name + "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), name + "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded(name + "w0", 4, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded(name + "w1", 4, "127.0.0.1", m.port(), w1)) {
            long[] c1 = run(name + "-c1", 1, new ClusterJobClient("127.0.0.1", m.port()), outMeta, scene, tmp);
            assertArrayEquals(single, c1, name + " cluster P1 versus the single-process run");
            long[] c2 = run(name + "-c2", 2, new ClusterJobClient("127.0.0.1", m.port()), outMeta, scene, tmp);
            assertArrayEquals(single, c2, name + " cluster P2 versus the single-process run");
        }
    }

    @Test
    void mapFunction_cluster(@TempDir Path tmp) throws Exception {
        assertClusterMatchesSingle("map", MAP_OUT,
                s -> s.fromSource(new CollectionSource(ORDER, orders())).map(DOUBLE_AMT), tmp);
    }

    @Test
    void flatMapFunction_cluster(@TempDir Path tmp) throws Exception {
        assertClusterMatchesSingle("flatmap", ORDER,
                s -> s.fromSource(new CollectionSource(ORDER, orders())).flatMap(EXPAND), tmp);
    }

    @Test
    void joinFunction_cluster(@TempDir Path tmp) throws Exception {
        assertClusterMatchesSingle("joinfn", JF_OUT,
                s -> {
                    DataSet l = s.fromSource(new CollectionSource(ORDER, orders()));
                    DataSet r = s.fromSource(new CollectionSource(DIM, dims()));
                    return l.join(r).withFunc("k", MUL);
                }, tmp);
    }

    @Test
    void mixedLambdaChain_cluster(@TempDir Path tmp) throws Exception {
        assertClusterMatchesSingle("mixed", ORDER,
                s -> s.fromSource(new CollectionSource(ORDER, orders()))
                        .flatMap(EXPAND)
                        .map(DOUBLE_AMT)
                        .filter("amt >= 10"), tmp);
    }
}

