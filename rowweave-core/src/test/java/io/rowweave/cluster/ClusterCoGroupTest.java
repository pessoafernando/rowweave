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
import io.rowweave.ops.join.CoGroup;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterCoGroupTest {

    private static final DataSchema LEFT = new DataSchema(
            new Field("lk", DataType.IntegerType), new Field("lv", DataType.StringType));
    private static final DataSchema RIGHT = new DataSchema(
            new Field("rk", DataType.IntegerType), new Field("rv", DataType.StringType));
    private static final DataSchema OUT = new DataSchema(
            new Field("k", DataType.IntegerType),
            new Field("lc", DataType.IntegerType),
            new Field("rc", DataType.IntegerType));

    private static final CoGroupFunction COUNT_FN = new CoGroupFunction() {
        @Override public DataSchema getResultSchema() { return OUT; }
        @Override public void coGroup(Iterable<DataRow> it1, Iterable<DataRow> it2, Collector c) {
            int lc = 0; Object key = null;
            for (DataRow r : it1) { lc++; key = r.get(0); }
            int rc = 0;
            for (DataRow r : it2) { rc++; if (key == null) key = r.get(0); }
            DataRow o = new DataRow(3); o.set(0, key); o.set(1, lc); o.set(2, rc);
            c.collect(o);
        }
    };

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

    private long[] run(String name, int parallelism, io.rowweave.cluster.JobClient client,
                       List<DataRow> left, List<DataRow> right, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(parallelism);
        DataSet l = s.fromSource(new CollectionSource(LEFT, left));
        DataSet r = s.fromSource(new CollectionSource(RIGHT, right));
        DataSet cg = l.coGroup(r, CoGroup.of(LEFT, RIGHT, new String[]{"lk"}, new String[]{"rk"}, COUNT_FN));
        cg.sinkTo(new FileSink(OUT, f.toString()));
        s.execute();
        return fingerprint(f);
    }

    @Test
    void clusterCoGroupMatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> left = new ArrayList<>(), right = new ArrayList<>();
        for (int i = 0; i < 5000; i++) {
            DataRow l = new DataRow(2); l.set(0, i % 30); l.set(1, "l" + i); left.add(l);
        }
        for (int i = 0; i < 4000; i++) {
            DataRow r = new DataRow(2); r.set(0, i % 25); r.set(1, "r" + i); right.add(r);
        }
        long[] oracle = run("cg-s1", 1, null, left, right, tmp);
        assertTrue(oracle[0] > 0, "coGroup emits output");

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m1 = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m1.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m1.port(), w1)) {
            long[] c1 = run("cg-c1", 1, new RealClusterJobClient("127.0.0.1", m1.port()), left, right, tmp);
            assertArrayEquals(oracle, c1, "cluster P1 coGroup differs from the single-process reference multiset");
        }
        try (MasterServer m2 = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m2.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m2.port(), w1)) {
            long[] c2 = run("cg-c2", 2, new RealClusterJobClient("127.0.0.1", m2.port()), left, right, tmp);
            assertArrayEquals(oracle, c2, "cluster P2 coGroup differs from the single-process reference multiset");
        }
    }
}

