package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.Union;
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

@Tag("multiprocess")
class ClusterUnionTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> rows(int base, int n) {
        List<DataRow> rs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            DataRow r = new DataRow(2);
            r.set(0, base + i % 20);
            r.set(1, new BigDecimal(i % 100));
            rs.add(r);
        }
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

    private long[] run(String name, int parallelism, io.rowweave.cluster.JobClient client,
                       List<DataRow> a, List<DataRow> b, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(parallelism);
        DataSet ua = s.fromSource(new CollectionSource(META, a));
        DataSet ub = s.fromSource(new CollectionSource(META, b));
        DataSet u = ua.union(ub, Union.of(META)).filter("amt >= 10");
        u.sinkTo(new FileSink(META, f.toString()));
        s.execute();
        return fingerprint(f);
    }

    @Test
    void clusterUnionMatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> a = rows(0, 3000), b = rows(100, 4000);
        long[] oracle = run("u-s1", 1, null, a, b, tmp);
        assertTrue(oracle[0] > 0, "union+filter emits output");

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m1 = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m1.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m1.port(), w1)) {
            long[] c1 = run("u-c1", 1, new RealClusterJobClient("127.0.0.1", m1.port()), a, b, tmp);
            assertArrayEquals(oracle, c1, "cluster P1 union differs from the single-process reference multiset");
        }
        try (MasterServer m2 = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m2.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m2.port(), w1)) {
            long[] c2 = run("u-c2", 2, new RealClusterJobClient("127.0.0.1", m2.port()), a, b, tmp);
            assertArrayEquals(oracle, c2, "cluster P2 union differs from the single-process reference multiset");
        }
    }
}

