package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.join.JoinType;
import io.rowweave.ops.join.SortMergeJoin;
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
class ClusterDiamondReJoinTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> rows() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 3000; i++) { DataRow r = new DataRow(2); r.set(0, i % 12); r.set(1, new BigDecimal(i % 60)); rs.add(r); }
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

    private long[] run(String name, int parallelism, io.rowweave.cluster.JobClient client, List<DataRow> data, Path dir) {
        Path f = dir.resolve(name + ".tsv");
        BatchSession s = client != null ? new BatchSession(name, name, client) : RowWeave.createBatchSession(name);
        s.setParallelism(parallelism);
        DataSet mid = s.fromSource(new CollectionSource(META, data)).filter("amt >= 5");
        DataSet b1 = mid.filter("k >= 2");
        DataSet b2 = mid.filter("k <= 9");
        DataSet joined = b1.join(b2, SortMergeJoin.of(META, META, new String[]{"k"}, new String[]{"k"}, JoinType.INNER));
        joined.sinkTo(new FileSink(joined.getSchema(), f.toString()));
        s.execute();
        return fingerprint(f);
    }

    @Test
    void clusterDiamondMatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows();
        long[] oracle = run("dia-s1", 1, null, data, tmp);
        assertTrue(oracle[0] > 0, "the diamond emits output");

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m1 = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m1.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m1.port(), w1)) {
            long[] c1 = run("dia-c1", 1, new ClusterJobClient("127.0.0.1", m1.port()), data, tmp);
            assertArrayEquals(oracle, c1, "cluster P1 diamond versus the single-process reference");
        }
        try (MasterServer m2 = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m2.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m2.port(), w1)) {
            long[] c2 = run("dia-c2", 2, new ClusterJobClient("127.0.0.1", m2.port()), data, tmp);
            assertArrayEquals(oracle, c2, "cluster P2 diamond versus the single-process reference");
        }
    }
}

