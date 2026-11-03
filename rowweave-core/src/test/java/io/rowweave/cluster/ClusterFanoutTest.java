package io.rowweave.cluster;

import io.rowweave.RowWeave;
import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class ClusterFanoutTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();
    private static final DataSchema TIME_META = new DataSchema(new Field("ts", DataType.TimestampType));
    private static volatile CountDownLatch timestampMutated;
    private static final MapFunction MUTATE_TIMESTAMP = row -> {
        row.getTimestamp(0).setNanos(987_654_321);
        timestampMutated.countDown();
        return row;
    };
    private static final MapFunction OBSERVE_TIMESTAMP = row -> {
        try {
            if (!timestampMutated.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("mutating fan-out branch did not run");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(interrupted);
        }
        return row;
    };

    private static List<DataRow> rows() {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < 20000; i++) {
            DataRow r = new DataRow(2); r.set(0, i % 15); r.set(1, new BigDecimal(i % 100)); rs.add(r);
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

    private long[][] run(String name, int parallelism, io.rowweave.cluster.JobClient client, Source input, Path dir) {
        Path f1 = dir.resolve(name + "-1.tsv"), f2 = dir.resolve(name + "-2.tsv");
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(parallelism);
        DataSet mid = s.fromSource(input).filter("amt >= 10");
        DataSet b1 = mid.filter("k >= 7");
        b1.sinkTo(new FileSink(b1.getSchema(), f1.toString()));
        DataSet b2 = mid.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"));
        b2.sinkTo(new FileSink(GB, f2.toString()));
        s.execute();
        return new long[][]{fingerprint(f1), fingerprint(f2)};
    }

    @Test
    void clusterFanoutMatchesSingle(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows();
        Source input = ClusterTestFileInput.write(tmp.resolve("fanout-source.tsv"), META, data);
        long[][] oracle = run("fo-s1", 1, null, input, tmp);
        assertTrue(oracle[0][0] > 0 && oracle[1][0] > 0, "both branches emit output");

        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m1 = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m1.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m1.port(), w1)) {
            long[][] c1 = run("fo-c1", 1, new RealClusterJobClient("127.0.0.1", m1.port()), input, tmp);
            assertArrayEquals(oracle[0], c1[0], "cluster P1 fan-out branch 1 versus the single-process run");
            assertArrayEquals(oracle[1], c1[1], "cluster P1 fan-out branch 2 versus the single-process run");
        }
        try (MasterServer m2 = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m2.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m2.port(), w1)) {
            long[][] c2 = run("fo-c2", 2, new RealClusterJobClient("127.0.0.1", m2.port()), input, tmp);
            assertArrayEquals(oracle[0], c2[0], "cluster P2 fan-out branch 1 versus the single-process run");
            assertArrayEquals(oracle[1], c2[1], "cluster P2 fan-out branch 2 versus the single-process run");
        }
    }

    @Test
    void clusterFanoutMutableTimestampDoesNotBleed(@TempDir Path tmp) throws Exception {
        Path mutatedPath = tmp.resolve("mutated.tsv");
        Path observedPath = tmp.resolve("observed.tsv");
        Timestamp timestamp = new Timestamp(1_700_000_000_123L);
        timestamp.setNanos(123_456_789);
        DataRow row = new DataRow(1);
        row.set(0, timestamp);
        timestampMutated = new CountDownLatch(1);

        File workerDir = tmp.resolve("worker").toFile();
        assertTrue(workerDir.mkdirs());
        try (MasterServer master = new MasterServer(5000);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "snapshot-w0", 8, "127.0.0.1", master.port(), workerDir)) {
            ExecutionEnvironment session = new ExecutionEnvironment("snapshot-fanout", "snapshot-fanout",
                    new RealClusterJobClient("127.0.0.1", master.port()));
            session.setParallelism(1);
            DataSet shared = session.fromSource(new CollectionSource(TIME_META, Collections.singletonList(row)));
            shared.map(MUTATE_TIMESTAMP).sinkTo(new FileSink(TIME_META, mutatedPath.toString()));
            shared.map(OBSERVE_TIMESTAMP).sinkTo(new FileSink(TIME_META, observedPath.toString()));
            session.execute();
        }

        List<DataRow> mutated = FileSink.read(TIME_META, mutatedPath.toString());
        List<DataRow> observed = FileSink.read(TIME_META, observedPath.toString());
        assertEquals(1, mutated.size());
        assertEquals(1, observed.size());
        assertEquals(987_654_321, mutated.get(0).getTimestamp(0).getNanos());
        assertEquals(123_456_789, observed.get(0).getTimestamp(0).getNanos(),
                "a Timestamp mutated in one cluster branch must not reach the other Sink");
    }
}

