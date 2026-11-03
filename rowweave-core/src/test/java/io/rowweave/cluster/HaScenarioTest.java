package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.Source;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class HaScenarioTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < n; i++) { DataRow r = new DataRow(2); r.set(0, i % 8); r.set(1, new BigDecimal(i % 50)); rs.add(r); }
        return rs;
    }

    private static final MapFunction SLOW_MAP = in -> {
        try { Thread.sleep(1); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        DataRow o = new DataRow(2); o.set(0, in.get(0)); o.set(1, in.get(1)); return o;
    };

    private static long countLines(Path p) {
        try { return Files.lines(p).count(); } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static void submitGroupBy(BatchSession s, Source input, Path out, MapFunction map) {
        io.rowweave.DataSet ds = s.fromSource(input);
        if (map != null) ds = ds.map(map);
        ds.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
          .sinkTo(new FileSink(ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema(), out.toString()));
    }

    @Test
    void dynamicallyAddedWorkerServesSubsequentJob(@TempDir Path tmp) throws Exception {
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0)) {
            BatchSession s1 = new BatchSession("j1", "j1", new ClusterJobClient("127.0.0.1", m.port()));
            s1.setParallelism(1);
            Path f1 = tmp.resolve("j1.tsv");
            submitGroupBy(s1, new CollectionSource(META, rows(800)), f1, null);
            s1.execute();
            assertEquals(8, countLines(f1), "the baseline job (1 worker) completes");

            try (WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
                Thread.sleep(300);
                assertTrue(m.workers().size() >= 2, "the master sees the dynamically added worker (local heartbeat map)");

                BatchSession s2 = new BatchSession("j2", "j2", new ClusterJobClient("127.0.0.1", m.port()));
                s2.setParallelism(2);
                Path f2 = tmp.resolve("j2.tsv");
                submitGroupBy(s2, new CollectionSource(META, rows(1600)), f2, null);
                s2.execute();
                assertEquals(8, countLines(f2), "the following P=2 job succeeds (the dynamically added worker gets assigned)");
            }
        }
    }

    @Test
    void dynamicWorkerDeathFailsFastWithClientException(@TempDir Path tmp) throws Exception {
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        MasterServer m = new MasterServer(2000); // staleness 2s
        WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
        WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1);
        try {
            BatchSession s = new BatchSession("dead", "dead", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("dead.tsv");
            submitGroupBy(s, ClusterTestFileInput.write(
                    tmp.resolve("dead-source.tsv"), META, rows(200000)), f, SLOW_MAP);

            final Throwable[] err = new Throwable[1];
            Thread committer = new Thread(() -> {
                try { s.execute(); } catch (Throwable t) { err[0] = t; }
            });
            committer.start();
            awaitRunning(a1);
            a1.close();
            committer.join(30000);
            assertFalse(committer.isAlive(), "with a dead worker the job must not hang forever (progress-based fail fast)");
            assertNotNull(err[0], "a dead worker makes the job fail and the client observes the exception");
            assertTrue(String.valueOf(err[0]).toLowerCase().contains("worker lost")
                            || String.valueOf(err[0]).toLowerCase().contains("fail")
                            || String.valueOf(err[0]).toLowerCase().contains("reset")
                            || String.valueOf(err[0]).toLowerCase().contains("closed"),
                    "the failure should point at a lost worker, actual: " + err[0]);
        } finally {
            a0.close();
            try { a1.close(); } catch (RuntimeException ignored) { }
            m.close();
        }
    }

    private static void awaitRunning(WorkerServer worker) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000L;
        while (worker.runningStageCount() == 0 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(worker.runningStageCount() > 0, "the target Worker must already hold the in-flight stage before the fault is injected");
    }

    @Test
    void inflightJobFailsFastWhenMasterDies(@TempDir Path tmp) throws Exception {
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        MasterServer m = new MasterServer(5000);
        WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
        WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1);
        try {
            BatchSession s = new BatchSession("mdie", "mdie", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("mdie.tsv");
            submitGroupBy(s, ClusterTestFileInput.write(
                    tmp.resolve("master-death-source.tsv"), META, rows(200000)), f, SLOW_MAP);

            final Throwable[] err = new Throwable[1];
            Thread committer = new Thread(() -> {
                try { s.execute(); } catch (Throwable t) { err[0] = t; }
            });
            committer.start();
            Thread.sleep(500);
            m.close();
            committer.join(30000);
            assertFalse(committer.isAlive(), "with the master down an in-flight job must not hang forever");
            assertNotNull(err[0], "a master failure makes the in-flight job fail fast and the client observes the exception (no continuation)");
        } finally {
            a0.close();
            a1.close();
            try { m.close(); } catch (RuntimeException ignored) { }
        }
    }
}

