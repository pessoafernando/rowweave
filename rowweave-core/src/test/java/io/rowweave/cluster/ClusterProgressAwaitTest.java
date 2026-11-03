package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
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
class ClusterProgressAwaitTest {

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

    @Test
    void slowButAliveJobNotKilled(@TempDir Path tmp) throws Exception {
        String timeoutKey = "rowweave.cluster.submitTimeoutMs";
        String cleanupKey = "rowweave.cluster.submitCleanupGraceMs";
        String previousTimeout = System.setProperty(timeoutKey, "10000");
        String previousCleanup = System.setProperty(cleanupKey, "3000");
        try {
            List<DataRow> data = rows(3000);
            File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
            File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
            try (MasterServer m = new MasterServer(5000);
                 WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
                 WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
                BatchSession s = new BatchSession("slow", "slow", new ClusterJobClient("127.0.0.1", m.port()));
                s.setParallelism(2);
                Path f = tmp.resolve("slow.tsv");
                s.fromSource(new CollectionSource(META, data))
                        .map(SLOW_MAP)
                        .groupBy(io.rowweave.ops.ParallelGroupByAgg.of(META, "k").sum("amt"))
                        .sinkTo(new FileSink(
                                io.rowweave.ops.ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema(),
                                f.toString()));
                long t0 = System.currentTimeMillis();
                s.execute();
                long ms = System.currentTimeMillis() - t0;
                assertEquals(8, countLines(f), "8 groups keyed by k, the slow job completes (not killed by mistake)");
                assertTrue(ms >= 1000, "the job really is slow (took " + ms + "ms, the sleeps accumulate)");
                assertTrue(ms < 10_000, "the slow job should finish inside the requested execution budget: " + ms + "ms");
            }
        } finally {
            restore(timeoutKey, previousTimeout);
            restore(cleanupKey, previousCleanup);
        }
    }

    @Test
    void deadWorkerFailsFast(@TempDir Path tmp) throws Exception {
        List<DataRow> data = rows(200000);
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        MasterServer m = new MasterServer(2000); // staleness 2s
        WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
        WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1);
        try {
            BatchSession s = new BatchSession("dead", "dead", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("dead.tsv");
            DataSet ds = s.fromSource(ClusterTestFileInput.write(
                            tmp.resolve("dead-source.tsv"), META, data))
                    .map(SLOW_MAP)
                    .groupBy(io.rowweave.ops.ParallelGroupByAgg.of(META, "k").sum("amt"));
            ds.sinkTo(new FileSink(ds.getSchema(), f.toString()));

            final Throwable[] err = new Throwable[1];
            Thread committer = new Thread(() -> {
                try { s.execute(); } catch (Throwable t) { err[0] = t; }
            });
            committer.start();
            awaitRunning(a0);
            a0.close();
            committer.join(30000);
            assertFalse(committer.isAlive(), "with a dead worker the job must not hang forever (progress-based detection fails fast)");
            assertNotNull(err[0], "a dead worker makes the job fail");
            assertTrue(String.valueOf(err[0]).toLowerCase().contains("worker lost")
                            || String.valueOf(err[0]).toLowerCase().contains("fail")
                            || String.valueOf(err[0]).toLowerCase().contains("reset")
                            || String.valueOf(err[0]).toLowerCase().contains("closed"),
                    "the failure should point at a lost worker or a dropped connection, actual: " + err[0]);
        } finally {
            a1.close();
            try { a0.close(); } catch (RuntimeException ignored) { }
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

    private static void restore(String key, String previous) {
        if (previous == null) System.clearProperty(key);
        else System.setProperty(key, previous);
    }
}

