package io.rowweave.monitor.http;

import io.rowweave.RowWeave;
import io.rowweave.DataSet;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.bench.CsvInput;
import io.rowweave.cluster.TenDagData;
import io.rowweave.ops.ParallelGroupByAgg;
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
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Self-test for the {@link RealProcessCluster} harness: it starts three real processes, one
 * master and two workers in separate JVMs, and commits a keyed job with parallelism at least two.
 * The result is checked against the single-machine output as a multiset, and both workers are
 * asserted to have taken part by reading instance distribution over the monitor HTTP API and
 * expecting at least two workers. That assertion is the guard for genuine cross-JVM distributed
 * execution; without it a test can repeat the mistake of RealProcessClusterTest, where no
 * parallelism was set, every task landed on one worker and socket shuffle never happened. Work runs
 * through BatchSession.commit with CsvInput rather than an embedded list source, which is
 * closer to real use and the only shape that survives process boundaries.
 */
@Tag("multiprocess")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RealProcessClusterHarnessTest {

    private Path dir;
    private String orders;
    private static final DataSchema ORDER = TenDagData.ORDER;
    private static final DataSchema GB = ParallelGroupByAgg.of(ORDER, "dept").sum("amount").getOutputSchema();

    @BeforeAll
    void setUp() throws Exception {
        dir = Files.createTempDirectory("realproc-harness");
        orders = TenDagData.orders(dir, "orders", 50_000);
    }

    @AfterAll
    void tearDown() {
        if (dir != null) deleteRec(dir.toFile());
    }

    private static void deleteRec(File f) {
        File[] ch = f.listFiles();
        if (ch != null) for (File c : ch) deleteRec(c);
        f.delete();
    }

    /** A keyed shuffle over source -> groupBy(dept).sum(amount), with the single machine at parallelism one as the oracle. */
    private List<String> runGroupBy(int par, io.rowweave.cluster.JobClient client, String jobName, Path out) {
        BatchSession s = client != null ? new BatchSession(jobName, jobName, client) : RowWeave.createBatchSession(jobName);
        s.setParallelism(par);
        DataSet src = s.fromSource(new CsvInput(orders, ORDER));
        src.groupBy(ParallelGroupByAgg.of(ORDER, "dept").sum("amount"))
                .sinkTo(new FileSink(GB, out.toString()));
        s.execute();
        return fingerprint(out);
    }

    private List<String> fingerprint(Path p) {
        List<String> rows = new ArrayList<>();
        for (DataRow r : FileSink.read(GB, p.toString())) {
            BigDecimal v = ((BigDecimal) r.get(1)).setScale(10, RoundingMode.HALF_UP);
            rows.add(r.get(0) + "|" + v.toPlainString());
        }
        rows.sort(null);
        return rows;
    }

    @Test
    void realThreeProcessClusterP2MatchesSingleAndUsesBothWorkers() throws Exception {
        List<String> oracle = runGroupBy(1, null, "single", dir.resolve("single.tsv"));
        assertFalse(oracle.isEmpty(), "single-process oracle produces output");

        Path childLogs = Files.createDirectory(dir.resolve("child-logs"));
        String key = "rowweave.benchmark.childLogDir";
        String traceKey = "rowweave.benchmark.traceHeartbeats";
        String previous = System.setProperty(key, childLogs.toString());
        String previousTrace = System.setProperty(traceKey, "true");
        try (RealProcessCluster cluster = new RealProcessCluster(2, 4)) {
            List<String> clusterResult = runGroupBy(2, cluster.newClient(), "rpc-p2", dir.resolve("cluster.tsv"));
            assertEquals(oracle, clusterResult, "the real 3-process cluster at P2 returns the same groupBy multiset as the single process");

            // Assert both workers really took part, by reading the instance distribution through the monitor HTTP API.
            List<String> jobIds = cluster.jobIds();
            assertFalse(jobIds.isEmpty(), "monitor should have recorded the job");
            Set<String> allWorkers = new java.util.LinkedHashSet<>();
            for (String jid : jobIds) {
                allWorkers.addAll(cluster.stageWorkerDistribution(jid));
            }
            assertTrue(allWorkers.size() >= 2,
                    "the stage instances of a P2 keyed job must really span 2 workers (cross-process shuffle happens), actual distribution:" + allWorkers);
            for (String role : new String[]{"master", "worker-0", "worker-1"}) {
                Path stdout = childLogs.resolve(role + ".stdout.log");
                Path jvm = childLogs.resolve(role + ".jvm.log");
                assertTrue(Files.exists(stdout), role + " child stdout must survive the run");
                assertTrue(Files.exists(jvm), role + " JVM activity must survive the run");
                assertTrue(Files.size(stdout) > 0L, role + " stdout must contain readiness");
            }
            long traceDeadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(4);
            while (System.nanoTime() < traceDeadline
                    && (!new String(Files.readAllBytes(childLogs.resolve("master.stdout.log")),
                            java.nio.charset.StandardCharsets.UTF_8).contains("heartbeat trace received")
                    || !new String(Files.readAllBytes(childLogs.resolve("worker-0.stdout.log")),
                            java.nio.charset.StandardCharsets.UTF_8).contains("heartbeat trace sent")
                    || !new String(Files.readAllBytes(childLogs.resolve("worker-1.stdout.log")),
                            java.nio.charset.StandardCharsets.UTF_8).contains("heartbeat trace sent"))) {
                Thread.sleep(100L);
            }
            assertTrue(new String(Files.readAllBytes(childLogs.resolve("master.stdout.log")),
                    java.nio.charset.StandardCharsets.UTF_8).contains("heartbeat trace received"),
                    new String(Files.readAllBytes(childLogs.resolve("master.stdout.log")),
                            java.nio.charset.StandardCharsets.UTF_8));
            assertTrue(new String(Files.readAllBytes(childLogs.resolve("worker-0.stdout.log")),
                    java.nio.charset.StandardCharsets.UTF_8).contains("heartbeat trace sent"));
            assertTrue(new String(Files.readAllBytes(childLogs.resolve("worker-1.stdout.log")),
                    java.nio.charset.StandardCharsets.UTF_8).contains("heartbeat trace sent"));
        } finally {
            if (previous == null) System.clearProperty(key);
            else System.setProperty(key, previous);
            if (previousTrace == null) System.clearProperty(traceKey);
            else System.setProperty(traceKey, previousTrace);
        }
    }
}
