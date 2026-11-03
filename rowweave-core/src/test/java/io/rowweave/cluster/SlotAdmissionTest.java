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
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class SlotAdmissionTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    @BeforeEach
    void isolateAdmissionLedgerFromDowngradePolicy() {
        System.setProperty("rowweave.cluster.downgrade", "false");
    }

    @AfterEach
    void clearProps() {
        System.clearProperty("rowweave.cluster.admission");
        System.clearProperty("rowweave.cluster.admitTimeoutMs");
        System.clearProperty("rowweave.cluster.downgrade");
        System.clearProperty("rowweave.cluster.downgradeGraceMs");
    }

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            DataRow r = new DataRow(2);
            r.set(0, i % 20);
            r.set(1, new BigDecimal(i % 100));
            rs.add(r);
        }
        return rs;
    }

    private long runGroupBy(int par, JobClient client, String name, Path out) {
        ExecutionEnvironment s = client != null ? new ExecutionEnvironment(name, name, client) : RowWeave.createEnvironment(name);
        s.setParallelism(par);
        s.fromSource(new CollectionSource(META, rows(4000)))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                .sinkTo(new FileSink(GB, out.toString()));
        s.execute();
        long n = 0;
        for (DataRow ignored : FileSink.read(GB, out.toString())) n++;
        return n;
    }

    @Test
    void admissionGrantsWhenEnoughSlots(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            long n = runGroupBy(2, new RealClusterJobClient("127.0.0.1", m.port()), "adm-ok", tmp.resolve("ok.tsv"));
            assertEquals(20, n, "admission passes and groupBy yields 20 groups");
        }
    }

    @Test
    void admissionRejectsWhenInsufficient(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        System.setProperty("rowweave.cluster.admitTimeoutMs", "1000");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 1, "127.0.0.1", m.port(), w0)) {
            ExecutionEnvironment s = new ExecutionEnvironment("adm-rej", "adm-rej", new RealClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            s.fromSource(new CollectionSource(META, rows(4000)))
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                    .sinkTo(new FileSink(GB, tmp.resolve("rej.tsv").toString()));
            RuntimeException ex = assertThrows(RuntimeException.class, s::execute);
            assertTrue(String.valueOf(ex.getMessage()).contains("slot")
                            || String.valueOf(ex.getMessage()).toLowerCase().contains("insufficient"),
                    "fails because slots are insufficient: " + ex.getMessage());
        }
    }

    /**
     * admission=false only bypasses the master's slot ledger, never the worker's hard physical slot
     * limit. The master must not report insufficient first; once a real ASSIGN reaches the worker it must
     * fail closed, and capacity returns after the accepted stage is cancelled.
     */
    @Test
    void admissionBypassDoesNotDisableWorkerCapacityFence(@TempDir Path tmp) throws Exception {
        System.setProperty("rowweave.cluster.admission", "false");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 1, "127.0.0.1", m.port(), w0)) {
            RuntimeException ex = assertThrows(RuntimeException.class, () ->
                    runGroupBy(2, new RealClusterJobClient("127.0.0.1", m.port()),
                            "adm-off-over-cap", tmp.resolve("off-over-cap.tsv")));
            String msg = String.valueOf(ex.getMessage());
            assertTrue(msg.contains("stage assignment capacity exhausted"),
                    "with the Master bypassed the Worker physical capacity gate refuses: " + msg);
            assertFalse(msg.contains("insufficient cluster slots"),
                    "admission=false must not take the Master slot refusal path: " + msg);

            long deadline = System.currentTimeMillis() + 5000;
            while (a0.runningStageCount() != 0 && System.currentTimeMillis() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(0, a0.runningStageCount(), "a failed assignment converges by cancelling the stages this Job already accepted");
            assertEquals(1L, a0.freeSlots(), "failure convergence returns the Worker capacity in full");
        }
    }

    /** With admission=false and physical capacity to spare, the job still runs normally; only the master ledger reservation and its degradation are skipped. */
    @Test
    void admissionCanBeDisabledWithinWorkerCapacity(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "false");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 8, "127.0.0.1", m.port(), w0)) {
            long n = runGroupBy(2, new RealClusterJobClient("127.0.0.1", m.port()),
                    "adm-off-safe", tmp.resolve("off-safe.tsv"));
            assertEquals(20, n, "with Master admission off and enough Worker capacity the job completes normally");
        }
    }

    @Test
    void slotsReleasedAfterJobCompletes(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {
            RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", m.port());
            assertEquals(20, runGroupBy(2, client, "seq1", tmp.resolve("s1.tsv")), "job1");
            assertEquals(20, runGroupBy(2, client, "seq2", tmp.resolve("s2.tsv")), "job2 runs after the release");
        }
    }

    @Test
    void gangSchedulingNoDeadlockWhenSlotsExactlyEnough(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(30), () -> {
            try (MasterServer m = new MasterServer(5000);
                 WorkerServer a0 = WorkerServer.newUnguarded("w0", 8, "127.0.0.1", m.port(), w0);
                 WorkerServer a1 = WorkerServer.newUnguarded("w1", 8, "127.0.0.1", m.port(), w1)) {
                long n = runGroupBy(2, new RealClusterJobClient("127.0.0.1", m.port()), "gang", tmp.resolve("gang.tsv"));
                assertEquals(20, n, "gang admission for the whole job pipelines its stages without deadlock and finishes 20 groups");
            }
        });
    }

    @Test
    void demandExactBoundaryGrantsAndRejects(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        System.setProperty("rowweave.cluster.admitTimeoutMs", "1500");
        int demand;
        File probe = new File(tmp.toFile(), "probe"); probe.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 128, "127.0.0.1", m.port(), probe)) {
            assertEquals(20, runGroupBy(2, new RealClusterJobClient("127.0.0.1", m.port()),
                    "probe", tmp.resolve("probe.tsv")), "runs to completion with ample slots");
            PhysicalPlan plan = m.lastPhysicalPlan();
            assertNotNull(plan, "a physical plan exists");
            demand = plan.isWholeGraph() ? 1 : plan.instances().size();
            assertTrue(demand >= 2, "the boundary method needs demand >= 2 (measured demand for a single worker keyed job =" + demand + ")");
        }
        File wOk = new File(tmp.toFile(), "wOk"); wOk.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", demand, "127.0.0.1", m.port(), wOk)) {
            assertEquals(20, runGroupBy(2, new RealClusterJobClient("127.0.0.1", m.port()),
                    "exact", tmp.resolve("exact.tsv")),
                    "slots == demand(" + demand + ") is exactly enough and passes because demand is not over-counted");
        }
        File wBad = new File(tmp.toFile(), "wBad"); wBad.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", demand - 1, "127.0.0.1", m.port(), wBad)) {
            ExecutionEnvironment s = new ExecutionEnvironment("under", "under", new RealClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            s.fromSource(new CollectionSource(META, rows(4000)))
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                    .sinkTo(new FileSink(GB, tmp.resolve("under.tsv").toString()));
            RuntimeException ex = assertThrows(RuntimeException.class, s::execute);
            String msg = String.valueOf(ex.getMessage());
            assertTrue(msg.contains("insufficient"), "slots == demand-1 is refused for shortage (demand is not under-counted and never oversold): " + msg);
            assertTrue(msg.contains("need=") && msg.contains("free=") && msg.contains("cap="),
                    "the error carries per-worker need/free/cap diagnostics: " + msg);
            assertTrue(msg.contains("totalDemand=" + demand),
                    "the error totalDemand equals the real demand (" + demand + "):" + msg);
        }
    }

    @Test
    void secondJobWaitsThenSucceedsAfterFirstReleases(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        System.setProperty("rowweave.cluster.admitTimeoutMs", "30000");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(40), () -> {
            // One worker's slots are exactly enough for a single job (generous values so the first job passes; a second concurrent job waits because the ledger is full).
            try (MasterServer m = new MasterServer(5000);
                 WorkerServer a0 = WorkerServer.newUnguarded("w0", 8, "127.0.0.1", m.port(), w0)) {
                RealClusterJobClient client = new RealClusterJobClient("127.0.0.1", m.port());
                java.util.concurrent.ExecutorService pool = java.util.concurrent.Executors.newFixedThreadPool(2);
                try {
                    java.util.concurrent.Future<Long> j1 = pool.submit(() ->
                            runGroupBy(2, client, "wait1", tmp.resolve("wait1.tsv")));
                    java.util.concurrent.Future<Long> j2 = pool.submit(() ->
                            runGroupBy(2, client, "wait2", tmp.resolve("wait2.tsv")));
                    assertEquals(20, j1.get(35, java.util.concurrent.TimeUnit.SECONDS), "job1 completes");
                    assertEquals(20, j2.get(35, java.util.concurrent.TimeUnit.SECONDS), "job2 completes after waiting for the release");
                } finally {
                    pool.shutdownNow();
                }
            }
        });
    }
}

