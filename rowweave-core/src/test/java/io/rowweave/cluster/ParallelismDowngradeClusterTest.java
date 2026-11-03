package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.AfterEach;
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
class ParallelismDowngradeClusterTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    @AfterEach
    void clearProps() {
        System.clearProperty("rowweave.cluster.admission");
        System.clearProperty("rowweave.cluster.downgrade");
        System.clearProperty("rowweave.cluster.downgradeGraceMs");
        System.clearProperty("rowweave.cluster.admitTimeoutMs");
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

    private long runGroupBy(int par, MasterServer m, String name, Path out) {
        BatchSession s = new BatchSession(name, name, new ClusterJobClient("127.0.0.1", m.port()));
        s.setParallelism(par);
        s.fromSource(new CollectionSource(META, rows(4000)))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                .orderBy("k")
                .sinkTo(new FileSink(GB, out.toString()));
        s.execute();
        long n = 0;
        for (DataRow ignored : FileSink.read(GB, out.toString())) n++;
        return n;
    }

    private int keyedStageInstances(MasterServer m) {
        PhysicalPlan plan = m.lastPhysicalPlan();
        int max = 1;
        for (int st = 0; st < 8; st++) {
            int c = plan.instancesOf(st).size();
            if (c > max) max = c;
        }
        return max;
    }

    @Test
    void downgradesToFitAndResultCorrect(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        System.setProperty("rowweave.cluster.downgradeGraceMs", "200");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        assertTimeoutPreemptively(java.time.Duration.ofSeconds(30), () -> {
            InMemoryJobMonitor monitor = new InMemoryJobMonitor(8);
            try (MasterServer m = new MasterServer(5000, 0, new InProcessRegistry(), monitor);
                 WorkerServer a0 = WorkerServer.newUnguarded("w0", 4, "127.0.0.1", m.port(), w0)) {
                long n = runGroupBy(8, m, "dg", tmp.resolve("dg.tsv"));
                assertEquals(20, n, "groupBy still yields 20 groups after the downgrade (the result is independent of parallelism)");
                int eff = keyedStageInstances(m);
                assertEquals(2, eff, "with 4 slots parallelism 2 demands 4 and fits while parallelism 3 demands 5 and does not, so 2 is chosen");
                assertEquals(1, monitor.jobs().size());
                JobView view = monitor.jobs().get(0);
                assertEquals(8, view.desiredParallelism());
                assertEquals(2, view.effectiveParallelism());
                assertTrue(view.downgraded(), "the default downgrade must show desired 8 and effective 2 in the monitor");
                assertEquals("FINISHED", view.state());
            }
        });
    }

    @Test
    void explicitFalsePreservesDesiredOnlyAdmission(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        System.setProperty("rowweave.cluster.downgrade", "false");
        System.setProperty("rowweave.cluster.admitTimeoutMs", "1000");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 4, "127.0.0.1", m.port(), w0)) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> runGroupBy(8, m, "legacy", tmp.resolve("legacy.tsv")));
            assertTrue(String.valueOf(failure.getMessage()).toLowerCase().contains("insufficient"),
                    "an explicit false keeps the desired-only refusal semantics: " + failure.getMessage());
        }
    }

    @Test
    void noDowngradeWhenAmple(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        System.setProperty("rowweave.cluster.downgrade", "true");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 64, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 64, "127.0.0.1", m.port(), w1)) {
            assertEquals(20, runGroupBy(3, m, "amp", tmp.resolve("amp.tsv")), "runs to completion with ample capacity");
            assertEquals(3, keyedStageInstances(m), "ample capacity keeps effective parallelism equal to the desired 3 with no downgrade");
        }
    }

    @Test
    void failsWhenEvenP1Infeasible(@TempDir Path tmp) {
        System.setProperty("rowweave.cluster.admission", "true");
        System.setProperty("rowweave.cluster.downgrade", "true");
        System.setProperty("rowweave.cluster.downgradeGraceMs", "100");
        System.setProperty("rowweave.cluster.admitTimeoutMs", "1500");
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        try (MasterServer m = new MasterServer(5000);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 1, "127.0.0.1", m.port(), w0)) {
            BatchSession s = new BatchSession("fail", "fail", new ClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(4);
            s.fromSource(new CollectionSource(META, rows(4000)))
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                    .orderBy("k")
                    .sinkTo(new FileSink(GB, tmp.resolve("fail.tsv").toString()));
            RuntimeException ex = assertThrows(RuntimeException.class, s::execute);
            assertTrue(String.valueOf(ex.getMessage()).toLowerCase().contains("insufficient"),
                    "nothing fits even at parallelism 1 so the job fails: " + ex.getMessage());
        }
    }
}

