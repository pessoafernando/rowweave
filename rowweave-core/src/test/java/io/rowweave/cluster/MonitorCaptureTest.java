package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.ops.ParallelGroupByAgg;
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
class MonitorCaptureTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < n; i++) { DataRow r = new DataRow(2); r.set(0, i % 8); r.set(1, new BigDecimal(i % 50)); rs.add(r); }
        return rs;
    }

    @Test
    void monitorCapturesJobLifecycle(@TempDir Path tmp) throws Exception {
        InMemoryJobMonitor mon = new InMemoryJobMonitor(100);
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        try (MasterServer m = new MasterServer(5000, 0,
                     new io.rowweave.cluster.registry.InProcessRegistry(), mon);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
             WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1)) {

            ExecutionEnvironment s = new ExecutionEnvironment("cap", "cap", new RealClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("cap.tsv");
            s.fromSource(new CollectionSource(META, "monitor-source", rows(1600)))
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                    .sinkTo(new FileSink(
                            ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema(), f.toString()));
            s.execute();

            List<JobView> jobs = mon.jobs();
            assertEquals(1, jobs.size(), "the monitor captured 1 job");
            JobView j = jobs.get(0);
            assertEquals("FINISHED", j.state(), "the job completion status is FINISHED");
            assertTrue(j.endTime() > 0, "a completion time is recorded");
            assertFalse(j.stages().isEmpty(), "stages captured");
            assertTrue(j.dagJson().contains("\"nodes\""), "the DAG JSON contains nodes");
            assertTrue(j.dagJson().contains("\"edges\""), "the DAG JSON contains edges (the groupBy shuffle dependency)");
            assertTrue(j.stages().stream().anyMatch(st -> "SINK".equals(st.role())), "a SINK stage is present");
            io.rowweave.monitor.StageView source = j.stages().stream()
                    .filter(st -> "SOURCE".equals(st.role())).findFirst().orElseThrow(
                            () -> new AssertionError("a SOURCE stage is present"));
            assertEquals(1, source.sourceSplitCount());
            assertEquals(io.rowweave.monitor.StageView.SOURCE_REPLAY_EXACT,
                    source.sourceReplayMode());
            assertEquals(16, source.sourceDescriptorDigestPrefix().length());
            assertFalse(source.sourceDescriptorDigestPrefix().contains("monitor-source"));
            assertTrue(j.dagJson().contains("\"channel\""), "DAG edges should carry the channel type (used for monitor coloring)");
        }
    }

    @Test
    void monitorEdgeChannelLocalOnSingleWorker(@TempDir Path tmp) throws Exception {
        InMemoryJobMonitor mon = new InMemoryJobMonitor(100);
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        try (MasterServer m = new MasterServer(5000, 0,
                     new io.rowweave.cluster.registry.InProcessRegistry(), mon);
             WorkerServer a0 = WorkerServer.newUnguarded("w0", 8, "127.0.0.1", m.port(), w0)) {
            ExecutionEnvironment s = new ExecutionEnvironment("localviz", "localviz", new RealClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(3);
            Path f = tmp.resolve("localviz.tsv");
            s.fromSource(new CollectionSource(META, rows(1600)))
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                    .sinkTo(new FileSink(
                            ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema(), f.toString()));
            s.execute();
            JobView j = mon.jobs().get(0);
            assertTrue(j.dagJson().contains("\"channel\":\"local\""), "edges of a single worker should be local: " + j.dagJson());
            assertFalse(j.dagJson().contains("\"channel\":\"remote\""), "a single worker should have no remote edge: " + j.dagJson());
            boolean anyLocalChannel = j.stages().stream()
                    .anyMatch(st -> st.inputChannels().contains("local"));
            assertTrue(anyLocalChannel, "at least one stage input channel is local");
        }
    }

    @Test
    void monitorCapturesFailedJob(@TempDir Path tmp) throws Exception {
        InMemoryJobMonitor mon = new InMemoryJobMonitor(100);
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        MasterServer m = new MasterServer(2000, 0,
                new io.rowweave.cluster.registry.InProcessRegistry(), mon);
        WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", m.port(), w0);
        WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", m.port(), w1);
        try {
            ExecutionEnvironment s = new ExecutionEnvironment("fail", "fail", new RealClusterJobClient("127.0.0.1", m.port()));
            s.setParallelism(2);
            Path f = tmp.resolve("fail.tsv");
            s.fromSource(ClusterTestFileInput.write(
                            tmp.resolve("failed-job-source.tsv"), META, rows(200000)))
                    .map(in -> { try { Thread.sleep(1); } catch (InterruptedException ignored) { } return in; })
                    .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                    .sinkTo(new FileSink(
                            ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema(), f.toString()));
            final Throwable[] err = new Throwable[1];
            Thread t = new Thread(() -> { try { s.execute(); } catch (Throwable x) { err[0] = x; } });
            t.start();
            awaitRunning(a1);
            a1.close();
            t.join(30000);
            assertNotNull(err[0], "the job should fail");
            long deadline = System.currentTimeMillis() + 10000;
            while (System.currentTimeMillis() < deadline) {
                if (!mon.jobs().isEmpty() && "FAILED".equals(mon.jobs().get(0).state())) break;
                Thread.sleep(50);
            }
            List<JobView> jobs = mon.jobs();
            assertEquals(1, jobs.size());
            assertEquals("FAILED", jobs.get(0).state(), "the monitor captured a FAILED status");
        } finally {
            try { a0.close(); } catch (RuntimeException ignored) { }
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
}

