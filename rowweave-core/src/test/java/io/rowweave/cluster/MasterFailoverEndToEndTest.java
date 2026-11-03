package io.rowweave.cluster;

import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.MapFunction;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcMessage;
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
class MasterFailoverEndToEndTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    private static List<DataRow> rows(int n) {
        List<DataRow> rs = new ArrayList<>();
        for (int i = 0; i < n; i++) { DataRow r = new DataRow(2); r.set(0, i % 8); r.set(1, new BigDecimal(i % 50)); rs.add(r); }
        return rs;
    }

    private static long countLines(Path p) {
        try { return Files.lines(p).count(); } catch (Exception e) { throw new RuntimeException(e); }
    }

    private static final MapFunction SLOW_MAP = in -> {
        try {
            Thread.sleep(2L);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        DataRow copy = new DataRow(2);
        copy.set(0, in.get(0));
        copy.set(1, in.get(1));
        return copy;
    };

    @Test
    void failoverNewLeaderSeesWorkersAndRunsJob(@TempDir Path tmp) throws Exception {
        ServiceRegistry registry = new InProcessRegistry();

        MasterServer mA = new MasterServer(5000, 0, registry);
        MasterServer mB = new MasterServer(5000, 0, registry);
        MasterElection eA = new MasterElection(registry, new Endpoint("127.0.0.1", mA.port()));
        MasterElection eB = new MasterElection(registry, new Endpoint("127.0.0.1", mB.port()));
        mA.bindLeadership(eA);
        mB.bindLeadership(eB);
        eA.start();
        eB.start();
        MasterServer leaderMaster = eA.isLeader() ? mA : mB;
        MasterElection leaderElection = eA.isLeader() ? eA : eB;
        MasterElection standbyElection = eA.isLeader() ? eB : eA;
        MasterServer standbyMaster = eA.isLeader() ? mB : mA;

        MasterLocator locator = new RegistryMasterLocator(registry);
        File w0 = new File(tmp.toFile(), "w0"); w0.mkdirs();
        File w1 = new File(tmp.toFile(), "w1"); w1.mkdirs();
        WorkerServer a0 = WorkerServer.newUnguarded("w0", 32, locator, w0);
        WorkerServer a1 = WorkerServer.newUnguarded("w1", 32, locator, w1);
        try {
            runJobAndAssert(tmp, "before", locator);

            leaderElection.close();
            leaderMaster.close();
            assertTrue(standbyElection.isLeader(), "the standby takes over as leader");

            Thread.sleep(1200);

            runJobAndAssert(tmp, "after", locator);
        } finally {
            a0.close();
            a1.close();
            standbyMaster.close();
            standbyElection.close();
        }
    }

    @Test
    void leadershipLossFailsInflightJobAndCancelsOldTermStages(@TempDir Path tmp) throws Exception {
        ServiceRegistry registry = new InProcessRegistry();
        MasterServer mA = new MasterServer(5_000, 0, registry);
        MasterServer mB = new MasterServer(5_000, 0, registry);
        MasterElection eA = new MasterElection(registry, new Endpoint("127.0.0.1", mA.port()));
        MasterElection eB = new MasterElection(registry, new Endpoint("127.0.0.1", mB.port()));
        mA.bindLeadership(eA);
        mB.bindLeadership(eB);
        eA.start();
        eB.start();
        MasterElection oldElection = eA.isLeader() ? eA : eB;
        MasterServer oldMaster = eA.isLeader() ? mA : mB;
        MasterElection newElection = eA.isLeader() ? eB : eA;
        MasterLocator locator = new RegistryMasterLocator(registry);
        File workerDir = tmp.resolve("term-worker").toFile();
        workerDir.mkdirs();
        WorkerServer worker = WorkerServer.newUnguarded("term-worker", 16, locator, workerDir);
        try {
            ExecutionEnvironment session = new ExecutionEnvironment("old-term", "old-term", new RealClusterJobClient(locator));
            session.setParallelism(1);
            Path output = tmp.resolve("old-term.tsv");
            session.fromSource(ClusterTestFileInput.write(
                            tmp.resolve("old-term-source.tsv"), META, rows(20_000)))
                    .map(SLOW_MAP)
                    .sinkTo(new FileSink(META, output.toString()));

            java.util.concurrent.atomic.AtomicReference<Throwable> failure =
                    new java.util.concurrent.atomic.AtomicReference<>();
            Thread submitter = new Thread(() -> {
                try {
                    session.execute();
                } catch (Throwable error) {
                    failure.set(error);
                }
            }, "old-term-submitter");
            submitter.start();

            long startDeadline = System.currentTimeMillis() + 8_000L;
            while (worker.runningStageCount() == 0 && System.currentTimeMillis() < startDeadline) {
                Thread.sleep(10L);
            }
            assertTrue(worker.runningStageCount() > 0, "the old term stage must be confirmed inside the Worker first");

            oldElection.close();

            submitter.join(8_000L);
            assertFalse(submitter.isAlive(), "losing leadership must release the old submit rather than wait for the stage timeout");
            assertNotNull(failure.get(), "a job from the old term must not quietly succeed across a failover");
            assertTrue(String.valueOf(failure.get()).toLowerCase().contains("leadership")
                            || String.valueOf(failure.get()).toLowerCase().contains("fail"),
                    "the failure should point at a leadership loss, actual: " + failure.get());

            long cancelDeadline = System.currentTimeMillis() + 8_000L;
            while (worker.runningStageCount() != 0 && System.currentTimeMillis() < cancelDeadline) {
                Thread.sleep(10L);
            }
            assertEquals(0, worker.runningStageCount(), "after a leader change the active stages of the old term must be cleared");
            assertTrue(newElection.isLeader(), "the standby should have taken over");

            RpcMessage staleSubmit = oldMaster.handler().apply(RpcMessage.submitJob(new byte[]{7}));
            assertEquals(RpcMessage.Type.JOB_RESULT, staleSubmit.type());
            assertFalse(staleSubmit.success());
            assertTrue(staleSubmit.errorMessage().contains("not active master leader"));

            Thread.sleep(1_200L);
            runJobAndAssert(tmp, "new-term", locator);
        } finally {
            worker.close();
            eA.close();
            eB.close();
            mA.close();
            mB.close();
        }
    }

    private void runJobAndAssert(Path tmp, String tag, MasterLocator locator) {
        ExecutionEnvironment s = new ExecutionEnvironment(tag, tag, new RealClusterJobClient(locator));
        s.setParallelism(2);
        Path f = tmp.resolve(tag + ".tsv");
        s.fromSource(new CollectionSource(META, rows(4000)))
                .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                .sinkTo(new FileSink(
                        ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema(), f.toString()));
        s.execute();
        assertEquals(8, countLines(f), tag + ": 8 groups keyed by k, the job completes");
    }
}

