package io.rowweave.cluster.registry.zk;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.RegistryMasterLocator;
import io.rowweave.cluster.WorkerServer;
import io.rowweave.ops.ParallelGroupByAgg;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** After a ZooKeeper master failover, the worker lease remains the single live truth across leaders. */
class ZkWorkerLeaseFailoverTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));

    @Test
    void healthyLeaseSurvivesLeaderFailoverAndDeadLeaseIsNotScheduled(@TempDir Path tmp) throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistryA = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry masterRegistryB = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry clientRegistry = new ZkServiceRegistry(zk.getConnectString())) {
            MasterServer masterA = new MasterServer(5000, 0, masterRegistryA);
            MasterServer masterB = new MasterServer(5000, 0, masterRegistryB);
            MasterElection electionA = new MasterElection(masterRegistryA,
                    new Endpoint("127.0.0.1", masterA.port()));
            MasterElection electionB = new MasterElection(masterRegistryB,
                    new Endpoint("127.0.0.1", masterB.port()));
            masterA.bindLeadership(electionA);
            masterB.bindLeadership(electionB);
            electionA.start();
            electionB.start();

            MasterServer oldLeader = electionA.isLeader() ? masterA : masterB;
            MasterElection oldElection = electionA.isLeader() ? electionA : electionB;
            MasterServer newLeader = electionA.isLeader() ? masterB : masterA;
            MasterElection newElection = electionA.isLeader() ? electionB : electionA;
            ZkServiceRegistry newRegistry = electionA.isLeader() ? masterRegistryB : masterRegistryA;

            ZkServiceRegistry workerRegistry = new ZkServiceRegistry(zk.getConnectString());
            WorkerServer worker = WorkerServer.newUnguarded("ha-worker", 16,
                    new RegistryMasterLocator(workerRegistry), tmp.resolve("worker").toFile());
            try {
                awaitTrue(() -> hasWorker(newRegistry, "ha-worker"), 5000);
                runJob(tmp.resolve("before.tsv"), clientRegistry);

                oldElection.close();
                oldLeader.close();
                awaitTrue(newElection::isLeader, 5000);
                awaitTrue(() -> newLeader.worker("ha-worker") != null, 3000);
                runJob(tmp.resolve("after.tsv"), clientRegistry);

                workerRegistry.close();
                awaitTrue(() -> !hasWorker(newRegistry, "ha-worker"), 10000);
                RuntimeException noWorker = assertThrows(RuntimeException.class,
                        () -> runJob(tmp.resolve("dead.tsv"), clientRegistry));
                assertTrue(noWorker.getMessage().contains("no registered worker"), noWorker.getMessage());
            } finally {
                worker.close();
                newElection.close();
                newLeader.close();
            }
        }
    }

    private static void runJob(Path output, ZkServiceRegistry clientRegistry) {
        ParallelGroupByAgg agg = ParallelGroupByAgg.of(META, "k").sum("amt");
        BatchSession session = new BatchSession("lease-ha", "lease-ha",
                new io.rowweave.cluster.ClusterJobClient(new RegistryMasterLocator(clientRegistry)));
        session.setParallelism(2);
        session.fromSource(new CollectionSource(META, rows()))
                .groupBy(agg)
                .sinkTo(new FileSink(agg.getOutputSchema(), output.toString()));
        session.execute();
        try (java.util.stream.Stream<String> lines = Files.lines(output)) {
            assertEquals(4L, lines.count());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static List<DataRow> rows() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 400; i++) {
            DataRow row = new DataRow(2);
            row.set(0, i % 4);
            row.set(1, BigDecimal.valueOf(i));
            rows.add(row);
        }
        return rows;
    }

    private static boolean hasWorker(ZkServiceRegistry registry, String workerId) {
        return registry.liveWorkers().stream().anyMatch(worker -> worker.workerId().equals(workerId));
    }

    private static void awaitTrue(BooleanSupplier condition, long timeoutMillis) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(25L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                fail("interrupted while awaiting condition");
            }
        }
        fail("condition not met within " + timeoutMillis + "ms");
    }
}
