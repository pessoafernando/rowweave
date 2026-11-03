package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.RegistryMasterLocator;
import io.rowweave.cluster.WorkerServer;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.fail;

/** WOL2 and WOL3: the real owner of a worker lease and its close lifecycle under genuine ZooKeeper sessions. */
class ZkWorkerLeaseOwnershipTest {

    @Test
    void closingWorkerRegistrySessionRemovesWorkerWhileMasterSessionStaysAlive(@TempDir Path tmp) throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistry = new ZkServiceRegistry(zk.getConnectString())) {
            MasterServer master = new MasterServer(5000, 0, masterRegistry);
            MasterElection election = new MasterElection(masterRegistry,
                    new Endpoint("127.0.0.1", master.port()));
            master.bindLeadership(election);
            election.start();
            ZkServiceRegistry workerRegistry = new ZkServiceRegistry(zk.getConnectString());
            WorkerServer worker = WorkerServer.newUnguarded("lease-owner", 2,
                    new RegistryMasterLocator(workerRegistry), tmp.toFile());
            try {
                awaitTrue(() -> hasWorker(masterRegistry, "lease-owner"), 5000);

                workerRegistry.close();

                awaitTrue(() -> !hasWorker(masterRegistry, "lease-owner"), 10000);
            } finally {
                worker.close();
                election.close();
                master.close();
                // workerRegistry may already be closed; ZkServiceRegistry.close is not guaranteed idempotent.
            }
        }
    }

    @Test
    void gracefulWorkerCloseDeregistersBeforeRegistrySessionClose(@TempDir Path tmp) throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistry = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry = new ZkServiceRegistry(zk.getConnectString())) {
            MasterServer master = new MasterServer(5000, 0, masterRegistry);
            MasterElection election = new MasterElection(masterRegistry,
                    new Endpoint("127.0.0.1", master.port()));
            master.bindLeadership(election);
            election.start();
            WorkerServer worker = WorkerServer.newUnguarded("graceful-worker", 2,
                    new RegistryMasterLocator(workerRegistry), tmp.toFile());
            try {
                awaitTrue(() -> hasWorker(masterRegistry, "graceful-worker"), 5000);

                worker.close();

                awaitTrue(() -> !hasWorker(masterRegistry, "graceful-worker"), 5000);
                Thread.sleep(1200L);  // Outlast the default heartbeat period: a late task must not resurrect the lease.
                if (hasWorker(masterRegistry, "graceful-worker")) {
                    fail("closed worker lease was recreated by a late heartbeat task");
                }
            } finally {
                worker.close();
                election.close();
                master.close();
            }
        }
    }

    @Test
    void heartbeatReportRecreatesExternallyDeletedOwnedLease(@TempDir Path tmp) throws Exception {
        try (TestingServer zk = new TestingServer(true);
             ZkServiceRegistry masterRegistry = new ZkServiceRegistry(zk.getConnectString());
             ZkServiceRegistry workerRegistry = new ZkServiceRegistry(zk.getConnectString())) {
            MasterServer master = new MasterServer(5000, 0, masterRegistry);
            MasterElection election = new MasterElection(masterRegistry,
                    new Endpoint("127.0.0.1", master.port()));
            master.bindLeadership(election);
            election.start();
            WorkerServer worker = WorkerServer.newUnguarded("recovering-worker", 2,
                    new RegistryMasterLocator(workerRegistry), tmp.toFile());
            try {
                awaitTrue(() -> hasWorker(masterRegistry, "recovering-worker"), 5000);
                workerRegistry.deregister("recovering-worker");
                awaitTrue(() -> !hasWorker(masterRegistry, "recovering-worker"), 5000);

                awaitTrue(() -> hasWorker(masterRegistry, "recovering-worker"), 3000);
            } finally {
                worker.close();
                election.close();
                master.close();
            }
        }
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
