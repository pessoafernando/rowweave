package io.rowweave.cluster.registry.zk;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.cluster.RegistryMasterLocator;
import io.rowweave.cluster.WorkerServer;
import io.rowweave.commit.ManifestCommitOutcome;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Semantics of a real ZooKeeper leader change across the manifest visibility point. */
class ZkManifestLeaderFailoverTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    @Test
    void leaderLossBeforeManifestPublishKeepsOldGenerationVisible(@TempDir Path tmp)
            throws Exception {
        try (Fixture fixture = new Fixture(tmp)) {
            Path domain = tmp.resolve("before-domain");
            fixture.client.submitAttempt(graph(domain, 1L), "old-attempt");
            long oldGeneration = new ManifestFileStore(domain).latest().generation();

            fixture.masterA.setManifestPublishHooks(fixture.electionA::close, null);
            assertThrows(RuntimeException.class,
                    () -> fixture.client.submitAttempt(graph(domain, 2L), "before-attempt"));
            fixture.awaitStandbyLeader();

            assertEquals(oldGeneration, new ManifestFileStore(domain).latest().generation());
            assertEquals(1L, firstValue(domain));
            assertEquals(ManifestCommitOutcome.State.PREPARING,
                    fixture.client.queryManifestOutcome(domain.toString(), "before-attempt").state());
        }
    }

    @Test
    void leaderLossAfterManifestPublishIsQueryableAsCommitted(@TempDir Path tmp)
            throws Exception {
        try (Fixture fixture = new Fixture(tmp)) {
            Path domain = tmp.resolve("after-domain");
            fixture.masterA.setManifestPublishHooks(null, fixture.electionA::close);

            assertThrows(RuntimeException.class,
                    () -> fixture.client.submitAttempt(graph(domain, 9L), "after-attempt"));
            fixture.awaitStandbyLeader();

            ManifestCommitOutcome outcome = fixture.client.queryManifestOutcome(
                    domain.toString(), "after-attempt");
            assertEquals(ManifestCommitOutcome.State.COMMITTED, outcome.state());
            assertEquals(9L, firstValue(domain));
        }
    }

    private static PlanGraph graph(Path domain, long id) {
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, Arrays.asList(row(id))),
                PlanNode.sink(META, 0,
                        new ManifestFileSink(META, domain.toString(), "ledger")).build()));
    }

    private static DataRow row(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static long firstValue(Path domain) {
        try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                META, domain.toString(), "ledger").openReader()) {
            assertTrue(reader.hasNext());
            return (Long) reader.next().get(0);
        }
    }

    private static final class Fixture implements AutoCloseable {
        final TestingServer zk;
        final ZkServiceRegistry registryA;
        final ZkServiceRegistry registryB;
        final ZkServiceRegistry workerRegistry;
        final ZkServiceRegistry clientRegistry;
        final MasterServer masterA;
        final MasterServer masterB;
        final MasterElection electionA;
        final MasterElection electionB;
        final WorkerServer worker;
        final ClusterJobClient client;

        Fixture(Path tmp) throws Exception {
            zk = new TestingServer(true);
            String connect = zk.getConnectString();
            registryA = new ZkServiceRegistry(connect);
            registryB = new ZkServiceRegistry(connect);
            workerRegistry = new ZkServiceRegistry(connect);
            clientRegistry = new ZkServiceRegistry(connect);
            masterA = new MasterServer(5_000L, 0, registryA);
            masterB = new MasterServer(5_000L, 0, registryB);
            electionA = new MasterElection(
                    registryA, new Endpoint("127.0.0.1", masterA.port()));
            electionB = new MasterElection(
                    registryB, new Endpoint("127.0.0.1", masterB.port()));
            masterA.bindLeadership(electionA);
            masterB.bindLeadership(electionB);
            electionA.start();
            electionB.start();
            File spill = tmp.resolve("worker-spill").toFile();
            assertTrue(spill.mkdirs());
            worker = WorkerServer.newUnguarded(
                    "manifest-ha-worker", 16,
                    new RegistryMasterLocator(workerRegistry), spill);
            client = new ClusterJobClient(new RegistryMasterLocator(clientRegistry));
            long deadline = System.currentTimeMillis() + 5_000L;
            while (registryA.worker("manifest-ha-worker") == null
                    && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }
            assertTrue(electionA.isLeader());
        }

        void awaitStandbyLeader() throws InterruptedException {
            long deadline = System.currentTimeMillis() + 5_000L;
            while (!electionB.isLeader() && System.currentTimeMillis() < deadline) {
                Thread.sleep(20L);
            }
            assertTrue(electionB.isLeader(), "standby Master must acquire a new ZK term");
        }

        @Override
        public void close() throws Exception {
            worker.close();
            electionA.close();
            electionB.close();
            masterA.close();
            masterB.close();
            registryA.close();
            registryB.close();
            workerRegistry.close();
            clientRegistry.close();
            zk.close();
        }
    }
}
