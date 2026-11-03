package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterMainWorkerLocatorTest {

    private static ClusterConfig cfg(String registry, String zkConnect) {
        Properties p = new Properties();
        p.setProperty("rowweave.worker.enable", "true");
        if (registry != null) p.setProperty("rowweave.registry", registry);
        if (zkConnect != null) p.setProperty("rowweave.zk.connectString", zkConnect);
        return ClusterConfig.fromProperties(p);
    }

    @Test
    void zkCombinedWorkerLocatesRemoteLeaderNotLocal() {
        ServiceRegistry registry = new InProcessRegistry();
        registry.tryClaimExclusive(new RegistryWorker(
                MasterServer.MASTER_LEADER_ROLE_ID, "10.0.0.9", 29000, 0));

        MasterLocator locator = ClusterMain.workerMasterLocator(
                cfg("zk", "zk1:2181"), registry, /*localMasterPort*/ 12345, null);

        Endpoint leader = locator.locateLeader();
        assertEquals("10.0.0.9", leader.host(), "in ZK mode the worker must address the leader from the registry, not a local endpoint");
        assertEquals(29000, leader.port(), "the leader port must come from the registry, not the local master port");
    }

    @Test
    void inprocessCombinedWorkerUsesLocalMaster() {
        MasterLocator locator = ClusterMain.workerMasterLocator(
                cfg("inprocess", null), new InProcessRegistry(), /*localMasterPort*/ 12345, null);
        Endpoint m = locator.locateLeader();
        assertEquals("127.0.0.1", m.host());
        assertEquals(12345, m.port());
    }

    @Test
    void inprocessPureWorkerUsesConfiguredMaster() {
        Properties p = new Properties();
        p.setProperty("rowweave.worker.enable", "true");
        p.setProperty("rowweave.worker.master.host", "10.0.0.5");
        p.setProperty("rowweave.worker.master.port", "19000");
        MasterLocator locator = ClusterMain.workerMasterLocator(
                ClusterConfig.fromProperties(p), new InProcessRegistry(), /*localMasterPort*/ -1,
                null);
        Endpoint m = locator.locateLeader();
        assertEquals("10.0.0.5", m.host());
        assertEquals(19000, m.port());
    }

    @Test
    void fixedMasterStartsRetentionButZkMasterWaitsForElection() {
        MasterServer fixed = new MasterServer(
                5_000L, 0, new InProcessRegistry());
        MasterServer awaitingElection = new MasterServer(
                5_000L, 0, new InProcessRegistry());
        try {
            ClusterMain.startFixedMasterLeadershipServices(
                    fixed, cfg("inprocess", null));
            ClusterMain.startFixedMasterLeadershipServices(
                    awaitingElection, cfg("zk", "zk1:2181"));

            assertTrue(fixed.terminalRunJanitorActive());
            assertFalse(awaitingElection.terminalRunJanitorActive());
        } finally {
            fixed.close();
            awaitingElection.close();
        }
    }
}

