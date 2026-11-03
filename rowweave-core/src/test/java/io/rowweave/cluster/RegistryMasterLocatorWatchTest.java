package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RegistryMasterLocatorWatchTest {

    @Test
    void notifiesOnLeaderChange() {
        ServiceRegistry registry = new InProcessRegistry();
        registry.tryClaimExclusive(new RegistryWorker(
                MasterServer.MASTER_LEADER_ROLE_ID, "10.0.0.1", 19000, 0));

        RegistryMasterLocator locator = new RegistryMasterLocator(registry);
        AtomicInteger hits = new AtomicInteger();
        locator.addLeaderChangeListener(hits::incrementAndGet);

        registry.deregister(MasterServer.MASTER_LEADER_ROLE_ID);
        registry.tryClaimExclusive(new RegistryWorker(
                MasterServer.MASTER_LEADER_ROLE_ID, "10.0.0.2", 29000, 0));

        assertTrue(hits.get() >= 1, "a leader role change fires the leaderChangeListener (measured " + hits.get() + ")");
        Endpoint now = locator.locateLeader();
        assertEquals("10.0.0.2", now.host());
        assertEquals(29000, now.port());
    }

    @Test
    void ignoresNonLeaderWorkerEvents() {
        ServiceRegistry registry = new InProcessRegistry();
        RegistryMasterLocator locator = new RegistryMasterLocator(registry);
        AtomicInteger hits = new AtomicInteger();
        locator.addLeaderChangeListener(hits::incrementAndGet);

        registry.register(new RegistryWorker("w0", "10.0.0.5", 20000, 8));
        registry.deregister("w0");

        assertEquals(0, hits.get(), "a worker joining or leaving must not fire the leader change callback");
    }
}

