package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MasterLocatorTest {

    private static Endpoint ep(int port) { return new Endpoint("127.0.0.1", port); }

    @Test
    void fixedLocatorReturnsStaticEndpoint() {
        MasterLocator loc = new FixedMasterLocator(new Endpoint("h", 7000));
        assertEquals(new Endpoint("h", 7000), loc.locateLeader());
        assertEquals(7000, loc.locateLeader().port());
    }

    @Test
    void registryLocatorReturnsCurrentLeader() {
        ServiceRegistry reg = new InProcessRegistry();
        MasterLocator loc = new RegistryMasterLocator(reg);
        try (MasterElection e = new MasterElection(reg, ep(9000))) {
            e.start();
            assertEquals(ep(9000), loc.locateLeader(), "the current leader endpoint is found");
        }
    }

    @Test
    void registryLocatorFailFastWhenNoLeader() {
        ServiceRegistry reg = new InProcessRegistry();
        MasterLocator loc = new RegistryMasterLocator(reg);
        assertThrows(IllegalStateException.class, loc::locateLeader, "with no leader the lookup fails fast instead of staying silent");
    }

    @Test
    void registryLocatorUsesSingleLeaderPointLookupInsteadOfFullSnapshot() {
        RegistryWorker leader = new RegistryWorker(MasterServer.MASTER_LEADER_ROLE_ID,
                "leader-host", 9123, 0x01020304, 0x05060708);
        PointOnlyRegistry reg = new PointOnlyRegistry(leader);

        RegistryMasterLocator.LeaderLease lease = new RegistryMasterLocator(reg).locateLeaderLease();

        assertEquals(new Endpoint("leader-host", 9123), lease.endpoint);
        assertEquals(((long) 0x01020304 << 32) | 0x05060708L, lease.termToken);
        assertEquals(1, reg.pointLookups);
        assertEquals(MasterServer.MASTER_LEADER_ROLE_ID, reg.requestedId);
    }

    @Test
    void registryLocatorSeesNewLeaderAfterFailover() {
        ServiceRegistry reg = new InProcessRegistry();
        MasterLocator loc = new RegistryMasterLocator(reg);
        MasterElection a = new MasterElection(reg, ep(9000));
        MasterElection b = new MasterElection(reg, ep(9001));
        a.start();
        b.start();
        Endpoint before = loc.locateLeader();
        MasterElection leader = a.isLeader() ? a : b;
        MasterElection standby = a.isLeader() ? b : a;
        assertEquals(leader.self(), before, "before the switch it points at the current leader");

        leader.close();  // failover
        Endpoint after = loc.locateLeader();
        assertEquals(standby.self(), after, "after the switch it points at the new leader (standby takeover)");
        assertNotEquals(before, after, "the address really changed");

        standby.close();
    }

    private static final class PointOnlyRegistry implements ServiceRegistry {
        private final RegistryWorker leader;
        private int pointLookups;
        private String requestedId;

        private PointOnlyRegistry(RegistryWorker leader) {
            this.leader = leader;
        }

        @Override public RegistryWorker worker(String workerId) {
            pointLookups++;
            requestedId = workerId;
            return leader.workerId().equals(workerId) ? leader : null;
        }

        @Override public List<RegistryWorker> liveWorkers() {
            throw new AssertionError("leader lookup must not scan the full registry snapshot");
        }

        @Override public void register(RegistryWorker worker) { throw new UnsupportedOperationException(); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) { throw new UnsupportedOperationException(); }
        @Override public void report(RegistryWorker worker) { throw new UnsupportedOperationException(); }
        @Override public void renew(String workerId) { throw new UnsupportedOperationException(); }
        @Override public void deregister(String workerId) { throw new UnsupportedOperationException(); }
        @Override public void watch(WatchListener listener) { throw new UnsupportedOperationException(); }
    }
}

