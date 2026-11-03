package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.ServiceRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class MasterElectionTest {

    private static Endpoint ep(int port) { return new Endpoint("127.0.0.1", port); }

    @Test
    void singleMasterBecomesLeader() {
        ServiceRegistry reg = new InProcessRegistry();
        try (MasterElection e = new MasterElection(reg, ep(9000))) {
            e.start();
            assertTrue(e.isLeader(), "a lone master claims leadership");
        }
    }

    @Test
    void twoMastersOnlyOneLeader() {
        ServiceRegistry reg = new InProcessRegistry();
        try (MasterElection a = new MasterElection(reg, ep(9000));
             MasterElection b = new MasterElection(reg, ep(9001))) {
            a.start();
            b.start();
            assertTrue(a.isLeader() ^ b.isLeader(), "exactly one of them is the leader (exclusive or)");
        }
    }

    @Test
    void standbyTakesOverAfterLeaderCloses() {
        ServiceRegistry reg = new InProcessRegistry();
        MasterElection a = new MasterElection(reg, ep(9000));
        MasterElection b = new MasterElection(reg, ep(9001));
        a.start();
        b.start();
        MasterElection leader = a.isLeader() ? a : b;
        MasterElection standby = a.isLeader() ? b : a;
        assertFalse(standby.isLeader(), "the standby is not the leader before takeover");

        leader.close();
        assertTrue(standby.isLeader(), "after the leader releases, the standby takes over automatically");

        standby.close();
    }

    @Test
    void leaderEndpointVisibleInRegistry() {
        ServiceRegistry reg = new InProcessRegistry();
        try (MasterElection e = new MasterElection(reg, ep(9000))) {
            e.start();
            boolean found = reg.liveWorkers().stream()
                    .anyMatch(w -> MasterServer.MASTER_LEADER_ROLE_ID.equals(w.workerId())
                            && w.port() == 9000);
            assertTrue(found, "the leader endpoint registers in the registry under its role id so a Locator can find it");
        }
    }

    @Test
    void closeIsIdempotent() {
        ServiceRegistry reg = new InProcessRegistry();
        MasterElection e = new MasterElection(reg, ep(9000));
        e.start();
        e.close();
        e.close();
        assertFalse(e.isLeader());
    }

    @Test
    void removingOwnedLeaderRoleEmitsLossBeforeReelection() {
        ServiceRegistry reg = new InProcessRegistry();
        List<Boolean> transitions = new ArrayList<>();
        try (MasterElection e = new MasterElection(reg, ep(9000))) {
            e.addLeadershipListener(transitions::add);
            e.start();
            assertEquals(Arrays.asList(true), transitions, "the first claim should notify a gain");

            reg.deregister(MasterElection.MASTER_LEADER_ID);

            assertEquals(Arrays.asList(true, false, true), transitions,
                    "deleting our own role must notify a loss first, and only then can it be reclaimed under a new term");
            assertTrue(e.isLeader());
            assertTrue(reg.liveWorkers().stream().anyMatch(w ->
                    MasterElection.MASTER_LEADER_ID.equals(w.workerId()) && w.port() == 9000));
        }
    }

    @Test
    void failedReelectionKeepsFollowerAndDoesNotPoisonOtherWatchers() {
        FailingReelectionRegistry reg = new FailingReelectionRegistry();
        java.util.concurrent.atomic.AtomicInteger downstreamRemoved =
                new java.util.concurrent.atomic.AtomicInteger();
        try (MasterElection e = new MasterElection(reg, ep(9000))) {
            e.start();
            reg.watch(event -> {
                if (event.type() == ServiceRegistry.EventType.REMOVED
                        && MasterElection.MASTER_LEADER_ID.equals(event.worker().workerId())) {
                    downstreamRemoved.incrementAndGet();
                }
            });
            reg.failClaims = true;

            reg.deregister(MasterElection.MASTER_LEADER_ID);

            assertFalse(e.isLeader(), "after a failed re-claim the master must stay a follower");
            assertEquals(1, downstreamRemoved.get(),
                    "an exception while re-claiming in election must not cut off the Worker and standby observers of the same registry event");
        }
    }

    private static final class FailingReelectionRegistry implements ServiceRegistry {
        private final InProcessRegistry delegate = new InProcessRegistry();
        private volatile boolean failClaims;

        @Override public void register(io.rowweave.cluster.registry.RegistryWorker worker) {
            delegate.register(worker);
        }
        @Override public boolean tryClaimExclusive(io.rowweave.cluster.registry.RegistryWorker worker) {
            if (failClaims) {
                throw new IllegalStateException("simulated expired registry session");
            }
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(io.rowweave.cluster.registry.RegistryWorker worker) {
            delegate.report(worker);
        }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public java.util.List<io.rowweave.cluster.registry.RegistryWorker> liveWorkers() {
            return delegate.liveWorkers();
        }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}

