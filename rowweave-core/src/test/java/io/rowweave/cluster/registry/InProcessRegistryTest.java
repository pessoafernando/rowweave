package io.rowweave.cluster.registry;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ServiceRegistry contract: register, tryClaimExclusive for an exclusive CAS claim,
 * deregister, renew and watch. InProcessRegistry is a pure in-memory implementation with no
 * external dependency. The election primitive is pinned down with several threads so that
 * concurrent claims admit exactly one winner.
 */
class InProcessRegistryTest {

    @Test
    void workerRegistrationRemainsMasterOwnedForCompatibility() {
        assertFalse(new InProcessRegistry().workerRegistrationOwnedByWorker());
    }

    private static RegistryWorker w(String id, int port) {
        return new RegistryWorker(id, "127.0.0.1", port, 4);
    }

    @Test
    void registerThenLiveWorkersReflectsIt() {
        ServiceRegistry reg = new InProcessRegistry();
        reg.register(w("w0", 100));
        reg.register(w("w1", 101));
        List<String> ids = new ArrayList<>();
        for (RegistryWorker rw : reg.liveWorkers()) ids.add(rw.workerId());
        assertEquals(2, reg.liveWorkers().size());
        assertTrue(ids.contains("w0") && ids.contains("w1"));
    }

    @Test
    void pointLookupReturnsCurrentWorkerAndMissingAsNull() {
        ServiceRegistry reg = new InProcessRegistry();
        RegistryWorker expected = new RegistryWorker(
                "point", "127.0.0.1", 1200, 8, 2200, "inc-point");
        reg.register(expected);

        assertSame(expected, reg.worker("point"));
        assertNull(reg.worker("missing"));
        reg.deregister("point");
        assertNull(reg.worker("point"));
    }

    @Test
    void defaultPointLookupKeepsLegacyRegistryImplementationsCompatible() {
        RegistryWorker expected = new RegistryWorker(
                "legacy", "127.0.0.1", 1201, 8, 2201, "inc-legacy");
        ServiceRegistry legacy = new LegacyRegistry(expected);

        assertSame(expected, legacy.worker("legacy"));
        assertNull(legacy.worker("missing"));
    }

    @Test
    void registerIsIdempotentOverwrite() {
        ServiceRegistry reg = new InProcessRegistry();
        reg.register(w("w0", 100));
        reg.register(new RegistryWorker("w0", "127.0.0.1", 200, 8));  // the same id overwrites, as a re-report
        assertEquals(1, reg.liveWorkers().size());
        assertEquals(200, reg.liveWorkers().get(0).port());
        assertEquals(8, reg.liveWorkers().get(0).slots());
    }

    @Test
    void deregisterRemoves() {
        ServiceRegistry reg = new InProcessRegistry();
        reg.register(w("w0", 100));
        reg.deregister("w0");
        assertTrue(reg.liveWorkers().isEmpty());
        reg.deregister("nonexistent");  // a no-op that does not throw
    }

    @Test
    void tryClaimExclusiveOnlyFirstWins() {
        ServiceRegistry reg = new InProcessRegistry();
        assertTrue(reg.tryClaimExclusive(w("leader-role", 500)), "the first claim wins");
        assertFalse(reg.tryClaimExclusive(w("leader-role", 501)), "a claim against a held value fails and does not overwrite");
        // the second claim does not overwrite the incumbent holder
        RegistryWorker held = reg.liveWorkers().stream()
                .filter(x -> x.workerId().equals("leader-role")).findFirst().orElseThrow(AssertionError::new);
        assertEquals(500, held.port());
    }

    @Test
    void tryClaimExclusiveReclaimableAfterDeregister() {
        ServiceRegistry reg = new InProcessRegistry();
        assertTrue(reg.tryClaimExclusive(w("leader-role", 500)));
        reg.deregister("leader-role");  // the leader is released, by crash or on purpose
        assertTrue(reg.tryClaimExclusive(w("leader-role", 600)), "a new leader can claim it after the release");
        assertEquals(600, reg.liveWorkers().get(0).port());
    }

    /** Core of the election primitive: N threads claim the same role concurrently and exactly one must win. */
    @Test
    void concurrentClaimExactlyOneWins() throws Exception {
        ServiceRegistry reg = new InProcessRegistry();
        int n = 32;
        ExecutorService pool = Executors.newFixedThreadPool(n);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger wins = new AtomicInteger();
        List<java.util.concurrent.Future<?>> fs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            final int port = 1000 + i;
            fs.add(pool.submit(() -> {
                try { start.await(); } catch (InterruptedException ignored) { }
                if (reg.tryClaimExclusive(w("leader-role", port))) wins.incrementAndGet();
            }));
        }
        start.countDown();
        for (java.util.concurrent.Future<?> f : fs) f.get();
        pool.shutdown();
        assertEquals(1, wins.get(), "exactly one concurrent claim wins");
    }

    @Test
    void watchFiresAddedAndRemoved() {
        ServiceRegistry reg = new InProcessRegistry();
        List<String> events = new CopyOnWriteArrayList<>();
        reg.watch(e -> events.add(e.type() + ":" + e.worker().workerId()));
        reg.register(w("w0", 100));
        reg.deregister("w0");
        assertTrue(events.contains("ADDED:w0"), "register yields ADDED, observed: " + events);
        assertTrue(events.contains("REMOVED:w0"), "deregister yields REMOVED, observed: " + events);
    }

    @Test
    void renewOnKnownKeepsAlive_unknownIsNoop() {
        ServiceRegistry reg = new InProcessRegistry();
        reg.register(w("w0", 100));
        reg.renew("w0");  // registered: renew, which keeps the entry alive here without throwing
        assertEquals(1, reg.liveWorkers().size());
        reg.renew("ghost");  // unregistered: a no-op that does not throw
        assertEquals(1, reg.liveWorkers().size());
    }

    /** Simulates a third-party implementation that does not override worker(String), which must keep using the interface default point lookup. */
    private static final class LegacyRegistry implements ServiceRegistry {
        private final List<RegistryWorker> workers;

        private LegacyRegistry(RegistryWorker worker) {
            this.workers = java.util.Collections.singletonList(worker);
        }

        @Override public void register(RegistryWorker worker) { throw new UnsupportedOperationException(); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) { throw new UnsupportedOperationException(); }
        @Override public void report(RegistryWorker worker) { throw new UnsupportedOperationException(); }
        @Override public void renew(String workerId) { throw new UnsupportedOperationException(); }
        @Override public void deregister(String workerId) { throw new UnsupportedOperationException(); }
        @Override public List<RegistryWorker> liveWorkers() { return workers; }
        @Override public void watch(WatchListener listener) { throw new UnsupportedOperationException(); }
    }
}
