package io.rowweave.cluster.registry.zk;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.run.DurableStageAssignmentAuthorityProvider;
import io.rowweave.run.RunStateStoreProvider;
import io.rowweave.upgrade.UpgradeAuthorityStoreProvider;
import io.rowweave.upgrade.StoreFormatHeader;
import io.rowweave.upgrade.UpgradeOwner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Contract test for ZkServiceRegistry, run against an embedded Curator TestingServer so no system
 * ZooKeeper is needed. It checks the implementation against the same ServiceRegistry contract as
 * InProcessRegistry (register, tryClaimExclusive CAS, deregister, liveWorkers, watch) to show the
 * behaviour matches, and it checks the cross-instance view: two ZkServiceRegistry instances on one
 * ZooKeeper give the process-separation model where one writes and the other reads.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ZkServiceRegistryTest {

    private TestingServer zk;

    @BeforeAll
    void startZk() throws Exception {
        zk = new TestingServer(true);  // Embedded ZooKeeper on a random port.
    }

    @AfterAll
    void stopZk() throws Exception {
        if (zk != null) zk.close();
    }

    private ZkServiceRegistry connect() {
        return new ZkServiceRegistry(zk.getConnectString());
    }

    private static RegistryWorker w(String id, int port) {
        return new RegistryWorker(id, "127.0.0.1", port, 4);
    }

    @Test
    void declaresWorkerOwnedEphemeralRegistration() {
        try (ZkServiceRegistry registry = connect()) {
            assertTrue(registry.workerRegistrationOwnedByWorker());
        }
    }

    @Test
    void exposesAssignmentAuthorityAtTheRunStateLinearizationPoint() {
        try (ZkServiceRegistry registry = connect()) {
            assertTrue(registry instanceof DurableStageAssignmentAuthorityProvider);
            assertSame(((RunStateStoreProvider) registry).runStateStore(),
                    ((DurableStageAssignmentAuthorityProvider) registry)
                            .durableStageAssignmentAuthority());
        }
    }

    @Test
    void exposesStoreUpgradeAuthorityOnlyWhenExplicitlyConfigured()
            throws Exception {
        String root = "/rowweave/store-upgrade-test";
        try (CuratorFramework bootstrap = CuratorFrameworkFactory.newClient(
                zk.getConnectString(), new ExponentialBackoffRetry(25, 3))) {
            bootstrap.start();
            assertTrue(bootstrap.blockUntilConnected(10, TimeUnit.SECONDS));
            bootstrap.create().creatingParentsIfNeeded().forPath(
                    root + "/namespaces/legacy/rowweave/existing",
                    new byte[]{1});
            new ZkUpgradeAuthority(bootstrap, root).bootstrap(
                    new StoreFormatHeader(1, "store", 1, 0, digest('a')),
                    new StoreFormatHeader(1, "store", 1, 1, digest('b')),
                    1L, "legacy", "shadow", new UpgradeOwner(
                            1L, "master", "inc", 1L), 5_000L);

            try (ZkServiceRegistry ordinary = connect();
                 ZkServiceRegistry configured = new ZkServiceRegistry(
                         zk.getConnectString(), root)) {
                assertNull(((UpgradeAuthorityStoreProvider) ordinary)
                        .upgradeAuthorityStore());
                assertTrue(((UpgradeAuthorityStoreProvider) configured)
                        .upgradeAuthorityStore() instanceof ZkUpgradeAuthority);
                assertNotNull(bootstrap.checkExists().forPath(root
                        + "/namespaces/legacy/rowweave/run-state-v1/runs"));
            }
        }
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static void awaitTrue(java.util.function.BooleanSupplier cond, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) return;
            try { Thread.sleep(30); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
        }
        fail("condition not met within " + timeoutMs + "ms");
    }

    @Test
    void registerThenLiveWorkers() {
        try (ZkServiceRegistry reg = connect()) {
            reg.register(w("w0", 100));
            reg.register(w("w1", 101));
            awaitTrue(() -> reg.liveWorkers().size() == 2, 5000);
            List<String> ids = new java.util.ArrayList<>();
            for (RegistryWorker rw : reg.liveWorkers()) ids.add(rw.workerId());
            assertTrue(ids.contains("w0") && ids.contains("w1"));
        }
    }

    @Test
    void registerIdempotentOverwrite() {
        try (ZkServiceRegistry reg = connect()) {
            reg.register(w("wov", 100));
            reg.register(new RegistryWorker("wov", "127.0.0.1", 200, 8));
            awaitTrue(() -> {
                for (RegistryWorker rw : reg.liveWorkers()) {
                    if (rw.workerId().equals("wov")) return rw.port() == 200 && rw.slots() == 8;
                }
                return false;
            }, 5000);
        }
    }

    /** The mux dataPort of a RegistryWorker survives a ZooKeeper session unchanged. */
    @Test
    void registryWorkerDataPortSurvivesCrossInstanceRoundTrip() {
        try (ZkServiceRegistry owner = connect(); ZkServiceRegistry observer = connect()) {
            owner.register(new RegistryWorker("wmux", "127.0.0.1", 100, 8, 23001));
            awaitTrue(() -> observer.liveWorkers().stream()
                    .anyMatch(x -> x.workerId().equals("wmux") && x.dataPort() == 23001), 5000);
        }
    }

    @Test
    void registryWorkerIncarnationSurvivesCrossInstanceRoundTrip() {
        try (ZkServiceRegistry owner = connect(); ZkServiceRegistry observer = connect()) {
            owner.register(new RegistryWorker(
                    "winc", "127.0.0.1", 101, 8, 23002, "incarnation-zk"));
            awaitTrue(() -> observer.liveWorkers().stream().anyMatch(x ->
                    x.workerId().equals("winc") && "incarnation-zk".equals(x.incarnationId())), 5000);
        }
    }

    @Test
    void pointLookupPreservesIdentityAndObservesLeaseRemoval() {
        try (ZkServiceRegistry owner = connect(); ZkServiceRegistry observer = connect()) {
            owner.register(new RegistryWorker(
                    "point-zk", "10.0.0.7", 17001, 9, 27001, "inc-zk-point"));
            awaitTrue(() -> observer.worker("point-zk") != null, 5000);
            RegistryWorker found = observer.worker("point-zk");
            assertEquals("10.0.0.7", found.host());
            assertEquals(17001, found.port());
            assertEquals(9, found.slots());
            assertEquals(27001, found.dataPort());
            assertEquals("inc-zk-point", found.incarnationId());

            owner.deregister("point-zk");
            awaitTrue(() -> observer.worker("point-zk") == null, 5000);
        }
    }

    /** Another session must not rewrite the value while leaving the old ephemeral owner in place. */
    @Test
    void foreignSessionCannotOverwriteWorkerLeaseOwner() {
        try (ZkServiceRegistry owner = connect(); ZkServiceRegistry intruder = connect()) {
            owner.register(w("owned", 100));
            assertThrows(RuntimeException.class,
                    () -> intruder.register(w("owned", 200)),
                    "the same workerId across sessions must fail closed instead of tearing payload/owner identity apart");
            assertTrue(owner.liveWorkers().stream()
                    .anyMatch(x -> x.workerId().equals("owned") && x.port() == 100));
        }
    }

    /** An older or foreign session must not delete the current owner lease by workerId alone. */
    @Test
    void foreignSessionCannotDeregisterWorkerLeaseOwner() {
        try (ZkServiceRegistry owner = connect(); ZkServiceRegistry intruder = connect()) {
            owner.register(w("delete-owned", 300));
            assertDoesNotThrow(() -> intruder.deregister("delete-owned"),
                    "a late close from an old incarnation/leader must be a safe no-op");
            assertTrue(owner.liveWorkers().stream()
                    .anyMatch(x -> x.workerId().equals("delete-owned") && x.port() == 300));
        }
    }

    /** Existing four-field payloads stay readable by an upgraded reader. */
    @Test
    void legacyFourFieldPayloadDecodesWithMuxDisabled() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            writeLegacyUtf(out, "legacy");
            writeLegacyUtf(out, "127.0.0.1");
            out.writeInt(1234);
            out.writeInt(7);
        }
        Method decode = ZkServiceRegistry.class.getDeclaredMethod("decode", byte[].class);
        decode.setAccessible(true);
        RegistryWorker worker = (RegistryWorker) decode.invoke(null, (Object) bytes.toByteArray());
        assertEquals("legacy", worker.workerId());
        assertNull(worker.incarnationId());
        assertEquals(1234, worker.port());
        assertEquals(7, worker.slots());
        assertEquals(-1, worker.dataPort());
    }

    private static void writeLegacyUtf(DataOutputStream out, String value) throws Exception {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    @Test
    void tryClaimExclusiveOnlyFirstWins() {
        try (ZkServiceRegistry reg = connect()) {
            assertTrue(reg.tryClaimExclusive(w("role-a", 500)), "the first claim wins");
            assertFalse(reg.tryClaimExclusive(w("role-a", 501)), "an occupied claim returns false and does not overwrite");
        }
    }

    @Test
    void deregisterRemovesAndReclaimable() {
        try (ZkServiceRegistry reg = connect()) {
            assertTrue(reg.tryClaimExclusive(w("role-b", 500)));
            reg.deregister("role-b");
            awaitTrue(() -> reg.liveWorkers().stream().noneMatch(x -> x.workerId().equals("role-b")), 5000);
            assertTrue(reg.tryClaimExclusive(w("role-b", 600)), "the claim can be taken again after release");
        }
    }

    @Test
    void watchFiresAddedAndRemoved() {
        try (ZkServiceRegistry reg = connect()) {
            List<String> events = new CopyOnWriteArrayList<>();
            reg.watch(e -> events.add(e.type() + ":" + e.worker().workerId()));
            reg.register(w("ww", 100));
            awaitTrue(() -> events.stream().anyMatch(s -> s.equals("ADDED:ww")), 5000);
            reg.deregister("ww");
            awaitTrue(() -> events.stream().anyMatch(s -> s.equals("REMOVED:ww")), 5000);
        }
    }

    /** Cross-instance view, the heart of the process model: regA writes and regB reads, so state is shared globally rather than held in one registry. */
    @Test
    void crossInstanceVisibility() {
        try (ZkServiceRegistry regA = connect(); ZkServiceRegistry regB = connect()) {
            regA.register(w("shared", 700));
            awaitTrue(() -> regB.liveWorkers().stream().anyMatch(x -> x.workerId().equals("shared") && x.port() == 700),
                    5000);
        }
    }

    /** Cross-instance CAS: two instances race for one role and only one wins, on the atomicity of an EPHEMERAL create. */
    @Test
    void crossInstanceExclusiveOnlyOneWins() {
        try (ZkServiceRegistry regA = connect(); ZkServiceRegistry regB = connect()) {
            boolean a = regA.tryClaimExclusive(w("xrole", 800));
            boolean b = regB.tryClaimExclusive(w("xrole", 801));
            assertTrue(a ^ b, "exactly one instance wins the cross-instance claim");
        }
    }

    /** A closed session expires its EPHEMERAL lease, so the other instance sees REMOVED and can claim the role again: failover timing. */
    @Test
    void sessionCloseFreesEphemeralRole() {
        ZkServiceRegistry leader = connect();
        try (ZkServiceRegistry observer = connect()) {
            AtomicInteger removed = new AtomicInteger();
            observer.watch(e -> {
                if (e.type() == ServiceRegistry.EventType.REMOVED && e.worker().workerId().equals("leaderRole")) {
                    removed.incrementAndGet();
                }
            });
            assertTrue(leader.tryClaimExclusive(w("leaderRole", 900)));
            awaitTrue(() -> observer.liveWorkers().stream().anyMatch(x -> x.workerId().equals("leaderRole")), 5000);

            leader.close();  // Closing the session deletes its EPHEMERAL node, so the lease reads as REMOVED.

            awaitTrue(() -> removed.get() >= 1, 10000);  // Waits out the ZooKeeper session timeout and the asynchronous watch.
            assertTrue(observer.tryClaimExclusive(w("leaderRole", 901)), "an observer can take over once the leader session expires");
        }
    }
}
