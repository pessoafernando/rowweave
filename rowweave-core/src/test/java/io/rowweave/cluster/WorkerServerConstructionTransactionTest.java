package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.monitor.JobMonitorRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerServerConstructionTransactionTest {

    @Test
    void everyFixedLoopbackMuxResourceStepRollsBackAndReleasesGuard(
            @TempDir Path tmp) throws Exception {
        List<WorkerConstructionStep> steps = Arrays.asList(
                WorkerConstructionStep.LIVE_CLAIM,
                WorkerConstructionStep.STAGE_POOL,
                WorkerConstructionStep.LOCAL_SHUFFLE,
                WorkerConstructionStep.REPORT_CACHE,
                WorkerConstructionStep.HEARTBEAT_CACHE,
                WorkerConstructionStep.DATA_INBOUND,
                WorkerConstructionStep.MUX_POOL,
                WorkerConstructionStep.PREPARED_OUTPUTS,
                WorkerConstructionStep.RPC_BOUND,
                WorkerConstructionStep.RPC_ACTIVE,
                WorkerConstructionStep.MASTER_REGISTRATION,
                WorkerConstructionStep.HEARTBEAT);
        try (DeploymentRuntime masterRuntime = workerRuntime(true);
             DeploymentRuntime workerRuntime = workerRuntime(true);
             MasterServer master = new MasterServer(
                     5_000L, 0, new InProcessRegistry(),
                     JobMonitorRegistry.NOOP, null, masterRuntime)) {
            MasterLocator locator = new FixedMasterLocator(
                    new Endpoint("127.0.0.1", master.port()));
            for (WorkerConstructionStep step : steps) {
                String workerId = "fixed-failure-" + step;
                RuntimeException primary = new RuntimeException("injected-" + step);

                RuntimeException observed = assertThrows(RuntimeException.class, () ->
                        new WorkerServer(workerId, 2, locator, tmp.toFile(), true,
                                WorkerServer.RuntimeSource.borrowed(workerRuntime), 4,
                                actual -> { if (actual == step) throw primary; }));

                assertSame(primary, observed, step.toString());
                assertDoesNotThrow(workerRuntime::requireAdmissionOpen,
                        "borrowed runtime was closed at " + step);
                await(() -> master.worker(workerId) == null,
                        "failed Worker remained registered at " + step);
                try (WorkerServer replacement = new WorkerServer(
                        workerId + "-replacement", 2, locator, tmp.toFile(), true,
                        WorkerServer.RuntimeSource.borrowed(workerRuntime), 4,
                        RoleConstructionProbe.NOOP)) {
                    assertTrue(replacement.port() > 0);
                }
            }
        }
    }

    @Test
    void registryPostInitFailuresRemoveListenerLeaseAndMasterRegistration(
            @TempDir Path tmp) throws Exception {
        WorkerOwnedRegistry registry = new WorkerOwnedRegistry();
        try (DeploymentRuntime runtime = workerRuntime(true);
             MasterServer master = new MasterServer(
                     5_000L, 0, registry, JobMonitorRegistry.NOOP, null, runtime)) {
            registry.register(new RegistryWorker(
                    MasterServer.MASTER_LEADER_ROLE_ID,
                    "127.0.0.1", master.port(), 0));
            RegistryMasterLocator locator = new RegistryMasterLocator(registry);
            List<WorkerConstructionStep> steps = Arrays.asList(
                    WorkerConstructionStep.LEADER_SUBSCRIPTION,
                    WorkerConstructionStep.WORKER_LEASE,
                    WorkerConstructionStep.MASTER_REGISTRATION,
                    WorkerConstructionStep.HEARTBEAT);
            for (WorkerConstructionStep step : steps) {
                String workerId = "registry-failure-" + step;
                Error primary = new AssertionError("injected-" + step);

                Error observed = assertThrows(Error.class, () -> new WorkerServer(
                        workerId, 2, locator, tmp.toFile(), false,
                        WorkerServer.RuntimeSource.borrowed(runtime), 4,
                        actual -> { if (actual == step) throw primary; }));

                assertSame(primary, observed, step.toString());
                assertEquals(0, locator.leaderChangeListenerCount(), step.toString());
                await(() -> registry.worker(workerId) == null,
                        "failed Worker lease remained at " + step);
                await(() -> master.worker(workerId) == null,
                        "failed Worker remained in Master at " + step);
                assertDoesNotThrow(runtime::requireAdmissionOpen);
            }
        }
    }

    @Test
    void muxOffSkipsOnlyConditionalDataResources(@TempDir Path tmp) {
        List<Enum<?>> seen = new ArrayList<>();
        try (DeploymentRuntime masterRuntime = workerRuntime(true);
             DeploymentRuntime workerRuntime = workerRuntime(false);
             MasterServer master = new MasterServer(
                     5_000L, 0, new InProcessRegistry(),
                     JobMonitorRegistry.NOOP, null, masterRuntime);
             WorkerServer worker = new WorkerServer(
                     "mux-off-worker", 2,
                     new FixedMasterLocator(new Endpoint("127.0.0.1", master.port())),
                     tmp.toFile(), false,
                     WorkerServer.RuntimeSource.borrowed(workerRuntime), 4, seen::add)) {
            assertEquals(Arrays.asList(
                    WorkerConstructionStep.STAGE_POOL,
                    WorkerConstructionStep.LOCAL_SHUFFLE,
                    WorkerConstructionStep.REPORT_CACHE,
                    WorkerConstructionStep.HEARTBEAT_CACHE,
                    WorkerConstructionStep.PREPARED_OUTPUTS,
                    WorkerConstructionStep.RPC_BOUND,
                    WorkerConstructionStep.RPC_ACTIVE,
                    WorkerConstructionStep.MASTER_REGISTRATION,
                    WorkerConstructionStep.HEARTBEAT), seen);
        }
    }

    @Test
    void ownedRuntimeIsClosedAtEarlyAndLateFailureAndGuardCanBeReclaimed(
            @TempDir Path tmp) {
        AtomicReference<DeploymentRuntime> earlyRuntime = new AtomicReference<>();
        RuntimeException early = new RuntimeException("injected-owned-runtime");
        assertSame(early, assertThrows(RuntimeException.class, () -> new WorkerServer(
                "owned-early", 1,
                new FixedMasterLocator(new Endpoint("127.0.0.1", 1)),
                tmp.toFile(), true,
                WorkerServer.RuntimeSource.owned(() -> {
                    DeploymentRuntime created = workerRuntime(true);
                    earlyRuntime.set(created);
                    return created;
                }), 2, step -> {
                    if (step == WorkerConstructionStep.OWNED_RUNTIME) throw early;
                })));
        assertThrows(RuntimeException.class, earlyRuntime.get()::requireAdmissionOpen);

        try (DeploymentRuntime masterRuntime = workerRuntime(true);
             MasterServer master = new MasterServer(
                     5_000L, 0, new InProcessRegistry(),
                     JobMonitorRegistry.NOOP, null, masterRuntime)) {
            MasterLocator locator = new FixedMasterLocator(
                    new Endpoint("127.0.0.1", master.port()));
            AtomicReference<DeploymentRuntime> lateRuntime = new AtomicReference<>();
            Error late = new AssertionError("injected-rpc-active");
            assertSame(late, assertThrows(Error.class, () -> new WorkerServer(
                    "owned-late", 1, locator, tmp.toFile(), true,
                    WorkerServer.RuntimeSource.owned(() -> {
                        DeploymentRuntime created = workerRuntime(true);
                        lateRuntime.set(created);
                        return created;
                    }), 2, step -> {
                        if (step == WorkerConstructionStep.RPC_ACTIVE) throw late;
                    })));
            assertThrows(RuntimeException.class, lateRuntime.get()::requireAdmissionOpen);

            try (DeploymentRuntime replacementRuntime = workerRuntime(true);
                 WorkerServer replacement = new WorkerServer(
                         "owned-late-replacement", 1, locator, tmp.toFile(), true,
                         WorkerServer.RuntimeSource.borrowed(replacementRuntime), 2,
                         RoleConstructionProbe.NOOP)) {
                assertTrue(replacement.port() > 0);
            }
        }
    }

    @Test
    void unreachableMasterStillClosesWorkerAndReleasesGuard(@TempDir Path tmp)
            throws Exception {
        int deadPort = freePort();
        try (DeploymentRuntime runtime = workerRuntime(true)) {
            assertThrows(RuntimeException.class, () -> new WorkerServer(
                    "unreachable-worker", 1,
                    new FixedMasterLocator(new Endpoint("127.0.0.1", deadPort)),
                    tmp.toFile(), true,
                    WorkerServer.RuntimeSource.borrowed(runtime), 2,
                    RoleConstructionProbe.NOOP));
            assertDoesNotThrow(runtime::requireAdmissionOpen);

            try (MasterServer master = new MasterServer(5_000L);
                 WorkerServer replacement = new WorkerServer(
                         "unreachable-replacement", 1,
                         new FixedMasterLocator(
                                 new Endpoint("127.0.0.1", master.port())),
                         tmp.toFile(), true,
                         WorkerServer.RuntimeSource.borrowed(runtime), 2,
                         RoleConstructionProbe.NOOP)) {
                assertTrue(replacement.port() > 0);
            }
        }
    }

    @Test
    void purePropertyValidationRunsBeforeOwnedRuntimeFactory(@TempDir Path tmp) {
        String key = "rowweave.worker.closeDrainMs";
        String previous = System.getProperty(key);
        AtomicInteger creates = new AtomicInteger();
        try {
            System.setProperty(key, "-1");
            assertThrows(IllegalArgumentException.class, () -> new WorkerServer(
                    "invalid-property-worker", 1,
                    new FixedMasterLocator(new Endpoint("127.0.0.1", 1)),
                    tmp.toFile(), true,
                    WorkerServer.RuntimeSource.owned(() -> {
                        creates.incrementAndGet();
                        return workerRuntime(true);
                    }), 2, RoleConstructionProbe.NOOP));
            assertEquals(0, creates.get());
        } finally {
            if (previous == null) System.clearProperty(key);
            else System.setProperty(key, previous);
        }
    }

    private static DeploymentRuntime workerRuntime(boolean mux) {
        Properties properties = new Properties();
        properties.setProperty(ShuffleTransportConfig.MUX_PROPERTY,
                Boolean.toString(mux));
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(properties);
        return DeploymentRuntime.load(
                properties, security, 1024L * 1024L * 1024L, null);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void await(BooleanSupplier condition, String message)
            throws InterruptedException {
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(2L);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(condition.getAsBoolean(), message);
    }

    private static final class WorkerOwnedRegistry implements ServiceRegistry {
        private final InProcessRegistry delegate = new InProcessRegistry();

        @Override public boolean workerRegistrationOwnedByWorker() { return true; }
        @Override public void register(RegistryWorker worker) { delegate.register(worker); }
        @Override public boolean tryClaimExclusive(RegistryWorker worker) {
            return delegate.tryClaimExclusive(worker);
        }
        @Override public void report(RegistryWorker worker) { delegate.report(worker); }
        @Override public void renew(String workerId) { delegate.renew(workerId); }
        @Override public void deregister(String workerId) { delegate.deregister(workerId); }
        @Override public List<RegistryWorker> liveWorkers() { return delegate.liveWorkers(); }
        @Override public RegistryWorker worker(String workerId) { return delegate.worker(workerId); }
        @Override public void watch(WatchListener listener) { delegate.watch(listener); }
    }
}
