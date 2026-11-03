package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.RpcCodec;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.monitor.JobMonitorRegistry;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MasterServerConstructionTransactionTest {

    private static final AtomicInteger NEXT_TEST_PORT = new AtomicInteger(20_000);

    @Test
    void everyMasterResourceStepRollsBackAndReleasesFixedPort() throws Exception {
        List<MasterConstructionStep> steps = Arrays.asList(
                MasterConstructionStep.RPC_CACHE,
                MasterConstructionStep.TERMINAL_JANITOR,
                MasterConstructionStep.ARTIFACT_JANITOR,
                MasterConstructionStep.RECOVERY_INVENTORY,
                MasterConstructionStep.ASSIGN_DISPATCHER,
                MasterConstructionStep.CANCEL_COORDINATOR,
                MasterConstructionStep.ASSIGN_LANES,
                MasterConstructionStep.RPC_BOUND,
                MasterConstructionStep.RPC_ACTIVE,
                MasterConstructionStep.HEARTBEAT);
        try (DeploymentRuntime runtime = runtime()) {
            for (MasterConstructionStep step : steps) {
                int port = freePort();
                InProcessRegistry registry = new InProcessRegistry();
                RuntimeException primary = new RuntimeException("injected-" + step);
                RoleConstructionProbe probe = actual -> {
                    if (actual == step) throw primary;
                };

                RuntimeException observed = assertThrows(RuntimeException.class, () ->
                        new MasterServer(5_000L, port, registry,
                                JobMonitorRegistry.NOOP, registry.artifactRepository(),
                                MasterServer.RuntimeSource.borrowed(runtime), probe));

                assertSame(primary, observed, step.toString());
                assertDoesNotThrow(runtime::requireAdmissionOpen,
                        "borrowed runtime was closed at " + step);
                try (MasterServer replacement = new MasterServer(
                        5_000L, port, registry, JobMonitorRegistry.NOOP,
                        registry.artifactRepository(), runtime)) {
                    // Rebinding the exact port proves dormant/active RPC ownership converged.
                }
            }
        }
    }

    @Test
    void registryWithoutDurableCapabilitiesSkipsConditionalResources() {
        java.util.ArrayList<Enum<?>> seen = new java.util.ArrayList<>();
        try (DeploymentRuntime runtime = runtime();
             MasterServer master = new MasterServer(5_000L, 0, new BareRegistry(),
                     JobMonitorRegistry.NOOP, null,
                     MasterServer.RuntimeSource.borrowed(runtime), seen::add)) {
            assertEquals(Arrays.asList(
                    MasterConstructionStep.RPC_CACHE,
                    MasterConstructionStep.ASSIGN_DISPATCHER,
                    MasterConstructionStep.CANCEL_COORDINATOR,
                    MasterConstructionStep.ASSIGN_LANES,
                    MasterConstructionStep.RPC_BOUND,
                    MasterConstructionStep.RPC_ACTIVE,
                    MasterConstructionStep.HEARTBEAT), seen);
        }
    }

    @Test
    void ownedRuntimeIsCreatedInsideTransactionAndClosedWhenItsStepFails() {
        AtomicReference<DeploymentRuntime> created = new AtomicReference<>();
        RuntimeException primary = new RuntimeException("injected-owned-runtime");
        MasterServer.RuntimeSource source = MasterServer.RuntimeSource.owned(() -> {
            DeploymentRuntime runtime = runtime();
            created.set(runtime);
            return runtime;
        });

        RuntimeException observed = assertThrows(RuntimeException.class, () ->
                new MasterServer(5_000L, 0, new InProcessRegistry(),
                        JobMonitorRegistry.NOOP, null, source,
                        step -> { if (step == MasterConstructionStep.OWNED_RUNTIME) throw primary; }));

        assertSame(primary, observed);
        assertThrows(RuntimeException.class, created.get()::requireAdmissionOpen);
    }

    @Test
    void lateOwnedFailureClosesRuntimeAndErrorRemainsPrimary() {
        AtomicReference<DeploymentRuntime> created = new AtomicReference<>();
        Error primary = new AssertionError("injected-rpc-active");
        MasterServer.RuntimeSource source = MasterServer.RuntimeSource.owned(() -> {
            DeploymentRuntime runtime = runtime();
            created.set(runtime);
            return runtime;
        });

        Error observed = assertThrows(Error.class, () ->
                new MasterServer(5_000L, 0, new InProcessRegistry(),
                        JobMonitorRegistry.NOOP, null, source,
                        step -> { if (step == MasterConstructionStep.RPC_ACTIVE) throw primary; }));

        assertSame(primary, observed);
        assertThrows(RuntimeException.class, created.get()::requireAdmissionOpen);
    }

    @Test
    void fixedPortConflictRollsBackEarlierResourcesAndKeepsBorrowedRuntimeOpen()
            throws Exception {
        try (DeploymentRuntime runtime = runtime();
             ServerSocket occupied = reserveTestPort()) {
            int port = occupied.getLocalPort();
            assertThrows(RuntimeException.class, () -> new MasterServer(
                    5_000L, port, new InProcessRegistry(),
                    JobMonitorRegistry.NOOP, null, runtime));
            assertDoesNotThrow(runtime::requireAdmissionOpen);
            occupied.close();
            try (MasterServer replacement = new MasterServer(
                    5_000L, port, new InProcessRegistry(),
                    JobMonitorRegistry.NOOP, null, runtime)) {
                // exact-port replacement proves pre-bind resources did not block recovery
            }
        }
    }

    @Test
    void fixedPortCanQueueRequestAtRpcBoundButHandlerCannotRunBeforeActivation()
            throws Exception {
        int port = freePort();
        RuntimeException primary = new RuntimeException("stop-at-rpc-bound");
        AtomicReference<Socket> queuedRef = new AtomicReference<>();
        try (DeploymentRuntime runtime = runtime()) {
            RuntimeException observed = assertThrows(RuntimeException.class, () ->
                    new MasterServer(5_000L, port, new InProcessRegistry(),
                            JobMonitorRegistry.NOOP, null,
                            MasterServer.RuntimeSource.borrowed(runtime), step -> {
                        if (step != MasterConstructionStep.RPC_BOUND) return;
                        try {
                            Socket queued = new Socket("127.0.0.1", port);
                            queuedRef.set(queued);
                            queued.setSoTimeout(150);
                            RpcCodec.encodeTo(RpcMessage.heartbeat("early"),
                                    queued.getOutputStream());
                            queued.getOutputStream().flush();
                            assertThrows(SocketTimeoutException.class,
                                    () -> queued.getInputStream().read());
                        } catch (RuntimeException runtimeFailure) {
                            throw runtimeFailure;
                        } catch (Exception checkedFailure) {
                            throw new RuntimeException(checkedFailure);
                        }
                        throw primary;
                    }));
            assertSame(primary, observed);
            try (Socket queued = queuedRef.get()) {
                try {
                    assertEquals(-1, queued.getInputStream().read());
                } catch (java.io.IOException reset) {
                    // Dormant rollback may terminate the queued connection by FIN or RST.
                }
            }
            try (MasterServer replacement = new MasterServer(
                    5_000L, port, new InProcessRegistry(),
                    JobMonitorRegistry.NOOP, null, runtime)) {
                // exact port is reusable after dormant rollback
            }
        }
    }

    @Test
    void purePropertyValidationRunsBeforeOwnedRuntimeFactory() {
        String key = "rowweave.master.closeDrainMs";
        String previous = System.getProperty(key);
        AtomicInteger creates = new AtomicInteger();
        try {
            System.setProperty(key, "-1");
            MasterServer.RuntimeSource source = MasterServer.RuntimeSource.owned(() -> {
                creates.incrementAndGet();
                return runtime();
            });
            assertThrows(IllegalArgumentException.class, () -> new MasterServer(
                    5_000L, 0, new InProcessRegistry(), JobMonitorRegistry.NOOP,
                    null, source, RoleConstructionProbe.NOOP));
            assertEquals(0, creates.get());
        } finally {
            if (previous == null) System.clearProperty(key);
            else System.setProperty(key, previous);
        }
    }

    private static DeploymentRuntime runtime() {
        Properties properties = new Properties();
        return DeploymentRuntime.load(properties,
                ControlSecurityConfig.fromProperties(properties),
                1024L * 1024L * 1024L);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = reserveTestPort()) {
            return socket.getLocalPort();
        }
    }

    private static ServerSocket reserveTestPort() throws Exception {
        IOException lastFailure = null;
        for (int attempt = 0; attempt < 10_000; attempt++) {
            int candidate = NEXT_TEST_PORT.getAndUpdate(port -> port >= 39_999 ? 20_000 : port + 1);
            ServerSocket socket = new ServerSocket();
            try {
                socket.bind(new InetSocketAddress("127.0.0.1", candidate));
                return socket;
            } catch (IOException occupied) {
                lastFailure = occupied;
                socket.close();
            }
        }
        throw new IOException("no non-ephemeral test port available", lastFailure);
    }

    private static final class BareRegistry implements ServiceRegistry {
        private final InProcessRegistry delegate = new InProcessRegistry();

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
