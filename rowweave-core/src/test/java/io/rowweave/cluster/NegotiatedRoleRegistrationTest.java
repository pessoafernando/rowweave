package io.rowweave.cluster;

import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.ControlMessageId;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.ProtocolHelloConfig;
import io.rowweave.cluster.rpc.ProtocolHelloV1;
import io.rowweave.cluster.rpc.ProtocolHelloV1Codec;
import io.rowweave.cluster.rpc.ProtocolRevisionRange;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.upgrade.StoreFormatHeader;
import io.rowweave.upgrade.WriterRevisionDecision;
import io.rowweave.upgrade.WriterRevisionGate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NegotiatedRoleRegistrationTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void masterPersistsTheHandshakeHelloForTheExactWorkerIncarnation() {
        ProtocolHelloV1 hello = hello();
        Properties properties = properties(hello);
        ControlSecurityConfig security =
                ControlSecurityConfig.fromProperties(properties);
        InProcessRegistry registry = new InProcessRegistry();
        try (DeploymentRuntime masterRuntime = DeploymentRuntime.load(
                     properties, security, Runtime.getRuntime().maxMemory());
             DeploymentRuntime workerRuntime = DeploymentRuntime.load(
                     properties, security, Runtime.getRuntime().maxMemory(), null);
             MasterServer master = new MasterServer(5_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     null, masterRuntime)) {
            master.startFixedLeadershipServices();
            try (WorkerServer worker = new WorkerServer(
                    "negotiated-worker", 4,
                    new FixedMasterLocator(new Endpoint(
                            "127.0.0.1", master.port())),
                    temporaryDirectory.toFile(), workerRuntime)) {
                RegistryWorker registered = registry.liveWorkers().stream()
                        .filter(candidate -> "negotiated-worker".equals(
                                candidate.workerId()))
                        .findFirst().orElse(null);
                assertNotNull(registered);
                assertNotNull(registered.incarnationId());
                assertEquals(hello, registered.protocolHello());
                assertEquals(hello, master.worker("negotiated-worker")
                        .protocolHello());
                assertEquals(1, schedulableWorkers(master).size());
                assertTrue(schedulableWorkers(master,
                        Collections.singleton("rowweave.runtime.n-only.v1"))
                        .isEmpty());
                registry.report(new RegistryWorker(
                        registered.workerId(), registered.host(),
                        registered.port(), registered.slots(),
                        registered.dataPort(), registered.incarnationId(),
                        registered.securityMetadata(),
                        registered.dataPlaneRegistration(), hello('b')));
                assertTrue(schedulableWorkers(master).isEmpty(),
                        "assignment lookup must reject a registry hello that was not negotiated");
                try (ClusterJobClient client = new ClusterJobClient(
                        new FixedMasterLocator(new Endpoint(
                                "127.0.0.1", master.port())), masterRuntime)) {
                    assertNull(client.queryRun("missing-run"));
                }
            }
        }
    }

    @Test
    void mixedWorkersStayOnTheLowestWriterUntilTheOldIncarnationIsReplaced() {
        ProtocolHelloV1 current = hello('a', 0, 1);
        ProtocolHelloV1 previous = hello('a', 0, 0);
        Properties masterProperties = properties(current);
        ControlSecurityConfig security =
                ControlSecurityConfig.fromProperties(masterProperties);
        InProcessRegistry registry = new InProcessRegistry();
        try (DeploymentRuntime masterRuntime = DeploymentRuntime.load(
                     masterProperties, security, Runtime.getRuntime().maxMemory());
             DeploymentRuntime oldRuntime = DeploymentRuntime.load(
                     properties(previous), security,
                     Runtime.getRuntime().maxMemory(), null);
             DeploymentRuntime currentRuntime = DeploymentRuntime.load(
                     properties(current), security,
                     Runtime.getRuntime().maxMemory(), null);
             MasterServer master = new MasterServer(5_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     null, masterRuntime)) {
            master.startFixedLeadershipServices();
            FixedMasterLocator locator = new FixedMasterLocator(
                    new Endpoint("127.0.0.1", master.port()));
            try (WorkerServer oldWorker = WorkerServer.newUnguarded(
                         "rolling-old", 4, locator,
                         temporaryDirectory.resolve("old").toFile(), oldRuntime);
                 WorkerServer newWorker = WorkerServer.newUnguarded(
                         "rolling-new", 4, locator,
                         temporaryDirectory.resolve("new").toFile(), currentRuntime)) {
                WriterRevisionDecision mixed = WriterRevisionGate.evaluate(
                        new StoreFormatHeader(1, "rolling-store", 1, 0,
                                repeat('a', 64)),
                        workerHellos(registry));
                assertEquals(0, mixed.maximumCommonRevision());
            }

            try (DeploymentRuntime replacementRuntime = DeploymentRuntime.load(
                         properties(current), security,
                         Runtime.getRuntime().maxMemory(), null);
                 WorkerServer replacement = WorkerServer.newUnguarded(
                         "rolling-old", 4, locator,
                         temporaryDirectory.resolve("replacement").toFile(),
                         replacementRuntime)) {
                WriterRevisionDecision upgraded = WriterRevisionGate.evaluate(
                        new StoreFormatHeader(1, "rolling-store", 1, 0,
                                repeat('a', 64)),
                        workerHellos(registry));
                assertEquals(1, upgraded.requirePromotion(1));
            }
        }
    }

    private static Properties properties(ProtocolHelloV1 hello) {
        Properties properties = new Properties();
        properties.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY,
                hex(ProtocolHelloV1Codec.encode(hello)));
        return properties;
    }

    private static ProtocolHelloV1 hello() {
        return hello('a');
    }

    private static ProtocolHelloV1 hello(char digest) {
        return hello(digest, 0, 1);
    }

    private static ProtocolHelloV1 hello(
            char digest, int minimumWriter, int maximumWriter) {
        List<Integer> messageIds = new ArrayList<>();
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            messageIds.add(ControlMessageId.wireId(type));
        }
        return new ProtocolHelloV1(repeat(digest, 64), 1,
                new ProtocolRevisionRange(0, 1),
                new ProtocolRevisionRange(minimumWriter, maximumWriter), messageIds,
                Collections.singleton(1), Collections.singleton(7),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton("rowweave.runtime.batch.v1"),
                Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap(),
                80 * 1024 * 1024, 128L * 1024 * 1024);
    }

    private static List<ProtocolHelloV1> workerHellos(
            InProcessRegistry registry) {
        return registry.liveWorkers().stream()
                .map(RegistryWorker::protocolHello)
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private static List<RegistryWorker> schedulableWorkers(MasterServer master) {
        try {
            Method method = MasterServer.class.getDeclaredMethod(
                    "schedulableWorkers");
            method.setAccessible(true);
            return (List<RegistryWorker>) method.invoke(master);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
    }

    @SuppressWarnings("unchecked")
    private static List<RegistryWorker> schedulableWorkers(
            MasterServer master, java.util.Collection<String> capabilities) {
        try {
            Method method = MasterServer.class.getDeclaredMethod(
                    "schedulableWorkers", java.util.Collection.class);
            method.setAccessible(true);
            return (List<RegistryWorker>) method.invoke(master, capabilities);
        } catch (ReflectiveOperationException failure) {
            throw new AssertionError(failure);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            int unsigned = value & 0xff;
            result.append("0123456789abcdef".charAt(unsigned >>> 4));
            result.append("0123456789abcdef".charAt(unsigned & 0x0f));
        }
        return result.toString();
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
