package io.rowweave.cluster;

import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.FixedMasterLocator;
import io.rowweave.cluster.MasterServer;
import io.rowweave.cluster.WorkerServer;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.rpc.ControlMessageId;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.ProtocolHelloConfig;
import io.rowweave.cluster.rpc.ProtocolHelloV1;
import io.rowweave.cluster.rpc.ProtocolHelloV1Codec;
import io.rowweave.cluster.rpc.ProtocolRevisionRange;
import io.rowweave.cluster.rpc.RpcClient;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.deployment.DeploymentRuntime;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlProtocolRollingProcessTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    @Timeout(30)
    void currentMasterRunsWithFrozenPreviousAndCurrentWorkers() throws Exception {
        ProtocolHelloV1 current = currentHello();
        Properties properties = properties(current);
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(properties);
        InProcessRegistry registry = new InProcessRegistry();
        try (DeploymentRuntime masterRuntime = DeploymentRuntime.load(
                     properties, security, Runtime.getRuntime().maxMemory());
             DeploymentRuntime workerRuntime = DeploymentRuntime.load(
                     properties, security, Runtime.getRuntime().maxMemory(), null);
             MasterServer master = new MasterServer(5_000L, 0, registry,
                     io.rowweave.monitor.JobMonitorRegistry.NOOP,
                     null, masterRuntime)) {
            master.startFixedLeadershipServices();
            Process frozen = startRunner("worker", "127.0.0.1",
                    String.valueOf(master.port()), "frozen-worker");
            try (BufferedReader output = reader(frozen);
                 BufferedWriter input = writer(frozen);
                 WorkerServer worker = WorkerServer.newUnguarded(
                         "current-worker", 1,
                         new FixedMasterLocator(new Endpoint(
                                 "127.0.0.1", master.port())),
                         temporaryDirectory.resolve("current").toFile(), workerRuntime)) {
                assertEquals("REGISTERED|ACK|writer=0", output.readLine());
                await(() -> registry.worker("frozen-worker") != null
                        && registry.worker("current-worker") != null);
                RegistryWorker previous = registry.worker("frozen-worker");
                assertNotNull(previous.protocolHello());
                assertEquals(0, previous.protocolHello().writerRevisions().maximum());
                assertEquals(1, registry.worker("current-worker")
                        .protocolHello().writerRevisions().maximum());

                Process frozenClient = startRunner("client", "127.0.0.1",
                        String.valueOf(master.port()));
                try (BufferedReader clientOutput = reader(frozenClient)) {
                    assertEquals("CLIENT|RUN_SNAPSHOT|writer=0",
                            clientOutput.readLine());
                    assertTrue(frozenClient.waitFor(10, TimeUnit.SECONDS));
                    assertEquals(0, frozenClient.exitValue(),
                            drain(clientOutput));
                } finally {
                    frozenClient.destroyForcibly();
                }
                input.write("STOP\n");
                input.flush();
                assertTrue(frozen.waitFor(10, TimeUnit.SECONDS));
                assertEquals(0, frozen.exitValue(), drain(output));
                await(() -> registry.worker("frozen-worker") == null);
                assertNotNull(registry.worker("current-worker"));
            } finally {
                frozen.destroyForcibly();
            }
        }
    }

    @Test
    @Timeout(30)
    void frozenPreviousMasterAcceptsCurrentWorkerAndCurrentClient()
            throws Exception {
        Process frozenMaster = startRunner("serve");
        try (BufferedReader output = reader(frozenMaster)) {
            String ready = output.readLine();
            assertNotNull(ready);
            assertTrue(ready.startsWith("READY|"), ready);
            int port = Integer.parseInt(ready.substring("READY|".length()));
            ProtocolHelloV1 current = currentHello();
            Properties properties = properties(current);
            ControlSecurityConfig security =
                    ControlSecurityConfig.fromProperties(properties);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         properties, security, Runtime.getRuntime().maxMemory(),
                         null);
                 WorkerServer worker = WorkerServer.newUnguarded(
                         "current-worker", 1,
                         new FixedMasterLocator(new Endpoint("127.0.0.1", port)),
                         temporaryDirectory.resolve("worker").toFile(), runtime);
                 RpcClient client = RpcClient.negotiated(
                         "127.0.0.1", port, current)) {
                RpcMessage response = client.call(RpcMessage.queryRun("missing"));
                assertEquals(RpcMessage.Type.RUN_SNAPSHOT, response.type());
                assertFalse(response.success());
            }
            assertTrue(frozenMaster.waitFor(10, TimeUnit.SECONDS));
            assertEquals(0, frozenMaster.exitValue());
            String events = drain(output);
            assertTrue(events.contains("REGISTER_WORKER"), events);
            assertTrue(events.contains("QUERY_RUN"), events);
            assertTrue(events.contains("DEREGISTER_WORKER"), events);
        } finally {
            frozenMaster.destroyForcibly();
        }
    }

    @Test
    @Timeout(30)
    void frozenWorkerRenegotiatesAfterMasterReplacement() throws Exception {
        ProtocolHelloV1 current = currentHello();
        Properties properties = properties(current);
        ControlSecurityConfig security =
                ControlSecurityConfig.fromProperties(properties);
        InProcessRegistry registry = new InProcessRegistry();
        Process frozen = null;
        try (DeploymentRuntime firstRuntime = DeploymentRuntime.load(
                     properties, security, Runtime.getRuntime().maxMemory());
             DeploymentRuntime secondRuntime = DeploymentRuntime.load(
                     properties, security, Runtime.getRuntime().maxMemory(), null)) {
            MasterServer first = master(registry, firstRuntime);
            frozen = startRunner("worker", "127.0.0.1",
                    String.valueOf(first.port()), "moving-worker");
            try (BufferedReader output = reader(frozen);
                 BufferedWriter input = writer(frozen)) {
                assertEquals("REGISTERED|ACK|writer=0", output.readLine());
                await(() -> first.worker("moving-worker") != null);
                first.close();
                try (MasterServer replacement = master(registry, secondRuntime)) {
                    input.write("MOVE|" + replacement.port());
                    input.newLine();
                    input.flush();
                    assertEquals("MOVED|ACK|writer=0", output.readLine());
                    await(() -> replacement.worker("moving-worker") != null);
                    input.write("STOP");
                    input.newLine();
                    input.flush();
                    assertTrue(frozen.waitFor(10, TimeUnit.SECONDS));
                    assertEquals(0, frozen.exitValue(), drain(output));
                }
            }
        } finally {
            if (frozen != null) frozen.destroyForcibly();
        }
    }

    @Test
    @Timeout(30)
    void frozenMasterRejectsNewRequiredExtensionBeforeBusinessDispatch()
            throws Exception {
        Process frozenMaster = startRunner("serve");
        try (BufferedReader output = reader(frozenMaster)) {
            String ready = output.readLine();
            assertNotNull(ready);
            int port = Integer.parseInt(ready.substring("READY|".length()));
            try (Socket peer = new Socket("127.0.0.1", port)) {
                peer.getOutputStream().write(ProtocolHelloV1Codec.encode(
                        currentHelloWithRequiredExtension()));
                peer.getOutputStream().flush();
                try {
                    assertEquals(-1, peer.getInputStream().read());
                } catch (java.net.SocketException reset) {
                    // Fail-closed shutdown may surface as EOF or RST.
                }
            }
            assertTrue(frozenMaster.waitFor(20, TimeUnit.SECONDS));
            String events = drain(output);
            assertFalse(events.contains("REGISTER_WORKER"), events);
            assertTrue(events.contains("incompatible peer hello"), events);
        } finally {
            frozenMaster.destroyForcibly();
        }
    }

    private static Process startRunner(String... arguments) throws Exception {
        List<String> command = new ArrayList<String>();
        command.add(Paths.get(System.getProperty("java.home"), "bin", "java")
                .toString());
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add("compat.v1_4.ControlProtocolV14Runner");
        Collections.addAll(command, arguments);
        return new ProcessBuilder(command).redirectErrorStream(true).start();
    }

    private static MasterServer master(
            InProcessRegistry registry, DeploymentRuntime runtime) {
        MasterServer master = new MasterServer(5_000L, 0, registry,
                io.rowweave.monitor.JobMonitorRegistry.NOOP, null, runtime);
        master.startFixedLeadershipServices();
        return master;
    }

    private static BufferedReader reader(Process process) {
        return new BufferedReader(new InputStreamReader(
                process.getInputStream(), StandardCharsets.UTF_8));
    }

    private static BufferedWriter writer(Process process) {
        return new BufferedWriter(new OutputStreamWriter(
                process.getOutputStream(), StandardCharsets.UTF_8));
    }

    private static String drain(BufferedReader reader) throws Exception {
        StringBuilder result = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            if (result.length() > 0) result.append('\n');
            result.append(line);
        }
        return result.toString();
    }

    private static ProtocolHelloV1 currentHello() {
        List<Integer> messageIds = new ArrayList<Integer>();
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            messageIds.add(ControlMessageId.wireId(type));
        }
        return new ProtocolHelloV1(repeat('a', 64), 1,
                new ProtocolRevisionRange(0, 1),
                new ProtocolRevisionRange(0, 1), messageIds,
                Collections.singleton(1), Collections.singleton(7),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton("rowweave.runtime.batch.v1"),
                Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap(),
                80 * 1024 * 1024, 128L * 1024 * 1024);
    }

    private static ProtocolHelloV1 currentHelloWithRequiredExtension() {
        List<Integer> messageIds = new ArrayList<Integer>();
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            messageIds.add(ControlMessageId.wireId(type));
        }
        return new ProtocolHelloV1(repeat('a', 64), 1,
                new ProtocolRevisionRange(0, 1),
                new ProtocolRevisionRange(0, 1), messageIds,
                Collections.singleton(1), Collections.singleton(7),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton("rowweave.runtime.batch.v1"),
                Collections.singleton(201),
                Collections.singletonMap(201, new byte[] {1}),
                80 * 1024 * 1024, 128L * 1024 * 1024);
    }

    private static Properties properties(ProtocolHelloV1 hello) {
        Properties properties = new Properties();
        properties.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY,
                hex(ProtocolHelloV1Codec.encode(hello)));
        return properties;
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte item : bytes) {
            value.append(Character.forDigit((item >>> 4) & 0xf, 16));
            value.append(Character.forDigit(item & 0xf, 16));
        }
        return value.toString();
    }

    private static String repeat(char value, int count) {
        char[] values = new char[count];
        java.util.Arrays.fill(values, value);
        return new String(values);
    }

    private static void await(Check check) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (!check.ok() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(check.ok(), "condition did not become true");
    }

    private interface Check {
        boolean ok();
    }
}
