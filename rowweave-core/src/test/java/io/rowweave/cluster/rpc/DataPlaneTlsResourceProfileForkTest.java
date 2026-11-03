package io.rowweave.cluster.rpc;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.ReferenceCountUtil;
import io.rowweave.cluster.ProcessTlsEnvelope;
import io.rowweave.cluster.DataPlaneConfig;
import io.rowweave.cluster.TransportSecurityTestAccess;
import io.rowweave.sched.net.DataPlaneHandshakeAdmission;
import io.rowweave.sched.net.DataInboundRegistry;
import io.rowweave.sched.net.FrameCodec;
import io.rowweave.sched.net.WorkerDataHello;
import io.rowweave.sched.net.WorkerDataIdentity;
import io.rowweave.sched.net.WorkerDataServer;
import io.rowweave.deployment.DeploymentRuntime;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.function.Executable;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DataPlaneTlsResourceProfileForkTest {

    private static final String PASSWORD = "changeit";

    @Test
    @Timeout(300)
    void certifiedBuildStaysWithinAdmissionChargeAcrossRequiredProfiles(
            @TempDir Path temp) throws Exception {
        Fixtures fixtures = material(temp);
        System.out.println("TLS_BOUNDARY_CHAIN_BYTES="
                + fixtures.boundary.server.certificateChainBytes);
        assertBootstrapBoundary(fixtures.single);
        assertTopologyRoleGatePrecedesRoleResources(fixtures.single);
        assertRuntimeCloseRejectsActiveHandshake(fixtures.single);
        assertRuntimeNettyPropertyDriftRejected(fixtures.single);
        List<Executable> profileChecks = new ArrayList<>();
        profileChecks.add(() -> assertProfile(fixtures.single, "default"));
        profileChecks.add(() -> assertProfile(fixtures.single, "fragmented"));
        profileChecks.add(() -> assertProfile(fixtures.single, "failed"));
        profileChecks.add(() -> assertProfile(fixtures.single, "wrong-host"));
        profileChecks.add(() -> assertProfile(fixtures.single, "renegotiate"));
        profileChecks.add(() -> assertProfile(fixtures.single, "callback-failure"));
        for (int run = 0; run < 3; run++) {
            profileChecks.add(() -> assertProfile(
                    fixtures.single, "mixed-concurrent"));
        }
        profileChecks.add(() -> assertDirectionalBoundaryProfile(
                fixtures.boundary, "boundary", temp));
        for (int run = 0; run < 3; run++) {
            profileChecks.add(() -> assertDirectionalBoundaryProfile(
                    fixtures.boundary, "concurrent-boundary", temp));
        }
        assertAll("certified TLS resource profiles", profileChecks);
    }

    @Test
    @Tag("profile")
    @Timeout(180)
    void reservedV1MaximumPartialFrameStaysWithinHeapDirectAndRssEnvelope(
            @TempDir Path temp) throws Exception {
        Material material = material(temp).single;
        long scratch = 64L * 1024L;
        long frameCharge = FrameCodec.V1_HEADER_BYTES
                + (long) FrameCodec.MAX_FRAME_BYTES + 128L;
        long ingressBytes = 4L * scratch + frameCharge;
        List<String> command = securityCommand(material);
        command.add("-Drowweave.shuffle.maxConnections=4");
        command.add("-Drowweave.shuffle.ingressResidentBytes=" + ingressBytes);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(IsolatedReservedIngressProbe.class.getName());
        Path coordination = Files.createDirectory(temp.resolve("isolated-ingress"));
        Path receiverLog = coordination.resolve("receiver.log");
        Path senderLog = coordination.resolve("sender.log");
        List<String> receiverCommand = new ArrayList<>(command);
        Collections.addAll(receiverCommand, "receiver", coordination.toString());
        Process receiver = new ProcessBuilder(receiverCommand).redirectErrorStream(true)
                .redirectOutput(receiverLog.toFile()).start();
        Process sender = null;
        try {
            IsolatedReservedIngressProbe.awaitMarker(coordination.resolve("port"), 15L);
            List<String> senderCommand = new ArrayList<>(command);
            Collections.addAll(senderCommand, "sender", coordination.toString());
            sender = new ProcessBuilder(senderCommand).redirectErrorStream(true)
                    .redirectOutput(senderLog.toFile()).start();
            awaitProcess(receiver, 120L, "isolated reserved V1 receiver profile");
            awaitProcess(sender, 20L, "isolated reserved V1 sender profile");
            String receiverText = new String(Files.readAllBytes(receiverLog), StandardCharsets.UTF_8);
            String senderText = new String(Files.readAllBytes(senderLog), StandardCharsets.UTF_8);
            assertEquals(0, receiver.exitValue(), receiverText);
            assertEquals(0, sender.exitValue(), senderText);
            assertTrue(receiverText.contains("RESERVED_INGRESS_PROFILE_OK"), receiverText);
            assertTrue(receiverText.contains("provider=SunJSSE"), receiverText);
            assertTrue(receiverText.contains("receivedBytes=" + (FrameCodec.MAX_FRAME_BYTES - 1)), receiverText);
            assertTrue(senderText.contains("ISOLATED_INGRESS_SENDER_OK"), senderText);
            System.out.println(receiverText.trim());
            System.out.println(senderText.trim());
        } catch (Exception | AssertionError failure) {
            if (Files.isRegularFile(receiverLog)) {
                System.out.println("ISOLATED_RECEIVER_FAILURE_LOG\n"
                        + new String(Files.readAllBytes(receiverLog), StandardCharsets.UTF_8));
            }
            if (Files.isRegularFile(senderLog)) {
                System.out.println("ISOLATED_SENDER_FAILURE_LOG\n"
                        + new String(Files.readAllBytes(senderLog), StandardCharsets.UTF_8));
            }
            throw failure;
        } finally {
            if (sender != null && sender.isAlive()) {
                sender.destroyForcibly().waitFor(5L, TimeUnit.SECONDS);
            }
            if (receiver.isAlive()) {
                receiver.destroyForcibly().waitFor(5L, TimeUnit.SECONDS);
            }
        }
    }

    private static void assertBootstrapBoundary(Material material) throws Exception {
        List<String> command = securityCommand(material);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(LateBootstrapProbe.class.getName());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = awaitProcess(process, 15L, "late bootstrap");
        String text = new String(output, StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), text);
        assertTrue(text.contains("LATE_CONFIG_ORIGIN_REJECTED"), text);
        assertTrue(text.contains("PREMAIN_JSSE_UNSUPPORTED"), text);
    }

    private static void assertRuntimeCloseRejectsActiveHandshake(Material material)
            throws Exception {
        List<String> command = securityCommand(material);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(RuntimeCloseProbe.class.getName());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = awaitProcess(process, 15L, "runtime active handshake close");
        String text = new String(output, StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), text);
        assertTrue(text.contains("RUNTIME_ACTIVE_HANDSHAKE_REJECTED"), text);
    }

    private static void assertRuntimeNettyPropertyDriftRejected(Material material)
            throws Exception {
        List<String> command = securityCommand(material);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(RuntimeNettyDriftProbe.class.getName());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = awaitProcess(process, 15L, "runtime Netty property drift");
        String text = new String(output, StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), text);
        assertTrue(text.contains("RUNTIME_NETTY_DRIFT_REJECTED"), text);
    }

    private static void assertTopologyRoleGatePrecedesRoleResources(Material material)
            throws Exception {
        List<String> command = securityCommand(material);
        command.add("-Drowweave.master.enable=true");
        command.add("-Drowweave.worker.enable=true");
        command.add("-Drowweave.worker.slots=8");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(io.rowweave.cluster.ClusterMain.class.getName());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = awaitProcess(process, 15L, "topology role gate");
        String text = new String(output, StandardCharsets.UTF_8);
        assertTrue(process.exitValue() != 0, text);
        assertTrue(text.contains("requires local MASTER role"), text);
        assertTrue(!text.contains("MASTER_PORT="), text);
        assertTrue(!text.contains("WORKER_READY="), text);
    }

    private static void assertProfile(Material material, String mode, String... extra)
            throws Exception {
        List<String> command = securityCommand(material);
        Collections.addAll(command, extra);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(Probe.class.getName());
        command.add(mode);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = awaitProcess(process, 30L, mode);
        String text = new String(output, StandardCharsets.UTF_8);
        assertEquals(0, process.exitValue(), text);
        assertTrue(text.contains("PROFILE_OK mode=" + mode), text);
        assertTrue(text.contains("provider=SunJSSE"), text);
        System.out.println(text.trim());
    }

    private static List<String> securityCommand(Material material) {
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java").getAbsolutePath());
        command.add("-Xms128m");
        command.add("-Xmx4096m");
        command.add("-XX:MaxDirectMemorySize=256m");
        command.add("-XX:MaxMetaspaceSize=256m");
        command.add("-XX:ReservedCodeCacheSize=128m");
        command.add("-Xss1m");
        command.add("-XX:ActiveProcessorCount=4");
        command.add("-Drowweave.process.memoryLimitBytes=8589934592");
        command.add("-Drowweave.control.residentBytes=234881024");
        command.add("-Drowweave.deployment.memoryBytes=8388608");
        command.add("-Drowweave.shuffle.ingressResidentBytes=100663296");
        command.add("-Drowweave.control.securityProfile=MTLS");
        command.add("-Drowweave.control.host=127.0.0.1");
        command.add("-D" + ControlTlsContext.KEY_STORE_PROPERTY + '=' + material.keyStore);
        command.add("-D" + ControlTlsContext.KEY_STORE_PASSWORD_PROPERTY + '=' + PASSWORD);
        command.add("-D" + ControlTlsContext.TRUST_STORE_PROPERTY + '=' + material.trustStore);
        command.add("-D" + ControlTlsContext.TRUST_STORE_PASSWORD_PROPERTY + '=' + PASSWORD);
        command.add("-D" + ControlTlsContext.AUTHZ_FILE_PROPERTY + '=' + material.authz);
        command.add("-Drowweave.test.tlsCertificateChainBytes="
                + material.certificateChainBytes);
        return command;
    }

    private static void assertDirectionalBoundaryProfile(
            BoundaryMaterial material, String mode, Path temp) throws Exception {
        assertAll("directional TLS boundary orientations",
                () -> assertDirectionalBoundaryPair(
                        material.server, material.client, mode, "large-server", temp),
                () -> assertDirectionalBoundaryPair(
                        material.client, material.server, mode, "large-client", temp));
    }

    private static void assertDirectionalBoundaryPair(
            Material serverMaterial, Material clientMaterial, String mode,
            String orientation, Path temp) throws Exception {
        Path portFile = temp.resolve(mode + '-' + orientation + "-port.txt");
        Path readyFile = temp.resolve(mode + '-' + orientation + "-ready.txt");
        Files.deleteIfExists(portFile);
        Files.deleteIfExists(readyFile);
        Files.deleteIfExists(readyFile.resolveSibling(
                readyFile.getFileName().toString() + ".warm"));
        Process server = new ProcessBuilder(directionalCommand(
                serverMaterial, clientMaterial, "server", mode,
                portFile, readyFile, 0)).redirectErrorStream(true).start();
        ExecutorService drains = Executors.newFixedThreadPool(2);
        Future<byte[]> serverOutput = drains.submit(() -> {
            try (InputStream input = server.getInputStream()) {
                return readAll(input);
            }
        });
        Process client = null;
        try {
            int port = awaitPort(portFile, server);
            client = new ProcessBuilder(directionalCommand(
                    clientMaterial, serverMaterial, "client", mode,
                    portFile, readyFile, port)).redirectErrorStream(true).start();
            final Process observedClient = client;
            Future<byte[]> clientOutput = drains.submit(() -> {
                try (InputStream input = observedClient.getInputStream()) {
                    return readAll(input);
                }
            });
            boolean clientExited = client.waitFor(30, TimeUnit.SECONDS);
            boolean serverExited = server.waitFor(30, TimeUnit.SECONDS);
            if (!clientExited) client.destroyForcibly();
            if (!serverExited) server.destroyForcibly();
            String clientText = new String(clientOutput.get(5, TimeUnit.SECONDS),
                    StandardCharsets.UTF_8);
            String serverText = new String(serverOutput.get(5, TimeUnit.SECONDS),
                    StandardCharsets.UTF_8);
            assertTrue(clientExited, clientText);
            assertTrue(serverExited, serverText);
            String pairText = "CLIENT:\n" + clientText + "\nSERVER:\n" + serverText;
            assertEquals(0, server.exitValue(), pairText);
            assertEquals(0, client.exitValue(), pairText);
            assertTrue(clientText.contains(
                    "DIRECTION_PROFILE_OK role=client mode=" + mode), clientText);
            assertTrue(serverText.contains(
                    "DIRECTION_PROFILE_OK role=server mode=" + mode), serverText);
            System.out.println(serverText.trim());
            System.out.println(clientText.trim());
        } finally {
            if (client != null && client.isAlive()) client.destroyForcibly();
            if (server.isAlive()) server.destroyForcibly();
            drains.shutdownNow();
            drains.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    private static List<String> directionalCommand(
            Material own, Material peer, String role, String mode,
            Path portFile, Path readyFile, int port) {
        List<String> command = securityCommand(own);
        command.add("-Djdk.tls.maxHandshakeMessageSize=65536");
        command.add("-Djdk.tls.maxCertificateChainLength=10");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(DirectionalProbe.class.getName());
        command.add(role);
        command.add(mode);
        command.add(portFile.toString());
        command.add(readyFile.toString());
        command.add(Integer.toString(port));
        command.add(peer.fingerprint);
        command.add(Integer.toString(peer.certificateCount));
        command.add(Long.toString(peer.certificateChainBytes));
        return command;
    }

    private static int awaitPort(Path portFile, Process server) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            if (Files.isRegularFile(portFile)) {
                String value = new String(Files.readAllBytes(portFile),
                        StandardCharsets.US_ASCII).trim();
                if (!value.isEmpty()) return Integer.parseInt(value);
            }
            if (!server.isAlive()) {
                throw new AssertionError("directional TLS server exited before publishing port");
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("directional TLS server did not publish port");
    }

    private static byte[] awaitProcess(Process process, long timeoutSeconds, String name)
            throws Exception {
        ExecutorService drainer = Executors.newSingleThreadExecutor();
        Future<byte[]> captured = drainer.submit(() -> {
            try (InputStream input = process.getInputStream()) {
                return readAll(input);
            }
        });
        try {
            boolean exited = process.waitFor(timeoutSeconds, TimeUnit.SECONDS);
            if (!exited) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
            assertTrue(exited, name + " child did not exit");
            return captured.get(5, TimeUnit.SECONDS);
        } finally {
            drainer.shutdownNow();
            drainer.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    public static final class Probe {
        private Probe() {
        }

        public static void main(String[] args) {
            Runner runner = null;
            try {
                String mode = args.length == 0 ? "default" : args[0];
                ProcessTlsEnvelope proof = TransportSecurityTestAccess.bootstrap();
                proof.requireCertifiedForMtls();
                ControlSecurityConfig control = ControlSecurityConfig.fromProperties(
                        System.getProperties(), proof);
                Properties controlOnlyProperties = new Properties();
                controlOnlyProperties.putAll(System.getProperties());
                controlOnlyProperties.setProperty("rowweave.shuffle.mux", "false");
                controlOnlyProperties.setProperty(
                        "rowweave.shuffle.maxConcurrentHandshakes", "999");
                try (DeploymentRuntime controlOnly = DeploymentRuntime.load(
                        controlOnlyProperties, control,
                        Runtime.getRuntime().maxMemory())) {
                    try {
                        controlOnly.requireDataPlaneConfig();
                        throw new AssertionError(
                                "control-only runtime exposed Worker data-plane config");
                    } catch (IllegalStateException expected) {
                    }
                    try {
                        controlOnly.requireDataPlaneSecurityContext();
                        throw new AssertionError(
                                "control-only runtime exposed Worker TLS context");
                    } catch (IllegalStateException expected) {
                    }
                }
                DataPlaneConfig dataPlaneConfig = DataPlaneConfig.fromProperties(
                        System.getProperties(), control, Runtime.getRuntime().maxMemory());
                ProcessTlsEnvelope.DataPlaneOwner dataPlaneOwner =
                        proof.claimDataPlaneOwner(dataPlaneConfig);
                try {
                    proof.claimDataPlaneOwner(dataPlaneConfig);
                    throw new AssertionError("second process data-plane owner was accepted");
                } catch (IllegalStateException expected) {
                    if (expected.getMessage() == null
                            || !expected.getMessage().contains("already claimed")) {
                        throw expected;
                    }
                }
                DataPlaneHandshakeAdmission admission = dataPlaneOwner.admission();
                DataPlaneSecurityContext security = control.dataPlaneSecurityContext(
                        proof, 10_000L, dataPlaneOwner);
                try {
                    control.dataPlaneSecurityContext(proof, 10_000L, dataPlaneOwner);
                    throw new AssertionError("second process TLS context was accepted");
                } catch (IllegalStateException expected) {
                    if (expected.getMessage() == null
                            || !expected.getMessage().contains("already created")) {
                        throw expected;
                    }
                }
                Properties replaced = new Properties();
                replaced.putAll(System.getProperties());
                replaced.setProperty("rowweave.shuffle.maxConnections", "255");
                try {
                    DeploymentRuntime.load(replaced, control,
                            Runtime.getRuntime().maxMemory(), proof);
                    throw new AssertionError("replacement startup snapshot was accepted");
                } catch (IllegalStateException expected) {
                    // The proof cannot be paired with later control/data configuration.
                }
                runner = new Runner(mode, security, admission);
                ProfileResult result = runner.measure();
                requireProfileWithin("cold", mode, result.cold,
                        DataPlaneHandshakeAdmission.requiredResidentBytes(
                                result.cold.directionalHandshakes));
                requireProfileWithin("retained", mode, result.retained,
                        DataPlaneHandshakeAdmission.PROCESS_COLD_RESERVE_BYTES);
                requireProfileWithin("warm", mode, result.warm,
                        Math.multiplyExact(
                                (long) result.warm.directionalHandshakes,
                                DataPlaneHandshakeAdmission.HANDSHAKE_CHARGE_BYTES));
                System.out.println("PROFILE_OK mode=" + mode
                        + " vendor=" + proof.javaVendor()
                        + " runtime=" + proof.javaRuntimeVersion()
                        + " provider=" + proof.providerName()
                        + " handshakeLimit=" + optional(
                                proof.configuredMaxHandshakeMessageBytes())
                        + " chainLimit=" + optional(
                                proof.configuredMaxCertificateChainLength())
                        + " coldHeapDelta=" + result.cold.heapDelta
                        + " coldDirectDelta=" + result.cold.directDelta
                        + " coldAllocatorHeapDelta=" + result.cold.allocatorHeapDelta
                        + " coldAllocatorDirectDelta=" + result.cold.allocatorDirectDelta
                        + " retainedHeapDelta=" + result.retained.heapDelta
                        + " retainedDirectDelta=" + result.retained.directDelta
                        + " warmHeapDelta=" + result.warm.heapDelta
                        + " warmDirectDelta=" + result.warm.directDelta
                        + " warmAllocatorHeapDelta=" + result.warm.allocatorHeapDelta
                        + " warmAllocatorDirectDelta=" + result.warm.allocatorDirectDelta);
            } catch (Throwable failure) {
                failure.printStackTrace(System.out);
                System.exit(23);
            } finally {
                if (runner != null) runner.close();
            }
        }

        private static void requireProfileWithin(
                String phase, String mode, Profile profile, long reservedBytes) {
            long resident = Math.addExact(profile.heapDelta, profile.directDelta);
            long allocatorResident = Math.addExact(
                    profile.allocatorHeapDelta, profile.allocatorDirectDelta);
            long limit = reservedBytes - reservedBytes / 5L;
            System.out.println("PROFILE_MEASURED phase=" + phase + " mode=" + mode
                    + " heapDelta=" + profile.heapDelta
                    + " directDelta=" + profile.directDelta
                    + " allocatorHeapDelta=" + profile.allocatorHeapDelta
                    + " allocatorDirectDelta=" + profile.allocatorDirectDelta
                    + " reserved=" + reservedBytes
                    + " certifiedPeakLimit=" + limit);
            if (resident > limit || allocatorResident > limit) {
                throw new AssertionError(phase + " TLS accounting peak global=" + resident
                        + ", allocator=" + allocatorResident
                        + " exceeds 80% certified reserve " + limit
                        + " (heap=" + profile.heapDelta
                        + ", direct=" + profile.directDelta
                        + ", allocatorHeap=" + profile.allocatorHeapDelta
                        + ", allocatorDirect=" + profile.allocatorDirectDelta
                        + ", reserved=" + reservedBytes + ')');
            }
        }

        private static String optional(java.util.OptionalInt value) {
            return value.isPresent() ? Integer.toString(value.getAsInt()) : "absent";
        }
    }

    /** Measure only the production receiver JVM, never an uncharged test sender. */
    public static final class IsolatedReservedIngressProbe {
        private IsolatedReservedIngressProbe() { }

        public static void main(String[] args) {
            try {
                Path root = java.nio.file.Paths.get(args[1]);
                if ("receiver".equals(args[0])) receive(root);
                else if ("sender".equals(args[0])) send(root);
                else throw new IllegalArgumentException("unknown ingress probe role");
            } catch (Throwable failure) {
                failure.printStackTrace(System.out);
                System.exit(41);
            }
        }

        private static void receive(Path root) throws Exception {
            ProcessTlsEnvelope proof = TransportSecurityTestAccess.bootstrap();
            proof.requireCertifiedForMtls();
            ControlSecurityConfig control = control(proof);
            DataPlaneConfig config = DataPlaneConfig.fromProperties(
                    System.getProperties(), control, Runtime.getRuntime().maxMemory());
            DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                    config.maxConcurrentHandshakes(), config.handshakeResidentBytes());
            DataPlaneSecurityContext security = new DataPlaneSecurityContext(
                    control.tlsContext(), control.securityMetadata(), config.handshakeTimeoutMillis(), admission);
            WorkerDataIdentity identity = identity("destination", control, security);
            try (WorkerDataServer server = new WorkerDataServer(
                    new DataInboundRegistry(), 5_000L, 0, config, security, admission, identity)) {
                marker(root.resolve("port"), Integer.toString(server.port()));
                ReservedIngressProbe.awaitIngressState(server, 4, 4L * 64L * 1024L);
                forceCollection();
                long heapBaseline = heapUsed();
                long directBaseline = directUsed();
                long rssBaseline = ReservedIngressProbe.rssBytes();
                marker(root.resolve("baseline"), "ready");
                long frameCharge = FrameCodec.V1_HEADER_BYTES + (long) FrameCodec.MAX_FRAME_BYTES + 128L;
                ReservedIngressProbe.awaitIngressState(server, 1, 64L * 1024L + frameCharge);
                marker(root.resolve("reserved"), "ready");
                awaitMarker(root.resolve("sent"), 30L);
                // Reservation alone proves allocation, not receipt of the maximum
                // partial payload. Observe the real decoder on its owning EventLoop.
                int received = awaitPartialPayload(server);
                forceCollection();
                long heapDelta = Math.max(0L, heapUsed() - heapBaseline);
                long directDelta = Math.max(0L, directUsed() - directBaseline);
                long rssDelta = Math.max(0L, ReservedIngressProbe.rssBytes() - rssBaseline);
                long ingressBytes = 4L * 64L * 1024L + frameCharge;
                String measurement = " provider=" + proof.providerName()
                        + " heapDelta=" + heapDelta + " directDelta=" + directDelta
                        + " rssDelta=" + rssDelta + " ingressBudget=" + ingressBytes
                        + " receivedBytes=" + received;
                System.out.println("RESERVED_INGRESS_PROFILE_MEASURED" + measurement);
                if (heapDelta < 60L * 1024L * 1024L || heapDelta > ingressBytes + 16L * 1024L * 1024L) {
                    throw new AssertionError("reserved ingress heap envelope exceeded: " + measurement);
                }
                if (directDelta > 16L * 1024L * 1024L) {
                    throw new AssertionError("reserved ingress direct envelope exceeded: " + measurement);
                }
                if (rssDelta > ingressBytes + 96L * 1024L * 1024L) {
                    throw new AssertionError("reserved ingress RSS envelope exceeded: " + measurement);
                }
                System.out.println("RESERVED_INGRESS_PROFILE_OK" + measurement);
                marker(root.resolve("measured"), "ready");
                ReservedIngressProbe.awaitIngressState(server, 0, 0L);
            } finally {
                admission.close();
            }
        }

        private static void send(Path root) throws Exception {
            ProcessTlsEnvelope proof = TransportSecurityTestAccess.bootstrap();
            proof.requireCertifiedForMtls();
            ControlSecurityConfig control = control(proof);
            DataPlaneConfig config = DataPlaneConfig.fromProperties(
                    System.getProperties(), control, Runtime.getRuntime().maxMemory());
            DataPlaneHandshakeAdmission admission = DataPlaneHandshakeAdmission.create(
                    config.maxConcurrentHandshakes(), config.handshakeResidentBytes());
            DataPlaneSecurityContext security = new DataPlaneSecurityContext(
                    control.tlsContext(), control.securityMetadata(), config.handshakeTimeoutMillis(), admission);
            EventLoopGroup clients = new NioEventLoopGroup(2);
            List<ReservedIngressProbe.RawDataClient> connections = new ArrayList<>();
            try {
                int port = Integer.parseInt(new String(Files.readAllBytes(root.resolve("port")), StandardCharsets.US_ASCII));
                WorkerDataIdentity identity = identity("source", control, security);
                for (int index = 0; index < 4; index++) {
                    ReservedIngressProbe.RawDataClient client = ReservedIngressProbe.connectRawDataClient(
                            clients, security, admission, port, config.handshakeTimeoutMillis());
                    connections.add(client);
                    client.channel.writeAndFlush(Unpooled.wrappedBuffer(WorkerDataHello.encode(identity))).sync();
                    client.tls.releaseAfterAuthentication();
                }
                awaitMarker(root.resolve("baseline"), 15L);
                java.util.UUID target = java.util.UUID.randomUUID();
                for (ReservedIngressProbe.RawDataClient client : connections) {
                    client.channel.writeAndFlush(Unpooled.buffer(FrameCodec.V1_HEADER_BYTES)
                            .writeByte(FrameCodec.FRAME_RECORD)
                            .writeLong(target.getMostSignificantBits()).writeLong(target.getLeastSignificantBits())
                            .writeInt(1).writeInt(FrameCodec.MAX_FRAME_BYTES)).sync();
                }
                awaitMarker(root.resolve("reserved"), 15L);
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
                ReservedIngressProbe.RawDataClient survivor = null;
                while (System.nanoTime() < deadline) {
                    int active = 0;
                    for (ReservedIngressProbe.RawDataClient client : connections) {
                        if (client.channel.isActive()) { active++; survivor = client; }
                    }
                    if (active == 1) break;
                    survivor = null;
                    Thread.sleep(10L);
                }
                if (survivor == null) throw new AssertionError("maximum-frame survivor unavailable");
                int remaining = FrameCodec.MAX_FRAME_BYTES - 1;
                while (remaining > 0) {
                    int bytes = Math.min(16 * 1024, remaining);
                    // One bounded write at a time; no sender queue is part of the
                    // receiver's ingress resource measurement or reservation.
                    survivor.channel.writeAndFlush(Unpooled.buffer(bytes).writeZero(bytes)).sync();
                    remaining -= bytes;
                }
                marker(root.resolve("sent"), "ready");
                awaitMarker(root.resolve("measured"), 30L);
                System.out.println("ISOLATED_INGRESS_SENDER_OK bytes=" + (FrameCodec.MAX_FRAME_BYTES - 1));
            } finally {
                for (ReservedIngressProbe.RawDataClient client : connections) client.close();
                clients.shutdownGracefully().syncUninterruptibly();
                admission.close();
            }
        }

        private static ControlSecurityConfig control(ProcessTlsEnvelope proof) throws Exception {
            char[] deploymentChars = new char[64];
            java.util.Arrays.fill(deploymentChars, 'd');
            return ControlSecurityConfig.fromProperties(System.getProperties(), proof)
                    .withDeploymentSetId(new String(deploymentChars));
        }

        private static WorkerDataIdentity identity(String role, ControlSecurityConfig control,
                DataPlaneSecurityContext security) {
            return WorkerDataIdentity.authenticated("profile-" + role, "profile-" + role + "-incarnation",
                    DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1, control.securityMetadata().deploymentSetId(),
                    control.securityMetadata().authzSetId(), security.localCertificateFingerprint());
        }

        private static void marker(Path file, String value) throws Exception {
            Path staged = file.resolveSibling(file.getFileName() + ".tmp");
            Files.write(staged, value.getBytes(StandardCharsets.US_ASCII));
            Files.move(staged, file, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        }

        private static void awaitMarker(Path file, long seconds) throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(seconds);
            while (!Files.isRegularFile(file)) {
                if (System.nanoTime() >= deadline) throw new AssertionError("ingress probe marker unavailable: " + file.getFileName());
                Thread.sleep(10L);
            }
        }

        private static int awaitPartialPayload(WorkerDataServer server) throws Exception {
            java.lang.reflect.Field childrenField = WorkerDataServer.class.getDeclaredField("childChannels");
            childrenField.setAccessible(true);
            io.netty.channel.group.ChannelGroup children = (io.netty.channel.group.ChannelGroup) childrenField.get(server);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15L);
            while (System.nanoTime() < deadline) {
                for (Channel channel : children) {
                    int received = channel.eventLoop().submit(() -> {
                        for (io.netty.channel.ChannelHandler handler : channel.pipeline().toMap().values()) {
                            if (handler.getClass().getSimpleName().equals("ReservedV1FrameDecoder")) {
                                java.lang.reflect.Field payload = handler.getClass().getDeclaredField("payload");
                                java.lang.reflect.Field count = handler.getClass().getDeclaredField("payloadLength");
                                payload.setAccessible(true);
                                count.setAccessible(true);
                                byte[] data = (byte[]) payload.get(handler);
                                if (data != null && data.length == FrameCodec.MAX_FRAME_BYTES) return count.getInt(handler);
                            }
                        }
                        return 0;
                    }).get(2L, TimeUnit.SECONDS);
                    if (received == FrameCodec.MAX_FRAME_BYTES - 1) return received;
                }
                Thread.sleep(10L);
            }
            throw new AssertionError("maximum partial payload was not received");
        }
    }

    private static final class ReservedIngressProbe {
        private ReservedIngressProbe() { }

        private static RawDataClient connectRawDataClient(
                EventLoopGroup group, DataPlaneSecurityContext security,
                DataPlaneHandshakeAdmission admission, int port,
                long timeoutMillis) throws Exception {
            long deadline = System.nanoTime()
                    + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
            DataPlaneHandshakeAdmission.Lease lease =
                    admission.acquireOutbound(deadline);
            AtomicReference<DataPlaneSecurityContext.AdmittedTlsChannel> installed =
                    new AtomicReference<>();
            try {
                Bootstrap bootstrap = new Bootstrap()
                        .group(group)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel channel) {
                                installed.set(security.installClientHandler(
                                        channel.pipeline(), "127.0.0.1", port,
                                        lease, deadline));
                                channel.pipeline().addLast(
                                        new ChannelInboundHandlerAdapter() {
                                            @Override
                                            public void channelRead(
                                                    ChannelHandlerContext context,
                                                    Object message) {
                                                ReferenceCountUtil.release(message);
                                            }

                                            @Override
                                            public void exceptionCaught(
                                                    ChannelHandlerContext context,
                                                    Throwable cause) {
                                                context.close();
                                            }
                                        });
                            }
                        });
                Channel channel = bootstrap.connect("127.0.0.1", port).sync().channel();
                DataPlaneSecurityContext.AdmittedTlsChannel tls = installed.get();
                tls.awaitHandshakeSuccess();
                security.requireWorkerPeer(tls, security.localCertificateFingerprint());
                return new RawDataClient(channel, tls);
            } finally {
                lease.close();
            }
        }

        private static void awaitIngressState(
                WorkerDataServer server, int connections, long residentBytes)
                throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
            while (System.nanoTime() < deadline) {
                Object snapshot = ingressSnapshot(server);
                java.lang.reflect.Method active = snapshot.getClass()
                        .getDeclaredMethod("activeConnections");
                java.lang.reflect.Method resident = snapshot.getClass()
                        .getDeclaredMethod("residentBytes");
                active.setAccessible(true);
                resident.setAccessible(true);
                if ((Integer) active.invoke(snapshot) == connections
                        && (Long) resident.invoke(snapshot) == residentBytes) return;
                Thread.sleep(10L);
            }
            throw new AssertionError("reserved ingress state did not converge: "
                    + ingressSnapshot(server));
        }

        private static Object ingressSnapshot(WorkerDataServer server) throws Exception {
            java.lang.reflect.Method snapshot = WorkerDataServer.class
                    .getDeclaredMethod("admissionSnapshot");
            snapshot.setAccessible(true);
            return snapshot.invoke(server);
        }

        private static long rssBytes() throws Exception {
            String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
            String pid = runtimeName.substring(0, runtimeName.indexOf('@'));
            Process process = new ProcessBuilder(
                    "ps", "-o", "rss=", "-p", pid).redirectErrorStream(true).start();
            byte[] output = readAll(process.getInputStream());
            if (process.waitFor() != 0) {
                throw new AssertionError("ps RSS probe failed: "
                        + new String(output, StandardCharsets.UTF_8));
            }
            return Long.parseLong(new String(output, StandardCharsets.US_ASCII).trim())
                    * 1024L;
        }

        private static final class RawDataClient implements AutoCloseable {
            private final Channel channel;
            private final DataPlaneSecurityContext.AdmittedTlsChannel tls;

            private RawDataClient(
                    Channel channel,
                    DataPlaneSecurityContext.AdmittedTlsChannel tls) {
                this.channel = channel;
                this.tls = tls;
            }

            @Override
            public void close() {
                channel.close().syncUninterruptibly();
            }
        }
    }

    public static final class RuntimeCloseProbe {
        private RuntimeCloseProbe() {
        }

        public static void main(String[] args) {
            DeploymentRuntime runtime = null;
            DataPlaneHandshakeAdmission.Lease active = null;
            EmbeddedChannel channel = null;
            try {
                ProcessTlsEnvelope proof = TransportSecurityTestAccess.bootstrap();
                proof.requireCertifiedForMtls();
                ControlSecurityConfig control = ControlSecurityConfig.fromProperties(
                        System.getProperties(), proof);
                runtime = DeploymentRuntime.load(System.getProperties(), control,
                        Runtime.getRuntime().maxMemory(), proof);
                DataPlaneHandshakeAdmission admission =
                        runtime.requireDataPlaneHandshakeAdmission();
                active = admission.acquireInbound(
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
                channel = new EmbeddedChannel();
                runtime.requireDataPlaneSecurityContext()
                        .installServerHandler(channel.pipeline(), active);
                try {
                    runtime.close();
                    throw new AssertionError(
                            "runtime close accepted a live handshake lease");
                } catch (IllegalStateException expected) {
                    if (expected.getMessage() == null
                            || !expected.getMessage().contains("active leases")) {
                        throw expected;
                    }
                }
                System.out.println("RUNTIME_ACTIVE_HANDSHAKE_REJECTED");
            } catch (Throwable failure) {
                failure.printStackTrace(System.out);
                System.exit(31);
            } finally {
                if (channel != null) channel.finishAndReleaseAll();
                if (active != null) active.close();
                if (runtime != null) runtime.close();
            }
        }
    }

    public static final class RuntimeNettyDriftProbe {
        private RuntimeNettyDriftProbe() {
        }

        public static void main(String[] args) {
            DataPlaneHandshakeAdmission admission = null;
            EmbeddedChannel channel = null;
            try {
                ProcessTlsEnvelope proof = TransportSecurityTestAccess.bootstrap();
                proof.requireCertifiedForMtls();
                ControlSecurityConfig control = ControlSecurityConfig.fromProperties(
                        System.getProperties(), proof);
                DataPlaneConfig config = DataPlaneConfig.fromProperties(
                        System.getProperties(), control, Runtime.getRuntime().maxMemory());
                ProcessTlsEnvelope.DataPlaneOwner owner =
                        proof.claimDataPlaneOwner(config);
                admission = owner.admission();
                DataPlaneSecurityContext security = control.dataPlaneSecurityContext(
                        proof, config.handshakeTimeoutMillis(), owner);
                channel = new EmbeddedChannel();
                System.setProperty("io.netty.noPreferDirect", "true");
                try {
                    security.configureChannel(channel.config());
                    throw new AssertionError(
                            "runtime Netty property drift reached a data-plane channel");
                } catch (IllegalStateException expected) {
                    if (expected.getMessage() == null
                            || !expected.getMessage().contains(
                                    "unsupported io.netty.* runtime property")) {
                        throw expected;
                    }
                } finally {
                    System.clearProperty("io.netty.noPreferDirect");
                }
                System.out.println("RUNTIME_NETTY_DRIFT_REJECTED");
            } catch (Throwable failure) {
                failure.printStackTrace(System.out);
                System.exit(31);
            } finally {
                System.clearProperty("io.netty.noPreferDirect");
                if (channel != null) channel.finishAndReleaseAll();
                if (admission != null) admission.close();
            }
        }
    }

    public static final class DirectionalProbe {
        private DirectionalProbe() {
        }

        public static void main(String[] args) {
            DirectionalRunner runner = null;
            try {
                String role = args[0];
                String mode = args[1];
                Path portFile = java.nio.file.Paths.get(args[2]);
                Path readyFile = java.nio.file.Paths.get(args[3]);
                int port = Integer.parseInt(args[4]);
                byte[] expectedFingerprint = decodeFingerprint(args[5]);
                int expectedCertificateCount = Integer.parseInt(args[6]);
                long expectedCertificateBytes = Long.parseLong(args[7]);
                ProcessTlsEnvelope proof = TransportSecurityTestAccess.bootstrap();
                proof.requireCertifiedForMtls();
                ControlSecurityConfig control = ControlSecurityConfig.fromProperties(
                        System.getProperties(), proof);
                DataPlaneConfig dataPlaneConfig = DataPlaneConfig.fromProperties(
                        System.getProperties(), control, Runtime.getRuntime().maxMemory());
                ProcessTlsEnvelope.DataPlaneOwner owner =
                        proof.claimDataPlaneOwner(dataPlaneConfig);
                DataPlaneSecurityContext security = control.dataPlaneSecurityContext(
                        proof, dataPlaneConfig.handshakeTimeoutMillis(), owner);
                runner = new DirectionalRunner(
                        role, mode, portFile, readyFile, port,
                        security, owner.admission(),
                        expectedFingerprint, expectedCertificateCount,
                        expectedCertificateBytes);
                ProfileResult result = runner.measure();
                Probe.requireProfileWithin("directional-cold-" + role, mode,
                        result.cold,
                        DataPlaneHandshakeAdmission.requiredResidentBytes(
                                result.cold.directionalHandshakes));
                Probe.requireProfileWithin("directional-warm-" + role, mode,
                        result.warm,
                        Math.multiplyExact(
                                (long) result.warm.directionalHandshakes,
                                DataPlaneHandshakeAdmission.HANDSHAKE_CHARGE_BYTES));
                Probe.requireProfileWithin("directional-retained-" + role, mode,
                        result.retained,
                        DataPlaneHandshakeAdmission.PROCESS_COLD_RESERVE_BYTES);
                System.out.println("DIRECTION_PROFILE_OK role=" + role
                        + " mode=" + mode
                        + " coldHeapDelta=" + result.cold.heapDelta
                        + " coldDirectDelta=" + result.cold.directDelta
                        + " warmHeapDelta=" + result.warm.heapDelta
                        + " warmDirectDelta=" + result.warm.directDelta
                        + " retainedHeapDelta=" + result.retained.heapDelta
                        + " retainedDirectDelta=" + result.retained.directDelta
                        + " peerCertificateBytes=" + expectedCertificateBytes);
            } catch (Throwable failure) {
                failure.printStackTrace(System.out);
                System.exit(29);
            } finally {
                if (runner != null) runner.close();
            }
        }
    }

    private static final class DirectionalRunner implements AutoCloseable {
        private final String role;
        private final String mode;
        private final Path portFile;
        private final Path readyFile;
        private final int peerPort;
        private final DataPlaneSecurityContext security;
        private final DataPlaneHandshakeAdmission admission;
        private final byte[] expectedFingerprint;
        private final int expectedCertificateCount;
        private final long expectedCertificateBytes;
        private final EventLoopGroup boss = new NioEventLoopGroup(1);
        private final EventLoopGroup ioCols = new NioEventLoopGroup(2);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicInteger tlsPeerIdentity = new AtomicInteger(20_000);
        private Channel listener;

        private DirectionalRunner(
                String role, String mode, Path portFile, Path readyFile, int peerPort,
                DataPlaneSecurityContext security,
                DataPlaneHandshakeAdmission admission,
                byte[] expectedFingerprint, int expectedCertificateCount,
                long expectedCertificateBytes) {
            if (!"server".equals(role) && !"client".equals(role)) {
                throw new IllegalArgumentException("unknown directional TLS role: " + role);
            }
            this.role = role;
            this.mode = mode;
            this.portFile = portFile;
            this.readyFile = readyFile;
            this.peerPort = peerPort;
            this.security = security;
            this.admission = admission;
            this.expectedFingerprint = expectedFingerprint;
            this.expectedCertificateCount = expectedCertificateCount;
            this.expectedCertificateBytes = expectedCertificateBytes;
        }

        private ProfileResult measure() throws Exception {
            int measuredConnections = "concurrent-boundary".equals(mode) ? 2 : 1;
            return "server".equals(role)
                    ? measureServer(measuredConnections)
                    : measureClient(measuredConnections);
        }

        private ProfileResult measureServer(int measuredConnections) throws Exception {
            AtomicReference<ServerRound> activeRound = new AtomicReference<>(
                    new ServerRound(measuredConnections));
            listener = new ServerBootstrap()
                    .group(boss, ioCols)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.AUTO_READ, false)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            ServerRound round = activeRound.get();
                            if (round == null) {
                                channel.close();
                                throw new IllegalStateException(
                                        "directional server accepted outside a measurement round");
                            }
                            security.configureChannel(channel.config());
                            DataPlaneHandshakeAdmission.Lease lease =
                                    admission.acquireInbound(handshakeDeadline());
                            channel.pipeline().addLast(
                                    round.cleanupTracker.newHandler());
                            channel.pipeline().addLast(round.tlsReadGate.newHandler());
                            DataPlaneSecurityContext.AdmittedTlsChannel ssl =
                                    security.installServerHandler(
                                            channel.pipeline(), lease);
                            ssl.whenHandshakeComplete(done -> {
                                try {
                                    if (!done.success()) throw done.failure();
                                    requireExpectedPeer(ssl);
                                    ssl.releaseAfterAuthentication();
                                } catch (Throwable peerFailure) {
                                    failure.compareAndSet(null, peerFailure);
                                } finally {
                                    channel.close().addListener(
                                            ignored -> round.measured.countDown());
                                }
                            });
                            channel.pipeline().addLast(new QuietExceptionHandler());
                            round.heldChannels.add(channel);
                            round.accepted.countDown();
                            if (round.accepted.getCount() == 0L) {
                                if (admission.snapshot().activeInbound()
                                        != measuredConnections) {
                                    failure.compareAndSet(null, new AssertionError(
                                            "inbound handshake slots were not concurrently held"));
                                }
                                SocketChannel held;
                                while ((held = round.heldChannels.poll()) != null) {
                                    held.config().setAutoRead(true);
                                }
                            }
                        }
                    })
                    .bind("127.0.0.1", 0).sync().channel();
            Files.write(portFile, Integer.toString(
                    ((InetSocketAddress) listener.localAddress()).getPort())
                    .getBytes(StandardCharsets.US_ASCII));
            prewarmHandler(DataPlaneHandshakeAdmission.Direction.INBOUND);
            forceCollection();
            long coldHeapBaseline = heapUsed();
            long coldDirectBaseline = directUsed();
            Sampler coldSampler = new Sampler(
                    coldHeapBaseline, coldDirectBaseline, security);
            Thread coldSamplerThread = startSampler(coldSampler, "inbound-cold");
            Files.write(readyFile, new byte[]{1});
            try {
                awaitServerRound(activeRound.get(), measuredConnections, "cold");
            } finally {
                coldSampler.stop();
                coldSamplerThread.join(5_000L);
            }
            Profile cold = profile(coldSampler, coldHeapBaseline, coldDirectBaseline,
                    measuredConnections);
            activeRound.set(null);
            awaitAdmissionIdle();
            failure.set(null);
            forceCollection();
            Profile retained = new Profile(
                    Math.max(0L, heapUsed() - coldHeapBaseline),
                    Math.max(0L, directUsed() - coldDirectBaseline),
                    Math.max(0L, security.allocatedHeapBytes()
                            - coldSampler.allocatorHeapBaseline),
                    Math.max(0L, security.allocatedDirectBytes()
                            - coldSampler.allocatorDirectBaseline), 0);

            long warmHeapBaseline = heapUsed();
            long warmDirectBaseline = directUsed();
            ServerRound warmRound = new ServerRound(measuredConnections);
            activeRound.set(warmRound);
            Sampler warmSampler = new Sampler(
                    warmHeapBaseline, warmDirectBaseline, security);
            Thread warmSamplerThread = startSampler(warmSampler, "inbound-warm");
            Files.write(warmReadyFile(), new byte[]{1});
            try {
                awaitServerRound(warmRound, measuredConnections, "warm");
            } finally {
                warmSampler.stop();
                warmSamplerThread.join(5_000L);
                activeRound.set(null);
            }
            return new ProfileResult(cold,
                    profile(warmSampler, warmHeapBaseline, warmDirectBaseline,
                            measuredConnections),
                    retained);
        }

        private ProfileResult measureClient(int measuredConnections) throws Exception {
            prewarmHandler(DataPlaneHandshakeAdmission.Direction.OUTBOUND);
            awaitReadyFile();
            forceCollection();
            long coldHeapBaseline = heapUsed();
            long coldDirectBaseline = directUsed();
            Sampler coldSampler = new Sampler(
                    coldHeapBaseline, coldDirectBaseline, security);
            Thread coldSamplerThread = startSampler(coldSampler, "outbound-cold");
            try {
                connectRound(measuredConnections);
            } finally {
                coldSampler.stop();
                coldSamplerThread.join(5_000L);
            }
            requireNoFailure();
            Profile cold = profile(coldSampler, coldHeapBaseline, coldDirectBaseline,
                    measuredConnections);
            awaitAdmissionIdle();
            forceCollection();
            Profile retained = new Profile(
                    Math.max(0L, heapUsed() - coldHeapBaseline),
                    Math.max(0L, directUsed() - coldDirectBaseline),
                    Math.max(0L, security.allocatedHeapBytes()
                            - coldSampler.allocatorHeapBaseline),
                    Math.max(0L, security.allocatedDirectBytes()
                            - coldSampler.allocatorDirectBaseline), 0);

            awaitWarmReadyFile();
            forceCollection();
            long warmHeapBaseline = heapUsed();
            long warmDirectBaseline = directUsed();
            Sampler warmSampler = new Sampler(
                    warmHeapBaseline, warmDirectBaseline, security);
            Thread warmSamplerThread = startSampler(warmSampler, "outbound-warm");
            try {
                connectRound(measuredConnections);
            } finally {
                warmSampler.stop();
                warmSamplerThread.join(5_000L);
            }
            requireNoFailure();
            return new ProfileResult(cold,
                    profile(warmSampler, warmHeapBaseline, warmDirectBaseline,
                            measuredConnections),
                    retained);
        }

        private void connectRound(int measuredConnections) throws Exception {
            TlsReadGate tlsReadGate = new TlsReadGate(measuredConnections, () -> {
                if (admission.snapshot().activeOutbound() != measuredConnections) {
                    throw new AssertionError(
                            "outbound TLS records reached engines without all leases active");
                }
            });
            RoundCleanupTracker cleanupTracker =
                    new RoundCleanupTracker(measuredConnections);
            if (measuredConnections == 1) {
                connectOnce(tlsReadGate, cleanupTracker, null, null);
                cleanupTracker.await("directional client");
                return;
            }
            ExecutorService callers = Executors.newFixedThreadPool(measuredConnections);
            CountDownLatch leased = new CountDownLatch(measuredConnections);
            CountDownLatch releaseConnections = new CountDownLatch(1);
            try {
                CountDownLatch start = new CountDownLatch(1);
                List<Future<?>> calls = new ArrayList<>();
                for (int i = 0; i < measuredConnections; i++) {
                    calls.add(callers.submit(() -> {
                        start.await();
                        connectOnce(tlsReadGate, cleanupTracker,
                                leased, releaseConnections);
                        return null;
                    }));
                }
                start.countDown();
                if (!leased.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "outbound leases were not acquired concurrently");
                }
                if (admission.snapshot().activeOutbound() != measuredConnections) {
                    throw new AssertionError(
                            "outbound handshake slots were not concurrently held");
                }
                releaseConnections.countDown();
                for (Future<?> call : calls) call.get(15, TimeUnit.SECONDS);
                if (admission.snapshot().peakPerDirection() < measuredConnections) {
                    throw new AssertionError(
                            "outbound handshake concurrency was not reached");
                }
                cleanupTracker.await("directional client");
            } finally {
                releaseConnections.countDown();
                callers.shutdownNow();
                callers.awaitTermination(5, TimeUnit.SECONDS);
            }
        }

        private void connectOnce(TlsReadGate tlsReadGate,
                                 RoundCleanupTracker cleanupTracker,
                                 CountDownLatch leased,
                                 CountDownLatch releaseConnections) throws Exception {
            DataPlaneHandshakeAdmission.Lease lease =
                    admission.acquireOutbound(handshakeDeadline());
            if (leased != null) {
                leased.countDown();
                if (!releaseConnections.await(5, TimeUnit.SECONDS)) {
                    lease.close();
                    throw new IllegalStateException(
                            "outbound concurrent handshake barrier timed out");
                }
            }
            AtomicReference<DataPlaneSecurityContext.AdmittedTlsChannel> installed =
                    new AtomicReference<>();
            Channel channel = null;
            try {
                channel = new Bootstrap()
                        .group(ioCols)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel socketChannel) {
                                security.configureChannel(socketChannel.config());
                                socketChannel.pipeline().addLast(
                                        cleanupTracker.newHandler());
                                socketChannel.pipeline().addLast(
                                        tlsReadGate.newHandler());
                                DataPlaneSecurityContext.AdmittedTlsChannel ssl =
                                        security.installClientHandler(
                                                socketChannel.pipeline(), "127.0.0.1",
                                                tlsPeerIdentity.getAndIncrement(), lease);
                                installed.set(ssl);
                                socketChannel.pipeline().addLast(new QuietExceptionHandler());
                            }
                        })
                        .connect("127.0.0.1", peerPort).sync().channel();
                DataPlaneSecurityContext.AdmittedTlsChannel ssl = installed.get();
                ssl.awaitHandshakeSuccess();
                requireExpectedPeer(ssl);
                ssl.releaseAfterAuthentication();
                if (!channel.closeFuture().await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "directional server did not complete and close TLS handshake");
                }
            } finally {
                if (channel != null) channel.close().syncUninterruptibly();
                lease.close();
            }
        }

        private void prewarmHandler(DataPlaneHandshakeAdmission.Direction direction)
                throws Exception {
            DataPlaneHandshakeAdmission.Lease lease = direction
                    == DataPlaneHandshakeAdmission.Direction.INBOUND
                    ? admission.acquireInbound(handshakeDeadline())
                    : admission.acquireOutbound(handshakeDeadline());
            EmbeddedChannel channel = new EmbeddedChannel();
            if (direction == DataPlaneHandshakeAdmission.Direction.INBOUND) {
                security.installServerHandler(channel.pipeline(), lease);
            } else {
                security.installClientHandler(
                        channel.pipeline(), "127.0.0.1",
                        tlsPeerIdentity.getAndIncrement(), lease);
            }
            channel.finishAndReleaseAll();
            if (admission.snapshot().activeTotal() != 0) {
                throw new AssertionError("prewarm handler leaked admission");
            }
        }

        private void requireExpectedPeer(
                DataPlaneSecurityContext.AdmittedTlsChannel ssl) throws Exception {
            security.requireWorkerPeer(ssl, expectedFingerprint);
            java.security.cert.Certificate[] chain = ssl.peerCertificates();
            if (chain.length != expectedCertificateCount) {
                throw new IllegalStateException("expected peer certificate chain "
                        + expectedCertificateCount + " but got " + chain.length);
            }
            long encoded = 0L;
            for (java.security.cert.Certificate certificate : chain) {
                encoded += certificate.getEncoded().length;
            }
            if (encoded != expectedCertificateBytes) {
                throw new IllegalStateException("expected peer certificate bytes "
                        + expectedCertificateBytes + " but got " + encoded);
            }
            if (expectedCertificateCount == 10
                    && (encoded < 56L * 1024L || encoded >= 64L * 1024L)) {
                throw new IllegalStateException(
                        "single peer Certificate payload is not near 64 KiB: " + encoded);
            }
        }

        private void awaitReadyFile() throws Exception {
            long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!Files.isRegularFile(readyFile)) {
                if (System.nanoTime() >= waitUntil) {
                    throw new IllegalStateException("server did not enter measurement window");
                }
                Thread.sleep(5L);
            }
        }

        private void awaitWarmReadyFile() throws Exception {
            Path warmReady = warmReadyFile();
            long waitUntil = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!Files.isRegularFile(warmReady)) {
                if (System.nanoTime() >= waitUntil) {
                    throw new IllegalStateException(
                            "server did not enter warm measurement window");
                }
                Thread.sleep(5L);
            }
        }

        private Path warmReadyFile() {
            return readyFile.resolveSibling(
                    readyFile.getFileName().toString() + ".warm");
        }

        private void awaitServerRound(
                ServerRound round, int measuredConnections, String phase)
                throws Exception {
            if (!round.measured.await(15, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "server " + phase + " measured handshake timed out");
            }
            requireNoFailure();
            round.cleanupTracker.await("directional server " + phase);
            if (admission.snapshot().peakPerDirection() < measuredConnections) {
                throw new AssertionError(
                        "inbound handshake concurrency was not reached");
            }
        }

        private void awaitAdmissionIdle() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (admission.snapshot().activeTotal() != 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(1L);
            }
            if (admission.snapshot().activeTotal() != 0) {
                throw new AssertionError(
                        "directional TLS measurement leaked handshake admission");
            }
        }

        private void requireNoFailure() {
            Throwable observed = failure.get();
            if (observed != null) throw new AssertionError(observed);
        }

        private static long handshakeDeadline() {
            return System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        }

        private final class ServerRound {
            private final CountDownLatch measured;
            private final CountDownLatch accepted;
            private final Queue<SocketChannel> heldChannels =
                    new ConcurrentLinkedQueue<>();
            private final RoundCleanupTracker cleanupTracker;
            private final TlsReadGate tlsReadGate;

            private ServerRound(int measuredConnections) {
                measured = new CountDownLatch(measuredConnections);
                accepted = new CountDownLatch(measuredConnections);
                cleanupTracker = new RoundCleanupTracker(measuredConnections);
                tlsReadGate = new TlsReadGate(measuredConnections, () -> {
                    if (admission.snapshot().activeInbound()
                            != measuredConnections) {
                        throw new AssertionError(
                                "inbound TLS records reached engines without all leases active");
                    }
                });
            }
        }

        @Override
        public void close() {
            if (listener != null) listener.close().syncUninterruptibly();
            ioCols.shutdownGracefully(0L, 5L, TimeUnit.SECONDS).syncUninterruptibly();
            boss.shutdownGracefully(0L, 5L, TimeUnit.SECONDS).syncUninterruptibly();
            admission.close();
        }
    }

    public static final class LateBootstrapProbe {
        private LateBootstrapProbe() {
        }

        public static void main(String[] args) {
            try {
                javax.net.ssl.SSLContext.getDefault();
                ControlSecurityConfig beforeBootstrap =
                        ControlSecurityConfig.fromProperties(System.getProperties());
                ProcessTlsEnvelope proof = TransportSecurityTestAccess.bootstrap();
                try {
                    DeploymentRuntime.load(
                            System.getProperties(), beforeBootstrap,
                            Runtime.getRuntime().maxMemory(), proof);
                    throw new AssertionError(
                            "control configuration created before bootstrap was accepted");
                } catch (IllegalStateException expected) {
                    if (expected.getMessage() == null || !expected.getMessage().contains(
                            "lacks certified bootstrap origin")) {
                        throw expected;
                    }
                }
                java.lang.reflect.Method bootstrap = Class.forName(
                        "io.rowweave.cluster.TransportSecurityBootstrap")
                        .getDeclaredMethod("bootstrap");
                if (java.lang.reflect.Modifier.isPublic(bootstrap.getModifiers())) {
                    throw new AssertionError(
                            "generic code can invoke a late production bootstrap");
                }
                System.out.println(
                        "LATE_CONFIG_ORIGIN_REJECTED PREMAIN_JSSE_UNSUPPORTED");
            } catch (Throwable failure) {
                failure.printStackTrace(System.out);
                System.exit(31);
            }
        }
    }

    private static final class Runner implements AutoCloseable {
        private final String mode;
        private final DataPlaneSecurityContext security;
        private final DataPlaneHandshakeAdmission admission;
        private final EventLoopGroup boss = new NioEventLoopGroup(1);
        private final EventLoopGroup ioCols = new NioEventLoopGroup(2);
        private final Queue<DataPlaneSecurityContext.AdmittedTlsChannel> serverHandshakes =
                new ConcurrentLinkedQueue<>();
        private final Queue<Channel> serverChannels = new ConcurrentLinkedQueue<>();
        private final AtomicReference<Throwable> peerFailure = new AtomicReference<>();
        private final AtomicBoolean measuring = new AtomicBoolean();
        private final AtomicBoolean corrupted = new AtomicBoolean();
        private final AtomicBoolean callbackFailureTriggered = new AtomicBoolean();
        private final AtomicReference<TlsReadGate> activeReadGate =
                new AtomicReference<>();
        private final AtomicReference<RoundCleanupTracker> activeCleanupTracker =
                new AtomicReference<>();
        private final AtomicInteger tlsPeerIdentity = new AtomicInteger(10_000);
        private Channel listener;

        private Runner(String mode, DataPlaneSecurityContext security,
                       DataPlaneHandshakeAdmission admission) {
            this.mode = mode;
            this.security = security;
            this.admission = admission;
        }

        private ProfileResult measure() throws Exception {
            listener = new ServerBootstrap()
                    .group(boss, ioCols)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.AUTO_READ, true)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel channel) throws Exception {
                            security.configureChannel(channel.config());
                            DataPlaneHandshakeAdmission.Lease lease =
                                    admission.acquireInbound(deadline());
                            if ("fragmented".equals(mode) || "failed".equals(mode)) {
                                channel.pipeline().addLast(new FragmentOrCorrupt(
                                        measuring, corrupted, "failed".equals(mode)));
                            }
                            RoundCleanupTracker cleanupTracker =
                                    activeCleanupTracker.get();
                            if (cleanupTracker != null) {
                                channel.pipeline().addLast(cleanupTracker.newHandler());
                            }
                            TlsReadGate gate = activeReadGate.get();
                            if (gate != null) channel.pipeline().addLast(gate.newHandler());
                            DataPlaneSecurityContext.AdmittedTlsChannel ssl =
                                    security.installServerHandler(
                                            channel.pipeline(), lease);
                            if ("callback-failure".equals(mode)) {
                                ssl.whenHandshakeComplete(done -> {
                                    if (done.success()) {
                                        callbackFailureTriggered.set(true);
                                        throw new AssertionError(
                                                "injected handshake callback failure");
                                    }
                                });
                            } else {
                                ssl.whenHandshakeComplete(done -> {
                                    try {
                                        if (done.success()) {
                                            requireExpectedPeerChain(ssl);
                                            security.requireWorkerPeer(ssl,
                                                    security.localCertificateFingerprint());
                                            ssl.releaseAfterAuthentication();
                                            channel.writeAndFlush(Unpooled.wrappedBuffer(
                                                    new byte[]{1}));
                                        }
                                    } catch (Throwable failure) {
                                        peerFailure.compareAndSet(null, failure);
                                        channel.close();
                                    }
                                });
                            }
                            channel.pipeline().addLast(new QuietExceptionHandler());
                            serverChannels.add(channel);
                            serverHandshakes.add(ssl);
                        }
                    })
                    .bind("127.0.0.1", 0).sync().channel();

            prewarmHandlers();
            boolean mixedConcurrent = "mixed-concurrent".equals(mode);
            int connections = mixedConcurrent ? 2 : 1;
            int directionalHandshakes = connections * 2;
            forceCollection();
            long coldHeapBaseline = heapUsed();
            long coldDirectBaseline = directUsed();
            Profile cold = measureConnections(
                    connections, directionalHandshakes,
                    coldHeapBaseline, coldDirectBaseline);
            awaitAdmissionIdle();
            resetRoundState();
            forceCollection();
            Profile retained = new Profile(
                    Math.max(0L, heapUsed() - coldHeapBaseline),
                    Math.max(0L, directUsed() - coldDirectBaseline),
                    security.allocatedHeapBytes(),
                    security.allocatedDirectBytes(), 0);
            long warmHeapBaseline = heapUsed();
            long warmDirectBaseline = directUsed();
            Profile warm = measureConnections(
                    connections, directionalHandshakes,
                    warmHeapBaseline, warmDirectBaseline);
            return new ProfileResult(cold, warm, retained);
        }

        private Profile measureConnections(
                int connections, int directionalHandshakes,
                long heapBaseline, long directBaseline) throws Exception {
            TlsReadGate gate = connections == 1 ? null
                    : new TlsReadGate(connections, () -> {
                        DataPlaneHandshakeAdmission.Snapshot snapshot =
                                admission.snapshot();
                        if (snapshot.activeInbound() != connections
                                || snapshot.activeOutbound() != connections) {
                            throw new AssertionError(
                                    "mixed TLS records reached engines without all leases active");
                        }
                    });
            RoundCleanupTracker cleanupTracker =
                    new RoundCleanupTracker(directionalHandshakes);
            activeReadGate.set(gate);
            activeCleanupTracker.set(cleanupTracker);
            Sampler sampler = new Sampler(heapBaseline, directBaseline, security);
            Thread samplerThread = new Thread(sampler, "tls-resource-sampler");
            samplerThread.setDaemon(true);
            measuring.set(true);
            samplerThread.start();
            try {
                if (connections == 1) {
                    connect("failed".equals(mode) || "wrong-host".equals(mode));
                } else {
                    connectConcurrently(connections);
                }
                awaitServerChannelsClosed(connections);
                cleanupTracker.await("mixed client/server");
            } finally {
                activeReadGate.compareAndSet(gate, null);
                activeCleanupTracker.compareAndSet(cleanupTracker, null);
                measuring.set(false);
                sampler.stop();
                samplerThread.join(5_000L);
            }
            Throwable validation = peerFailure.get();
            if (validation != null) throw new AssertionError(validation);
            if ("callback-failure".equals(mode)
                    && !callbackFailureTriggered.get()) {
                throw new AssertionError("injected handshake callback did not run");
            }
            return profile(sampler, heapBaseline, directBaseline,
                    directionalHandshakes);
        }

        private void connectConcurrently(int connections) throws Exception {
            ExecutorService callers = Executors.newFixedThreadPool(connections);
            CountDownLatch start = new CountDownLatch(1);
            try {
                List<Future<?>> calls = new ArrayList<>();
                for (int i = 0; i < connections; i++) {
                    calls.add(callers.submit(() -> {
                        start.await();
                        connect(false);
                        return null;
                    }));
                }
                start.countDown();
                for (Future<?> call : calls) call.get(15, TimeUnit.SECONDS);
                if (admission.snapshot().peakPerDirection() < connections) {
                    throw new AssertionError(
                            "mixed handshake direction did not reach full concurrency");
                }
            } finally {
                start.countDown();
                callers.shutdownNow();
                callers.awaitTermination(5, TimeUnit.SECONDS);
            }
        }

        private void awaitAdmissionIdle() throws Exception {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (admission.snapshot().activeTotal() != 0
                    && System.nanoTime() < deadline) {
                Thread.sleep(1L);
            }
            if (admission.snapshot().activeTotal() != 0) {
                throw new AssertionError("TLS measurement leaked handshake admission");
            }
        }

        private void resetRoundState() {
            serverHandshakes.clear();
            serverChannels.clear();
            peerFailure.set(null);
            corrupted.set(false);
            callbackFailureTriggered.set(false);
        }

        private void awaitServerChannelsClosed(int connections) throws Exception {
            List<Channel> observed = new ArrayList<>();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (observed.size() < connections && System.nanoTime() < deadline) {
                Channel channel = serverChannels.poll();
                if (channel == null) Thread.sleep(1L);
                else observed.add(channel);
            }
            if (observed.size() != connections) {
                throw new IllegalStateException(
                        "server channels were not all installed: " + observed.size()
                                + '/' + connections);
            }
            for (Channel channel : observed) {
                if (!channel.closeFuture().await(10, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "server channel did not close before TLS resource baseline");
                }
            }
        }

        private void prewarmHandlers() throws Exception {
            DataPlaneHandshakeAdmission.Lease inbound =
                    admission.acquireInbound(deadline());
            EmbeddedChannel serverChannel = new EmbeddedChannel();
            security.installServerHandler(serverChannel.pipeline(), inbound);
            DataPlaneHandshakeAdmission.Lease outbound =
                    admission.acquireOutbound(deadline());
            EmbeddedChannel clientChannel = new EmbeddedChannel();
            security.installClientHandler(clientChannel.pipeline(), "127.0.0.1",
                    ((InetSocketAddress) listener.localAddress()).getPort(), outbound);
            serverChannel.finishAndReleaseAll();
            clientChannel.finishAndReleaseAll();
            if (admission.snapshot().activeTotal() != 0) {
                throw new AssertionError("local TLS prewarm leaked admission");
            }
        }

        private void connect(boolean expectFailure) throws Exception {
            DataPlaneHandshakeAdmission.Lease lease =
                    admission.acquireOutbound(deadline());
            AtomicReference<DataPlaneSecurityContext.AdmittedTlsChannel> clientSsl =
                    new AtomicReference<>();
            CountDownLatch serverConfirmed = new CountDownLatch(1);
            Channel channel = null;
            try {
                Bootstrap bootstrap = new Bootstrap()
                        .group(ioCols)
                        .channel(NioSocketChannel.class)
                        .handler(new ChannelInitializer<SocketChannel>() {
                            @Override
                            protected void initChannel(SocketChannel socketChannel) {
                                security.configureChannel(socketChannel.config());
                                if ("fragmented".equals(mode)) {
                                    socketChannel.pipeline().addLast(new FragmentOrCorrupt(
                                            measuring, corrupted, false));
                                }
                                RoundCleanupTracker cleanupTracker =
                                        activeCleanupTracker.get();
                                if (cleanupTracker != null) {
                                    socketChannel.pipeline().addLast(
                                            cleanupTracker.newHandler());
                                }
                                String peerHost = "wrong-host".equals(mode) && measuring.get()
                                        ? "not-local.invalid" : "127.0.0.1";
                                DataPlaneSecurityContext.AdmittedTlsChannel ssl =
                                        security.installClientHandler(
                                                socketChannel.pipeline(), peerHost,
                                                tlsPeerIdentity.getAndIncrement(), lease);
                                clientSsl.set(ssl);
                                socketChannel.pipeline().addLast(
                                        new ChannelInboundHandlerAdapter() {
                                            @Override
                                            public void channelRead(
                                                    ChannelHandlerContext context,
                                                    Object message) {
                                                ReferenceCountUtil.release(message);
                                                serverConfirmed.countDown();
                                            }

                                            @Override
                                            public void exceptionCaught(
                                                    ChannelHandlerContext context,
                                                    Throwable cause) {
                                                context.close();
                                            }
                                        });
                            }
                        });
                channel = bootstrap.connect(listener.localAddress()).sync().channel();
                DataPlaneSecurityContext.AdmittedTlsChannel ssl = clientSsl.get();
                if ("callback-failure".equals(mode)) {
                    if (!ssl.awaitHandshake(10, TimeUnit.SECONDS)) {
                        throw new AssertionError(
                                "client TLS handshake did not complete before callback failure");
                    }
                    if (!channel.closeFuture().await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError(
                                "callback failure did not close the channel");
                    }
                } else if (expectFailure) {
                    DataPlaneSecurityContext.AdmittedTlsChannel serverFuture =
                            awaitServerHandshake();
                    boolean clientFailed = !ssl.awaitHandshake(10, TimeUnit.SECONDS)
                            || !ssl.handshakeSucceeded();
                    boolean serverFailed = !serverFuture.awaitHandshake(
                            10, TimeUnit.SECONDS) || !serverFuture.handshakeSucceeded();
                    if (!clientFailed && !serverFailed) {
                        throw new AssertionError("corrupted TLS handshake unexpectedly succeeded");
                    }
                } else {
                    ssl.awaitHandshakeSuccess();
                    if (!serverConfirmed.await(10, TimeUnit.SECONDS)) {
                        DataPlaneSecurityContext.AdmittedTlsChannel pending =
                                serverHandshakes.peek();
                        throw new IllegalStateException(
                                "server did not confirm completed TLS handshake; serverDone="
                                        + (pending != null && pending.handshakeDone())
                                        + " serverCause="
                                        + (pending == null ? "missing"
                                                : pending.handshakeFailure()),
                                peerFailure.get());
                    }
                    security.requireWorkerPeer(
                            ssl, security.localCertificateFingerprint());
                    requireExpectedPeerChain(ssl);
                    ssl.releaseAfterAuthentication();
                    if ("renegotiate".equals(mode) && measuring.get()) {
                        SslHandler raw = channel.pipeline().get(SslHandler.class);
                        io.netty.util.concurrent.Future<Channel> renegotiation =
                                raw.renegotiate();
                        assertTrue(renegotiation.await(5, TimeUnit.SECONDS));
                        assertTrue(!renegotiation.isSuccess());
                        assertTrue(channel.closeFuture().await(5, TimeUnit.SECONDS));
                        assertEquals(0, admission.snapshot().activeOutbound());
                    }
                }
            } finally {
                if (channel != null) channel.close().syncUninterruptibly();
                lease.close();
            }
        }

        private DataPlaneSecurityContext.AdmittedTlsChannel awaitServerHandshake()
                throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            DataPlaneSecurityContext.AdmittedTlsChannel result;
            while ((result = serverHandshakes.poll()) == null) {
                if (System.nanoTime() >= deadline) {
                    throw new IllegalStateException("server handshake was not installed");
                }
                Thread.sleep(1L);
            }
            return result;
        }

        private void requireExpectedPeerChain(
                DataPlaneSecurityContext.AdmittedTlsChannel ssl) throws Exception {
            int expected = 1;
            java.security.cert.Certificate[] peer = ssl.peerCertificates();
            int actual = peer.length;
            if (actual != expected) {
                throw new IllegalStateException(
                        "expected peer certificate chain " + expected + " but got " + actual);
            }
            long encodedBytes = 0L;
            for (java.security.cert.Certificate certificate : peer) {
                encodedBytes += certificate.getEncoded().length;
            }
            long configured = Long.parseLong(System.getProperty(
                    "rowweave.test.tlsCertificateChainBytes"));
            if (encodedBytes != configured) {
                throw new IllegalStateException("expected encoded peer chain bytes "
                        + configured + " but got " + encodedBytes);
            }
        }

        @Override
        public void close() {
            if (listener != null) listener.close().syncUninterruptibly();
            ioCols.shutdownGracefully(0L, 5L, TimeUnit.SECONDS).syncUninterruptibly();
            boss.shutdownGracefully(0L, 5L, TimeUnit.SECONDS).syncUninterruptibly();
            admission.close();
        }

        private static long deadline() {
            return System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        }
    }

    private static final class RoundCleanupTracker {
        private final CountDownLatch removed;

        private RoundCleanupTracker(int channels) {
            removed = new CountDownLatch(channels);
        }

        private ChannelInboundHandlerAdapter newHandler() {
            return new ChannelInboundHandlerAdapter() {
                private final AtomicBoolean terminal = new AtomicBoolean();

                @Override
                public void handlerRemoved(ChannelHandlerContext context) throws Exception {
                    if (terminal.compareAndSet(false, true)) removed.countDown();
                    super.handlerRemoved(context);
                }
            };
        }

        private void await(String description) throws Exception {
            if (!removed.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        description + " pipelines were not fully destroyed; remaining="
                                + removed.getCount());
            }
        }
    }

    private static final class TlsReadGate {
        private final CountDownLatch firstRecords;
        private final Runnable onOpen;
        private final List<PendingRead> pending = new ArrayList<>();
        private boolean open;
        private boolean draining;
        private boolean failed;

        private TlsReadGate(int connections, Runnable onOpen) {
            this.firstRecords = new CountDownLatch(connections);
            this.onOpen = onOpen;
        }

        private ChannelInboundHandlerAdapter newHandler() {
            return new ChannelInboundHandlerAdapter() {
                private boolean announced;

                @Override
                public void channelRead(ChannelHandlerContext context, Object message) {
                    boolean opening = false;
                    PendingRead direct = null;
                    synchronized (TlsReadGate.this) {
                        if (failed) {
                            ReferenceCountUtil.release(message);
                            context.close();
                            return;
                        } else if (open) {
                            direct = new PendingRead(context, message);
                        } else {
                            pending.add(new PendingRead(context, message));
                            if (!announced) {
                                announced = true;
                                firstRecords.countDown();
                            }
                            if (firstRecords.getCount() == 0L && !draining) {
                                draining = true;
                                opening = true;
                            }
                        }
                    }
                    if (direct != null) {
                        forward(direct);
                        return;
                    }
                    if (!opening) return;
                    if (opening) {
                        try {
                            onOpen.run();
                        } catch (Throwable failure) {
                            abort(failure);
                            context.fireExceptionCaught(failure);
                            return;
                        }
                    }
                    drainPending();
                }

                @Override
                public void channelReadComplete(ChannelHandlerContext context) {
                    PendingRead complete = PendingRead.readComplete(context);
                    boolean forward;
                    synchronized (TlsReadGate.this) {
                        forward = open && !failed;
                        if (!forward && !failed) pending.add(complete);
                    }
                    if (forward) {
                        forward(complete);
                    }
                }

                @Override
                public void channelInactive(ChannelHandlerContext context) throws Exception {
                    abort(new IOException("TLS record gate channel became inactive"));
                    context.fireChannelInactive();
                }

                @Override
                public void handlerRemoved(ChannelHandlerContext context) throws Exception {
                    abort(new IOException("TLS record gate handler was removed"));
                    super.handlerRemoved(context);
                }

                private void drainPending() {
                    while (true) {
                        List<PendingRead> release;
                        synchronized (TlsReadGate.this) {
                            if (failed) return;
                            if (pending.isEmpty()) {
                                draining = false;
                                open = true;
                                return;
                            }
                            release = new ArrayList<>(pending);
                            pending.clear();
                        }
                        for (PendingRead read : release) forward(read);
                    }
                }

                private void forward(PendingRead read) {
                    try {
                        read.forward();
                    } catch (Throwable failure) {
                        read.fail();
                        read.context.fireExceptionCaught(failure);
                    }
                }

                private void abort(Throwable failure) {
                    List<PendingRead> rejected = null;
                    synchronized (TlsReadGate.this) {
                        if (!open) {
                            failed = true;
                            draining = false;
                            open = false;
                            rejected = new ArrayList<>();
                            rejected.addAll(pending);
                            pending.clear();
                        }
                    }
                    if (rejected != null) {
                        for (PendingRead read : rejected) read.fail();
                    }
                }
            };
        }
    }

    private static final class PendingRead {
        private final ChannelHandlerContext context;
        private final Object message;
        private final boolean readComplete;
        private final AtomicBoolean consumed = new AtomicBoolean();

        private PendingRead(ChannelHandlerContext context, Object message) {
            this(context, message, false);
        }

        private PendingRead(ChannelHandlerContext context, Object message,
                            boolean readComplete) {
            this.context = context;
            this.message = message;
            this.readComplete = readComplete;
        }

        private static PendingRead readComplete(ChannelHandlerContext context) {
            return new PendingRead(context, null, true);
        }

        private void forward() {
            context.executor().execute(() -> {
                if (!consumed.compareAndSet(false, true)) return;
                if (context.channel().isActive()) {
                    if (readComplete) context.fireChannelReadComplete();
                    else context.fireChannelRead(message);
                } else if (!readComplete) {
                    ReferenceCountUtil.release(message);
                }
            });
        }

        private void fail() {
            if (consumed.compareAndSet(false, true) && !readComplete) {
                ReferenceCountUtil.release(message);
            }
            context.close();
        }
    }

    private static final class QuietExceptionHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
            context.close();
        }
    }

    private static final class FragmentOrCorrupt extends ChannelInboundHandlerAdapter {
        private final AtomicBoolean measuring;
        private final AtomicBoolean corrupted;
        private final boolean corrupt;

        private FragmentOrCorrupt(AtomicBoolean measuring,
                                  AtomicBoolean corrupted,
                                  boolean corrupt) {
            this.measuring = measuring;
            this.corrupted = corrupted;
            this.corrupt = corrupt;
        }

        @Override
        public void channelRead(ChannelHandlerContext context, Object message) {
            if (!(message instanceof ByteBuf)) {
                context.fireChannelRead(message);
                return;
            }
            ByteBuf bytes = (ByteBuf) message;
            try {
                if (corrupt && measuring.get() && corrupted.compareAndSet(false, true)
                        && bytes.isReadable()) {
                    int index = bytes.readerIndex();
                    bytes.setByte(index, bytes.getByte(index) ^ 1);
                }
                while (bytes.isReadable()) {
                    context.fireChannelRead(bytes.readRetainedSlice(1));
                }
            } finally {
                ReferenceCountUtil.release(bytes);
            }
        }
    }

    private static final class Sampler implements Runnable {
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final AtomicLong heapPeak;
        private final AtomicLong directPeak;
        private final DataPlaneSecurityContext security;
        private final long allocatorHeapBaseline;
        private final long allocatorDirectBaseline;
        private final AtomicLong allocatorHeapPeak;
        private final AtomicLong allocatorDirectPeak;

        private Sampler(long heapBaseline, long directBaseline,
                        DataPlaneSecurityContext security) {
            this.heapPeak = new AtomicLong(heapBaseline);
            this.directPeak = new AtomicLong(directBaseline);
            this.security = security;
            this.allocatorHeapBaseline = security.allocatedHeapBytes();
            this.allocatorDirectBaseline = security.allocatedDirectBytes();
            this.allocatorHeapPeak = new AtomicLong(allocatorHeapBaseline);
            this.allocatorDirectPeak = new AtomicLong(allocatorDirectBaseline);
        }

        @Override
        public void run() {
            while (running.get()) {
                updateMaximum(heapPeak, heapUsed());
                updateMaximum(directPeak, directUsed());
                updateMaximum(allocatorHeapPeak, security.allocatedHeapBytes());
                updateMaximum(allocatorDirectPeak, security.allocatedDirectBytes());
                Thread.yield();
            }
            updateMaximum(heapPeak, heapUsed());
            updateMaximum(directPeak, directUsed());
            updateMaximum(allocatorHeapPeak, security.allocatedHeapBytes());
            updateMaximum(allocatorDirectPeak, security.allocatedDirectBytes());
        }

        private void stop() {
            running.set(false);
        }
    }

    private static final class Profile {
        private final long heapDelta;
        private final long directDelta;
        private final long allocatorHeapDelta;
        private final long allocatorDirectDelta;
        private final int directionalHandshakes;

        private Profile(long heapDelta, long directDelta,
                        long allocatorHeapDelta, long allocatorDirectDelta,
                        int directionalHandshakes) {
            this.heapDelta = heapDelta;
            this.directDelta = directDelta;
            this.allocatorHeapDelta = allocatorHeapDelta;
            this.allocatorDirectDelta = allocatorDirectDelta;
            this.directionalHandshakes = directionalHandshakes;
        }
    }

    private static final class ProfileResult {
        private final Profile cold;
        private final Profile warm;
        private final Profile retained;

        private ProfileResult(Profile cold, Profile warm, Profile retained) {
            this.cold = cold;
            this.warm = warm;
            this.retained = retained;
        }
    }

    private static long heapUsed() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();
    }

    private static long directUsed() {
        for (BufferPoolMXBean pool : ManagementFactory.getPlatformMXBeans(
                BufferPoolMXBean.class)) {
            if ("direct".equals(pool.getName())) return pool.getMemoryUsed();
        }
        return 0L;
    }

    private static void updateMaximum(AtomicLong maximum, long value) {
        long current = maximum.get();
        while (value > current && !maximum.compareAndSet(current, value)) {
            current = maximum.get();
        }
    }

    private static void forceCollection() throws InterruptedException {
        System.gc();
        Thread.sleep(100L);
        System.gc();
        Thread.sleep(100L);
    }

    private static Thread startSampler(Sampler sampler, String direction) {
        Thread thread = new Thread(sampler, "tls-" + direction + "-resource-sampler");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static Profile profile(Sampler sampler, long heapBaseline,
                                   long directBaseline, int directionalHandshakes) {
        return new Profile(
                Math.max(0L, sampler.heapPeak.get() - heapBaseline),
                Math.max(0L, sampler.directPeak.get() - directBaseline),
                Math.max(0L, sampler.allocatorHeapPeak.get()
                        - sampler.allocatorHeapBaseline),
                Math.max(0L, sampler.allocatorDirectPeak.get()
                        - sampler.allocatorDirectBaseline),
                directionalHandshakes);
    }

    private static byte[] decodeFingerprint(String hex) {
        if (hex == null || hex.length() != 64) {
            throw new IllegalArgumentException("expected fingerprint must be 64 hex chars");
        }
        byte[] bytes = new byte[32];
        for (int i = 0; i < bytes.length; i++) {
            int high = Character.digit(hex.charAt(i * 2), 16);
            int low = Character.digit(hex.charAt(i * 2 + 1), 16);
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException("expected fingerprint is not hex");
            }
            bytes[i] = (byte) ((high << 4) | low);
        }
        return bytes;
    }

    private static Fixtures material(Path temp) throws Exception {
        Path keyStore = temp.resolve("worker.p12");
        Path certificate = temp.resolve("worker.cer");
        Path trustStore = temp.resolve("trust.p12");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=worker", "-validity", "30",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-exportcert", "-alias", "identity", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-file", certificate.toString());
        keytool("-importcert", "-noprompt", "-alias", "worker",
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        Path authz = temp.resolve("authz.txt");
        Files.write(authz, (ControlTlsContext.fingerprint(leaf) + "=WORKER\n")
                .getBytes(StandardCharsets.US_ASCII));
        Material single = new Material(
                keyStore, trustStore, authz, certificate, certificate,
                ControlTlsContext.fingerprint(leaf), 1, leaf.getEncoded().length);
        Material chain10 = chain10Material(temp);
        Path crossAuthz = temp.resolve("cross-authz.txt");
        Files.write(crossAuthz, (single.fingerprint + "=WORKER\n"
                + chain10.fingerprint + "=WORKER\n")
                .getBytes(StandardCharsets.US_ASCII));
        Path serverTrust = temp.resolve("boundary-server-trust.p12");
        keytool("-importcert", "-noprompt", "-alias", "client",
                "-file", single.identityCertificate.toString(), "-storetype", "PKCS12",
                "-keystore", serverTrust.toString(), "-storepass", PASSWORD);
        Path clientTrust = temp.resolve("boundary-client-trust.p12");
        keytool("-importcert", "-noprompt", "-alias", "server-root",
                "-file", chain10.trustAnchorCertificate.toString(), "-storetype", "PKCS12",
                "-keystore", clientTrust.toString(), "-storepass", PASSWORD);
        Material boundaryServer = chain10.withTrustAndAuthz(serverTrust, crossAuthz);
        Material boundaryClient = single.withTrustAndAuthz(clientTrust, crossAuthz);
        return new Fixtures(single,
                new BoundaryMaterial(boundaryServer, boundaryClient));
    }

    private static Material chain10Material(Path temp) throws Exception {
        List<Path> caStores = new ArrayList<>();
        List<Path> caCertificates = new ArrayList<>();
        Path rootStore = temp.resolve("chain-ca-0.p12");
        Path rootCertificate = temp.resolve("chain-ca-0.cer");
        keytool("-genkeypair", "-alias", "ca0", "-keyalg", "EC",
                "-groupname", "secp256r1", "-storetype", "PKCS12",
                "-keystore", rootStore.toString(), "-storepass", PASSWORD,
                "-keypass", PASSWORD, "-dname", "CN=chain-ca-0",
                "-validity", "30", "-ext", "bc:c",
                "-ext", paddingExtension());
        keytool("-exportcert", "-alias", "ca0", "-keystore", rootStore.toString(),
                "-storepass", PASSWORD, "-file", rootCertificate.toString());
        caStores.add(rootStore);
        caCertificates.add(rootCertificate);

        for (int index = 1; index <= 8; index++) {
            Path store = temp.resolve("chain-ca-" + index + ".p12");
            Path request = temp.resolve("chain-ca-" + index + ".csr");
            Path certificate = temp.resolve("chain-ca-" + index + ".cer");
            String alias = "ca" + index;
            keytool("-genkeypair", "-alias", alias, "-keyalg", "EC",
                    "-groupname", "secp256r1", "-storetype", "PKCS12",
                    "-keystore", store.toString(), "-storepass", PASSWORD,
                    "-keypass", PASSWORD, "-dname", "CN=chain-ca-" + index,
                    "-validity", "30", "-ext", "bc:c");
            keytool("-certreq", "-alias", alias, "-keystore", store.toString(),
                    "-storepass", PASSWORD, "-file", request.toString());
            keytool("-gencert", "-alias", "ca" + (index - 1),
                    "-keystore", caStores.get(index - 1).toString(),
                    "-storepass", PASSWORD, "-infile", request.toString(),
                    "-outfile", certificate.toString(), "-validity", "30",
                    "-ext", "bc:c", "-ext", paddingExtension());
            importCaChain(store, caCertificates);
            keytool("-importcert", "-noprompt", "-alias", alias,
                    "-file", certificate.toString(), "-keystore", store.toString(),
                    "-storepass", PASSWORD);
            caStores.add(store);
            caCertificates.add(certificate);
        }

        Path workerStore = temp.resolve("chain-worker.p12");
        Path workerRequest = temp.resolve("chain-worker.csr");
        Path workerCertificate = temp.resolve("chain-worker.cer");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "EC",
                "-groupname", "secp256r1", "-storetype", "PKCS12",
                "-keystore", workerStore.toString(), "-storepass", PASSWORD,
                "-keypass", PASSWORD, "-dname", "CN=chain-worker",
                "-validity", "30", "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-certreq", "-alias", "identity", "-keystore", workerStore.toString(),
                "-storepass", PASSWORD, "-file", workerRequest.toString(),
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-gencert", "-alias", "ca8", "-keystore",
                caStores.get(8).toString(), "-storepass", PASSWORD,
                "-infile", workerRequest.toString(), "-outfile",
                workerCertificate.toString(), "-validity", "30",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth",
                "-ext", paddingExtension());
        importCaChain(workerStore, caCertificates);
        keytool("-importcert", "-noprompt", "-alias", "identity",
                "-file", workerCertificate.toString(), "-keystore", workerStore.toString(),
                "-storepass", PASSWORD);

        KeyStore loaded = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(workerStore)) {
            loaded.load(input, PASSWORD.toCharArray());
        }
        java.security.cert.Certificate[] chain = loaded.getCertificateChain("identity");
        if (chain.length != 10) {
            throw new AssertionError("generated TLS fixture is not a 10-certificate chain");
        }
        long chainBytes = 0L;
        for (java.security.cert.Certificate member : chain) {
            chainBytes += member.getEncoded().length;
        }
        if (chainBytes < 56L * 1024L || chainBytes >= 64L * 1024L) {
            throw new AssertionError(
                    "generated single-peer TLS fixture is not near 64 KiB: "
                            + chainBytes);
        }
        Path trustStore = temp.resolve("chain-trust.p12");
        keytool("-importcert", "-noprompt", "-alias", "root",
                "-file", rootCertificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(workerCertificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        Path authz = temp.resolve("chain-authz.txt");
        Files.write(authz, (ControlTlsContext.fingerprint(leaf) + "=WORKER\n")
                .getBytes(StandardCharsets.US_ASCII));
        return new Material(workerStore, trustStore, authz,
                workerCertificate, rootCertificate,
                ControlTlsContext.fingerprint(leaf), 10, chainBytes);
    }

    private static String paddingExtension() {
        StringBuilder value = new StringBuilder("1.3.6.1.4.1.55555.205=04:82:15:18:");
        for (int index = 0; index < 5_400; index++) {
            if (index > 0) value.append(':');
            value.append("00");
        }
        return value.toString();
    }

    private static void importCaChain(Path targetStore, List<Path> certificates)
            throws Exception {
        for (int index = 0; index < certificates.size(); index++) {
            keytool("-importcert", "-noprompt", "-alias", "trusted-ca-" + index,
                    "-file", certificates.get(index).toString(),
                    "-keystore", targetStore.toString(), "-storepass", PASSWORD);
        }
    }

    private static void keytool(String... arguments) throws Exception {
        List<String> command = io.rowweave.test.ForkedJvm.keytoolCommand(arguments);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output;
        try (InputStream input = process.getInputStream()) {
            output = readAll(input);
        }
        if (process.waitFor() != 0) {
            throw new AssertionError("keytool failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(InputStream input) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int read; (read = input.read(buffer)) >= 0; ) bytes.write(buffer, 0, read);
        return bytes.toByteArray();
    }

    private static final class Material {
        private final Path keyStore;
        private final Path trustStore;
        private final Path authz;
        private final Path identityCertificate;
        private final Path trustAnchorCertificate;
        private final String fingerprint;
        private final int certificateCount;
        private final long certificateChainBytes;

        private Material(Path keyStore, Path trustStore, Path authz,
                         Path identityCertificate, Path trustAnchorCertificate,
                         String fingerprint, int certificateCount,
                         long certificateChainBytes) {
            this.keyStore = keyStore;
            this.trustStore = trustStore;
            this.authz = authz;
            this.identityCertificate = identityCertificate;
            this.trustAnchorCertificate = trustAnchorCertificate;
            this.fingerprint = fingerprint;
            this.certificateCount = certificateCount;
            this.certificateChainBytes = certificateChainBytes;
        }

        private Material withTrustAndAuthz(Path replacementTrust, Path replacementAuthz) {
            return new Material(keyStore, replacementTrust, replacementAuthz,
                    identityCertificate, trustAnchorCertificate, fingerprint,
                    certificateCount, certificateChainBytes);
        }
    }

    private static final class BoundaryMaterial {
        private final Material server;
        private final Material client;

        private BoundaryMaterial(Material server, Material client) {
            this.server = server;
            this.client = client;
        }
    }

    private static final class Fixtures {
        private final Material single;
        private final BoundaryMaterial boundary;

        private Fixtures(Material single, BoundaryMaterial boundary) {
            this.single = single;
            this.boundary = boundary;
        }
    }
}

