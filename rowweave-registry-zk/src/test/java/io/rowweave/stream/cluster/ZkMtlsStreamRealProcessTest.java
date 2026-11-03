package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.StreamJobClient;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSession;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.zk.ZkStreamJobClient;
import io.rowweave.cluster.registry.zk.ZkServiceRegistry;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamJobStatus;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceDiscoverySpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.test.ForkedJvm;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.Inet4Address;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** real role JVMs combine non-loopback mTLS and ZK HA. */
@Tag("multiprocess")
class ZkMtlsStreamRealProcessTest {
    private static final String PASSWORD = "changeit";

    @Test
    @Timeout(300)
    void independentRolesSurviveLeaderReplacementOverZkAndMtls(
            @TempDir Path root) throws Exception {
        String host = nonLoopbackIpv4();
        Map<String, Identity> identities = new LinkedHashMap<>();
        for (String role : new String[] {
                "master-a", "master-b", "source", "relay", "sink",
                "client"}) {
            identities.put(role, identity(root, role, host));
        }
        Path trustStore = root.resolve("trust.p12");
        for (Map.Entry<String, Identity> identity : identities.entrySet()) {
            importCertificate(trustStore, identity.getKey(),
                    identity.getValue().certificate);
        }
        Path authz = root.resolve("authz.txt");
        StringBuilder grants = new StringBuilder();
        for (Map.Entry<String, Identity> identity : identities.entrySet()) {
            String role = identity.getKey();
            grants.append(identity.getValue().fingerprint).append('=')
                    .append(role.startsWith("master-") ? "MASTER"
                            : "client".equals(role) ? "CLIENT" : "WORKER")
                    .append('\n');
        }
        Files.write(authz, grants.toString().getBytes(StandardCharsets.US_ASCII));

        try (TestingServer zk = new TestingServer(true)) {
            Map<String, Process> processes = new LinkedHashMap<>();
            try {
                for (String role : new String[] {
                        "source", "relay", "sink", "master-a",
                        "master-b"}) {
                    processes.put(role, launch(role, root,
                            zk.getConnectString(), host,
                            identities.get(role), trustStore, authz));
                }
                for (String role : processes.keySet()) {
                    awaitNonEmpty(root.resolve(role + ".ready"), 20_000L);
                }

                ControlSecurityConfig clientSecurity = security(
                        identities.get("client"), trustStore, authz, host);
                try (ZkServiceRegistry observer =
                             new ZkServiceRegistry(zk.getConnectString());
                     StreamJobClient client = new ZkStreamJobClient(
                             zk.getConnectString(), clientSecurity)) {
                    RegistryWorker first = awaitLeader(observer, 20_000L);
                    assertEquals(host, first.host());
                    awaitWorkerCount(observer, 3, 20_000L);
                    StreamJobHandle job = session(client).submit(
                            StreamSubmissionId.of("zk-mtls-process-submit"));
                    assertEquals(StreamJobStatus.RUNNING,
                            job.awaitRunning(20_000L));
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            job.triggerCheckpoint(StreamCommandId.of("cut-1"))
                                    .awaitCompletion(20_000L));
                    awaitVisibleCount(root.resolve("sink-store"), 1, 10_000L);
                    StreamJobEvidence initial = awaitCompleteEvidence(
                            job, 20_000L);
                    assertEquals(3, initial.placementTaskCount());
                    assertEquals(3, initial.reportedTaskCount());
                    assertEquals(1L,
                            initial.lifetimeProgress().recordsRead());
                    assertEquals(1L,
                            initial.lifetimeProgress().recordsWritten());
                    String runId = initial.runId();
                    int initialAttempt = initial.attemptOrdinal();

                    stopRole("source", root, processes);
                    restartRole("source", root, zk.getConnectString(), host,
                            identities, trustStore, authz, processes);
                    awaitWorkerCount(observer, 3, 20_000L);
                    StreamJobHandle afterSource = awaitRunning(
                            client, job, 30_000L);
                    StreamJobEvidence sourceReplacement =
                            awaitAttemptAfter(afterSource, initialAttempt,
                                    30_000L);
                    assertEquals(runId, sourceReplacement.runId());
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            afterSource.triggerCheckpoint(StreamCommandId.of(
                                            "cut-after-source-replacement"))
                                    .awaitCompletion(20_000L));

                    stopRole("relay", root, processes);
                    restartRole("relay", root, zk.getConnectString(), host,
                            identities, trustStore, authz, processes);
                    awaitWorkerCount(observer, 3, 20_000L);
                    StreamJobHandle afterRelay = awaitRunning(
                            client, job, 30_000L);
                    StreamJobEvidence relayReplacement =
                            awaitAttemptAfter(afterRelay,
                                    sourceReplacement.attemptOrdinal(),
                                    30_000L);
                    assertEquals(runId, relayReplacement.runId());
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            afterRelay.rescale(StreamCommandId.of(
                                            "scale-to-two"), 2)
                                    .awaitCompletion(30_000L));
                    assertEquals(StreamJobStatus.RUNNING,
                            afterRelay.status());

                    String activeRole = roleForPort(root, first.port());
                    stopRole(activeRole, root, processes);

                    RegistryWorker replacement = awaitDifferentLeader(
                            observer, first.port(), 30_000L);
                    assertNotEquals(first.port(), replacement.port());
                    StreamJobHandle attached = awaitRunning(
                            client, job, 30_000L);
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            attached.triggerCheckpoint(
                                            StreamCommandId.of("cut-2"))
                                    .awaitCompletion(20_000L));
                    StreamJobEvidence firstLeaderReplacement =
                            awaitCompleteEvidence(attached, 20_000L);
                    assertEquals(runId, firstLeaderReplacement.runId());
                    assertTrue(firstLeaderReplacement.coordinatorTerm()
                            > relayReplacement.coordinatorTerm());

                    restartRole(activeRole, root, zk.getConnectString(), host,
                            identities, trustStore, authz, processes);
                    String secondActiveRole = roleForPort(
                            root, replacement.port());
                    stopRole(secondActiveRole, root, processes);
                    RegistryWorker secondReplacement = awaitDifferentLeader(
                            observer, replacement.port(), 30_000L);
                    assertNotEquals(replacement.port(),
                            secondReplacement.port());
                    StreamJobHandle twiceAttached = awaitRunning(
                            client, job, 30_000L);
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            twiceAttached.triggerCheckpoint(
                                            StreamCommandId.of("cut-3"))
                                    .awaitCompletion(20_000L));
                    StreamJobEvidence finalEvidence = awaitCompleteEvidence(
                            twiceAttached, 20_000L);
                    assertEquals(runId, finalEvidence.runId());
                    assertTrue(finalEvidence.coordinatorTerm()
                            > firstLeaderReplacement.coordinatorTerm());
                    assertTrue(finalEvidence.attemptOrdinal()
                            >= relayReplacement.attemptOrdinal());
                    assertEquals(1L,
                            finalEvidence.lifetimeProgress().recordsRead());
                    assertEquals(1L,
                            finalEvidence.lifetimeProgress().recordsWritten());
                    assertTrue(finalEvidence
                            .latestCheckpointDurationMillis() > 0L);
                    assertTrue(finalEvidence.latestCheckpointBytes() > 0L);
                    awaitVisibleCount(root.resolve("sink-store"), 4, 10_000L);
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            twiceAttached.cancel(StreamCommandId.of("cancel"))
                                    .awaitCompletion(20_000L));
                }
            } finally {
                Files.write(root.resolve("stop-all"), new byte[] {1});
                for (Map.Entry<String, Process> process : processes.entrySet()) {
                    if (process.getValue().isAlive()) {
                        boolean exited = process.getValue().waitFor(
                                15L, TimeUnit.SECONDS);
                        if (!exited) process.getValue().destroyForcibly();
                        assertTrue(exited, () -> log(root, process.getKey()));
                        assertEquals(0, process.getValue().exitValue(),
                                () -> log(root, process.getKey()));
                    }
                }
            }
        }
    }

    @Test
    @Timeout(240)
    void recoversCompleteEvidenceAfterZkOutageAndSourceSessionExpiration(
            @TempDir Path root) throws Exception {
        String host = nonLoopbackIpv4();
        Map<String, Identity> identities = new LinkedHashMap<>();
        for (String role : new String[] {
                "master-a", "master-b", "source", "relay", "sink",
                "client"}) {
            identities.put(role, identity(root, role, host));
        }
        Path trustStore = root.resolve("trust.p12");
        for (Map.Entry<String, Identity> identity : identities.entrySet()) {
            importCertificate(trustStore, identity.getKey(),
                    identity.getValue().certificate);
        }
        Path authz = root.resolve("authz.txt");
        StringBuilder grants = new StringBuilder();
        for (Map.Entry<String, Identity> identity : identities.entrySet()) {
            String role = identity.getKey();
            grants.append(identity.getValue().fingerprint).append('=')
                    .append(role.startsWith("master-") ? "MASTER"
                            : "client".equals(role) ? "CLIENT" : "WORKER")
                    .append('\n');
        }
        Files.write(authz, grants.toString().getBytes(StandardCharsets.US_ASCII));

        try (TestingServer zk = new TestingServer(true)) {
            Map<String, Process> processes = new LinkedHashMap<>();
            try {
                for (String role : new String[] {
                        "source", "relay", "sink", "master-a",
                        "master-b"}) {
                    processes.put(role, launch(role, root,
                            zk.getConnectString(), host,
                            identities.get(role), trustStore, authz));
                }
                for (String role : processes.keySet()) {
                    awaitNonEmpty(root.resolve(role + ".ready"), 20_000L);
                }
                ControlSecurityConfig clientSecurity = security(
                        identities.get("client"), trustStore, authz, host);
                try (ZkServiceRegistry observer =
                             new ZkServiceRegistry(zk.getConnectString());
                     StreamJobClient client = new ZkStreamJobClient(
                             zk.getConnectString(), clientSecurity)) {
                    awaitLeader(observer, 20_000L);
                    awaitWorkerCount(observer, 3, 20_000L);
                    StreamJobHandle job = session(client).submit(
                            StreamSubmissionId.of("zk-all-session-expiry"));
                    assertEquals(StreamJobStatus.RUNNING,
                            job.awaitRunning(20_000L));
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            job.triggerCheckpoint(StreamCommandId.of("before-expiry"))
                                    .awaitCompletion(20_000L));
                    StreamJobEvidence before = awaitCompleteEvidence(
                            job, 20_000L);
                    assertEquals(3, before.placementTaskCount());
                    assertEquals(3, before.reportedTaskCount());

                    zk.stop();
                    Thread.sleep(30_000L); // Exceeds the negotiated 20s ZK session timeout.
                    zk.restart();
                    awaitLeader(observer, 30_000L);
                    awaitWorkerCount(observer, 3, 30_000L);
                    for (Map.Entry<String, Process> role : processes.entrySet()) {
                        assertTrue(role.getValue().isAlive(),
                                () -> role.getKey() + " exited during ZK outage: "
                                        + log(root, role.getKey()));
                    }
                    StreamJobHandle recovered = awaitRunning(
                            client, job, 60_000L);
                    StreamJobEvidence after = awaitCompleteEvidence(
                            recovered, 60_000L);
                    assertEquals(before.runId(), after.runId());
                    assertEquals(3, after.placementTaskCount());
                    assertEquals(3, after.reportedTaskCount());
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            recovered.triggerCheckpoint(StreamCommandId.of("after-expiry"))
                                    .awaitCompletion(20_000L));
                    assertEquals(StreamJobStatus.RUNNING, recovered.status());

                    Files.write(root.resolve("expire-source"), new byte[] {1});
                    awaitNonEmpty(root.resolve("expired-source"), 10_000L);
                    StreamJobHandle afterSourceExpiry = awaitRunning(
                            client, job, 60_000L);
                    StreamJobEvidence sourceExpiry = awaitCompleteEvidence(
                            afterSourceExpiry, 60_000L);
                    assertEquals(before.runId(), sourceExpiry.runId());
                    assertEquals(3, sourceExpiry.placementTaskCount());
                    assertEquals(3, sourceExpiry.reportedTaskCount());
                    assertEquals(1L,
                            sourceExpiry.lifetimeProgress().recordsRead());
                    assertEquals(1L,
                            sourceExpiry.lifetimeProgress().recordsWritten());
                    assertEquals(StreamOperationStatus.SUCCEEDED,
                            afterSourceExpiry.triggerCheckpoint(StreamCommandId.of(
                                            "after-source-session-expiry"))
                                    .awaitCompletion(20_000L));
                    assertTrue(awaitCompleteEvidence(afterSourceExpiry,
                            20_000L).latestCheckpointDurationMillis() > 0L);
                    assertEquals(StreamJobStatus.RUNNING,
                            afterSourceExpiry.status());
                }
            } finally {
                Files.write(root.resolve("stop-all"), new byte[] {1});
                for (Map.Entry<String, Process> role : processes.entrySet()) {
                    if (role.getValue().isAlive()) {
                        boolean exited = role.getValue().waitFor(
                                15L, TimeUnit.SECONDS);
                        if (!exited) role.getValue().destroyForcibly();
                        assertTrue(exited, () -> log(root, role.getKey()));
                        assertEquals(0, role.getValue().exitValue(),
                                () -> log(root, role.getKey()));
                    }
                }
            }
        }
    }

    private static StreamSession session(StreamJobClient client) {
        StreamSession session = RowWeave.createStreamSession(
                "zk-mtls-process", "zk-mtls-process", client);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodicUnaligned(
                60_000L, 15_000L, 2));
        session.setRestartPolicy(StreamRestartPolicy.fixedDelay(20, 1_000L));
        session.setParallelism(1);
        session.setMaxParallelism(8);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                32_768L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.dynamicExactOrdered(
                        StreamZkMtlsProcessFixture.SCHEMA,
                        StreamZkMtlsProcessFixture.SOURCE,
                        StreamZkMtlsProcessFixture.SOURCE_CONTRACT,
                        new byte[] {1}, new StreamSourceDiscoverySpec(
                                25L, 5_000L, 4, 4, 2, 64, 64)))
                .uid("source")
                .map(StreamZkMtlsProcessFixture.relayFunction(),
                        StreamZkMtlsProcessFixture.SCHEMA)
                .uid("relay")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        StreamZkMtlsProcessFixture.SINK,
                        StreamZkMtlsProcessFixture.SINK_CONTRACT,
                        new byte[] {3}));
        return session;
    }

    private static Process launch(String role, Path root, String connect,
            String host, Identity identity, Path trustStore, Path authz)
            throws Exception {
        List<String> command = ForkedJvm.command(
                StreamZkMtlsProcessFixture.class.getName(), role,
                root.toString(), connect, host, identity.keyStore.toString(),
                trustStore.toString(), authz.toString());
        return new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(root.resolve(role + ".log").toFile()).start();
    }

    private static StreamJobHandle awaitRunning(StreamJobClient client,
            StreamJobHandle original, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        Throwable last = null;
        StreamJobStatus lastStatus = null;
        while (System.nanoTime() < deadline) {
            try {
                StreamJobHandle attached = client.attach(original.id());
                lastStatus = attached.awaitRunning(2_000L);
                if (lastStatus == StreamJobStatus.RUNNING) {
                    return attached;
                }
                if (lastStatus.terminal()) {
                    throw new IllegalStateException(
                            "replacement Master made the job terminal: "
                                    + lastStatus);
                }
            } catch (RuntimeException failure) {
                last = failure;
            }
            Thread.sleep(25L);
        }
        throw new IllegalStateException(
                "replacement Master did not resume the Stream job; lastStatus="
                        + lastStatus, last);
    }

    private static RegistryWorker awaitLeader(
            ZkServiceRegistry observer, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            RegistryWorker leader = observer.worker(
                    StreamClusterRoles.MASTER_LEADER_ROLE_ID);
            if (leader != null) return leader;
            Thread.sleep(25L);
        }
        throw new IllegalStateException("Stream leader was not elected");
    }

    private static StreamJobEvidence awaitCompleteEvidence(
            StreamJobHandle job, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        StreamJobEvidence evidence = job.evidence();
        while (System.nanoTime() < deadline && !evidence.complete()) {
            Thread.sleep(25L);
            evidence = job.evidence();
        }
        assertTrue(evidence.available());
        assertTrue(evidence.complete());
        return evidence;
    }

    private static StreamJobEvidence awaitAttemptAfter(
            StreamJobHandle job, int priorAttempt, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        StreamJobEvidence evidence = job.evidence();
        while (System.nanoTime() < deadline
                && (!evidence.complete()
                || evidence.attemptOrdinal() <= priorAttempt)) {
            Thread.sleep(25L);
            evidence = job.evidence();
        }
        assertTrue(evidence.available());
        assertTrue(evidence.complete());
        assertTrue(evidence.attemptOrdinal() > priorAttempt,
                "Worker replacement did not advance the attempt");
        return evidence;
    }

    private static void awaitWorkerCount(ZkServiceRegistry observer,
            int expected, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        int actual = 0;
        while (System.nanoTime() < deadline) {
            actual = 0;
            for (RegistryWorker worker : observer.liveWorkers()) {
                if (worker.workerId().startsWith(
                        "__rowweave_stream_worker__")) {
                    actual++;
                }
            }
            if (actual == expected) return;
            Thread.sleep(25L);
        }
        throw new IllegalStateException("expected " + expected
                + " live Stream Workers but found " + actual);
    }

    private static void stopRole(String role, Path root,
            Map<String, Process> processes) throws Exception {
        Files.write(root.resolve("stop-" + role), new byte[] {1});
        Process process = processes.get(role);
        assertTrue(process != null && process.waitFor(
                15L, TimeUnit.SECONDS), role + " process did not stop");
        assertEquals(0, process.exitValue(), () -> log(root, role));
    }

    private static void restartRole(String role, Path root, String connect,
            String host, Map<String, Identity> identities, Path trustStore,
            Path authz, Map<String, Process> processes) throws Exception {
        Files.deleteIfExists(root.resolve("stop-" + role));
        Files.deleteIfExists(root.resolve(role + ".ready"));
        Process replacement = launch(role, root, connect, host,
                identities.get(role), trustStore, authz);
        processes.put(role, replacement);
        awaitNonEmpty(root.resolve(role + ".ready"), 20_000L);
    }

    private static RegistryWorker awaitDifferentLeader(
            ZkServiceRegistry observer, int oldPort, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            RegistryWorker leader = observer.worker(
                    StreamClusterRoles.MASTER_LEADER_ROLE_ID);
            if (leader != null && leader.port() != oldPort) return leader;
            Thread.sleep(25L);
        }
        throw new IllegalStateException("replacement Stream leader was not elected");
    }

    private static String roleForPort(Path root, int port) throws Exception {
        for (String role : new String[] {"master-a", "master-b"}) {
            if (Integer.parseInt(text(root.resolve(role + ".ready"))) == port) {
                return role;
            }
        }
        throw new IllegalStateException("active Master port is unknown: " + port);
    }

    private static void awaitVisibleCount(Path root, int expected,
            long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.isDirectory(root)) {
                try (java.util.stream.Stream<Path> files = Files.list(root)) {
                    if (files.filter(path -> path.getFileName().toString()
                            .endsWith(".visible")).count() >= expected) return;
                }
            }
            Thread.sleep(25L);
        }
        throw new IllegalStateException(
                "visible transaction count did not reach " + expected);
    }

    private static void awaitNonEmpty(Path file, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline
                && (!Files.exists(file) || Files.size(file) == 0L)) {
            Thread.sleep(25L);
        }
        assertTrue(Files.exists(file) && Files.size(file) > 0L,
                "process did not publish " + file);
    }

    private static String nonLoopbackIpv4() throws Exception {
        Enumeration<NetworkInterface> interfaces =
                NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface network = interfaces.nextElement();
            if (!network.isUp() || network.isLoopback()) continue;
            Enumeration<java.net.InetAddress> addresses =
                    network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                java.net.InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address
                        && !address.isLoopbackAddress()) {
                    return address.getHostAddress();
                }
            }
        }
        throw new IllegalStateException(
                "the cross-host Stream gate requires a non-loopback IPv4 address");
    }

    private static ControlSecurityConfig security(Identity identity,
            Path trustStore, Path authz, String host) {
        Properties properties = new Properties();
        properties.setProperty("rowweave.control.securityProfile", "MTLS");
        properties.setProperty("rowweave.control.host", host);
        properties.setProperty("rowweave.control.tls.keyStore",
                identity.keyStore.toString());
        properties.setProperty("rowweave.control.tls.keyStorePassword", PASSWORD);
        properties.setProperty("rowweave.control.tls.trustStore",
                trustStore.toString());
        properties.setProperty("rowweave.control.tls.trustStorePassword", PASSWORD);
        properties.setProperty("rowweave.control.tls.authzFile", authz.toString());
        return ControlSecurityConfig.fromProperties(properties);
    }

    private static Identity identity(Path root, String name, String host)
            throws Exception {
        Path keyStore = root.resolve(name + ".p12");
        Path certificate = root.resolve(name + ".cer");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=" + name, "-validity", "30",
                "-ext", "SAN=ip:" + host + ",dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-exportcert", "-alias", "identity", "-keystore",
                keyStore.toString(), "-storepass", PASSWORD,
                "-file", certificate.toString());
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        return new Identity(keyStore, certificate, hex(
                MessageDigest.getInstance("SHA-256").digest(
                        leaf.getEncoded())));
    }

    private static void importCertificate(Path trustStore, String alias,
            Path certificate) throws Exception {
        keytool("-importcert", "-noprompt", "-alias", alias,
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
    }

    private static void keytool(String... arguments) throws Exception {
        Process process = new ProcessBuilder(ForkedJvm.keytoolCommand(arguments))
                .redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        int exit = process.waitFor();
        if (exit != 0) {
            throw new AssertionError("keytool failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(InputStream input) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[4_096];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            output.write(buffer, 0, read);
        }
        return output.toByteArray();
    }

    private static String text(Path file) throws Exception {
        return new String(Files.readAllBytes(file), StandardCharsets.UTF_8)
                .trim();
    }

    private static String log(Path root, String role) {
        try {
            return text(root.resolve(role + ".log"));
        } catch (Exception ignored) {
            return "missing process log";
        }
    }

    private static String hex(byte[] value) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] result = new char[value.length * 2];
        for (int index = 0; index < value.length; index++) {
            int current = value[index] & 0xff;
            result[index * 2] = alphabet[current >>> 4];
            result[index * 2 + 1] = alphabet[current & 0x0f];
        }
        return new String(result);
    }

    private static final class Identity {
        private final Path keyStore;
        private final Path certificate;
        private final String fingerprint;

        private Identity(Path keyStore, Path certificate,
                String fingerprint) {
            this.keyStore = keyStore;
            this.certificate = certificate;
            this.fingerprint = fingerprint;
        }
    }
}
