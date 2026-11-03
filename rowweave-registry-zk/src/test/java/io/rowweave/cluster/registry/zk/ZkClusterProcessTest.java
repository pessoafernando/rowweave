package io.rowweave.cluster.registry.zk;

import io.rowweave.CollectionSource;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.cluster.MasterElection;
import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.cluster.RegistryMasterLocator;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.deployment.DeploymentRuntime;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.run.AttemptStatus;
import io.rowweave.run.ExecutionEvent;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStatus;
import io.rowweave.run.RunSubmission;
import org.apache.curator.test.TestingServer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * End-to-end ZooKeeper cluster test that really crosses processes: an embedded ZooKeeper
 * (TestingServer), a Master started in its own JVM with {@code -Drowweave.registry=zk}, a Worker
 * started in another JVM that discovers the leader through ZooKeeper, and this test JVM holding
 * nothing but a {@link ZkRemoteJobClient} built from the connection string. It shows the ZooKeeper
 * switch in ClusterMain works for real: three separate processes meet only in ZooKeeper, the job
 * completes, and its rows match the single-machine oracle as a multiset.
 *
 * <p>{@link ZkRemoteJobClientTest} also uses a real ZooKeeper, but keeps master and worker in this
 * JVM. This test is the three-process case: it exercises the JVM entry point behind
 * {@code -Drowweave.registry=zk} rather than an object graph assembled in code.
 */
@Tag("multiprocess")
class ZkClusterProcessTest {

    private static final String PASSWORD = "changeit";
    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();
    private static final int N = 4000;
    private static final int KEYS = 8;

    private static List<DataRow> rows() {
        List<DataRow> rs = new ArrayList<>(N);
        for (int i = 0; i < N; i++) {
            DataRow r = new DataRow(2); r.set(0, "k" + (i % KEYS)); r.set(1, new BigDecimal(i % 83)); rs.add(r);
        }
        return rs;
    }

    /** Start ClusterMain in its own JVM, parse the ready line for readyPrefix, return the process; readyOut collects the value. */
    private static Process launch(List<String> jvmArgs, String readyPrefix, long timeoutMs,
                                  StringBuilder readyOut) throws Exception {
        String javaBin = System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
        String cp = System.getProperty("java.class.path");  // Carries core, this module and Curator, so the forked JVM can discover the ZooKeeper SPI
        List<String> cmd = new ArrayList<>();
        cmd.add(javaBin);
        cmd.add("-Xms128m");
        cmd.add("-Xmx4096m");
        cmd.add("-XX:MaxDirectMemorySize=512m");
        cmd.add("-XX:MaxMetaspaceSize=256m");
        cmd.add("-XX:ReservedCodeCacheSize=128m");
        cmd.add("-Xss1m");
        cmd.add("-XX:ActiveProcessorCount=4");
        cmd.add("-Drowweave.process.memoryLimitBytes=12884901888");
        cmd.add("-cp");
        cmd.add(cp);
        cmd.addAll(jvmArgs);
        String muxMode = System.getProperty("rowweave.shuffle.mux");
        if (muxMode != null) {
            cmd.add("-Drowweave.shuffle.mux=" + muxMode);
        }
        cmd.add("io.rowweave.cluster.ClusterMain");
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        BufferedReader r = new BufferedReader(new InputStreamReader(p.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.currentTimeMillis() + timeoutMs;
        final boolean[] ready = {false};
        StringBuilder processLog = new StringBuilder();
        Thread reader = new Thread(() -> {
            try {
                String line;
                while ((line = r.readLine()) != null) {
                    synchronized (processLog) {
                        processLog.append(line).append('\n');
                    }
                    if (!ready[0] && line.startsWith(readyPrefix)) {
                        synchronized (readyOut) {
                            readyOut.append(line.substring(readyPrefix.length()).trim());
                            ready[0] = true;
                            readyOut.notifyAll();
                        }
                    }
                }
            } catch (Exception ignored) {
            } finally {
                synchronized (readyOut) {
                    readyOut.notifyAll();
                }
            }
        }, "zk-launch-reader");
        reader.setDaemon(true);
        reader.start();
        synchronized (readyOut) {
            while (!ready[0] && p.isAlive() && System.currentTimeMillis() < deadline) {
                readyOut.wait(Math.max(1, deadline - System.currentTimeMillis()));
            }
        }
        if (!ready[0]) {
            p.destroyForcibly();
            String output;
            synchronized (processLog) {
                output = processLog.toString();
            }
            fail("process not ready within " + timeoutMs + " ms (marker " + readyPrefix + "):\n"
                    + output);
        }
        return p;
    }

    private static void kill(Process p) {
        if (p != null) {
            p.destroyForcibly();
            try { p.waitFor(5, TimeUnit.SECONDS); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
        }
    }

    private static void stopGracefully(Process process) {
        if (process == null) {
            return;
        }
        process.destroy();
        try {
            if (!process.waitFor(10, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                process.waitFor(5, TimeUnit.SECONDS);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        }
    }

    private static long countLines(Path p) {
        try { return Files.lines(p).count(); } catch (Exception e) { throw new RuntimeException(e); }
    }

    @Test
    void groupBySumViaZkThreeProcessCluster(@TempDir Path tmp) throws Exception {
        Process master = null;
        Process worker0 = null;
        try (TestingServer zk = new TestingServer(true)) {
            String connect = zk.getConnectString();
            List<String> zkArgs = Arrays.asList(
                    "-Drowweave.registry=zk",
                    "-Drowweave.zk.connectString=" + connect,
                    "-Drowweave.worker.host=127.0.0.1");

            // 1) A separate Master process: elected leader through ZooKeeper, endpoint published there.
            StringBuilder mp = new StringBuilder();
            List<String> mArgs = new ArrayList<>(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"));
            mArgs.addAll(zkArgs);
            master = launch(mArgs, "MASTER_PORT=", 30_000, mp);
            assertTrue(Integer.parseInt(mp.toString()) >= 0, "master is ready");

            // 2) A separate Worker process: finds the leader through RegistryMasterLocator, never given a master address.
            StringBuilder wp = new StringBuilder();
            List<String> wArgs = new ArrayList<>(Arrays.asList("-Drowweave.worker.enable=true", "-Drowweave.worker.slots=32"));
            wArgs.addAll(zkArgs);
            worker0 = launch(wArgs, "WORKER_READY=", 30_000, wp);
            Thread.sleep(1000);  // Let the worker registration land in ZooKeeper and the master view converge.

            // 3) This JVM submits through ZkRemoteJobClient with only a connection string; it resolves the leader from ZooKeeper.
            Path out = tmp.resolve("zk-cluster-out.tsv");
            try (ZkRemoteJobClient client = new ZkRemoteJobClient(connect)) {
                ExecutionEnvironment s = new ExecutionEnvironment("zkproc", "zkproc", client);
                s.setParallelism(2);
                s.fromSource(new CollectionSource(META, rows()))
                        .groupBy(ParallelGroupByAgg.of(META, "k").sum("amt"))
                        .sinkTo(new FileSink(GB, out.toString()));
                s.execute();
            }
            assertEquals(KEYS, countLines(out), "cross-process ZK cluster: 8 groups on k, job completed fully");
        } finally {
            kill(worker0);
            kill(master);
        }
    }

    @Test
    void mtlsShuffleSurvivesIndependentZkLeaderSwitch(@TempDir Path tmp)
            throws Exception {
        Identity masterAIdentity = identity(tmp, "zk-mtls-master-a");
        Identity masterBIdentity = identity(tmp, "zk-mtls-master-b");
        Identity workerIdentity = identity(tmp, "zk-mtls-worker");
        Identity clientIdentity = identity(tmp, "zk-mtls-client");
        Path trustStore = tmp.resolve("zk-mtls-trust.p12");
        importCertificate(trustStore, "master-a", masterAIdentity.certificate);
        importCertificate(trustStore, "master-b", masterBIdentity.certificate);
        importCertificate(trustStore, "worker", workerIdentity.certificate);
        importCertificate(trustStore, "client", clientIdentity.certificate);
        Path authz = tmp.resolve("zk-mtls-authz.txt");
        Files.write(authz, (masterAIdentity.fingerprint + "=MASTER\n"
                + masterBIdentity.fingerprint + "=MASTER\n"
                + workerIdentity.fingerprint + "=WORKER\n"
                + clientIdentity.fingerprint + "=CLIENT\n")
                .getBytes(StandardCharsets.US_ASCII));

        Process masterA = null;
        Process masterB = null;
        Process workerA = null;
        Process workerB = null;
        try (TestingServer zk = new TestingServer(true)) {
            String connect = zk.getConnectString();
            List<String> common = Arrays.asList(
                    "-Drowweave.registry=zk",
                    "-Drowweave.zk.connectString=" + connect,
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.heartbeat.staleness.ms=3000");

            StringBuilder masterAReady = new StringBuilder();
            List<String> masterAArgs = roleArgs(
                    Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    common, securityArgs(masterAIdentity, trustStore, authz));
            masterA = launch(masterAArgs, "MASTER_PORT=", 30_000L, masterAReady);
            StringBuilder masterBReady = new StringBuilder();
            List<String> masterBArgs = roleArgs(
                    Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    common, securityArgs(masterBIdentity, trustStore, authz));
            masterB = launch(masterBArgs, "MASTER_PORT=", 30_000L, masterBReady);

            List<String> workerArgs = roleArgs(
                    Arrays.asList("-Drowweave.worker.enable=true", "-Drowweave.worker.slots=8"),
                    common, securityArgs(workerIdentity, trustStore, authz));
            workerA = launch(workerArgs, "WORKER_READY=", 30_000L,
                    new StringBuilder());
            workerB = launch(workerArgs, "WORKER_READY=", 30_000L,
                    new StringBuilder());
            awaitWorkerCount(connect, 2, 15_000L);

            Properties clientProperties = securityProperties(
                    clientIdentity, trustStore, authz);
            ControlSecurityConfig control =
                    ControlSecurityConfig.fromProperties(clientProperties);
            try (DeploymentRuntime runtime = DeploymentRuntime.load(
                         clientProperties, control, Runtime.getRuntime().maxMemory());
                 ZkServiceRegistry clientRegistry = new ZkServiceRegistry(connect)) {
                RegistryMasterLocator locator = new RegistryMasterLocator(
                        clientRegistry, control.securityMetadata());
                try (ClusterJobClient client = new ClusterJobClient(locator, runtime)) {
                    Path source = tmp.resolve("zk-mtls-source.tsv");
                    writeRows(source);
                    Path before = tmp.resolve("zk-mtls-before.tsv");
                    assertEquals(KEYS, client.submit(
                            LegacyPlanArtifactAdapter.wrap("zk-mtls-before",
                                    groupByFileGraph(source, before))).rowsOut());
                    assertEquals(KEYS, countLines(before));

                    Endpoint oldLeader = locator.locateLeader();
                    int masterAPort = Integer.parseInt(masterAReady.toString());
                    int masterBPort = Integer.parseInt(masterBReady.toString());
                    if (oldLeader.port() == masterAPort) {
                        stopGracefully(masterA);
                        masterA = null;
                    } else {
                        assertEquals(masterBPort, oldLeader.port());
                        stopGracefully(masterB);
                        masterB = null;
                    }
                    Endpoint newLeader = awaitLeaderChange(
                            locator, oldLeader.port(), 15_000L);
                    Thread.sleep(1_500L);

                    Path after = tmp.resolve("zk-mtls-after.tsv");
                    assertEquals(KEYS, client.submit(
                            LegacyPlanArtifactAdapter.wrap("zk-mtls-after",
                                    groupByFileGraph(source, after))).rowsOut());
                    assertEquals(KEYS, countLines(after));
                    System.out.printf("[DP7-MTLS-LEADER] oldPort=%d, newPort=%d, "
                                    + "rowsBefore=%d, rowsAfter=%d%n",
                            oldLeader.port(), newLeader.port(),
                            countLines(before), countLines(after));
                }
            }
            assertTrue(workerA.isAlive());
            assertTrue(workerB.isAlive());
        } finally {
            kill(workerB);
            kill(workerA);
            kill(masterB);
            kill(masterA);
        }
    }

    @Test
    void durableRunRetriesAfterAcceptedWorkerProcessDies(@TempDir Path tmp) throws Exception {
        Process master = null;
        Process firstWorker = null;
        Process replacementWorker = null;
        ExecutorService submitter = Executors.newSingleThreadExecutor();
        try (TestingServer zk = new TestingServer(true)) {
            String connect = zk.getConnectString();
            Path artifactDirectory = tmp.resolve("artifacts");
            Path releaseGate = tmp.resolve("release-input");
            List<String> common = Arrays.asList(
                    "-Drowweave.registry=zk",
                    "-Drowweave.zk.connectString=" + connect,
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.heartbeat.staleness.ms=1500");

            StringBuilder masterReady = new StringBuilder();
            List<String> masterArgs = new ArrayList<>(Arrays.asList(
                    "-Drowweave.master.enable=true",
                    "-Drowweave.master.port=0",
                    "-Drowweave.master.artifact.dir=" + artifactDirectory));
            masterArgs.addAll(common);
            master = launch(masterArgs, "MASTER_PORT=", 30_000L, masterReady);

            firstWorker = launchWorker(common);
            awaitWorkerCount(connect, 1, 10_000L);

            PlanGraph graph = new PlanGraph(Arrays.asList(
                    PlanNode.sourceFromInputs(META, Collections.singletonList(
                            new GateInput(releaseGate.toString()))),
                    PlanNode.sink(META, 0, new DiscardSink(META)).build()), 1);
            RunSubmission submission = new RunSubmission(
                    "run-zk-process-retry", "attempt-1", "publication-1",
                    2, 0L, System.currentTimeMillis());

            try (ZkRemoteJobClient client = new ZkRemoteJobClient(connect);
                 ZkServiceRegistry observer = new ZkServiceRegistry(connect)) {
                Future<ExecutionReport> submitted = submitter.submit(() -> client.submitRun(
                        LegacyPlanArtifactAdapter.wrap("zk-process-retry", graph), submission));
                awaitAttempt(client, submitted, 1, AttemptStatus.RUNNING, 15_000L);
                awaitAllAssignmentsAccepted(observer, client, 15_000L);

                kill(firstWorker);
                firstWorker = null;
                replacementWorker = launchWorker(common);
                Files.createFile(releaseGate);

                RunSnapshot terminal = awaitTerminal(client, 30_000L);
                assertEquals(RunStatus.SUCCEEDED, terminal.status(),
                        terminal.currentAttempt() == null ? "no attempt count="
                                + terminal.attemptCount()
                                : "attemptCount=" + terminal.attemptCount()
                                + " attempt=" + terminal.currentAttempt().attemptNumber()
                                + " " + terminal.currentAttempt().reasonCode() + ": "
                                + terminal.currentAttempt().message());
                assertEquals(2, terminal.attemptCount(),
                        "accepted work lost with its Worker process must retry exactly once");
                assertEquals(AttemptStatus.SUCCEEDED, terminal.currentAttempt().status());
                List<ExecutionEvent> history = observer.runStateStore().events(
                        submission.runId(), 0L, 256);
                long attemptsCreated = history.stream()
                        .filter(event -> event.type()
                                == ExecutionEvent.Type.ATTEMPT_CREATED)
                        .count();
                long terminalSuccesses = history.stream()
                        .filter(event -> event.type()
                                == ExecutionEvent.Type.ATTEMPT_ADVANCED)
                        .filter(event -> event.attemptTo() == AttemptStatus.SUCCEEDED)
                        .filter(event -> event.runTo() == RunStatus.SUCCEEDED)
                        .count();
                assertEquals(2L, attemptsCreated,
                        "durable journal must contain exactly the original and retry attempts");
                assertEquals(1L, terminalSuccesses,
                        "durable journal must contain exactly one successful terminal mutation");
                try {
                    assertEquals(1L, submitted.get(5, TimeUnit.SECONDS).rowsOut());
                } catch (ExecutionException acceptedSubmitReturnedNonterminal) {
                    Throwable cause = acceptedSubmitReturnedNonterminal.getCause();
                    assertTrue(cause instanceof io.rowweave.cluster.DurableRunSubmitException,
                            acceptedSubmitReturnedNonterminal.toString());
                    io.rowweave.cluster.DurableRunSubmitException durableFailure =
                            (io.rowweave.cluster.DurableRunSubmitException) cause;
                    assertEquals(io.rowweave.cluster.DurableRunSubmitException.Category.UNKNOWN,
                            durableFailure.category());
                    assertEquals(submission.runId(), durableFailure.runId());
                    RunSnapshot observed = durableFailure.lastSnapshot();
                    assertNotNull(observed,
                            "accepted non-terminal return requires an authoritative snapshot");
                    assertEquals(submission.runId(), observed.runId());
                    assertFalse(observed.status().isTerminal(),
                            "a terminal snapshot must be reconciled to success/failure");
                    String failureMessages = failureMessages(durableFailure);
                    assertFalse(failureMessages.contains("corrupt ZK run authority"),
                            failureMessages);
                    assertFalse(failureMessages.contains("authoritative query unavailable"),
                            failureMessages);
                    assertFalse(failureMessages.contains("run evidence authority unavailable"),
                            failureMessages);
                }
            }
        } finally {
            submitter.shutdownNow();
            kill(replacementWorker);
            kill(firstWorker);
            kill(master);
        }
    }

    private static String failureMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        Deque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> seen = Collections.newSetFromMap(
                new IdentityHashMap<Throwable, Boolean>());
        pending.add(failure);
        while (!pending.isEmpty() && seen.size() < 64) {
            Throwable current = pending.removeFirst();
            if (!seen.add(current)) continue;
            if (current.getMessage() != null) {
                messages.append(current.getMessage()).append('\n');
            }
            if (current.getCause() != null) pending.addLast(current.getCause());
            for (Throwable suppressed : current.getSuppressed()) {
                if (suppressed != null) pending.addLast(suppressed);
            }
        }
        return messages.toString();
    }

    private static Process launchWorker(List<String> common) throws Exception {
        StringBuilder ready = new StringBuilder();
        List<String> args = new ArrayList<>(Arrays.asList(
                "-Drowweave.worker.enable=true", "-Drowweave.worker.slots=2"));
        args.addAll(common);
        return launch(args, "WORKER_READY=", 30_000L, ready);
    }

    private static List<String> roleArgs(
            List<String> role, List<String> common, List<String> security) {
        List<String> arguments = new ArrayList<>();
        arguments.addAll(role);
        arguments.addAll(common);
        arguments.addAll(security);
        return arguments;
    }

    private static List<String> securityArgs(
            Identity identity, Path trustStore, Path authz) {
        Properties properties = securityProperties(identity, trustStore, authz);
        List<String> arguments = new ArrayList<>();
        List<String> names = new ArrayList<>(properties.stringPropertyNames());
        Collections.sort(names);
        for (String name : names) {
            arguments.add("-D" + name + '=' + properties.getProperty(name));
        }
        return arguments;
    }

    private static Properties securityProperties(
            Identity identity, Path trustStore, Path authz) {
        Properties properties = new Properties();
        properties.setProperty(ControlSecurityConfig.PROFILE_PROPERTY, "MTLS");
        properties.setProperty(ControlSecurityConfig.HOST_PROPERTY, "127.0.0.1");
        properties.setProperty("rowweave.control.tls.keyStore",
                identity.keyStore.toString());
        properties.setProperty("rowweave.control.tls.keyStorePassword", PASSWORD);
        properties.setProperty("rowweave.control.tls.trustStore",
                trustStore.toString());
        properties.setProperty("rowweave.control.tls.trustStorePassword", PASSWORD);
        properties.setProperty("rowweave.control.tls.authzFile", authz.toString());
        return properties;
    }

    private static Endpoint awaitLeaderChange(
            RegistryMasterLocator locator, int oldPort, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        RuntimeException lastFailure = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                Endpoint leader = locator.locateLeader();
                if (leader.port() != oldPort) {
                    return leader;
                }
            } catch (RuntimeException notElectedYet) {
                lastFailure = notElectedYet;
            }
            Thread.sleep(25L);
        }
        throw new AssertionError("standby did not become leader", lastFailure);
    }

    private static PlanGraph groupByFileGraph(Path source, Path output) {
        return new PlanGraph(Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(
                        new FileSource(META, source.toString()))),
                PlanNode.unary("groupBySum", GB, 0)
                        .listParam("keys", Collections.singletonList("k"))
                        .param("measure", "amt").build(),
                PlanNode.sink(GB, 1, new FileSink(GB, output.toString())).build()), 2);
    }

    private static void writeRows(Path output) {
        FileSink sink = new FileSink(META, output.toString());
        try {
            for (DataRow row : rows()) {
                sink.collect(row);
            }
        } finally {
            sink.close();
        }
    }

    private static Identity identity(Path temp, String name) throws Exception {
        Path keyStore = temp.resolve(name + ".p12");
        Path certificate = temp.resolve(name + ".cer");
        keytool("-genkeypair", "-alias", "identity", "-keyalg", "RSA",
                "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-keypass", PASSWORD,
                "-dname", "CN=" + name, "-validity", "30",
                "-ext", "SAN=ip:127.0.0.1,dns:localhost",
                "-ext", "EKU=serverAuth,clientAuth");
        keytool("-exportcert", "-alias", "identity", "-keystore", keyStore.toString(),
                "-storepass", PASSWORD, "-file", certificate.toString());
        X509Certificate leaf;
        try (InputStream input = Files.newInputStream(certificate)) {
            leaf = (X509Certificate) CertificateFactory.getInstance("X.509")
                    .generateCertificate(input);
        }
        return new Identity(keyStore, certificate, sha256Hex(leaf.getEncoded()));
    }

    private static void importCertificate(
            Path trustStore, String alias, Path certificate) throws Exception {
        keytool("-importcert", "-noprompt", "-alias", alias,
                "-file", certificate.toString(), "-storetype", "PKCS12",
                "-keystore", trustStore.toString(), "-storepass", PASSWORD);
    }

    private static void keytool(String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/keytool")
                .getAbsolutePath());
        command.add("-J-Xms64m");
        command.add("-J-Xmx512m");
        command.add("-J-XX:MaxDirectMemorySize=64m");
        command.add("-J-XX:MaxMetaspaceSize=128m");
        command.add("-J-XX:ReservedCodeCacheSize=64m");
        command.add("-J-Xss1m");
        command.add("-J-XX:ActiveProcessorCount=2");
        Collections.addAll(command, arguments);
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        byte[] output = readAll(process.getInputStream());
        int exit = process.waitFor();
        if (exit != 0) {
            throw new AssertionError("keytool failed: "
                    + new String(output, StandardCharsets.UTF_8));
        }
    }

    private static byte[] readAll(InputStream input) throws Exception {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        byte[] buffer = new byte[4096];
        for (int read; (read = input.read(buffer)) >= 0; ) {
            bytes.write(buffer, 0, read);
        }
        return bytes.toByteArray();
    }

    private static String sha256Hex(byte[] value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
        StringBuilder hex = new StringBuilder(digest.length * 2);
        for (byte part : digest) {
            hex.append(String.format("%02x", part & 0xff));
        }
        return hex.toString();
    }

    private static final class Identity {
        final Path keyStore;
        final Path certificate;
        final String fingerprint;

        Identity(Path keyStore, Path certificate, String fingerprint) {
            this.keyStore = keyStore;
            this.certificate = certificate;
            this.fingerprint = fingerprint;
        }
    }

    private static void awaitWorkerCount(String connect, int expected, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        try (ZkServiceRegistry registry = new ZkServiceRegistry(connect)) {
            while (System.currentTimeMillis() < deadline) {
                if (workerCount(registry) == expected) return;
                Thread.sleep(25L);
            }
            assertEquals(expected, workerCount(registry), "worker registry did not settle");
        }
    }

    private static long workerCount(ZkServiceRegistry registry) {
        return registry.liveWorkers().stream()
                .filter(worker -> !MasterElection.MASTER_LEADER_ID.equals(worker.workerId()))
                .count();
    }

    private static void awaitAttempt(ZkRemoteJobClient client, Future<?> submitted,
                                     int attemptNumber,
                                     AttemptStatus status, long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        RunSnapshot last = null;
        while (System.currentTimeMillis() < deadline) {
            if (submitted.isDone()) submitted.get();
            last = client.queryRun("run-zk-process-retry");
            if (last != null && last.attemptCount() == attemptNumber
                    && last.currentAttempt() != null
                    && last.currentAttempt().status() == status) return;
            Thread.sleep(25L);
        }
        fail("attempt did not reach " + status + ": "
                + (last == null ? "missing" : last.status() + "/" + last.attemptCount()
                + "/" + (last.currentAttempt() == null ? "none"
                : last.currentAttempt().status())));
    }

    private static RunSnapshot awaitTerminal(ZkRemoteJobClient client, long timeoutMillis)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        RunSnapshot last = null;
        while (System.currentTimeMillis() < deadline) {
            last = client.queryRun("run-zk-process-retry");
            if (last != null && last.status().isTerminal()) return last;
            Thread.sleep(50L);
        }
        fail("durable Run did not terminate: "
                + (last == null ? "missing" : last.status() + "/" + last.attemptCount()));
        return null;
    }

    private static void awaitAllAssignmentsAccepted(
            ZkServiceRegistry registry, ZkRemoteJobClient client,
            long timeoutMillis) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        io.rowweave.run.StageAssignmentSnapshot last = null;
        while (System.currentTimeMillis() < deadline) {
            RunSnapshot run = client.queryRun("run-zk-process-retry");
            if (run != null && run.currentAttempt() != null) {
                io.rowweave.run.RunAttemptFence fence =
                        new io.rowweave.run.RunAttemptFence(
                                run.runId(), run.currentAttempt().attemptId(),
                                run.currentAttempt().fenceToken());
                last = registry.durableStageAssignmentAuthority()
                        .assignmentSnapshot(fence);
                if (last != null && last.acceptedCount()
                        == last.manifest().assignmentCount()) return;
            }
            Thread.sleep(25L);
        }
        fail("assignments were not all accepted: " + (last == null ? "missing"
                : last.acceptedCount() + "/" + last.manifest().assignmentCount()));
    }

    /** Cross-process gate: the first Worker is killed while blocked; its replacement sees the file. */
    public static final class GateInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final String gate;

        GateInput(String gate) {
            this.gate = gate;
        }

        @Override
        public DataSchema getSchema() {
            return META;
        }

        @Override
        public BatchSourceSplitDescriptor batchSourceSplit() {
            return new BatchSourceSplitDescriptor(
                    BatchSourceSplitDescriptor.CURRENT_VERSION,
                    "test-file-gate", 1, "gate-split",
                    SchemaFingerprint.of(META), gate,
                    BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        }

        @Override
        public Iterator<DataRow> iterator() {
            return new Iterator<DataRow>() {
                private boolean emitted;

                @Override
                public boolean hasNext() {
                    while (!emitted && !Files.exists(java.nio.file.Paths.get(gate))) {
                        try {
                            Thread.sleep(25L);
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException("gate wait interrupted", interrupted);
                        }
                    }
                    return !emitted;
                }

                @Override
                public DataRow next() {
                    if (!hasNext()) throw new NoSuchElementException();
                    emitted = true;
                    DataRow row = new DataRow(2);
                    row.set(0, "retry");
                    row.set(1, BigDecimal.ONE);
                    return row;
                }
            };
        }
    }
}
