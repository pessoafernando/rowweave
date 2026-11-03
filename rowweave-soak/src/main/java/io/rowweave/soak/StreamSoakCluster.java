package io.rowweave.soak;

import io.rowweave.StreamClusterMain;
import io.rowweave.StreamJobClient;
import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.zk.ZkServiceRegistry;
import io.rowweave.cluster.registry.zk.ZkStreamJobClient;
import io.rowweave.stream.cluster.StreamClusterRoles;

import java.io.File;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

final class StreamSoakCluster implements AutoCloseable {
    private static final String[] WORKERS = {
            "worker-a", "worker-b", "worker-c"};
    private static final String[] MASTERS = {"master-a", "master-b"};

    private final Path root;
    private final String zkConnect;
    private final StreamSoakDeployments deployments;
    private final String host;
    private final Path trustStore;
    private final Path authz;
    private final Path s3Secret;
    private final Path postgresSecret;
    private final Map<String, StreamSoakIdentity> identities =
            new LinkedHashMap<>();
    private final Map<String, RoleProcess> processes = new LinkedHashMap<>();
    private final Map<String, Integer> generations = new LinkedHashMap<>();
    private final ZkServiceRegistry observer;
    private final StreamJobClient client;

    private StreamSoakCluster(Path root, String zkConnect,
            StreamSoakDeployments deployments, String host, Path trustStore,
            Path authz, Path s3Secret, Path postgresSecret,
            ZkServiceRegistry observer, StreamJobClient client) {
        this.root = root;
        this.zkConnect = zkConnect;
        this.deployments = deployments;
        this.host = host;
        this.trustStore = trustStore;
        this.authz = authz;
        this.s3Secret = s3Secret;
        this.postgresSecret = postgresSecret;
        this.observer = observer;
        this.client = client;
    }

    static StreamSoakCluster start(Path root, String zkConnect,
            StreamSoakDeployments deployments, byte[] s3Credentials,
            byte[] postgresCredentials) throws Exception {
        Files.createDirectories(root);
        String host = nonLoopbackIpv4();
        Path trustStore = root.resolve("cluster-trust.p12");
        Path authz = root.resolve("cluster-authz.txt");
        Path s3Secret = root.resolve("s3-secret.bin");
        Path postgresSecret = root.resolve("postgres-secret.bin");
        Files.write(s3Secret, s3Credentials);
        Files.write(postgresSecret, postgresCredentials);

        Map<String, StreamSoakIdentity> identities = new LinkedHashMap<>();
        for (String role : allRoles()) {
            identities.put(role, StreamSoakIdentity.create(root, role, host));
        }
        for (Map.Entry<String, StreamSoakIdentity> entry
                : identities.entrySet()) {
            StreamSoakIdentity.importCertificate(trustStore, entry.getKey(),
                    entry.getValue().certificate);
        }
        StringBuilder grants = new StringBuilder();
        for (Map.Entry<String, StreamSoakIdentity> entry
                : identities.entrySet()) {
            String role = entry.getKey();
            grants.append(entry.getValue().fingerprint).append('=')
                    .append(role.startsWith("master-") ? "MASTER"
                            : "client".equals(role) ? "CLIENT" : "WORKER")
                    .append('\n');
        }
        Files.write(authz, grants.toString()
                .getBytes(StandardCharsets.US_ASCII));
        ZkServiceRegistry observer = new ZkServiceRegistry(zkConnect);
        StreamJobClient client = new ZkStreamJobClient(zkConnect,
                identities.get("client").security(trustStore, authz,
                        deployments.deploymentSetId, host));
        StreamSoakCluster cluster = new StreamSoakCluster(root, zkConnect,
                deployments, host, trustStore, authz, s3Secret,
                postgresSecret, observer, client);
        cluster.identities.putAll(identities);
        try {
            for (String worker : WORKERS) cluster.launch(worker);
            for (String master : MASTERS) cluster.launch(master);
            cluster.awaitWorkerCount(3, 30_000L);
            cluster.awaitLeader(30_000L);
            return cluster;
        } catch (Throwable failure) {
            cluster.close();
            throw failure;
        }
    }

    StreamJobClient client() { return client; }
    ZkServiceRegistry observer() { return observer; }

    List<RoleProcess> roleProcesses() {
        return Collections.unmodifiableList(
                new ArrayList<>(processes.values()));
    }

    long replaceWorker(String role) throws Exception {
        if (!role.startsWith("worker-")) {
            throw new IllegalArgumentException("not a Worker role");
        }
        long started = System.nanoTime();
        stop(role);
        launch(role);
        awaitWorkerCount(3, 30_000L);
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    long replaceLeader() throws Exception {
        RegistryWorker current = awaitLeader(30_000L);
        String role = roleForPort(current.port());
        long started = System.nanoTime();
        stop(role);
        RegistryWorker replacement = awaitDifferentLeader(
                current.port(), 30_000L);
        launch(role);
        awaitLeader(30_000L);
        if (replacement.port() == current.port()) {
            throw new IllegalStateException("Master replacement did not occur");
        }
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    private void launch(String role) throws Exception {
        int generation = generations.getOrDefault(role, 0) + 1;
        generations.put(role, generation);
        Path log = root.resolve(String.format(java.util.Locale.ROOT,
                "%s-generation-%03d.log", role, generation));
        Process process = new ProcessBuilder(command(role))
                .redirectErrorStream(true).redirectOutput(log.toFile()).start();
        String prefix = role.startsWith("master-")
                ? "STREAM_MASTER_READY=" : "STREAM_WORKER_READY=";
        String endpoint = awaitReady(log, prefix, process, 30_000L);
        processes.put(role, new RoleProcess(role, process, log,
                parsePort(endpoint), root.resolve(role + "-state")));
    }

    private List<String> command(String role) throws Exception {
        boolean master = role.startsWith("master-");
        Path state = Files.createDirectories(root.resolve(role + "-state"));
        Path tmp = Files.createDirectories(state.resolve("tmp"));
        StreamSoakIdentity identity = identities.get(role);
        List<String> command = new ArrayList<>();
        command.add(new File(System.getProperty("java.home"), "bin/java")
                .getAbsolutePath());
        Collections.addAll(command,
                "-Xms64m", "-Xmx1024m",
                "-XX:MaxDirectMemorySize=256m",
                "-XX:MaxMetaspaceSize=256m",
                "-XX:ReservedCodeCacheSize=128m",
                "-Xss1m", "-XX:ActiveProcessorCount=4",
                "-Drowweave.process.memoryLimitBytes=4294967296",
                "-Djava.io.tmpdir=" + tmp,
                "-Djdk.tls.maxHandshakeMessageSize=65536",
                "-Djdk.tls.maxCertificateChainLength=10",
                "-Drowweave.control.securityProfile=MTLS",
                "-Drowweave.control.host=" + host,
                "-Drowweave.control.tls.keyStore=" + identity.keyStore,
                "-Drowweave.control.tls.keyStorePassword="
                        + StreamSoakIdentity.PASSWORD,
                "-Drowweave.control.tls.trustStore=" + trustStore,
                "-Drowweave.control.tls.trustStorePassword="
                        + StreamSoakIdentity.PASSWORD,
                "-Drowweave.control.tls.authzFile=" + authz,
                "-Drowweave.control.residentBytes=234881024",
                "-Drowweave.shuffle.ingressResidentBytes=268435456",
                "-Drowweave.stream.state.dir=" + state,
                "-Drowweave.registry=zk",
                "-Drowweave.zk.connectString=" + zkConnect);
        deployments.addProperties(command);
        if (master) {
            command.add("-Drowweave.stream.master.enable=true");
            command.add("-Drowweave.stream.master.port=0");
            command.add("-Drowweave.stream.master.artifact.dir="
                    + root.resolve("shared-artifacts"));
        } else {
            command.add("-Drowweave.stream.worker.enable=true");
            command.add("-Drowweave.stream.worker.id=stream-soak-" + role);
            command.add("-Drowweave.stream.worker.port=0");
            command.add("-Drowweave.stream.worker.taskSlots=12");
            command.add("-Drowweave.stream.worker.channelCapacity=8");
            command.add("-Drowweave.stream.worker.maximumFrameBytes=1048576");
            command.add("-Drowweave.stream.worker.maximumResidentBytes=134217728");
            command.add("-Drowweave.stream.worker.maximumThreads=24");
            command.add("-D" + StreamSoakSecretResolver.S3_SECRET_PATH
                    + '=' + s3Secret);
            command.add("-D" + StreamSoakSecretResolver.POSTGRES_SECRET_PATH
                    + '=' + postgresSecret);
            command.add("-D" + StreamSoakSecretResolver.RESOLUTION_LOG_PATH
                    + '=' + root.resolve(role + "-secret.log"));
        }
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(StreamClusterMain.class.getName());
        return command;
    }

    private RegistryWorker awaitLeader(long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            RegistryWorker leader = observer.worker(
                    StreamClusterRoles.MASTER_LEADER_ROLE_ID);
            if (leader != null) return leader;
            Thread.sleep(50L);
        }
        throw new IllegalStateException("Stream leader was not elected");
    }

    private RegistryWorker awaitDifferentLeader(int previous,
            long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            RegistryWorker leader = observer.worker(
                    StreamClusterRoles.MASTER_LEADER_ROLE_ID);
            if (leader != null && leader.port() != previous) return leader;
            Thread.sleep(50L);
        }
        throw new IllegalStateException("replacement Master was not elected");
    }

    private void awaitWorkerCount(int expected, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        int count = 0;
        while (System.nanoTime() < deadline) {
            count = 0;
            for (RegistryWorker worker : observer.liveWorkers()) {
                if (worker.workerId().startsWith(
                        "__rowweave_stream_worker__")) count++;
            }
            if (count == expected) return;
            Thread.sleep(50L);
        }
        throw new IllegalStateException("expected " + expected
                + " live Workers but found " + count);
    }

    private String roleForPort(int port) {
        for (RoleProcess process : processes.values()) {
            if (process.role.startsWith("master-")
                    && process.port == port) return process.role;
        }
        throw new IllegalStateException("active Master role is unknown");
    }

    private void stop(String role) throws Exception {
        RoleProcess current = processes.remove(role);
        if (current == null) throw new IllegalStateException(
                "role not running: " + role);
        current.process.destroy();
        if (!current.process.waitFor(15L, TimeUnit.SECONDS)) {
            current.process.destroyForcibly();
            if (!current.process.waitFor(5L, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "role could not be stopped: " + role);
            }
        }
    }

    private static String awaitReady(Path log, String prefix,
            Process process, long timeoutMillis) throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline) {
            if (Files.isRegularFile(log)) {
                for (String line : Files.readAllLines(
                        log, StandardCharsets.UTF_8)) {
                    if (line.startsWith(prefix)) {
                        return line.substring(prefix.length()).trim();
                    }
                }
            }
            if (!process.isAlive()) {
                throw new IllegalStateException(
                        "cluster role exited before ready: " + roleLog(log));
            }
            Thread.sleep(50L);
        }
        throw new IllegalStateException(
                "cluster role did not become ready: " + roleLog(log));
    }

    private static int parsePort(String endpoint) {
        int separator = endpoint.lastIndexOf(':');
        if (separator < 1) throw new IllegalStateException(
                "invalid cluster endpoint: " + endpoint);
        return Integer.parseInt(endpoint.substring(separator + 1));
    }

    private static String roleLog(Path log) throws Exception {
        if (!Files.exists(log)) return "<missing>";
        byte[] bytes = Files.readAllBytes(log);
        int start = Math.max(0, bytes.length - 16 * 1024);
        return new String(bytes, start, bytes.length - start,
                StandardCharsets.UTF_8);
    }

    private static List<String> allRoles() {
        List<String> result = new ArrayList<>();
        Collections.addAll(result, MASTERS);
        Collections.addAll(result, WORKERS);
        result.add("client");
        return result;
    }

    private static String nonLoopbackIpv4() throws Exception {
        Enumeration<NetworkInterface> interfaces =
                NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            NetworkInterface network = interfaces.nextElement();
            if (!network.isUp() || network.isLoopback()) continue;
            Enumeration<InetAddress> addresses = network.getInetAddresses();
            while (addresses.hasMoreElements()) {
                InetAddress address = addresses.nextElement();
                if (address instanceof Inet4Address
                        && !address.isLoopbackAddress()) {
                    return address.getHostAddress();
                }
            }
        }
        throw new IllegalStateException("non-loopback IPv4 is required");
    }

    @Override public void close() {
        try { client.close(); } catch (Exception ignored) { }
        List<String> roles = new ArrayList<>(processes.keySet());
        Collections.reverse(roles);
        for (String role : roles) {
            try { stop(role); } catch (Exception ignored) { }
        }
        try { observer.close(); } catch (Exception ignored) { }
    }

    static final class RoleProcess {
        final String role;
        final Process process;
        final Path log;
        final int port;
        final Path stateRoot;

        RoleProcess(String role, Process process, Path log, int port,
                Path stateRoot) {
            this.role = role;
            this.process = process;
            this.log = log;
            this.port = port;
            this.stateRoot = stateRoot;
        }
    }
}
