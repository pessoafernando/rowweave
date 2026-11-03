package io.rowweave.soak;

import io.rowweave.cluster.ClusterMain;
import io.rowweave.cluster.RealClusterJobClient;
import io.rowweave.plan.ExecutionReport;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/** Independent production ClusterMain processes used by the Batch qualification. */
final class ManagedMemorySoakCluster implements AutoCloseable {
    private final ManagedMemorySoakConfig config;
    private final Path roleRoot;
    private final List<RoleProcess> roles = new ArrayList<>();
    private RealClusterJobClient client;

    private ManagedMemorySoakCluster(ManagedMemorySoakConfig config,
                                    Path roleRoot) {
        this.config = config;
        this.roleRoot = roleRoot;
    }

    static ManagedMemorySoakCluster start(ManagedMemorySoakConfig config)
            throws Exception {
        if (config == null) {
            throw new IllegalArgumentException("config is required");
        }
        Path roleRoot = config.output.resolve("cluster-roles");
        Files.createDirectories(roleRoot);
        ManagedMemorySoakCluster cluster = new ManagedMemorySoakCluster(
                config, roleRoot);
        try {
            RoleProcess master = cluster.launchMaster();
            int masterPort = Integer.parseInt(master.readyValue);
            for (int i = 0; i < config.workerCount; i++) {
                cluster.launchWorker(i, masterPort);
            }
            cluster.client = new RealClusterJobClient(
                    "127.0.0.1", masterPort);
            return cluster;
        } catch (Throwable failure) {
            try {
                cluster.close();
            } catch (Throwable closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    ExecutionReport submit(Path source, Path output, String attemptId) {
        if (client == null) {
            throw new IllegalStateException("cluster is not running");
        }
        return client.submitAttempt(ManagedMemorySoakWorkload.graph(
                source, output, config.workerCount), attemptId);
    }

    List<RoleProcess> roles() {
        return Collections.unmodifiableList(roles);
    }

    Path roleRoot() { return roleRoot; }

    private RoleProcess launchMaster() throws Exception {
        List<String> properties = new ArrayList<>();
        properties.add("-Drowweave.master.enable=true");
        properties.add("-Drowweave.master.port=0");
        properties.add("-Drowweave.heartbeat.staleness.ms=30000");
        RoleProcess role = launch("master", properties, "MASTER_PORT=");
        roles.add(role);
        return role;
    }

    private void launchWorker(int ordinal, int masterPort) throws Exception {
        String name = "worker-" + ordinal;
        List<String> properties = new ArrayList<>();
        properties.add("-Drowweave.worker.enable=true");
        properties.add("-Drowweave.worker.master.host=127.0.0.1");
        properties.add("-Drowweave.worker.master.port=" + masterPort);
        properties.add("-Drowweave.worker.slots=" + config.workerSlots);
        properties.add("-Drowweave.worker.maxStageThreads=16");
        properties.add("-Drowweave.worker.memory="
                + config.workerMemoryBytes);
        properties.add("-Drowweave.worker.operator.minMemory="
                + config.operatorMinimumBytes);
        properties.add("-Drowweave.worker.operator.maxMemory="
                + config.operatorMaximumBytes);
        properties.add("-Drowweave.worker.merge.fanIn=8");
        properties.add("-Drowweave.heartbeat.staleness.ms=30000");
        RoleProcess role = launch(name, properties, "WORKER_READY=");
        roles.add(role);
    }

    private RoleProcess launch(String role, List<String> roleProperties,
                               String readyPrefix) throws Exception {
        Path directory = Files.createDirectories(roleRoot.resolve(role));
        Path temporary = Files.createDirectories(directory.resolve("tmp"));
        Path log = directory.resolve("role.log");
        List<String> command = new ArrayList<>();
        command.add(System.getProperty("java.home") + File.separator
                + "bin" + File.separator + "java");
        command.add("-Xms256m");
        command.add("-Xmx704m");
        command.add("-XX:MaxDirectMemorySize=256m");
        command.add("-XX:MaxMetaspaceSize=256m");
        command.add("-XX:ReservedCodeCacheSize=128m");
        command.add("-Xss1m");
        command.add("-XX:ActiveProcessorCount=4");
        command.add("-Drowweave.process.memoryLimitBytes=4294967296");
        command.add("-Drowweave.control.residentBytes=234881024");
        command.add("-Drowweave.deployment.memoryBytes=16777216");
        command.add("-Drowweave.shuffle.ingressResidentBytes=100663296");
        command.add("-Djava.io.tmpdir=" + temporary);
        command.add("-Dorg.slf4j.simpleLogger.showDateTime=true");
        command.add("-Dorg.slf4j.simpleLogger.dateTimeFormat=yyyy-MM-dd HH:mm:ss.SSS");
        command.addAll(roleProperties);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ClusterMain.class.getName());
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(log.toFile()))
                .start();
        RoleProcess started = new RoleProcess(role, process, log, temporary);
        started.readyValue = awaitReady(
                process, log, readyPrefix, 45_000L);
        return started;
    }

    private static String awaitReady(Process process, Path log,
                                     String prefix, long timeoutMillis)
            throws Exception {
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        while (System.nanoTime() < deadline && process.isAlive()) {
            if (Files.exists(log)) {
                for (String line : Files.readAllLines(
                        log, StandardCharsets.UTF_8)) {
                    if (line.startsWith(prefix)) {
                        String value = line.substring(prefix.length()).trim();
                        int space = value.indexOf(' ');
                        return space < 0 ? value : value.substring(0, space);
                    }
                }
            }
            Thread.sleep(50L);
        }
        String output = Files.exists(log)
                ? new String(Files.readAllBytes(log), StandardCharsets.UTF_8)
                : "";
        if (process.isAlive()) process.destroyForcibly();
        throw new IllegalStateException(
                "MANAGED_MEMORY_ROLE_NOT_READY: " + prefix + '\n' + output);
    }

    @Override public void close() throws Exception {
        Throwable primary = null;
        if (client != null) {
            try {
                client.close();
            } catch (Throwable failure) {
                primary = failure;
            }
            client = null;
        }
        for (int i = roles.size() - 1; i >= 0; i--) {
            RoleProcess role = roles.get(i);
            try {
                stop(role);
            } catch (Throwable failure) {
                if (primary == null) primary = failure;
                else primary.addSuppressed(failure);
            }
        }
        if (primary != null) {
            if (primary instanceof Exception) throw (Exception) primary;
            if (primary instanceof Error) throw (Error) primary;
            throw new RuntimeException(primary);
        }
    }

    private static void stop(RoleProcess role) throws Exception {
        if (!role.process.isAlive()) return;
        role.process.destroy();
        if (!role.process.waitFor(15L, TimeUnit.SECONDS)) {
            role.process.destroyForcibly();
            if (!role.process.waitFor(5L, TimeUnit.SECONDS)) {
                throw new IllegalStateException(
                        "role could not be stopped: " + role.role);
            }
        }
    }

    static final class RoleProcess {
        final String role;
        final Process process;
        final Path log;
        final Path temporary;
        String readyValue;

        RoleProcess(String role, Process process, Path log, Path temporary) {
            this.role = role;
            this.process = process;
            this.log = log;
            this.temporary = temporary;
        }
    }
}
