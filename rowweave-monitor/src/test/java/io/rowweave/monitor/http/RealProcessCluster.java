package io.rowweave.monitor.http;

import io.rowweave.cluster.ClusterJobClient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Test harness for a real process cluster. ProcessBuilder starts one independent Master process
 * with the monitor HTTP server and N independent Worker processes, each in its own JVM, while this
 * JVM acts as the JobClient and submits over RPC. It replaces a same-JVM pseudo cluster built from
 * MasterServer and WorkerServer instances, because here distributed execution really crosses JVMs
 * and the shuffle really travels over sockets.
 *
 * <p>The key capability is {@link #stageWorkerDistribution(String)}, which reads the workerId of
 * every instance of every stage in a JobView through the master monitor HTTP API. A client JVM
 * cannot read MasterServer.workers() inside a child process, so the monitor is the only way in.
 * That is what lets a test assert both workers took part, instead of repeating the mistake of
 * RealProcessClusterTest, where no parallelism was set and all the work landed on one worker.
 *
 * <p>Readiness protocol: the master prints MASTER_PORT= and MONITOR_PORT=, a worker prints
 * WORKER_READY=. For crash diagnosis the tail of each child stdout and stderr is captured
 * (redirectErrorStream), so a process that dies before announcing readiness reports the cause
 * rather than only a 30 second timeout.
 */
public final class RealProcessCluster implements AutoCloseable {

    private static final long READY_TIMEOUT_MS = 30_000;

    private final Process master;
    private final Path childLogDir;
    private final int masterPort;
    private final int monitorPort;
    private final List<Process> workers = new ArrayList<>();
    private final List<Proc> allProcs = new ArrayList<>();

    /** Start one master with the monitor plus workerCount workers, each with slots slots; the harness heap of 2GiB is used unless stated otherwise. */
    public RealProcessCluster(int workerCount, int slots) throws Exception {
        this(workerCount, slots, 0, 0);
    }

    /**
     * State the child heap in megabytes, where 0 keeps the harness default of 2GiB. A benchmark over
     * a lot of data wants a small worker heap, say 1024, so that managed memory really spills instead
     * of a large heap hiding that behaviour. The master computes no data, so a small heap is enough.
     */
    public RealProcessCluster(int workerCount, int slots, int workerHeapMb, int masterHeapMb) throws Exception {
        String configuredLogDir = System.getProperty("rowweave.benchmark.childLogDir");
        this.childLogDir = configuredLogDir == null ? null
                : Paths.get(configuredLogDir).toAbsolutePath().normalize();
        if (childLogDir != null) Files.createDirectories(childLogDir);
        java.util.List<String> masterArgs = new ArrayList<>();
        if (masterHeapMb > 0) masterArgs.add("-Xmx" + masterHeapMb + "m");
        if (masterHeapMb > 0) {
            if (masterHeapMb < 488) {
                throw new IllegalArgumentException(
                        "masterHeapMb must be >= 488 for the declared process budgets");
            }
            masterArgs.add("-Drowweave.control.residentBytes=234881024");
            masterArgs.add("-Drowweave.deployment.memoryBytes=8388608");
        }
        masterArgs.add("-Drowweave.master.enable=true");
        masterArgs.add("-Drowweave.master.port=0");
        masterArgs.add("-Drowweave.monitor.enable=true");
        masterArgs.add("-Drowweave.monitor.port=0");
        Proc m = launch(masterArgs, "MASTER_PORT=", "master");
        this.master = m.process;
        this.masterPort = Integer.parseInt(m.readyValue("MASTER_PORT="));
        this.monitorPort = Integer.parseInt(m.awaitLine("MONITOR_PORT=", READY_TIMEOUT_MS));
        for (int i = 0; i < workerCount; i++) {
            java.util.List<String> workerArgs = new ArrayList<>();
            if (workerHeapMb > 0) workerArgs.add("-Xmx" + workerHeapMb + "m");
            if (workerHeapMb > 0) {
                int managedMb = Math.min(workerHeapMb / 2, workerHeapMb - 744);
                if (managedMb < 16) {
                    throw new IllegalArgumentException(
                            "workerHeapMb must be >= 760 for the declared process budgets");
                }
                workerArgs.add("-Drowweave.control.residentBytes=234881024");
                workerArgs.add("-Drowweave.deployment.memoryBytes=8388608");
                workerArgs.add("-Drowweave.worker.memory="
                        + managedMb * 1024L * 1024L);
            }
            workerArgs.add("-Drowweave.worker.enable=true");
            workerArgs.add("-Drowweave.worker.master.port=" + masterPort);
            workerArgs.add("-Drowweave.worker.slots=" + slots);
            Proc w = launch(workerArgs, "WORKER_READY=", "worker-" + i);
            workers.add(w.process);
        }
    }

    public int masterPort() { return masterPort; }
    public int monitorPort() { return monitorPort; }

    /** A new JobClient bound to this cluster master, so each submit uses a fresh client as a real user would. */
    public ClusterJobClient newClient() {
        return new ClusterJobClient("127.0.0.1", masterPort);
    }

    /**
     * Read the distinct workerId of every instance of every stage of one job through the monitor HTTP
     * API, which is how a test asserts that work really spread across workers. An empty set means the
     * monitor never recorded the job, usually because it did not finish.
     */
    public Set<String> stageWorkerDistribution(String jobId) throws Exception {
        String json = httpGet("/api/jobs/" + jobId);
        Set<String> workers = new LinkedHashSet<>();
        // Pull every worker and workerId field out of the job detail JSON, at both stage and instance level.
        Matcher mm = Pattern.compile("\"worker(?:Id)?\":\"([^\"]+)\"").matcher(json);
        while (mm.find()) {
            workers.add(mm.group(1));
        }
        return workers;
    }

    /** List every jobId the monitor recorded, in the order they appeared. */
    public List<String> jobIds() throws Exception {
        String json = httpGet("/api/jobs");
        List<String> ids = new ArrayList<>();
        Matcher mm = Pattern.compile("\"jobId\":\"([^\"]+)\"").matcher(json);
        while (mm.find()) {
            ids.add(mm.group(1));
        }
        return ids;
    }

    private String httpGet(String path) throws Exception {
        URL url = new URL("http://127.0.0.1:" + monitorPort + path);
        HttpURLConnection c = (HttpURLConnection) url.openConnection();
        c.setConnectTimeout(5000);
        c.setReadTimeout(5000);
        try (BufferedReader r = new BufferedReader(new InputStreamReader(c.getInputStream(), StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = r.readLine()) != null) sb.append(line);
            return sb.toString();
        } finally {
            c.disconnect();
        }
    }

    // ---- fork the processes and parse readiness, keeping the output tail for crash diagnosis ----

    private Proc launch(List<String> jvmArgs, String firstReadyPrefix, String role) throws Exception {
        String cp = System.getProperty("java.class.path");
        List<String> cmd = io.rowweave.test.ForkedJvm.baseCommand();
        if (childLogDir != null) {
            jvmArgs.add("-Xlog:gc,safepoint:file=" + childLogDir.resolve(role + ".jvm.log")
                    + ":time,uptime,level,tags");
        }
        if (Boolean.getBoolean("rowweave.benchmark.traceHeartbeats")) {
            jvmArgs.add("-Drowweave.cluster.heartbeatTrace=true");
            jvmArgs.add("-Dlogback.cluster.level=INFO");
        }
        io.rowweave.test.ForkedJvm.appendJvmArguments(cmd, jvmArgs);
        cmd.add("-cp");
        cmd.add(cp);
        cmd.add("io.rowweave.cluster.ClusterMain");
        Process p = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        Proc proc = new Proc(p,
                childLogDir == null ? null : childLogDir.resolve(role + ".stdout.log"));
        allProcs.add(proc);
        proc.startReader();
        proc.awaitLine(firstReadyPrefix, READY_TIMEOUT_MS);  // Either readiness is announced or this throws, with the output tail attached.
        return proc;
    }

    @Override
    public void close() {
        for (Process w : workers) destroy(w);
        destroy(master);
    }

    private static void destroy(Process p) {
        if (p == null) return;
        p.destroyForcibly();
        try {
            p.waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    /** One child process and the reader for its stdout, which parses the ready line and keeps the tail for crash diagnosis. */
    private static final class Proc {
        final Process process;
        final Path stdoutLog;
        private final List<String> tail = new CopyOnWriteArrayList<>();  // Keeps the most recent lines.
        private volatile boolean exited;

        Proc(Process process, Path stdoutLog) {
            this.process = process;
            this.stdoutLog = stdoutLog;
        }

        void startReader() {
            Thread reader = new Thread(() -> {
                BufferedWriter log = null;
                try (BufferedReader r = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    if (stdoutLog != null) {
                        log = Files.newBufferedWriter(stdoutLog, StandardCharsets.UTF_8,
                                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
                    }
                    String line;
                    while ((line = r.readLine()) != null) {
                        if (log != null) {
                            try {
                                log.write(line);
                                log.newLine();
                                log.flush();
                            } catch (java.io.IOException writeFailure) {
                                try { log.close(); } catch (java.io.IOException ignored) { }
                                log = null; // Continue draining child stdout even if diagnostics fill the disk.
                            }
                        }
                        synchronized (tail) {
                            tail.add(line);
                            if (tail.size() > 50) tail.remove(0);
                            tail.notifyAll();
                        }
                    }
                } catch (Exception ignored) {
                } finally {
                    if (log != null) {
                        try { log.close(); } catch (java.io.IOException ignored) { }
                    }
                    exited = true;
                    synchronized (tail) { tail.notifyAll(); }
                }
            }, "realproc-reader");
            reader.setDaemon(true);
            reader.start();
        }

        /** Block until a line containing prefix arrives and return what follows it; on timeout or process exit, throw, with the output tail for diagnosis. */
        String awaitLine(String prefix, long timeoutMs) throws InterruptedException {
            long deadline = System.currentTimeMillis() + timeoutMs;
            synchronized (tail) {
                while (System.currentTimeMillis() < deadline) {
                    for (String l : tail) {
                        if (l.startsWith(prefix)) return l.substring(prefix.length()).trim();
                    }
                    if (exited) {
                        throw new IllegalStateException("child process exited before printing " + prefix + ". Output tail:\n"
                                + String.join("\n", tail));
                    }
                    tail.wait(Math.max(1, deadline - System.currentTimeMillis()));
                }
            }
            throw new IllegalStateException("child process did not become ready within " + timeoutMs + " ms (" + prefix + "). Output tail:\n"
                    + String.join("\n", tail));
        }

        String readyValue(String prefix) throws InterruptedException {
            return awaitLine(prefix, READY_TIMEOUT_MS);
        }
    }
}
