package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DataSet;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.FileSink;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataRow;
import io.rowweave.DataSchema;
import io.rowweave.RowWeave;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Tag("multiprocess")
class MuxFanoutProcessCertificationTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.IntegerType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();
    private static final int PARALLELISM = 8;
    private static final int MIN_LOGICAL_STREAMS_PER_CONNECTION = 4;
    private static final int MAX_PHYSICAL_CONNECTIONS = 4;

    private static List<DataRow> rows() {
        List<DataRow> out = new ArrayList<>();
        for (int i = 0; i < 20_000; i++) {
            DataRow row = new DataRow(2);
            row.set(0, i % 100);
            row.set(1, new java.math.BigDecimal(i % 100));
            out.add(row);
        }
        return out;
    }

    private static Process launch(List<String> jvmArgs, String readyPrefix, long timeoutMs,
                                  StringBuilder readyOut, List<String> processLog) throws Exception {
        List<String> cmd = io.rowweave.test.ForkedJvm.baseCommand();
        io.rowweave.test.ForkedJvm.appendJvmArguments(cmd, jvmArgs);
        cmd.add("-cp");
        cmd.add(System.getProperty("java.class.path"));
        String muxMode = System.getProperty(ShuffleTransportConfig.MUX_PROPERTY);
        if (muxMode != null) {
            cmd.add("-D" + ShuffleTransportConfig.MUX_PROPERTY + "=" + muxMode);
        }
        cmd.add("io.rowweave.cluster.ClusterMain");
        Process process = new ProcessBuilder(cmd).redirectErrorStream(true).start();
        BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8));
        long deadline = System.currentTimeMillis() + timeoutMs;
        final boolean[] ready = {false};
        Thread pump = new Thread(() -> {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    if (processLog != null) {
                        synchronized (processLog) {
                            processLog.add(line);
                        }
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
            }
        }, "mux-fanout-launch-reader");
        pump.setDaemon(true);
        pump.start();
        synchronized (readyOut) {
            while (!ready[0] && System.currentTimeMillis() < deadline) {
                readyOut.wait(Math.max(1L, deadline - System.currentTimeMillis()));
            }
        }
        if (!ready[0]) {
            process.destroyForcibly();
            fail("process not ready within " + timeoutMs + " ms (marker " + readyPrefix + ")");
        }
        return process;
    }

    private static void kill(Process process) {
        if (process != null) {
            process.destroyForcibly();
            try {
                process.waitFor(5, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static long[] fingerprint(Path path, boolean ordered) throws Exception {
        long count = 0;
        long digest = 1125899906842597L;
        long sum = 0L;
        try (BufferedReader r = java.nio.file.Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            String line;
            while ((line = r.readLine()) != null) {
                count++;
                long h = 1125899906842597L;
                for (int i = 0; i < line.length(); i++) {
                    h = 31 * h + line.charAt(i);
                }
                digest = 31L * digest + h;
                sum += h;
            }
        }
        return ordered ? new long[]{count, digest} : new long[]{count, sum};
    }

    private static long[][] runJob(String name, ExecutionEnvironment session, FileSource a, FileSource b,
                                   Path branchPlain, Path branchAgg) {
        session.setParallelism(PARALLELISM);
        DataSet mid = session.fromSource(a, b).filter("amt >= 10");
        DataSet plain = mid.filter("k >= 7");
        plain.sinkTo(new FileSink(plain.getSchema(), branchPlain.toString()));
        DataSet agg = mid.groupBy(ParallelGroupByAgg.of(META, "k").sum("amt")).orderBy("k");
        agg.sinkTo(new FileSink(GB, branchAgg.toString()));
        session.execute();
        try {
            return new long[][]{fingerprint(branchPlain, false), fingerprint(branchAgg, true)};
        } catch (Exception failure) {
            throw new RuntimeException(failure);
        }
    }

    @Test
    void workerPairConnectionCarriesAtLeastFourLogicalStreamsAcrossRealProcesses(
            @TempDir Path tmp) throws Exception {
        List<DataRow> data = rows();
        FileSource inputA = ClusterTestFileInput.write(tmp.resolve("src-a.tsv"), META,
                data.subList(0, data.size() / 2));
        FileSource inputB = ClusterTestFileInput.write(tmp.resolve("src-b.tsv"), META,
                data.subList(data.size() / 2, data.size()));

        long[][] oracle = runJob("mux-fanout-oracle", RowWeave.createEnvironment("mux-fanout-oracle"),
                inputA, inputB, tmp.resolve("oracle-plain.tsv"), tmp.resolve("oracle-agg.tsv"));
        assertTrue(oracle[0][0] > 0 && oracle[1][0] > 0, "both branches must produce output");

        StringBuilder masterReady = new StringBuilder();
        List<String> masterLog = new ArrayList<>();
        List<String> worker0Log = new ArrayList<>();
        List<String> worker1Log = new ArrayList<>();
        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            master = launch(Arrays.asList("-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", 30_000L, masterReady, masterLog);
            int masterPort = Integer.parseInt(masterReady.toString());
            assertTrue(masterPort > 0, "the master port is known");
            String logback = productionLogback();
            StringBuilder w0Ready = new StringBuilder();
            worker0 = launch(Arrays.asList("-Drowweave.worker.enable=true",
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=32",
                    "-Drowweave.worker.dir=" + tmp.resolve("w0"),
                    "-Dlogback.configurationFile=" + logback),
                    "WORKER_READY=", 30_000L, w0Ready, worker0Log);
            StringBuilder w1Ready = new StringBuilder();
            worker1 = launch(Arrays.asList("-Drowweave.worker.enable=true",
                    "-Drowweave.worker.host=127.0.0.1",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=32",
                    "-Drowweave.worker.dir=" + tmp.resolve("w1"),
                    "-Dlogback.configurationFile=" + logback),
                    "WORKER_READY=", 30_000L, w1Ready, worker1Log);

            long[][] cluster = runJob("mux-fanout-cluster",
                    new ExecutionEnvironment("mux-fanout-cluster", "mux-fanout-cluster",
                            new ClusterJobClient("127.0.0.1", masterPort)),
                    inputA, inputB, tmp.resolve("cluster-plain.tsv"),
                    tmp.resolve("cluster-agg.tsv"));

            assertEquals(oracle[0][0], cluster[0][0], "pass-through branch row count matches the single process run");
            assertEquals(oracle[0][1], cluster[0][1], "pass-through branch multiset fingerprint matches the single process run");
            assertEquals(oracle[1][0], cluster[1][0], "aggregated branch group count matches the single process run");
            assertEquals(oracle[1][1], cluster[1][1], "aggregated branch order-sensitive fingerprint matches the single process run");

            assertTrue(worker0.isAlive(), "worker0 is alive");
            assertTrue(worker1.isAlive(), "worker1 is alive");
            assertTrue(master.isAlive(), "master is alive");

            gracefulStop(worker0);
            gracefulStop(worker1);

            String logs = awaitLifetimeRecords(Arrays.asList(worker0Log, worker1Log, masterLog));
            List<String> open = linesContaining(logs, "ROWWEAVE_SHUFFLE_CONNECTION_OPEN");
            List<String> lifetime = linesContaining(
                    logs, "ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME");

            long maxLogicalStreams = 0L;
            long totalWireBytes = 0L;
            long totalLogicalStreams = 0L;
            for (String metric : lifetime) {
                long logicalStreams = metricValue(metric, "logicalStreams");
                maxLogicalStreams = Math.max(maxLogicalStreams, logicalStreams);
                totalLogicalStreams += logicalStreams;
                totalWireBytes += metricValue(metric, "wireBytes");
                assertTrue(metricValue(metric, "records") > 0L, metric);
            }
            assertTrue(maxLogicalStreams >= MIN_LOGICAL_STREAMS_PER_CONNECTION,
                    "one master with two workers on one host must measure at least " + MIN_LOGICAL_STREAMS_PER_CONNECTION
                            + " logical streams on a single connection; opens=" + open.size() + " observed: " + lifetime
                            + " | " + sample(logs));
            assertTrue(totalWireBytes > 0L, lifetime.toString());

            assertTrue(lifetime.size() <= MAX_PHYSICAL_CONNECTIONS,
                    "the physical connection count must stay decoupled from the target count (budget cap " + MAX_PHYSICAL_CONNECTIONS
                            + "), observed: " + lifetime);
            assertTrue(totalLogicalStreams > lifetime.size(),
                    "the logical stream total must strictly exceed the physical connections, observed streams=" + totalLogicalStreams
                            + " connections=" + lifetime.size() + " | " + lifetime);

            assertEquals(open.size(), lifetime.size(),
                    "every physical connection opened must leave a complete lifetime record (no half-closed or unrecorded connection): " + lifetime);

            assertFalse(logs.contains("reader idle"), sample(logs));
            assertFalse(logs.contains("mux physical connection closed"), sample(logs));

            System.out.printf("[MUX-FANOUT-CERTIFICATION] P=%d maxLogicalStreams=%d "
                            + "physicalConnections=%d opens=%d%n",
                    PARALLELISM, maxLogicalStreams, lifetime.size(), open.size());
        } finally {
            kill(worker0);
            kill(worker1);
            kill(master);
        }
    }

    private static void gracefulStop(Process process) {
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

    private static String sample(String logs) {
        StringBuilder sb = new StringBuilder();
        for (String line : logs.split("\n")) {
            if (line.contains("SHUFFLE") || line.contains("IdleState") || line.contains("mux")) {
                sb.append(line).append('\n');
            }
        }
        return sb.length() == 0 ? "(no shuffle log lines)" : sb.toString();
    }

    private static String productionLogback() throws Exception {
        return new java.io.File(ClusterMain.class.getProtectionDomain().getCodeSource()
                .getLocation().toURI()).toPath().resolve("logback.xml").toString();
    }

    private static String awaitLifetimeRecords(List<List<String>> workerLogs) throws Exception {
        long deadline = System.currentTimeMillis() + 15_000L;
        while (System.currentTimeMillis() < deadline) {
            String joined = join(workerLogs);
            if (joined.contains("ROWWEAVE_SHUFFLE_CONNECTION_LIFETIME")) {
                TimeUnit.MILLISECONDS.sleep(1_500L);
                return join(workerLogs);
            }
            TimeUnit.MILLISECONDS.sleep(200L);
        }
        return join(workerLogs);
    }

    private static String join(List<List<String>> workerLogs) {
        StringBuilder sb = new StringBuilder();
        for (List<String> log : workerLogs) {
            synchronized (log) {
                for (String line : log) {
                    sb.append(line).append('\n');
                }
            }
        }
        return sb.toString();
    }

    private static List<String> linesContaining(String text, String token) {
        List<String> hits = new ArrayList<>();
        for (String line : text.split("\n")) {
            if (line.contains(token)) {
                hits.add(line);
            }
        }
        return hits;
    }

    private static long metricValue(String metric, String name) {
        String marker = name + "=";
        int from = metric.indexOf(marker);
        assertTrue(from >= 0, metric + " is missing " + marker);
        int start = from + marker.length();
        int end = start;
        while (end < metric.length() && Character.isDigit(metric.charAt(end))) {
            end++;
        }
        assertTrue(end > start, metric);
        return Long.parseLong(metric.substring(start, end));
    }
}

