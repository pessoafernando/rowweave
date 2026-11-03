package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** one missing network EOS must converge in an independent M+2W cluster. */
@Tag("multiprocess")
class RealMissingEosConvergenceTest {
    private static final DataSchema META =
            new DataSchema(new Field("id", DataType.IntegerType));

    @Test
    @Timeout(value = 40, unit = TimeUnit.SECONDS)
    void missingEosTimesOutAllStagesAndLeavesTheClusterReusable(
            @TempDir Path temp) throws Exception {
        String timeoutKey = "rowweave.cluster.submitTimeoutMs";
        String cleanupKey = "rowweave.cluster.submitCleanupGraceMs";
        String previousTimeout = System.setProperty(timeoutKey, "1000");
        String previousCleanup = System.setProperty(cleanupKey, "3000");
        Child master = null;
        Child worker0 = null;
        Child worker1 = null;
        try {
            master = launch(Arrays.asList(
                    "-Drowweave.master.enable=true",
                    "-Drowweave.master.port=0",
                    "-Drowweave.shuffle.mux=false"), "MASTER_PORT=");
            int masterPort = Integer.parseInt(master.readyValue);
            String eosClaim = temp.resolve("dropped-eos.claim").toString();
            worker0 = launch(workerArgs(masterPort, eosClaim), "WORKER_READY=");
            worker1 = launch(workerArgs(masterPort, eosClaim), "WORKER_READY=");
            int worker0Port = workerPort(worker0.readyValue);
            int worker1Port = workerPort(worker1.readyValue);

            long started = System.nanoTime();
            RuntimeException failure;
            try (ClusterJobClient client =
                         new ClusterJobClient("127.0.0.1", masterPort)) {
                failure = assertThrows(RuntimeException.class,
                        () -> client.submitToSink(graph()));
            }
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(
                    System.nanoTime() - started);

            assertTrue(failure.getMessage().contains(
                            "CLUSTER_JOB_EXECUTION_TIMEOUT"),
                    failure.getMessage());
            assertTrue(elapsedMs >= 800L && elapsedMs < 5_500L,
                    "execution+cleanup deadline must bound the missing-EOS job: "
                            + elapsedMs + "ms");
            assertTrue(worker0.contains("ROWWEAVE_TEST_DROP_EOS")
                            ^ worker1.contains("ROWWEAVE_TEST_DROP_EOS"),
                    "exactly one Worker must prove the EOS fault fired: worker0="
                            + worker0.snapshot() + "\nworker1=" + worker1.snapshot());
            assertTrue(master.process.isAlive());
            assertTrue(worker0.process.isAlive());
            assertTrue(worker1.process.isAlive());

            System.setProperty(timeoutKey, "5000");
            System.setProperty(cleanupKey, "1000");
            try (ClusterJobClient client =
                         new ClusterJobClient("127.0.0.1", masterPort)) {
                assertEquals(32L, client.submitToSink(graph()),
                        "all canceled stage/slot ownership must be reusable");
            }

            worker0.stopGracefully();
            worker1.stopGracefully();
            master.stopGracefully();
            assertFalse(canConnect(worker0Port));
            assertFalse(canConnect(worker1Port));
            assertFalse(canConnect(masterPort));
        } finally {
            forceStop(worker0);
            forceStop(worker1);
            forceStop(master);
            restore(timeoutKey, previousTimeout);
            restore(cleanupKey, previousCleanup);
        }
    }

    private static List<String> workerArgs(int masterPort, String eosClaim) {
        List<String> args = new ArrayList<>(Arrays.asList(
                "-Drowweave.worker.enable=true",
                "-Drowweave.worker.master.port=" + masterPort,
                "-Drowweave.worker.slots=16",
                "-Drowweave.worker.host=127.0.0.1",
                "-Drowweave.shuffle.mux=false",
                "-Drowweave.test.shuffle.dropEosOnce=true",
                "-Drowweave.test.shuffle.dropEosClaim=" + eosClaim));
        return args;
    }

    private static PlanGraph graph() {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 128; i++) {
            DataRow row = new DataRow(1);
            row.set(0, i % 32);
            rows.add(row);
        }
        return new PlanGraph(Arrays.asList(
                PlanNode.source(META, rows),
                PlanNode.unary("distinct", META, 0)
                        .listParam("keys", Collections.singletonList("id")).build(),
                PlanNode.sink(META, 1, new DiscardSink(META)).build()), 2);
    }

    private static Child launch(List<String> jvmArgs, String readyPrefix) throws Exception {
        List<String> command = io.rowweave.test.ForkedJvm.baseCommand();
        io.rowweave.test.ForkedJvm.appendJvmArguments(command, jvmArgs);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        command.add(ClusterMain.class.getName());
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        Child child = new Child(process);
        child.startReader(readyPrefix);
        if (!child.awaitReady(15_000L)) {
            forceStop(child);
            throw new AssertionError("child did not become ready: " + child.snapshot());
        }
        return child;
    }

    private static int workerPort(String ready) {
        int marker = ready.lastIndexOf(" port=");
        if (marker < 0) throw new IllegalArgumentException("missing Worker port: " + ready);
        return Integer.parseInt(ready.substring(marker + " port=".length()));
    }

    private static boolean canConnect(int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 300);
            return true;
        } catch (IOException closed) {
            return false;
        }
    }

    private static void forceStop(Child child) {
        if (child == null || !child.process.isAlive()) return;
        child.process.destroyForcibly();
        try {
            child.process.waitFor(5L, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void restore(String key, String previous) {
        if (previous == null) System.clearProperty(key);
        else System.setProperty(key, previous);
    }

    private static final class Child {
        final Process process;
        final List<String> log = Collections.synchronizedList(new ArrayList<>());
        volatile String readyValue;
        volatile Thread readerThread;

        Child(Process process) {
            this.process = process;
        }

        void startReader(String readyPrefix) {
            Thread reader = new Thread(() -> {
                try (BufferedReader lines = new BufferedReader(new InputStreamReader(
                        process.getInputStream(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = lines.readLine()) != null) {
                        log.add(line);
                        if (readyValue == null && line.startsWith(readyPrefix)) {
                            synchronized (this) {
                                readyValue = line.substring(readyPrefix.length()).trim();
                                notifyAll();
                            }
                        }
                    }
                } catch (IOException ignoredAfterExit) {
                    // Process teardown closes stdout.
                }
            }, "missing-eos-child-log");
            reader.setDaemon(true);
            readerThread = reader;
            reader.start();
        }

        synchronized boolean awaitReady(long timeoutMs) throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
            while (readyValue == null && process.isAlive()) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) break;
                TimeUnit.NANOSECONDS.timedWait(this, remaining);
            }
            return readyValue != null;
        }

        void stopGracefully() throws InterruptedException {
            if (!process.isAlive()) return;
            process.destroy();
            assertTrue(process.waitFor(10L, TimeUnit.SECONDS), snapshot());
            Thread reader = readerThread;
            if (reader != null) reader.join(2_000L);
        }

        boolean contains(String text) {
            synchronized (log) {
                return log.stream().anyMatch(line -> line.contains(text));
            }
        }

        String snapshot() {
            synchronized (log) {
                return String.join("\n", log);
            }
        }
    }
}
