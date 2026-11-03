package io.rowweave.bench;

import io.rowweave.CollectionSource;
import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.RealClusterJobClient;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.local.LocalJobClient;
import io.rowweave.ops.RowValidation;
import io.rowweave.plan.ExecutionReport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/** repeatable narrow/wide classification cost profile, never a CI latency SLA. */
@Tag("profile")
class TypedRowClassificationProfileTest {

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void narrowAndWideLocalAndIndependentClusterP1P4Profile() throws Exception {
        int narrowRows = Integer.getInteger("rowweave.classification.profile.narrowRows", 50_000);
        int wideRows = Integer.getInteger("rowweave.classification.profile.wideRows", 10_000);
        int iterations = Integer.getInteger("rowweave.classification.profile.iters", 2);
        if (narrowRows <= 0 || wideRows <= 0 || iterations <= 0) {
            throw new IllegalArgumentException("profile row counts and iterations must be > 0");
        }

        Path work = Files.createTempDirectory("rowweave-classification-profile-");
        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            StringBuilder ready = new StringBuilder();
            master = launch(Arrays.asList(
                    "-Drowweave.master.enable=true", "-Drowweave.master.port=0"),
                    "MASTER_PORT=", ready);
            int masterPort = Integer.parseInt(ready.toString());
            List<String> workerArgs = Arrays.asList(
                    "-Xmx2g",
                    "-Drowweave.worker.enable=true",
                    "-Drowweave.worker.master.port=" + masterPort,
                    "-Drowweave.worker.slots=32",
                    "-Drowweave.worker.memory=268435456",
                    "-Drowweave.memory.bytes=268435456");
            worker0 = launch(workerArgs, "WORKER_READY=", new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", new StringBuilder());
            RealClusterJobClient cluster =
                    new RealClusterJobClient("127.0.0.1", masterPort);

            Profile narrow = profile(
                    shape(3, narrowRows, "narrow", work), iterations, cluster);
            Profile wide = profile(
                    shape(16, wideRows, "wide", work), iterations, cluster);

            System.out.println();
            System.out.println("=== typed-row classification profile ===");
            System.out.println("shape   rows   local-P1 local-P4 cluster-P1 cluster-P4");
            print("narrow", narrow);
            print("wide", wide);
            System.out.println("Each run asserts validRows + rejectedRows == input rows; "
                    + "times are best-of and are not an SLA.");
        } finally {
            kill(worker1);
            kill(worker0);
            kill(master);
            deleteRecursively(work.toFile());
        }
    }

    private static Profile profile(Shape shape, int iterations, JobClient cluster) {
        long localP1 = bestOf(shape, iterations, new LocalJobClient(false), 1);
        long localP4 = bestOf(shape, iterations, new LocalJobClient(false), 4);
        long clusterP1 = bestOf(shape, iterations, cluster, 1);
        long clusterP4 = bestOf(shape, iterations, cluster, 4);
        return new Profile(shape.rows, localP1, localP4, clusterP1, clusterP4);
    }

    private static long bestOf(
            Shape shape, int iterations, JobClient client, int parallelism) {
        long best = Long.MAX_VALUE;
        for (int i = 0; i < iterations; i++) {
            ExecutionEnvironment session = new ExecutionEnvironment(
                    "classification-profile", "classification profile");
            session.setParallelism(parallelism);
            DataSet source = session.fromSource(shape.inputs).uid("profile.source");
            DataRowClassification classified =
                    source.classifyRowsWithSourcePosition(shape.validation);
            classified.validRows().sinkTo(new DiscardSink(shape.meta));
            classified.rejectedRows().sinkTo(
                    new DiscardSink(classified.rejectedRows().getSchema()));
            long started = System.nanoTime();
            ExecutionReport report = client.submit(session.toPlanGraph());
            long elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertEquals(shape.rows, report.rowsOut(),
                    "classification must remain exhaustive under the profiled topology: client="
                            + client.getClass().getSimpleName() + " P=" + parallelism);
            best = Math.min(best, elapsed);
        }
        return best;
    }

    private static Shape shape(int width, int rows, String label, Path work) {
        Field[] fields = new Field[width];
        fields[0] = new Field("adaptor", DataType.StringType);
        fields[1] = new Field("period", DataType.IntegerType);
        for (int i = 2; i < width; i++) {
            fields[i] = new Field("payload_" + i, DataType.StringType);
        }
        DataSchema meta = new DataSchema(fields);
        RowValidation validation = RowValidation.builder(meta)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .build();
        int shards = 4;
        @SuppressWarnings("unchecked")
        List<DataRow>[] splitRows = new List[shards];
        for (int i = 0; i < shards; i++) splitRows[i] = new ArrayList<>();
        String padding = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";
        for (int i = 0; i < rows; i++) {
            DataRow row = new DataRow(width);
            row.set(0, i % 10 == 0 ? null : "adaptor-" + (i % 997));
            row.set(1, i % 20 == 0 ? 13 : (i % 12) + 1);
            for (int field = 2; field < width; field++) {
                row.set(field, padding + '-' + field + '-' + (i % 31));
            }
            splitRows[i % shards].add(row);
        }
        Path domain = work.resolve(label + "-source-domain");
        ExecutionEnvironment prepare = new ExecutionEnvironment(
                "classification-profile-source", "prepare exact manifest source");
        for (int i = 0; i < shards; i++) {
            prepare.fromSource(new CollectionSource(meta, splitRows[i]))
                    .sinkTo(new ManifestFileSink(
                            meta, domain.toString(), "split-" + i));
        }
        new LocalJobClient(false).submit(prepare.toPlanGraph());
        CommitManifest manifest = new ManifestFileStore(domain).latest();
        if (manifest == null) {
            throw new IllegalStateException("profile source manifest was not published");
        }
        Source[] inputs = new Source[shards];
        for (int i = 0; i < shards; i++) {
            inputs[i] = new ManifestFileSource(
                    meta, domain.toString(), "split-" + i, manifest.generation());
        }
        return new Shape(meta, validation, inputs, rows);
    }

    private static Process launch(
            List<String> jvmArgs, String readyPrefix, StringBuilder readyOut)
            throws Exception {
        String javaBin = System.getProperty("java.home")
                + File.separator + "bin" + File.separator + "java";
        List<String> command = new ArrayList<>();
        command.add(javaBin);
        command.add("-Xms128m");
        command.add("-Xmx1536m");
        command.add("-XX:MaxDirectMemorySize=256m");
        command.add("-XX:MaxMetaspaceSize=256m");
        command.add("-XX:ReservedCodeCacheSize=128m");
        command.add("-Xss1m");
        command.add("-XX:ActiveProcessorCount=4");
        command.add("-Drowweave.process.memoryLimitBytes=8589934592");
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
        io.rowweave.test.ForkedJvm.appendJvmArguments(command, jvmArgs);
        command.add("io.rowweave.cluster.ClusterMain");
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        BufferedReader reader = new BufferedReader(new InputStreamReader(
                process.getInputStream(), StandardCharsets.UTF_8));
        boolean[] ready = {false};
        StringBuilder processLog = new StringBuilder();
        Thread drain = new Thread(() -> {
            try {
                String line;
                while ((line = reader.readLine()) != null) {
                    synchronized (processLog) {
                        if (processLog.length() < 16_384) {
                            processLog.append(line).append('\n');
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
                // Process teardown closes the stream.
            }
        }, "classification-profile-process-drain");
        drain.setDaemon(true);
        drain.start();
        long deadline = System.currentTimeMillis() + 30_000L;
        synchronized (readyOut) {
            while (!ready[0] && System.currentTimeMillis() < deadline) {
                readyOut.wait(Math.max(1L, deadline - System.currentTimeMillis()));
            }
        }
        if (!ready[0]) {
            process.destroyForcibly();
            fail("process did not become ready: " + readyPrefix + "\n" + processLog);
        }
        return process;
    }

    private static void kill(Process process) {
        if (process == null) return;
        process.destroyForcibly();
        try {
            process.waitFor(5, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    private static void deleteRecursively(File file) {
        if (file == null) return;
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) deleteRecursively(child);
        }
        file.delete();
    }

    private static void print(String label, Profile profile) {
        System.out.printf("%-6s %6d %9d %9d %10d %10d ms%n",
                label, profile.rows, profile.localP1, profile.localP4,
                profile.clusterP1, profile.clusterP4);
    }

    private static final class Shape {
        private final DataSchema meta;
        private final RowValidation validation;
        private final Source[] inputs;
        private final int rows;

        private Shape(
                DataSchema meta, RowValidation validation, Source[] inputs, int rows) {
            this.meta = meta;
            this.validation = validation;
            this.inputs = inputs;
            this.rows = rows;
        }
    }

    private static final class Profile {
        private final int rows;
        private final long localP1;
        private final long localP4;
        private final long clusterP1;
        private final long clusterP4;

        private Profile(
                int rows, long localP1, long localP4,
                long clusterP1, long clusterP4) {
            this.rows = rows;
            this.localP1 = localP1;
            this.localP4 = localP4;
            this.clusterP1 = clusterP1;
            this.clusterP4 = clusterP4;
        }
    }
}
