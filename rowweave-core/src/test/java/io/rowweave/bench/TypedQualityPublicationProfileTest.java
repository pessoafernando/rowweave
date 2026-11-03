package io.rowweave.bench;

import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.ClusterJobClient;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.local.LocalJobClient;
import io.rowweave.mem.GlobalMemoryPool;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** paired two-member baseline versus typed-quality publication profile. */
@Tag("profile")
class TypedQualityPublicationProfileTest {

    private static final String OPERATOR_MAX_MEMORY =
            "rowweave.worker.operator.maxMemory";
    private static final String OPERATOR_MIN_MEMORY =
            "rowweave.worker.operator.minMemory";
    private static final long DEFAULT_OPERATOR_BYTES = 48L * 1024L;
    private static final long LOCAL_POOL_BYTES = 128L * 1024L * 1024L;

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void allValidAllRejectedAndHighReasonCardinalityP1P4Profile() throws Exception {
        int rows = Integer.getInteger("rowweave.quality.profile.rows", 30_000);
        int reasonBuckets = Integer.getInteger(
                "rowweave.quality.profile.reasonBuckets", 1_024);
        int iterations = Integer.getInteger("rowweave.quality.profile.iters", 2);
        long operatorBytes = Long.getLong(
                "rowweave.quality.profile.operatorBytes", DEFAULT_OPERATOR_BYTES);
        if (rows <= 0 || reasonBuckets <= 0 || reasonBuckets > 1_024
                || iterations <= 0 || operatorBytes <= 0L) {
            throw new IllegalArgumentException("quality profile parameters are invalid");
        }

        String oldMax = System.getProperty(OPERATOR_MAX_MEMORY);
        String oldMin = System.getProperty(OPERATOR_MIN_MEMORY);
        System.setProperty(OPERATOR_MAX_MEMORY, Long.toString(operatorBytes));
        System.setProperty(OPERATOR_MIN_MEMORY, Long.toString(operatorBytes));
        GlobalMemoryPool.configureForTest(LOCAL_POOL_BYTES);

        Path work = Files.createTempDirectory("rowweave-quality-publication-profile-");
        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            Shape empty = narrowShape("empty", 0, true, work);
            Shape allValid = narrowShape("all-valid", rows, true, work);
            Shape allRejected = narrowShape("all-rejected", rows, false, work);
            Shape highCardinality = highCardinalityShape(reasonBuckets, work);

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
                    "-Drowweave.worker.memory=" + LOCAL_POOL_BYTES,
                    "-Drowweave.memory.bytes=" + LOCAL_POOL_BYTES,
                    "-D" + OPERATOR_MAX_MEMORY + "=" + operatorBytes,
                    "-D" + OPERATOR_MIN_MEMORY + "=" + operatorBytes);
            worker0 = launch(workerArgs, "WORKER_READY=", new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", new StringBuilder());
            ClusterJobClient cluster =
                    new ClusterJobClient("127.0.0.1", masterPort);

            System.out.println();
            System.out.println("=== typed-quality publication profile ===");
            System.out.println("shape environment rows reasons baseline quality delta ratio spills");
            profile(empty, iterations, cluster, work);
            profile(allValid, iterations, cluster, work);
            profile(allRejected, iterations, cluster, work);
            profile(highCardinality, iterations, cluster, work);
            System.out.println("Baseline is the same classification published as two Manifest "
                    + "members; quality adds the canonical summary member. Best-of times are not an SLA.");
        } finally {
            kill(worker1);
            kill(worker0);
            kill(master);
            deleteRecursively(work.toFile());
            restore(OPERATOR_MAX_MEMORY, oldMax);
            restore(OPERATOR_MIN_MEMORY, oldMin);
            GlobalMemoryPool.resetForTest();
        }
    }

    private static void profile(
            Shape shape, int iterations, JobClient cluster, Path work) {
        runPair(shape, iterations, new LocalJobClient(false), 1, "local-P1", work);
        runPair(shape, iterations, new LocalJobClient(false), 4, "local-P4", work);
        runPair(shape, iterations, cluster, 1, "cluster-P1", work);
        runPair(shape, iterations, cluster, 4, "cluster-P4", work);
    }

    private static void runPair(
            Shape shape, int iterations, JobClient client, int parallelism,
            String environment, Path work) {
        AtomicInteger sequence = new AtomicInteger();
        Sample baseline = Sample.empty();
        Sample quality = Sample.empty();
        for (int iteration = 0; iteration < iterations; iteration++) {
            if ((iteration & 1) == 0) {
                baseline = baseline.best(run(shape, client, parallelism, false,
                        domain(work, shape.label, environment, "base", sequence)));
                quality = quality.best(run(shape, client, parallelism, true,
                        domain(work, shape.label, environment, "quality", sequence)));
            } else {
                quality = quality.best(run(shape, client, parallelism, true,
                        domain(work, shape.label, environment, "quality", sequence)));
                baseline = baseline.best(run(shape, client, parallelism, false,
                        domain(work, shape.label, environment, "base", sequence)));
            }
        }
        if (shape.requireLocalSpill && client instanceof LocalJobClient) {
            assertTrue(quality.spills > 0L,
                    "high-cardinality public Plan path must spill under the tiny budget");
        }
        long delta = quality.millis - baseline.millis;
        double ratio = baseline.millis == 0L
                ? Double.POSITIVE_INFINITY : (double) quality.millis / baseline.millis;
        System.out.printf("%-16s %-10s %6d %7d %8dms %7dms %+6dms %5.2fx %6d%n",
                shape.label, environment, shape.rows, shape.reasonBuckets,
                baseline.millis, quality.millis, delta, ratio, quality.spills);
    }

    private static Sample run(
            Shape shape, JobClient client, int parallelism, boolean quality,
            Path domain) {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-profile", "quality profile");
        session.setParallelism(parallelism);
        DataRowClassification classified = session.fromSource(shape.inputs)
                .uid("quality.profile.source")
                .classifyRowsWithSourcePosition(shape.validation);
        if (quality) {
            classified.writeToManifest(TypedQualityManifest.of(
                    domain.toString(), "valid", "rejected", "summary"));
        } else {
            classified.validRows().sinkTo(new ManifestFileSink(
                    shape.meta, domain.toString(), "valid"));
            classified.rejectedRows().sinkTo(new ManifestFileSink(
                    classified.rejectedRows().getSchema(),
                    domain.toString(), "rejected"));
        }

        long started = System.nanoTime();
        ExecutionReport report = client.submit(session.toPlanGraph());
        long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue(report.withinBudget(), "profile execution exceeded its acquired budget");
        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertEquals(quality ? 3 : 2, manifest.members().size());
        if (quality) {
            assertQualityOracle(shape, domain, manifest, report);
        } else {
            assertEquals(shape.rows, report.rowsOut());
        }
        return new Sample(millis, report.spillCount());
    }

    private static void assertQualityOracle(
            Shape shape, Path domain, CommitManifest manifest, ExecutionReport report) {
        long accepted = -1L;
        long rejected = -1L;
        long reasonTotal = 0L;
        int reasons = 0;
        try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                TypedQualityManifest.summarySchema(), domain.toString(), "summary",
                manifest.generation()).openReader()) {
            while (reader.hasNext()) {
                DataRow row = reader.next();
                if ("ACCEPTED_TOTAL".equals(row.getString(0))) {
                    accepted = row.getLong(5);
                } else if ("REJECTED_TOTAL".equals(row.getString(0))) {
                    rejected = row.getLong(5);
                } else if ("REJECTED_REASON".equals(row.getString(0))) {
                    reasons++;
                    reasonTotal = Math.addExact(reasonTotal, row.getLong(5));
                } else {
                    fail("unexpected quality metric kind: " + row.getString(0));
                }
            }
        }
        assertEquals(shape.validRows, accepted);
        assertEquals(shape.rejectedRows, rejected);
        assertEquals(shape.rejectedRows, reasonTotal);
        assertEquals(shape.reasonBuckets, reasons);
        assertEquals(shape.rows + 2L + shape.reasonBuckets, report.rowsOut());
    }

    private static Shape narrowShape(
            String label, int rows, boolean valid, Path work) {
        DataSchema meta = new DataSchema(
                new Field("adaptor", DataType.StringType),
                new Field("period", DataType.IntegerType),
                new Field("payload", DataType.StringType));
        RowValidation validation = RowValidation.builder(meta)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .build();
        List<DataRow> values = new ArrayList<>(rows);
        for (int i = 0; i < rows; i++) {
            values.add(row(valid ? "adaptor-" + (i % 997) : null,
                    (i % 12) + 1, "payload-" + (i % 31)));
        }
        Source[] inputs = prepareExactInputs(meta, values, label, work);
        return new Shape(label, meta, validation, inputs, rows,
                valid ? rows : 0L, valid ? 0L : rows, valid ? 0 : 1, false);
    }

    private static Shape highCardinalityShape(int reasonBuckets, Path work) {
        Field[] fields = new Field[reasonBuckets];
        for (int i = 0; i < reasonBuckets; i++) {
            fields[i] = new Field(String.format("field_%04d", i), DataType.StringType);
        }
        DataSchema meta = new DataSchema(fields);
        RowValidation.Builder validationBuilder = RowValidation.builder(meta);
        for (int field = 0; field < reasonBuckets; field++) {
            validationBuilder.notNull(String.format("field.%04d.required", field), field);
        }
        RowValidation validation = validationBuilder.build();
        List<DataRow> values = new ArrayList<>(reasonBuckets);
        for (int rejectedField = 0; rejectedField < reasonBuckets; rejectedField++) {
            DataRow value = new DataRow(reasonBuckets);
            for (int field = 0; field < reasonBuckets; field++) value.set(field, "value");
            value.set(rejectedField, null);
            values.add(value);
        }
        Source[] inputs = prepareExactInputs(meta, values, "high-cardinality", work);
        return new Shape("high-cardinality", meta, validation, inputs,
                reasonBuckets, 0L, reasonBuckets, reasonBuckets, true);
    }

    private static Source[] prepareExactInputs(
            DataSchema meta, List<DataRow> rows, String label, Path work) {
        int shardCount = 4;
        @SuppressWarnings("unchecked")
        List<DataRow>[] shards = new List[shardCount];
        for (int shard = 0; shard < shardCount; shard++) shards[shard] = new ArrayList<>();
        for (int row = 0; row < rows.size(); row++) shards[row % shardCount].add(rows.get(row));

        Path domain = work.resolve(label + "-source");
        ExecutionEnvironment prepare = new ExecutionEnvironment("quality-profile-source", "quality profile source");
        for (int shard = 0; shard < shardCount; shard++) {
            prepare.fromSource(new CollectionSource(meta, shards[shard]))
                    .sinkTo(new ManifestFileSink(
                            meta, domain.toString(), "split-" + shard));
        }
        new LocalJobClient(false).submit(prepare.toPlanGraph());
        CommitManifest manifest = new ManifestFileStore(domain).latest();
        Source[] inputs = new Source[shardCount];
        for (int shard = 0; shard < shardCount; shard++) {
            inputs[shard] = new ManifestFileSource(meta, domain.toString(),
                    "split-" + shard, manifest.generation());
        }
        return inputs;
    }

    private static Path domain(
            Path work, String shape, String environment, String mode,
            AtomicInteger sequence) {
        return work.resolve(shape + '-' + environment + '-' + mode + '-'
                + sequence.incrementAndGet());
    }

    private static Process launch(
            List<String> jvmArgs, String readyPrefix, StringBuilder readyOut)
            throws Exception {
        List<String> command = io.rowweave.test.ForkedJvm.baseCommand();
        io.rowweave.test.ForkedJvm.appendJvmArguments(command, jvmArgs);
        command.add("-cp");
        command.add(System.getProperty("java.class.path"));
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
        }, "quality-publication-profile-process-drain");
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

    private static void restore(String property, String value) {
        if (value == null) {
            System.clearProperty(property);
        } else {
            System.setProperty(property, value);
        }
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static final class Shape {
        private final String label;
        private final DataSchema meta;
        private final RowValidation validation;
        private final Source[] inputs;
        private final int rows;
        private final long validRows;
        private final long rejectedRows;
        private final int reasonBuckets;
        private final boolean requireLocalSpill;

        private Shape(String label, DataSchema meta, RowValidation validation,
                      Source[] inputs, int rows, long validRows, long rejectedRows,
                      int reasonBuckets, boolean requireLocalSpill) {
            this.label = label;
            this.meta = meta;
            this.validation = validation;
            this.inputs = inputs;
            this.rows = rows;
            this.validRows = validRows;
            this.rejectedRows = rejectedRows;
            this.reasonBuckets = reasonBuckets;
            this.requireLocalSpill = requireLocalSpill;
        }
    }

    private static final class Sample {
        private final long millis;
        private final long spills;

        private Sample(long millis, long spills) {
            this.millis = millis;
            this.spills = spills;
        }

        private static Sample empty() {
            return new Sample(Long.MAX_VALUE, 0L);
        }

        private Sample best(Sample candidate) {
            return candidate.millis < millis ? candidate : this;
        }
    }
}
