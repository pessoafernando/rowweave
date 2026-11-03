package io.rowweave.bench;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.RealClusterJobClient;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.local.LocalJobClient;
import io.rowweave.mem.GlobalMemoryPool;
import io.rowweave.plan.ExecutionReport;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * paired ordinary Object source versus bounded row-reject publication profile.
 * Timings describe one machine and are deliberately not asserted as an SLA.
 */
@Tag("profile")
class ObjectStorageRowRejectProfileTest {

    private static final String OPERATOR_MAX_MEMORY =
            "rowweave.worker.operator.maxMemory";
    private static final String OPERATOR_MIN_MEMORY =
            "rowweave.worker.operator.minMemory";
    private static final long DEFAULT_OPERATOR_BYTES = 512L * 1024L;
    private static final long POOL_BYTES = 128L * 1024L * 1024L;
    private static final int SHARDS = 4;
    private static final int MAX_ROW_BYTES =
            ObjectStorageSnapshotSource.MIN_ROW_BYTES;
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("amount", DataType.BigDecimalType),
            new Field("payload", DataType.StringType));
    private static final String PAYLOAD =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            + "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void allValidMixedErrorsAndOversizeRecoveryP1P4Profile() throws Exception {
        int rows = Integer.getInteger("rowweave.object.quality.profile.rows", 20_000);
        int oversizeRows = Integer.getInteger(
                "rowweave.object.quality.profile.oversizeRows", 4_000);
        int iterations = Integer.getInteger(
                "rowweave.object.quality.profile.iters", 2);
        long operatorBytes = Long.getLong(
                "rowweave.object.quality.profile.operatorBytes",
                DEFAULT_OPERATOR_BYTES);
        if (rows <= 0 || oversizeRows <= 0 || iterations <= 0
                || operatorBytes < DEFAULT_OPERATOR_BYTES) {
            throw new IllegalArgumentException(
                    "Object quality profile parameters are invalid");
        }

        String oldMax = System.getProperty(OPERATOR_MAX_MEMORY);
        String oldMin = System.getProperty(OPERATOR_MIN_MEMORY);
        System.setProperty(OPERATOR_MAX_MEMORY, Long.toString(operatorBytes));
        System.setProperty(OPERATOR_MIN_MEMORY, Long.toString(operatorBytes));
        GlobalMemoryPool.configureForTest(POOL_BYTES);

        Path work = Files.createTempDirectory("rowweave-object-quality-profile-");
        Process master = null;
        Process worker0 = null;
        Process worker1 = null;
        try {
            Shape allValid = createShape(work, "all-valid", rows, ShapeKind.ALL_VALID);
            Shape mixed = createShape(work, "mixed-errors", rows, ShapeKind.MIXED);
            Shape oversize = createShape(
                    work, "oversize-recovery", oversizeRows, ShapeKind.OVERSIZE);

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
                    "-Drowweave.worker.memory=" + POOL_BYTES,
                    "-Drowweave.memory.bytes=" + POOL_BYTES,
                    "-D" + OPERATOR_MAX_MEMORY + "=" + operatorBytes,
                    "-D" + OPERATOR_MIN_MEMORY + "=" + operatorBytes);
            worker0 = launch(workerArgs, "WORKER_READY=", new StringBuilder());
            worker1 = launch(workerArgs, "WORKER_READY=", new StringBuilder());
            RealClusterJobClient cluster =
                    new RealClusterJobClient("127.0.0.1", masterPort);

            System.out.println();
            System.out.println("=== Object row-reject publication profile ===");
            System.out.println("shape environment records rawBytes valid rejected "
                    + "ordinary quality delta ratio peak/acquired spills");
            boolean localSpill = false;
            localSpill |= profile(allValid, iterations,
                    new LocalJobClient(false), 1, "local-P1", work);
            localSpill |= profile(allValid, iterations,
                    new LocalJobClient(false), 4, "local-P4", work);
            profile(allValid, iterations, cluster, 1, "cluster-P1", work);
            profile(allValid, iterations, cluster, 4, "cluster-P4", work);
            localSpill |= profile(mixed, iterations,
                    new LocalJobClient(false), 1, "local-P1", work);
            localSpill |= profile(mixed, iterations,
                    new LocalJobClient(false), 4, "local-P4", work);
            profile(mixed, iterations, cluster, 1, "cluster-P1", work);
            profile(mixed, iterations, cluster, 4, "cluster-P4", work);
            localSpill |= profile(oversize, iterations,
                    new LocalJobClient(false), 1, "local-P1", work);
            localSpill |= profile(oversize, iterations,
                    new LocalJobClient(false), 4, "local-P4", work);
            profile(oversize, iterations, cluster, 1, "cluster-P1", work);
            profile(oversize, iterations, cluster, 4, "cluster-P4", work);
            System.out.println("pressure-triggered spill observed by ExecutionReport="
                    + localSpill + "; canonical external-sort run flushes are not "
                    + "included in that counter");
            System.out.println("ordinary is available only for all-valid input; mixed and "
                    + "oversize costs are absolute quality-path measurements. Best-of "
                    + "timings and cluster memory counters are not an SLA.");
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

    private static boolean profile(
            Shape shape, int iterations, JobClient client, int parallelism,
            String environment, Path work) {
        Sample ordinary = Sample.empty();
        Sample quality = Sample.empty();
        AtomicInteger sequence = new AtomicInteger();
        for (int iteration = 0; iteration < iterations; iteration++) {
            if (shape.kind == ShapeKind.ALL_VALID && (iteration & 1) == 0) {
                ordinary = ordinary.best(runOrdinary(
                        shape, client, parallelism,
                        domain(work, shape.label, environment, "ordinary", sequence)));
            }
            quality = quality.best(runQuality(
                    shape, client, parallelism,
                    domain(work, shape.label, environment, "quality", sequence)));
            if (shape.kind == ShapeKind.ALL_VALID && (iteration & 1) != 0) {
                ordinary = ordinary.best(runOrdinary(
                        shape, client, parallelism,
                        domain(work, shape.label, environment, "ordinary", sequence)));
            }
        }
        String ordinaryMillis = ordinary.present ? ordinary.millis + "ms" : "n/a";
        String delta = ordinary.present
                ? String.format("%+dms", quality.millis - ordinary.millis) : "n/a";
        String ratio = ordinary.present
                ? String.format("%.2fx", (double) quality.millis
                        / Math.max(1L, ordinary.millis)) : "n/a";
        System.out.printf("%-18s %-10s %7d %9d %7d %8d %8s %7dms %8s "
                        + "%6s %d/%d %d%n",
                shape.label, environment, shape.records, shape.rawBytes,
                shape.valid, shape.rejected, ordinaryMillis, quality.millis,
                delta, ratio, quality.peak, quality.acquired, quality.spills);
        return client instanceof LocalJobClient && quality.spills > 0L;
    }

    private static Sample runOrdinary(
            Shape shape, JobClient client, int parallelism, Path domain) {
        ExecutionEnvironment session = new ExecutionEnvironment(
                "object-quality-profile-ordinary", "object quality profile ordinary");
        session.setParallelism(parallelism);
        session.fromSource(shape.inputs)
                .sinkTo(new ManifestFileSink(META, domain.toString(), "valid"));
        long started = System.nanoTime();
        ExecutionReport report = client.submit(session.toPlanGraph());
        long millis = elapsedMillis(started);
        assertTrue(report.withinBudget());
        assertEquals(shape.records, report.rowsOut());
        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertNotNull(manifest);
        assertEquals(1, manifest.members().size());
        assertEquals(shape.records, count(new ManifestFileSource(
                META, domain.toString(), "valid", manifest.generation())));
        return new Sample(millis, report);
    }

    private static Sample runQuality(
            Shape shape, JobClient client, int parallelism, Path domain) {
        ExecutionEnvironment session = new ExecutionEnvironment(
                "object-quality-profile", "object quality profile");
        session.setParallelism(parallelism);
        session.classifyObjectStorageRows("profile.object.source", shape.inputs)
                .writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary"));
        long started = System.nanoTime();
        ExecutionReport report = client.submit(session.toPlanGraph());
        long millis = elapsedMillis(started);
        assertTrue(report.withinBudget());
        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertNotNull(manifest);
        assertEquals(3, manifest.members().size());
        assertEquals(shape.valid, count(new ManifestFileSource(
                META, domain.toString(), "valid", manifest.generation())));
        assertEquals(shape.rejected, count(new ManifestFileSource(
                REJECTED_META, domain.toString(), "rejected",
                manifest.generation())));
        assertEquals(shape.records + 2L + shape.reasonBuckets, report.rowsOut());
        assertSummary(shape, domain, manifest.generation());
        return new Sample(millis, report);
    }

    private static void assertSummary(Shape shape, Path domain, long generation) {
        long accepted = -1L;
        long rejected = -1L;
        long reasonTotal = 0L;
        int reasons = 0;
        try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                TypedQualityManifest.summarySchema(), domain.toString(),
                "summary", generation).openReader()) {
            while (reader.hasNext()) {
                DataRow row = reader.next();
                String kind = row.getString(0);
                if ("ACCEPTED_TOTAL".equals(kind)) accepted = row.getLong(5);
                else if ("REJECTED_TOTAL".equals(kind)) rejected = row.getLong(5);
                else if ("REJECTED_REASON".equals(kind)) {
                    reasons++;
                    reasonTotal = Math.addExact(reasonTotal, row.getLong(5));
                } else fail("unexpected summary kind: " + kind);
            }
        }
        assertEquals(shape.valid, accepted);
        assertEquals(shape.rejected, rejected);
        assertEquals(shape.rejected, reasonTotal);
        assertEquals(shape.reasonBuckets, reasons);
    }

    private static long count(ManifestFileSource input) {
        long rows = 0L;
        try (io.rowweave.FileSource.Reader reader = input.openReader()) {
            while (reader.hasNext()) {
                reader.next();
                rows++;
            }
        }
        return rows;
    }

    private static Shape createShape(
            Path work, String label, int records, ShapeKind kind) throws Exception {
        ByteArrayOutputStream[] shards = new ByteArrayOutputStream[SHARDS];
        for (int i = 0; i < SHARDS; i++) shards[i] = new ByteArrayOutputStream();
        long valid = 0L;
        long rejected = 0L;
        int reasonBuckets = 0;
        for (int record = 0; record < records; record++) {
            ByteArrayOutputStream target = shards[record % SHARDS];
            if (kind == ShapeKind.MIXED && record % 20 < 3) {
                int error = record % 20;
                if (error == 0) target.write(bytes("bad\t1.00\t" + PAYLOAD + "\n"));
                else if (error == 1) target.write(new byte[] {
                        (byte) 0xff, '\t', '1', '.', '0', '0', '\n'});
                else target.write(bytes(record + "\t1.00\t" + PAYLOAD + "\r\n"));
                rejected++;
                reasonBuckets = 3;
            } else if (kind == ShapeKind.OVERSIZE && record % 20 == 0) {
                byte[] oversized = new byte[MAX_ROW_BYTES + 1];
                Arrays.fill(oversized, (byte) 'x');
                target.write(oversized);
                target.write('\n');
                rejected++;
                reasonBuckets = 1;
            } else {
                target.write(bytes(record + "\t" + (record % 997) + ".00\t"
                        + PAYLOAD + "\n"));
                valid++;
            }
        }
        ObjectStorageSnapshotSource[] inputs =
                new ObjectStorageSnapshotSource[SHARDS];
        long rawBytes = 0L;
        Path source = work.resolve(label + "-source");
        Files.createDirectories(source);
        for (int shard = 0; shard < SHARDS; shard++) {
            byte[] content = shards[shard].toByteArray();
            Path file = source.resolve("part-" + shard + ".rows");
            Files.write(file, content);
            rawBytes += content.length;
            inputs[shard] = objectInput(file, content);
        }
        return new Shape(label, kind, inputs, records, rawBytes,
                valid, rejected, reasonBuckets);
    }

    private static ObjectStorageSnapshotSource objectInput(Path file, byte[] content) {
        return new ObjectStorageSnapshotSource(
                META, new ProfileObjectProvider(), "profile-local", "none",
                "object-quality-profile", file.toAbsolutePath().toString(),
                "fixture-v1", content.length, sha256(content), MAX_ROW_BYTES);
    }

    private static Path domain(
            Path work, String shape, String environment, String mode,
            AtomicInteger sequence) {
        return work.resolve(shape + '-' + environment + '-' + mode + '-'
                + sequence.incrementAndGet());
    }

    private static long elapsedMillis(long started) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte item : digest) {
                result.append(Character.forDigit((item >>> 4) & 0xf, 16));
                result.append(Character.forDigit(item & 0xf, 16));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
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
                            readyOut.append(
                                    line.substring(readyPrefix.length()).trim());
                            ready[0] = true;
                            readyOut.notifyAll();
                        }
                    }
                }
            } catch (Exception ignored) {
                // Process teardown closes the stream.
            }
        }, "object-quality-profile-process-drain");
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
            fail("process did not become ready: " + readyPrefix + '\n' + processLog);
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

    private static void restore(String key, String value) {
        if (value == null) System.clearProperty(key);
        else System.setProperty(key, value);
    }

    private static void deleteRecursively(File file) {
        if (file == null) return;
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) deleteRecursively(child);
        }
        file.delete();
    }

    public static final class ProfileObjectProvider
            implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;

        public ProfileObjectProvider() { }

        @Override public String providerType() { return "object-quality-profile"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            try {
                return new ProfileObjectHandle(
                        request, Files.newInputStream(new File(
                                request.objectKey()).toPath()));
            } catch (IOException failure) {
                throw new IllegalStateException("fixture open failed", failure);
            }
        }
    }

    private static final class ProfileObjectHandle
            implements ObjectStorageReadHandle {
        private final ObjectStorageReadRequest request;
        private final InputStream stream;

        private ProfileObjectHandle(
                ObjectStorageReadRequest request, InputStream stream) {
            this.request = request;
            this.stream = stream;
        }

        @Override public String providerType() { return "object-quality-profile"; }
        @Override public int providerVersion() { return 1; }
        @Override public String versionId() { return request.versionId(); }
        @Override public long contentLength() { return request.contentLength(); }
        @Override public InputStream stream() { return stream; }
        @Override public void close() {
            try {
                stream.close();
            } catch (IOException failure) {
                throw new IllegalStateException("fixture close failed", failure);
            }
        }
    }

    private enum ShapeKind { ALL_VALID, MIXED, OVERSIZE }

    private static final class Shape {
        final String label;
        final ShapeKind kind;
        final ObjectStorageSnapshotSource[] inputs;
        final int records;
        final long rawBytes;
        final long valid;
        final long rejected;
        final int reasonBuckets;

        private Shape(
                String label, ShapeKind kind,
                ObjectStorageSnapshotSource[] inputs, int records, long rawBytes,
                long valid, long rejected, int reasonBuckets) {
            this.label = label;
            this.kind = kind;
            this.inputs = inputs;
            this.records = records;
            this.rawBytes = rawBytes;
            this.valid = valid;
            this.rejected = rejected;
            this.reasonBuckets = reasonBuckets;
        }
    }

    private static final class Sample {
        final boolean present;
        final long millis;
        final long peak;
        final long acquired;
        final long spills;

        private Sample(long millis, ExecutionReport report) {
            this(true, millis, report.peakReservedBytes(),
                    report.acquiredBytes(), report.spillCount());
        }

        private Sample(
                boolean present, long millis, long peak,
                long acquired, long spills) {
            this.present = present;
            this.millis = millis;
            this.peak = peak;
            this.acquired = acquired;
            this.spills = spills;
        }

        static Sample empty() {
            return new Sample(false, Long.MAX_VALUE, 0L, 0L, 0L);
        }

        Sample best(Sample candidate) {
            return !present || candidate.millis < millis ? candidate : this;
        }
    }
}
