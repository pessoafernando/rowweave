package io.rowweave.cluster;

import io.rowweave.DataSet;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.BatchSession;
import io.rowweave.SourceExecutor;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** the public Source[] path executes exact object snapshots in both runtimes. */
class ObjectStorageSnapshotRuntimeTest {

    private static final DataSchema INPUT_META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema OUTPUT_META = ParallelGroupByAgg
            .of(INPUT_META, "adaptor").sum("amount").getOutputSchema();

    @AfterEach
    void resetProvider() {
        RuntimeProvider.reset();
    }

    @Test
    void localP1AndP4MatchIndependentOracle(@TempDir Path temp) {
        Map<String, BigDecimal> expected = oracle();
        assertEquals(expected, run(null, 1, temp.resolve("local-p1.tsv")));
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());

        RuntimeProvider.reset();
        assertEquals(expected, run(null, 2, temp.resolve("local-p2.tsv")));
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());

        RuntimeProvider.reset();
        assertEquals(expected, run(null, 4, temp.resolve("local-p4.tsv")));
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());
    }

    @Test
    @Tag("multiprocess")
    void inProcessP1AndP4MatchIndependentOracle(@TempDir Path temp) throws Exception {
        Map<String, BigDecimal> expected = oracle();
        try (InProcessCluster cluster = new InProcessCluster(
                1, temp.resolve("cluster-p1").toFile())) {
            assertEquals(expected, run(cluster, 1, temp.resolve("cluster-p1.tsv")));
        }
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());

        RuntimeProvider.reset();
        try (InProcessCluster cluster = new InProcessCluster(
                2, temp.resolve("cluster-p2").toFile())) {
            assertEquals(expected, run(cluster, 2, temp.resolve("cluster-p2.tsv")));
        }
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());

        RuntimeProvider.reset();
        try (InProcessCluster cluster = new InProcessCluster(
                4, temp.resolve("cluster-p4").toFile())) {
            assertEquals(expected, run(cluster, 4, temp.resolve("cluster-p4.tsv")));
        }
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());
    }

    @Test
    void freshAttemptReopensSameSnapshotAfterCredentialRotation() {
        RuntimeProvider.install("retry-part", "a\t1.2500\nb\t2.0000\n");
        ObjectStorageSnapshotSource frozen = input("retry-part");
        String snapshotId = frozen.batchSourceSplit().snapshotId();

        RuntimeProvider.credentialGeneration.set(1);
        SourceExecutor<?> interrupted = frozen.createSourceExecutor();
        interrupted.open();
        assertEquals("a", interrupted.next().get(0));
        interrupted.close();

        RuntimeProvider.credentialGeneration.set(2);
        SourceExecutor<?> replacementAttempt = frozen.createSourceExecutor();
        replacementAttempt.open();
        List<String> pickedValues = new ArrayList<>();
        while (replacementAttempt.hasNext()) {
            pickedValues.add((String) replacementAttempt.next().get(0));
        }

        assertEquals(Arrays.asList("a", "b"), pickedValues,
                "replacement attempt must restart the immutable object from byte zero");
        assertEquals(snapshotId, frozen.batchSourceSplit().snapshotId());
        assertEquals(Arrays.asList(1, 2), RuntimeProvider.openedCredentialGenerations);
        assertEquals(Arrays.asList("credential-prod", "credential-prod"),
                RuntimeProvider.openedCredentialRefs);
        assertEquals(Arrays.asList("generation-42", "generation-42"),
                RuntimeProvider.openedVersions);
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());
    }

    private static Map<String, BigDecimal> run(
            JobClient client, int parallelism, Path output) {
        RuntimeProvider.install("part-a", "a\t1.2500\nb\t2.0000\n");
        RuntimeProvider.install("part-b", "a\t3.7500\nb\t-0.5000\n");
        BatchSession session = client == null
                ? new BatchSession("object-source", "object-source")
                : new BatchSession("object-source", "object-source", client);
        session.setParallelism(parallelism);
        DataSet source = session.fromSource(
                input("part-a"), input("part-b"));
        source.groupBy(ParallelGroupByAgg.of(INPUT_META, "adaptor").sum("amount"))
                .sinkTo(new FileSink(OUTPUT_META, output.toString()));
        session.execute();

        Map<String, BigDecimal> result = new HashMap<>();
        try (io.rowweave.FileSource.Reader rows =
                     new io.rowweave.FileSource(OUTPUT_META, output.toString()).openReader()) {
            while (rows.hasNext()) {
                DataRow row = rows.next();
                result.put((String) row.get(0), (BigDecimal) row.get(1));
            }
        }
        return result;
    }

    private static ObjectStorageSnapshotSource input(String key) {
        byte[] bytes = RuntimeProvider.objects.get(key);
        return new ObjectStorageSnapshotSource(
                INPUT_META, new RuntimeProvider(), "endpoint-prod", "credential-prod",
                "domain", key, "generation-42", bytes.length, sha256(bytes),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES);
    }

    private static Map<String, BigDecimal> oracle() {
        Map<String, BigDecimal> expected = new HashMap<>();
        expected.put("a", new BigDecimal("5.0000000000"));
        expected.put("b", new BigDecimal("1.5000000000"));
        return expected;
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            char[] output = new char[digest.length * 2];
            char[] digits = "0123456789abcdef".toCharArray();
            for (int i = 0; i < digest.length; i++) {
                output[i * 2] = digits[(digest[i] >>> 4) & 0xf];
                output[i * 2 + 1] = digits[digest[i] & 0xf];
            }
            return new String(output);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    public static final class RuntimeProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        static final AtomicInteger opens = new AtomicInteger();
        static final AtomicInteger closes = new AtomicInteger();
        static final AtomicInteger credentialGeneration = new AtomicInteger();
        static final List<Integer> openedCredentialGenerations =
                new CopyOnWriteArrayList<>();
        static final List<String> openedCredentialRefs =
                new CopyOnWriteArrayList<>();
        static final List<String> openedVersions = new CopyOnWriteArrayList<>();

        public RuntimeProvider() { }

        static void install(String key, String content) {
            objects.put(key, content.getBytes(StandardCharsets.UTF_8));
        }

        static void reset() {
            objects.clear();
            opens.set(0);
            closes.set(0);
            credentialGeneration.set(0);
            openedCredentialGenerations.clear();
            openedCredentialRefs.clear();
            openedVersions.clear();
        }

        @Override public String providerType() { return "runtime-fixture"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            byte[] bytes = objects.get(request.objectKey());
            if (bytes == null) throw new IllegalStateException("missing test object");
            opens.incrementAndGet();
            openedCredentialGenerations.add(credentialGeneration.get());
            openedCredentialRefs.add(request.credentialRef());
            openedVersions.add(request.versionId());
            return new RuntimeHandle(request, bytes);
        }
    }

    private static final class RuntimeHandle implements ObjectStorageReadHandle {
        private final ObjectStorageReadRequest request;
        private final InputStream stream;
        private boolean closed;

        private RuntimeHandle(ObjectStorageReadRequest request, byte[] bytes) {
            this.request = request;
            this.stream = new ByteArrayInputStream(bytes);
        }

        @Override public String providerType() { return "runtime-fixture"; }
        @Override public int providerVersion() { return 1; }
        @Override public String versionId() { return request.versionId(); }
        @Override public long contentLength() { return request.contentLength(); }
        @Override public InputStream stream() { return stream; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            RuntimeProvider.closes.incrementAndGet();
            try {
                stream.close();
            } catch (IOException impossible) {
                throw new IllegalStateException(impossible);
            }
        }
    }
}
