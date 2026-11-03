package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.ManifestFileSource;
import io.rowweave.ObjectStorageReadHandle;
import io.rowweave.ObjectStorageReadRequest;
import io.rowweave.ObjectStorageSnapshotSource;
import io.rowweave.ObjectStorageSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Object quality publication is one durable three-member unit. */
class ObjectStorageQualityDurabilityTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));
    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));

    @AfterEach
    void resetProvider() {
        DurableQualityProvider.reset();
    }

    @Test
    void malformedMiddleRecordPublishesTheExactThreeMemberOracle(
            @TempDir Path tmp) {
        Path domain = tmp.resolve("malformed-middle");
        byte[] content = bytes("1\t1.00\nbad\t2.00\n2\t2.00\n");
        DurableQualityProvider.objects.put("ledger.tsv", content);

        File w0 = tmp.resolve("oracle-w0").toFile();
        File w1 = tmp.resolve("oracle-w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        try (MasterServer master = new MasterServer(10_000L);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "oracle-w0", 16, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "oracle-w1", 16, "127.0.0.1", master.port(), w1)) {
            run(new ClusterJobClient("127.0.0.1", master.port()),
                    "object-quality-oracle", domain, input("ledger.tsv", content));

            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }

        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertEquals(3, manifest.members().size());
        assertEquals(Arrays.asList("1|1.00", "2|2.00"),
                validLines(read(META, domain, "valid", manifest.generation())));
        List<DataRow> rejected = read(
                REJECTED_META, domain, "rejected", manifest.generation());
        assertEquals(1, rejected.size());
        assertEquals(1L, rejected.get(0).getLong(2));
        assertEquals("QUALITY_RAW_FORMAT_INVALID", rejected.get(0).getString(3));
        assertEquals(Arrays.asList(
                "ACCEPTED_TOTAL|||-1||2",
                "REJECTED_REASON|QUALITY_RAW_FORMAT_INVALID|OBJECT_ROW_DECODE_V1|-1|ROWWEAVE_ROW_TEXT_V1|1",
                "REJECTED_TOTAL|||-1||1"), summaryLines(read(
                TypedQualityManifest.summarySchema(), domain, "summary",
                manifest.generation())));
        assertEquals(1, DurableQualityProvider.opens.get());
        assertEquals(1, DurableQualityProvider.closes.get());
    }

    @Test
    void terminalDigestFailureAbortsAllStagingAndKeepsPreviousGeneration(
            @TempDir Path tmp) throws Exception {
        Path domain = tmp.resolve("fatal-keeps-old");
        byte[] oldContent = bytes("1\t1.00\nbad\t2.00\n2\t2.00\n");
        DurableQualityProvider.objects.put("old.tsv", oldContent);

        File w0 = tmp.resolve("failure-w0").toFile();
        File w1 = tmp.resolve("failure-w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        try (MasterServer master = new MasterServer(10_000L);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "failure-w0", 16, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "failure-w1", 16, "127.0.0.1", master.port(), w1)) {
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());
            run(client, "object-quality-old", domain, input("old.tsv", oldContent));
            CommitManifest old = new ManifestFileStore(domain).latest();
            List<String> oldValid = validLines(read(
                    META, domain, "valid", old.generation()));
            List<String> oldSummary = summaryLines(read(
                    TypedQualityManifest.summarySchema(), domain, "summary",
                    old.generation()));

            byte[] declared = bytes("3\t3.00\n4\t4.00\n");
            ObjectStorageSnapshotSource corrupted = input("corrupt.tsv", declared);
            DurableQualityProvider.objects.put(
                    "corrupt.tsv", bytes("3\t3.00\n9\t9.00\n"));

            assertThrows(RuntimeException.class, () -> run(
                    client, "object-quality-corrupt", domain, corrupted));

            CommitManifest visible = new ManifestFileStore(domain).latest();
            assertEquals(old.generation(), visible.generation());
            assertEquals(old.attemptId(), visible.attemptId());
            assertEquals(oldValid, validLines(read(
                    META, domain, "valid", visible.generation())));
            assertEquals(oldSummary, summaryLines(read(
                    TypedQualityManifest.summarySchema(), domain, "summary",
                    visible.generation())));
            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }

        Path staging = domain.resolve("staging");
        if (Files.exists(staging)) {
            try (java.util.stream.Stream<Path> paths = Files.walk(staging)) {
                assertFalse(paths.anyMatch(Files::isRegularFile),
                        "failed Object quality publication retained staging data");
            }
        }
    }

    private static void run(
            JobClient client, String definitionId, Path domain,
            ObjectStorageSnapshotSource input) {
        ExecutionEnvironment session = new ExecutionEnvironment(
                definitionId, definitionId, client);
        session.setDefinitionId(definitionId);
        session.setParallelism(2);
        session.classifyObjectStorageRows("ledger.object.source", input)
                .writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary"));
        session.execute();
    }

    private static ObjectStorageSnapshotSource input(String key, byte[] declared) {
        DurableQualityProvider.objects.putIfAbsent(
                key, Arrays.copyOf(declared, declared.length));
        return new ObjectStorageSnapshotSource(
                META, new DurableQualityProvider(), "endpoint-test", "credential-test",
                "domain", key, "version-1", declared.length, sha256(declared),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES);
    }

    private static List<DataRow> read(
            DataSchema meta, Path domain, String member, long generation) {
        List<DataRow> result = new ArrayList<>();
        try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                meta, domain.toString(), member, generation).openReader()) {
            while (reader.hasNext()) result.add(reader.next());
        }
        return result;
    }

    private static List<String> validLines(List<DataRow> rows) {
        List<String> result = new ArrayList<>();
        for (DataRow row : rows) {
            result.add(row.getLong(0) + "|"
                    + ((BigDecimal) row.get(1)).toPlainString());
        }
        return result;
    }

    private static List<String> summaryLines(List<DataRow> rows) {
        List<String> result = new ArrayList<>();
        for (DataRow row : rows) {
            result.add(row.getString(0) + "|" + row.get(1) + "|" + row.get(2)
                    + "|" + row.getInteger(3) + "|" + row.get(4)
                    + "|" + row.getLong(5));
        }
        return result;
    }

    private static byte[] bytes(String text) {
        return text.getBytes(StandardCharsets.UTF_8);
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

    public static final class DurableQualityProvider
            implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        static final AtomicInteger opens = new AtomicInteger();
        static final AtomicInteger closes = new AtomicInteger();

        public DurableQualityProvider() { }

        static void reset() {
            objects.clear();
            opens.set(0);
            closes.set(0);
        }

        @Override public String providerType() { return "object-quality-durable"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            byte[] content = objects.get(request.objectKey());
            if (content == null) throw new IllegalStateException("missing fixture object");
            opens.incrementAndGet();
            return new DurableQualityHandle(
                    request, Arrays.copyOf(content, content.length));
        }
    }

    private static final class DurableQualityHandle
            implements ObjectStorageReadHandle {
        private final ObjectStorageReadRequest request;
        private final InputStream stream;
        private boolean closed;

        private DurableQualityHandle(
                ObjectStorageReadRequest request, byte[] content) {
            this.request = request;
            this.stream = new ByteArrayInputStream(content);
        }

        @Override public String providerType() { return "object-quality-durable"; }
        @Override public int providerVersion() { return 1; }
        @Override public String versionId() { return request.versionId(); }
        @Override public long contentLength() { return request.contentLength(); }
        @Override public InputStream stream() { return stream; }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            DurableQualityProvider.closes.incrementAndGet();
            try {
                stream.close();
            } catch (IOException impossible) {
                throw new IllegalStateException(impossible);
            }
        }
    }
}
