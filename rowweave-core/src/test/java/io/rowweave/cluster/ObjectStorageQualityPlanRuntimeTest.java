package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** local and real Master/Worker-fragment Object quality runtime. */
class ObjectStorageQualityPlanRuntimeTest {

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
        RuntimeProvider.reset();
    }

    @Test
    void localP1P2P4AreBitIdenticalToIndependentOracle(@TempDir Path temp) {
        Snapshot p1 = run(null, 1, temp.resolve("local-p1"));
        assertOracle(p1);
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());

        RuntimeProvider.reset();
        Snapshot p2 = run(null, 2, temp.resolve("local-p2"));
        assertEquals(p1, p2);
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());

        RuntimeProvider.reset();
        Snapshot p4 = run(null, 4, temp.resolve("local-p4"));
        assertEquals(p1, p4);
        assertEquals(2, RuntimeProvider.opens.get());
        assertEquals(2, RuntimeProvider.closes.get());
    }

    @Test
    @Tag("multiprocess")
    void inProcessClusterP1P2P4MatchesLocalOracle(@TempDir Path temp) {
        Snapshot expected = run(null, 1, temp.resolve("oracle"));
        RuntimeProvider.reset();

        try (InProcessCluster cluster = new InProcessCluster(
                4, temp.resolve("workers").toFile())) {
            Snapshot p1 = run(cluster, 1, temp.resolve("cluster-p1"));
            assertEquals(expected, p1);
            RuntimeProvider.reset();
            Snapshot p2 = run(cluster, 2, temp.resolve("cluster-p2"));
            assertEquals(expected, p2);
            RuntimeProvider.reset();
            Snapshot p4 = run(cluster, 4, temp.resolve("cluster-p4"));
            assertEquals(expected, p4);
        }
    }

    private static Snapshot run(JobClient client, int parallelism, Path domain) {
        byte[] first = bytes("2\t2.00\nbad\t9.0\n1\t1.00\n");
        byte[] second = concat(
                bytes("3\t3.00\r\n4\t4.00\n"),
                new byte[]{(byte) 0xc3, (byte) 0x28, (byte) '\n'});
        RuntimeProvider.objects.put("part-a", first);
        RuntimeProvider.objects.put("part-b", second);
        ExecutionEnvironment session = client == null
                ? new ExecutionEnvironment("object-quality-runtime", "object quality runtime")
                : new ExecutionEnvironment("object-quality-runtime", "object quality runtime", client);
        session.setParallelism(parallelism);
        session.classifyObjectStorageRows(
                        "ledger.object.source", input("part-a"), input("part-b"))
                .writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary"));
        session.execute();

        CommitManifest manifest = new ManifestFileStore(domain).latest();
        long generation = manifest.generation();
        return new Snapshot(
                validLines(read(new ManifestFileSource(
                        META, domain.toString(), "valid", generation))),
                rejectedLines(read(new ManifestFileSource(
                        REJECTED_META, domain.toString(), "rejected", generation))),
                summaryLines(read(new ManifestFileSource(
                        TypedQualityManifest.summarySchema(), domain.toString(),
                        "summary", generation))));
    }

    private static void assertOracle(Snapshot snapshot) {
        assertEquals(Arrays.asList("1|1.00", "2|2.00", "4|4.00"), snapshot.valid);
        List<String> codes = new ArrayList<>();
        for (String row : snapshot.rejected) {
            String[] fields = row.split("\\|", -1);
            codes.add(fields[3] + "|" + fields[2]);
        }
        Collections.sort(codes);
        assertEquals(Arrays.asList(
                "QUALITY_RAW_CR_INVALID|0",
                "QUALITY_RAW_FORMAT_INVALID|1",
                "QUALITY_RAW_UTF8_INVALID|2"), codes);
        assertEquals(Arrays.asList(
                "ACCEPTED_TOTAL|||-1||3",
                "REJECTED_REASON|QUALITY_RAW_CR_INVALID|OBJECT_ROW_DECODE_V1|-1|CR|1",
                "REJECTED_REASON|QUALITY_RAW_FORMAT_INVALID|OBJECT_ROW_DECODE_V1|-1|ROWWEAVE_ROW_TEXT_V1|1",
                "REJECTED_REASON|QUALITY_RAW_UTF8_INVALID|OBJECT_ROW_DECODE_V1|-1|UTF8|1",
                "REJECTED_TOTAL|||-1||3"), snapshot.summary);
    }

    private static ObjectStorageSnapshotSource input(String key) {
        byte[] content = RuntimeProvider.objects.get(key);
        return new ObjectStorageSnapshotSource(
                META, new RuntimeProvider(), "endpoint-test", "credential-test", "domain",
                key, "version-1", content.length, sha256(content),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES);
    }

    private static List<DataRow> read(Source input) {
        List<DataRow> rows = new ArrayList<>();
        for (DataRow row : input) rows.add(row);
        return rows;
    }

    private static List<String> validLines(List<DataRow> rows) {
        List<String> result = new ArrayList<>();
        for (DataRow row : rows) {
            result.add(row.getLong(0) + "|" + ((BigDecimal) row.get(1)).toPlainString());
        }
        return result;
    }

    private static List<String> rejectedLines(List<DataRow> rows) {
        List<String> result = new ArrayList<>();
        for (DataRow row : rows) {
            result.add(row.getString(0) + "|" + row.getString(1) + "|"
                    + row.getLong(2) + "|" + row.getString(3) + "|"
                    + row.getString(4) + "|" + row.getInteger(5) + "|"
                    + row.getString(6));
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

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
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

    private static final class Snapshot {
        private final List<String> valid;
        private final List<String> rejected;
        private final List<String> summary;

        private Snapshot(
                List<String> valid, List<String> rejected, List<String> summary) {
            this.valid = valid;
            this.rejected = rejected;
            this.summary = summary;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) return true;
            if (!(other instanceof Snapshot)) return false;
            Snapshot that = (Snapshot) other;
            return valid.equals(that.valid)
                    && rejected.equals(that.rejected)
                    && summary.equals(that.summary);
        }

        @Override public int hashCode() {
            return java.util.Objects.hash(valid, rejected, summary);
        }

        @Override public String toString() {
            return "Snapshot{" + valid + ", " + rejected + ", " + summary + '}';
        }
    }

    public static final class RuntimeProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static final Map<String, byte[]> objects = new ConcurrentHashMap<>();
        static final AtomicInteger opens = new AtomicInteger();
        static final AtomicInteger closes = new AtomicInteger();
        public RuntimeProvider() { }
        static void reset() {
            objects.clear();
            opens.set(0);
            closes.set(0);
        }
        @Override public String providerType() { return "object-quality-runtime"; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            byte[] content = objects.get(request.objectKey());
            if (content == null) throw new IllegalStateException("missing fixture object");
            opens.incrementAndGet();
            return new RuntimeHandle(request, Arrays.copyOf(content, content.length));
        }
    }

    private static final class RuntimeHandle implements ObjectStorageReadHandle {
        private final ObjectStorageReadRequest request;
        private final InputStream stream;
        private boolean closed;
        private RuntimeHandle(ObjectStorageReadRequest request, byte[] content) {
            this.request = request;
            this.stream = new ByteArrayInputStream(content);
        }
        @Override public String providerType() { return "object-quality-runtime"; }
        @Override public int providerVersion() { return 1; }
        @Override public String versionId() { return request.versionId(); }
        @Override public long contentLength() { return request.contentLength(); }
        @Override public InputStream stream() { return stream; }
        @Override public void close() {
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
