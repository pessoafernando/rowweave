package io.rowweave.connector.s3;

import io.rowweave.DataType;
import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.spi.StreamSourceEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3SnapshotReaderTest {
    private static final StreamSchema SCHEMA = new StreamSchema(Arrays.asList(
            new StreamField("id", "id", "id", DataType.LongType, false),
            new StreamField("amount", "amount", "amount",
                    DataType.BigDecimalType, true),
            new StreamField("posted_at", "posted_at", "posted_at",
                    DataType.TimestampType, true)));

    @Test
    void ndjsonIsFullyVerifiedBeforePushAndRestoresAtRecordBoundary(
            @TempDir Path temp) throws Exception {
        byte[] content = ("{\"id\":1,\"amount\":12.3400,"
                + "\"posted_at\":\"2026-09-27T01:02:03.123456789Z\"}\n"
                + "{\"id\":2,\"amount\":null,\"posted_at\":null}\n")
                .getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor descriptor = descriptor(
                "secret/path/ledger.ndjson", S3ObjectFormat.NDJSON_UTF8,
                content, SCHEMA);
        FakeClient client = new FakeClient(descriptor, content);
        S3SnapshotReader reader = new S3SnapshotReader(configuration(
                S3ObjectFormat.NDJSON_UTF8), descriptor, client, null, temp);
        StreamDataRowCodec rows = new StreamDataRowCodec(SCHEMA);

        StreamSourceEvent first = reader.nextEvent();
        assertEquals(content.length, client.bytesRead.get(),
                "the exact object must be complete and verified before row 1");
        DataRow row1 = rows.decode(first.record().rowFrame());
        assertEquals(1L, row1.getLong(0));
        assertEquals(new BigDecimal("12.3400"), row1.getBigDecimal(1));
        assertEquals(Timestamp.from(Instant.parse(
                "2026-09-27T01:02:03.123456789Z")), row1.getTimestamp(2));
        assertEquals(1L, S3SourcePositionCodec.decode(
                first.record().positionFrame()).completedRecords());
        assertEquals(1L, S3SourcePositionCodec.decode(
                reader.checkpointPositionFrame()).completedRecords());

        StreamSourceEvent second = reader.nextEvent();
        DataRow row2 = rows.decode(second.record().rowFrame());
        assertEquals(2L, row2.getLong(0));
        assertNull(row2.get(1));
        assertNull(row2.get(2));
        assertEquals(StreamSourceEvent.Kind.END_OF_INPUT,
                reader.nextEvent().kind());
        assertNull(reader.nextEvent());
        assertTrue(isEmpty(temp));
        assertEquals(1, client.closes.get());
        reader.close();
        reader.close();

        FakeClient restoredClient = new FakeClient(descriptor, content);
        S3SnapshotReader restored = new S3SnapshotReader(configuration(
                S3ObjectFormat.NDJSON_UTF8), descriptor, restoredClient,
                new S3SourcePosition(descriptor.splitId(), 1L), temp);
        DataRow replayed = rows.decode(restored.nextEvent().record().rowFrame());
        assertEquals(2L, replayed.getLong(0));
        assertEquals(StreamSourceEvent.Kind.END_OF_INPUT,
                restored.nextEvent().kind());
        assertTrue(isEmpty(temp));
    }

    @Test
    void rfc4180HeaderMultilineQuotesNullAndDecimalTypesAreDeterministic(
            @TempDir Path temp) throws Exception {
        StreamSchema csvSchema = new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("memo", "memo", "memo",
                        DataType.StringType, true),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
        byte[] content = ("id,memo,amount\r\n"
                + "1,\"line one\r\nline \"\"two\"\"\",100.2300\r\n"
                + "2,\\N,\\N\r\n").getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor descriptor = descriptor(
                "secret/path/ledger.csv", S3ObjectFormat.CSV_RFC4180,
                content, csvSchema);
        S3SnapshotReader reader = new S3SnapshotReader(configuration(
                S3ObjectFormat.CSV_RFC4180), descriptor,
                new FakeClient(descriptor, content), null, temp);
        StreamDataRowCodec rows = new StreamDataRowCodec(csvSchema);

        DataRow first = rows.decode(reader.nextEvent().record().rowFrame());
        assertEquals(1L, first.getLong(0));
        assertEquals("line one\r\nline \"two\"", first.getString(1));
        assertEquals(new BigDecimal("100.2300"), first.getBigDecimal(2));
        DataRow second = rows.decode(reader.nextEvent().record().rowFrame());
        assertEquals(2L, second.getLong(0));
        assertNull(second.get(1));
        assertNull(second.get(2));
        assertEquals(StreamSourceEvent.Kind.END_OF_INPUT,
                reader.nextEvent().kind());
        assertTrue(isEmpty(temp));
    }

    @Test
    void objectMutationSchemaDriftAndOversizeFailClosedWithoutLeakingLocator(
            @TempDir Path temp) throws Exception {
        byte[] valid = "{\"id\":1,\"amount\":1,\"posted_at\":null}\n"
                .getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor descriptor = descriptor(
                "secret/path/ledger.ndjson", S3ObjectFormat.NDJSON_UTF8,
                valid, SCHEMA);

        byte[] changed = valid.clone();
        changed[changed.length - 2] = '2';
        FakeClient changedClient = new FakeClient(descriptor, changed);
        assertFailure("S3_OBJECT_CHANGED", "secret/path",
                () -> new S3SnapshotReader(configuration(
                        S3ObjectFormat.NDJSON_UTF8), descriptor,
                        changedClient, null, temp).nextEvent());
        assertTrue(isEmpty(temp));
        assertEquals(1, changedClient.closes.get());

        byte[] unknown = ("{\"id\":1,\"amount\":1,\"posted_at\":null,"
                + "\"unknown\":7}\n").getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor unknownDescriptor = descriptor(
                "secret/path/unknown.ndjson", S3ObjectFormat.NDJSON_UTF8,
                unknown, SCHEMA);
        assertFailure("S3_ROW_SCHEMA_DRIFT", "secret/path",
                () -> new S3SnapshotReader(configuration(
                        S3ObjectFormat.NDJSON_UTF8), unknownDescriptor,
                        new FakeClient(unknownDescriptor, unknown), null,
                        temp).nextEvent());
        assertTrue(isEmpty(temp));

        byte[] wide = ("{\"id\":1,\"amount\":1,\"posted_at\":null,\"pad\":\""
                + repeat('x', 2048) + "\"}\n")
                .getBytes(StandardCharsets.UTF_8);
        S3ObjectDescriptor wideDescriptor = descriptor(
                "secret/path/wide.ndjson", S3ObjectFormat.NDJSON_UTF8,
                wide, SCHEMA);
        S3ConnectorConfiguration tinyRows = configuration(
                S3ObjectFormat.NDJSON_UTF8, 1024);
        assertFailure("S3_ROW_LIMIT", "secret/path",
                () -> new S3SnapshotReader(tinyRows, wideDescriptor,
                        new FakeClient(wideDescriptor, wide), null,
                        temp).nextEvent());
        assertTrue(isEmpty(temp));
    }

    @Test
    void ndjsonRowByteBudgetHonorsLimitMinusOneExactAndPlusOne(
            @TempDir Path temp) throws Exception {
        assertEquals(1L, readBoundaryRow(1023, temp));
        assertEquals(1L, readBoundaryRow(1024, temp));

        byte[] tooWide = boundaryContent(1025);
        S3ObjectDescriptor descriptor = descriptor(
                "secret/path/boundary.ndjson",
                S3ObjectFormat.NDJSON_UTF8, tooWide, SCHEMA);
        assertFailure("S3_ROW_LIMIT", "secret/path", () ->
                new S3SnapshotReader(configuration(
                        S3ObjectFormat.NDJSON_UTF8, 1024), descriptor,
                        new FakeClient(descriptor, tooWide), null, temp)
                        .nextEvent());
        assertTrue(isEmpty(temp));
    }

    private static long readBoundaryRow(int recordBytes, Path temp)
            throws Exception {
        byte[] content = boundaryContent(recordBytes);
        S3ObjectDescriptor descriptor = descriptor(
                "secret/path/boundary.ndjson",
                S3ObjectFormat.NDJSON_UTF8, content, SCHEMA);
        S3SnapshotReader reader = new S3SnapshotReader(configuration(
                S3ObjectFormat.NDJSON_UTF8, 1024), descriptor,
                new FakeClient(descriptor, content), null, temp);
        try {
            return new StreamDataRowCodec(SCHEMA).decode(
                    reader.nextEvent().record().rowFrame()).getLong(0);
        } finally {
            reader.close();
        }
    }

    private static byte[] boundaryContent(int recordBytes) {
        String row = "{\"id\":1,\"amount\":1,\"posted_at\":null}";
        byte[] record = (row + repeat(' ', recordBytes - row.length()))
                .getBytes(StandardCharsets.UTF_8);
        byte[] content = Arrays.copyOf(record, record.length + 1);
        content[content.length - 1] = '\n';
        return content;
    }

    private static S3ConnectorConfiguration configuration(
            S3ObjectFormat format) {
        return configuration(format, 64 * 1024);
    }

    private static S3ConnectorConfiguration configuration(
            S3ObjectFormat format, int maximumRowBytes) {
        return S3ConnectorConfiguration.builder()
                .endpoint(java.net.URI.create("https://objects.invalid"))
                .region("us-east-1").bucket("ledger-snapshots")
                .format(format).credentialsReference("vault/s3")
                .maximumObjectBytes(1024 * 1024)
                .maximumRowBytes(maximumRowBytes)
                .temporaryDiskBudgetBytes(1024 * 1024)
                .nullLiteral("\\N").build();
    }

    private static S3ObjectDescriptor descriptor(String key,
            S3ObjectFormat format, byte[] content, StreamSchema schema) {
        return S3ObjectDescriptor.versioned(
                "ledger-snapshots", key, "version-1", "etag-1",
                content.length, sha256(content), format, schema);
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder value = new StringBuilder(64);
            for (byte item : digest) value.append(String.format("%02x", item));
            return value.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static boolean isEmpty(Path directory) throws IOException {
        try (java.util.stream.Stream<Path> files = Files.list(directory)) {
            return !files.findAny().isPresent();
        }
    }

    private static void assertFailure(String code, String forbidden,
            ThrowingRunnable action) {
        Exception failure = assertThrows(Exception.class, action::run);
        assertTrue(failure.getMessage().startsWith(code),
                failure.getMessage());
        assertFalse(failure.getMessage().contains(forbidden),
                failure.getMessage());
    }

    private interface ThrowingRunnable { void run() throws Exception; }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class FakeClient implements S3ObjectClient {
        private final S3ObjectDescriptor descriptor;
        private final byte[] content;
        private final AtomicInteger bytesRead = new AtomicInteger();
        private final AtomicInteger closes = new AtomicInteger();

        private FakeClient(S3ObjectDescriptor descriptor, byte[] content) {
            this.descriptor = descriptor;
            this.content = content;
        }

        @Override public S3ObjectStream open(S3ObjectDescriptor ignored) {
            InputStream tracked = new ByteArrayInputStream(content) {
                @Override public synchronized int read(byte[] target,
                        int offset, int length) {
                    int read = super.read(target, offset, length);
                    if (read > 0) bytesRead.addAndGet(read);
                    return read;
                }
                @Override public synchronized int read() {
                    int read = super.read();
                    if (read >= 0) bytesRead.incrementAndGet();
                    return read;
                }
            };
            return new S3ObjectStream(tracked, descriptor.versionId(),
                    descriptor.etag(), content.length);
        }

        @Override public void close() { closes.incrementAndGet(); }
    }
}
