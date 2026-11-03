package io.rowweave;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** strict record decoding and exactly-one quality carrier. */
class ObjectStorageQualityInputExecutorTest {

    private static final String SOURCE_UID = "ledger.object.source";

    @AfterEach
    void resetProvider() {
        DecoderProvider.content = new byte[0];
        DecoderProvider.opens = 0;
        DecoderProvider.closes = 0;
    }

    @Test
    void decodesEveryCanonicalDataTypeAndPreservesDecimalScale() {
        DataSchema meta = new DataSchema(
                new Field("text", DataType.StringType),
                new Field("flag", DataType.BooleanType),
                new Field("integer", DataType.IntegerType),
                new Field("long", DataType.LongType),
                new Field("decimal", DataType.BigDecimalType),
                new Field("date", DataType.DateType),
                new Field("timestamp", DataType.TimestampType),
                new Field("nothing", DataType.NullType));
        byte[] content = utf8(
                "a\\tb\\n\\\\c\ttrue\t-12\t34\t123.4500\t1700000000000"
                        + "\t1700000000123:123456789\t\\N\n");

        List<DataRow> carriers = read(meta, content);

        assertEquals(1, carriers.size());
        DataRow carrier = carriers.get(0);
        assertEquals("a\tb\n\\c", carrier.get(0));
        assertEquals(true, carrier.get(1));
        assertEquals(-12, carrier.get(2));
        assertEquals(34L, carrier.get(3));
        assertEquals(new BigDecimal("123.4500"), carrier.get(4));
        assertEquals(4, ((BigDecimal) carrier.get(4)).scale());
        assertEquals(new Date(1700000000000L), carrier.get(5));
        Timestamp timestamp = new Timestamp(1700000000123L);
        timestamp.setNanos(123456789);
        assertEquals(timestamp, carrier.get(6));
        assertNull(carrier.get(7));
        assertValidCarrier(carrier, meta.getFieldCount());
        assertEquals(1, DecoderProvider.opens);
        assertEquals(1, DecoderProvider.closes);
    }

    @Test
    void everyPhysicalRecordProducesExactlyOneCarrierWithStableRejectedOrdinal() {
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));
        byte[] prefix = utf8("\n1\nbad\n2\r\n");
        byte[] suffix = new byte[]{(byte) 0xc3, (byte) 0x28, (byte) '\n', (byte) '3'};
        byte[] content = concat(prefix, suffix);

        List<DataRow> carriers = read(meta, content);

        assertEquals(6, carriers.size());
        assertRejected(carriers.get(0), 1, 0L,
                "QUALITY_RAW_FORMAT_INVALID", "ROWWEAVE_ROW_TEXT_V1");
        assertEquals(1L, carriers.get(1).get(0));
        assertValidCarrier(carriers.get(1), 1);
        assertRejected(carriers.get(2), 1, 2L,
                "QUALITY_RAW_FORMAT_INVALID", "ROWWEAVE_ROW_TEXT_V1");
        assertRejected(carriers.get(3), 1, 3L,
                "QUALITY_RAW_CR_INVALID", "CR");
        assertRejected(carriers.get(4), 1, 4L,
                "QUALITY_RAW_UTF8_INVALID", "UTF8");
        assertEquals(3L, carriers.get(5).get(0));
        assertValidCarrier(carriers.get(5), 1);
    }

    @Test
    void emptyObjectHasNoRecordAndFinalNonEmptySegmentIsARecord() {
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));

        assertTrue(read(meta, new byte[0]).isEmpty());
        List<DataRow> finalSegment = read(meta, utf8("7"));

        assertEquals(1, finalSegment.size());
        assertEquals(7L, finalSegment.get(0).get(0));
        assertValidCarrier(finalSegment.get(0), 1);
    }

    @Test
    void rejectsNoncanonicalRepresentationsForEveryDataType() {
        assertFormatRejected(DataType.StringType, "\\q");
        assertFormatRejected(DataType.StringType, "\\");
        assertFormatRejected(DataType.BooleanType, "TRUE");
        assertFormatRejected(DataType.BooleanType, "yes");
        assertFormatRejected(DataType.IntegerType, "+1");
        assertFormatRejected(DataType.IntegerType, "01");
        assertFormatRejected(DataType.LongType, "+1");
        assertFormatRejected(DataType.BigDecimalType, "1e+2");
        assertFormatRejected(DataType.DateType, "+1");
        assertFormatRejected(DataType.TimestampType, "0:000000001");
        assertFormatRejected(DataType.NullType, "value");
    }

    @Test
    void acceptsCanonicalNegativeScaleDecimalRepresentation() {
        DataSchema meta = new DataSchema(new Field("value", DataType.BigDecimalType));
        ObjectStorageQualityRecordDecoder decoder =
                new ObjectStorageQualityRecordDecoder(qualityInput(meta, new byte[0]));
        byte[] record = utf8("1E+2");

        DataRow carrier = decoder.classify(record, record.length, 0L, false);

        assertEquals(new java.math.BigDecimal("1E+2"), carrier.getBigDecimal(0));
        assertValidCarrier(carrier, 1);
    }

    @Test
    void errorPriorityIsRowLimitThenCrThenUtf8ThenFormat() {
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));
        ObjectStorageQualitySource input = qualityInput(meta, new byte[0]);
        ObjectStorageQualityRecordDecoder decoder =
                new ObjectStorageQualityRecordDecoder(input);
        byte[] crAndUtf8 = new byte[]{(byte) '\r', (byte) 0xc3, (byte) 0x28};

        assertRejected(decoder.classify(crAndUtf8, crAndUtf8.length, 11L, true),
                1, 11L, "QUALITY_RAW_ROW_LIMIT_EXCEEDED", "ROW_BYTES");
        assertRejected(decoder.classify(crAndUtf8, crAndUtf8.length, 12L, false),
                1, 12L, "QUALITY_RAW_CR_INVALID", "CR");
        byte[] invalidUtf8 = new byte[]{(byte) 0xc3, (byte) 0x28};
        assertRejected(decoder.classify(invalidUtf8, invalidUtf8.length, 13L, false),
                1, 13L, "QUALITY_RAW_UTF8_INVALID", "UTF8");
        byte[] invalidFormat = utf8("not-a-long");
        assertRejected(decoder.classify(
                        invalidFormat, invalidFormat.length, 14L, false),
                1, 14L, "QUALITY_RAW_FORMAT_INVALID", "ROWWEAVE_ROW_TEXT_V1");
    }

    @Test
    void oversizeRecordBecomesPayloadFreeRejectAfterBoundedResyncAdmission() {
        DataSchema meta = new DataSchema(new Field("id", DataType.StringType));
        byte[] content = new byte[ObjectStorageSnapshotSource.MIN_ROW_BYTES + 2];
        Arrays.fill(content, (byte) 'x');
        content[content.length - 1] = (byte) '\n';
        ObjectStorageQualitySource input = qualityInput(meta, content);
        SourceExecutor<?> executor = SourceExecutorFactory.create(input);
        executor.open();

        assertTrue(executor.hasNext());
        DataRow rejected = executor.next();

        assertRejected(rejected, 1, 0L,
                "QUALITY_RAW_ROW_LIMIT_EXCEEDED", "ROW_BYTES");
        assertFalse(executor.hasNext());
        assertEquals(1, DecoderProvider.closes);
        executor.close();
        assertEquals(1, DecoderProvider.closes);
    }

    private static void assertFormatRejected(DataType type, String token) {
        DataSchema meta = new DataSchema(new Field("value", type));
        ObjectStorageQualitySource input = qualityInput(meta, new byte[0]);
        ObjectStorageQualityRecordDecoder decoder =
                new ObjectStorageQualityRecordDecoder(input);
        byte[] record = utf8(token);
        assertRejected(decoder.classify(record, record.length, 0L, false),
                1, 0L, "QUALITY_RAW_FORMAT_INVALID", "ROWWEAVE_ROW_TEXT_V1");
    }

    private static List<DataRow> read(DataSchema meta, byte[] content) {
        ObjectStorageQualitySource input = qualityInput(meta, content);
        SourceExecutor<?> executor = SourceExecutorFactory.create(input);
        List<DataRow> rows = new ArrayList<>();
        executor.open();
        try {
            while (executor.hasNext()) rows.add(executor.next());
        } finally {
            executor.close();
        }
        return rows;
    }

    private static ObjectStorageQualitySource qualityInput(DataSchema meta, byte[] content) {
        DecoderProvider.content = Arrays.copyOf(content, content.length);
        return new ObjectStorageQualitySource(SOURCE_UID, new ObjectStorageSnapshotSource(
                meta, new DecoderProvider(), "endpoint-test", "credential-test", "domain",
                "ledger.tsv", "version-1", content.length, sha256(content),
                ObjectStorageSnapshotSource.MIN_ROW_BYTES));
    }

    private static void assertValidCarrier(DataRow carrier, int offset) {
        assertEquals(0, carrier.get(offset));
        assertEquals("", carrier.get(offset + 1));
        assertEquals("", carrier.get(offset + 2));
        assertEquals(0L, carrier.get(offset + 3));
        assertEquals("", carrier.get(offset + 4));
        assertEquals("", carrier.get(offset + 5));
        assertEquals(-1, carrier.get(offset + 6));
        assertEquals("", carrier.get(offset + 7));
    }

    private static void assertRejected(
            DataRow carrier, int offset, long ordinal, String code, String detail) {
        for (int i = 0; i < offset; i++) assertNull(carrier.get(i));
        assertEquals(1, carrier.get(offset));
        assertEquals(SOURCE_UID, carrier.get(offset + 1));
        assertTrue(((String) carrier.get(offset + 2)).matches("[0-9a-f]{64}"));
        assertEquals(ordinal, carrier.get(offset + 3));
        assertEquals(code, carrier.get(offset + 4));
        assertEquals("OBJECT_ROW_DECODE_V1", carrier.get(offset + 5));
        assertEquals(-1, carrier.get(offset + 6));
        assertEquals(detail, carrier.get(offset + 7));
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] result = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, result, first.length, second.length);
        return result;
    }

    private static String sha256(byte[] content) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(content);
            StringBuilder result = new StringBuilder(64);
            for (byte value : digest) {
                result.append(Character.forDigit((value >>> 4) & 0xf, 16));
                result.append(Character.forDigit(value & 0xf, 16));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    public static final class DecoderProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static byte[] content = new byte[0];
        static int opens;
        static int closes;
        public DecoderProvider() { }
        @Override public String providerType() { return "quality-decoder"; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            opens++;
            final byte[] snapshot = Arrays.copyOf(content, content.length);
            return new ObjectStorageReadHandle() {
                @Override public String providerType() { return "quality-decoder"; }
                @Override public int providerVersion() { return 1; }
                @Override public String versionId() { return request.versionId(); }
                @Override public long contentLength() { return snapshot.length; }
                @Override public InputStream stream() {
                    return new ByteArrayInputStream(snapshot);
                }
                @Override public void close() { closes++; }
            };
        }
    }
}
