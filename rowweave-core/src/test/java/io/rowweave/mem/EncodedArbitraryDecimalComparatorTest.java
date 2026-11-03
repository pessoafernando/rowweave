package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.ops.GroupKeyComparator;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncodedArbitraryDecimalComparatorTest {

    @Test
    void sameScaleArbitraryDecimalsMatchAuthorityWithoutFallback() {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        BigDecimal[] values = sameScaleValues(37);
        for (SortField.Direction direction : SortField.Direction.values()) {
            SortField[] fields = {new SortField(
                    0, direction, SortField.NullOrder.NULLS_LAST)};
            RowComparator authority = new RowComparator(meta, fields);
            EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                    meta, authority, fields);
            for (BigDecimal left : values) {
                for (BigDecimal right : values) {
                    int expected = Integer.signum(authority.compare(row(left), row(right)));
                    int actual = compareBoth(meta, encoded, row(left), row(right));
                    assertNotEquals(EncodedSortKeyComparator.FALLBACK, actual,
                            () -> "same-scale arbitrary decimals must stay encoded: "
                                    + left + " vs " + right);
                    assertEquals(expected, Integer.signum(actual),
                            () -> "left=" + left + " right=" + right);
                }
            }
        }
    }

    @Test
    void differentScaleAndMixedEncodingRequestWholeRowFallback() {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                meta, new RowComparator(meta, fields), fields);
        BigInteger huge = BigInteger.TEN.pow(63);
        assertEquals(EncodedSortKeyComparator.FALLBACK,
                compareBoth(meta, encoded,
                        row(new BigDecimal(huge, 2)), row(new BigDecimal(huge, 3))));
        assertEquals(EncodedSortKeyComparator.FALLBACK,
                compareBoth(meta, encoded,
                        row(new BigDecimal(huge, 2)), row(BigDecimal.ONE)));
    }

    @Test
    void defaultNullComparesDirectlyAsZeroAgainstArbitraryPrecision() {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        BigDecimal positive = new BigDecimal(BigInteger.TEN.pow(63), 7);
        BigDecimal negative = positive.negate();
        for (SortField.Direction direction : SortField.Direction.values()) {
            SortField[] fields = {new SortField(
                    0, direction, SortField.NullOrder.DEFAULT)};
            RowComparator authority = new RowComparator(meta, fields);
            EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                    meta, authority, fields);
            assertDirectAuthority(meta, authority, encoded, row((Object) null), row(positive));
            assertDirectAuthority(meta, authority, encoded, row((Object) null), row(negative));
            assertDirectAuthority(meta, authority, encoded, row(positive), row((Object) null));
            assertDirectAuthority(meta, authority, encoded, row(negative), row((Object) null));
        }
    }

    @Test
    void rejectsNonCanonicalOrTruncatedArbitraryPayloadsBeforeComparison() {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                meta, new RowComparator(meta, fields), fields);
        byte[] valid = encodedArbitrary(2, BigInteger.TEN.pow(63).toByteArray());
        byte[] canonicalZero = encodedArbitrary(2, new byte[]{0});

        assertEquals(0, encoded.compare(canonicalZero, canonicalZero));
        assertTrue(encoded.compare(canonicalZero, valid) < 0);

        assertThrows(IllegalArgumentException.class,
                () -> encoded.compare(encodedArbitrary(2, new byte[0]), valid));
        assertThrows(IllegalArgumentException.class,
                () -> encoded.compare(encodedArbitrary(2, new byte[]{0, 1}), valid));
        assertThrows(IllegalArgumentException.class,
                () -> encoded.compare(encodedArbitrary(2, new byte[]{-1, -2}), valid));

        byte[] truncated = java.util.Arrays.copyOf(valid, valid.length - 1);
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(truncated, valid));
        byte[] unknownTag = valid.clone();
        unknownTag[5] = 2;
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(unknownTag, valid));
    }

    @Test
    void randomizedSameScaleBinaryPayloadsMatchBigDecimalCompareTo() {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);
        Random random = new Random(0xA2B17A2L);
        for (int iteration = 0; iteration < 5_000; iteration++) {
            int bits = 64 + random.nextInt(3_400);
            int scale = random.nextInt();
            BigInteger left = arbitraryInteger(random, bits);
            BigInteger right = arbitraryInteger(random, bits);
            assertDirectAuthority(meta, authority, encoded,
                    row(new BigDecimal(left, scale)), row(new BigDecimal(right, scale)));
        }
    }

    @Test
    void groupAndMultiFieldComparatorsContinueAfterEqualArbitraryDecimal() {
        DataSchema meta = new DataSchema(
                new Field("amount", DataType.BigDecimalType),
                new Field("district", DataType.StringType));
        SortField[] fields = {
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)};
        BigDecimal amount = new BigDecimal(BigInteger.TEN.pow(255), 10);
        DataRow aaa = row(amount, "AAA");
        DataRow bbb = row(amount, "BBB");

        RowComparator rows = new RowComparator(meta, fields);
        EncodedSortKeyComparator rowEncoded = EncodedSortKeyComparator.create(meta, rows, fields);
        assertDirectAuthority(meta, rows, rowEncoded, aaa, bbb);

        int[] indices = {0, 1};
        SortField[] groupFields = {
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        GroupKeyComparator groups = new GroupKeyComparator(meta, indices);
        EncodedSortKeyComparator groupEncoded = EncodedSortKeyComparator.create(
                meta, groups, groupFields);
        int actual = compareBoth(meta, groupEncoded, aaa, bbb);
        assertNotEquals(EncodedSortKeyComparator.FALLBACK, actual);
        assertEquals(Integer.signum(groups.compare(aaa, bbb)), Integer.signum(actual));
    }

    @Test
    void arbitraryDecimalKeysRemainStableAcrossPointerAndPipelinedDiskRuns(
            @TempDir Path tempDir) {
        DataSchema meta = new DataSchema(
                new Field("amount", DataType.BigDecimalType),
                new Field("seq", DataType.LongType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        List<DataRow> input = new ArrayList<>();
        BigInteger base64 = BigInteger.TEN.pow(63);
        BigInteger base256 = BigInteger.TEN.pow(255);
        for (int i = 0; i < 2_000; i++) {
            BigInteger base = (i & 1) == 0 ? base64 : base256;
            BigInteger unscaled = base.add(BigInteger.valueOf((i * 2654435761L) % 997));
            if (i % 5 == 0) unscaled = unscaled.negate();
            input.add(row(new BigDecimal(unscaled, 8), (long) i));
        }
        List<DataRow> expected = new ArrayList<>(input);
        expected.sort(authority);
        assertEquals(fingerprint(expected), fingerprint(runDiskSort(
                meta, fields, authority, input, tempDir.resolve("pointer"), false)));
        assertEquals(fingerprint(expected), fingerprint(runDiskSort(
                meta, fields, authority, input, tempDir.resolve("pipelined"), true)));
    }

    @Test
    void arbitraryComparatorDoesNotConstructNumericObjectsOrScratch() throws Exception {
        String source = source("EncodedSortKeyComparator.java");
        int method = source.indexOf("private static <T> int compareCanonicalBigIntegers");
        int end = source.indexOf("compareCompactValues", method);
        String body = source.substring(method, end);
        assertEquals(-1, body.indexOf("new BigDecimal"));
        assertEquals(-1, body.indexOf("new BigInteger"));
        assertEquals(-1, body.indexOf("new byte["));
        assertEquals(-1, body.indexOf("double"));
    }

    private static BigDecimal[] sameScaleValues(int scale) {
        BigInteger p19 = BigInteger.TEN.pow(18);
        BigInteger p64 = BigInteger.TEN.pow(63);
        BigInteger p256 = BigInteger.TEN.pow(255);
        BigInteger p1024 = BigInteger.TEN.pow(1023);
        return new BigDecimal[]{
                new BigDecimal(p1024.negate(), scale),
                new BigDecimal(p256.negate().subtract(BigInteger.ONE), scale),
                new BigDecimal(p256.negate(), scale),
                new BigDecimal(p64.negate(), scale),
                new BigDecimal(p19.negate(), scale),
                new BigDecimal(p19, scale),
                new BigDecimal(p64, scale),
                new BigDecimal(p256, scale),
                new BigDecimal(p256.add(BigInteger.ONE), scale),
                new BigDecimal(p1024, scale)
        };
    }

    private static BigInteger arbitraryInteger(Random random, int bits) {
        BigInteger value = new BigInteger(bits, random).setBit(bits - 1);
        return random.nextBoolean() ? value : value.negate();
    }

    private static int compareBoth(DataSchema meta, EncodedSortKeyComparator encoded,
                                   DataRow left, DataRow right) {
        RowSerializer serializer = new RowSerializer(meta);
        byte[] leftBytes = serializer.serialize(left);
        byte[] rightBytes = serializer.serialize(right);
        int arrayResult = encoded.compare(leftBytes, rightBytes);

        HeapPool pool = new HeapPool();
        MemorySegment leftSegment = pool.allocate(leftBytes.length);
        MemorySegment rightSegment = pool.allocate(rightBytes.length);
        try {
            leftSegment.putBytes(0, leftBytes);
            rightSegment.putBytes(0, rightBytes);
            assertEquals(arrayResult, encoded.compare(
                    leftSegment, leftBytes.length, rightSegment, rightBytes.length));
            return arrayResult;
        } finally {
            pool.free(leftSegment);
            pool.free(rightSegment);
            pool.trim();
        }
    }

    private static void assertDirectAuthority(DataSchema meta, RowComparator authority,
                                              EncodedSortKeyComparator encoded,
                                              DataRow left, DataRow right) {
        int actual = compareBoth(meta, encoded, left, right);
        assertNotEquals(EncodedSortKeyComparator.FALLBACK, actual);
        assertEquals(Integer.signum(authority.compare(left, right)), Integer.signum(actual));
    }

    private static byte[] encodedArbitrary(int scale, byte[] payload) {
        byte[] encoded = new byte[10 + payload.length];
        putInt(encoded, 1, scale);
        encoded[5] = 1;
        putInt(encoded, 6, payload.length);
        System.arraycopy(payload, 0, encoded, 10, payload.length);
        return encoded;
    }

    private static void putInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static List<DataRow> runDiskSort(DataSchema meta, SortField[] fields,
                                          Comparator<DataRow> comparator, List<DataRow> input,
                                          Path spillDir, boolean pipelined) {
        DiskSpillStore store = new DiskSpillStore(spillDir.toFile());
        MemoryManager memory = new MemoryManager(8L * 1024 * 1024);
        SortBuffer buffer = pipelined
                ? new PipelinedExternalSortBuffer(meta, comparator, fields, memory,
                31, 3, store, HeapPool::new)
                : new PointerSortBuffer(meta, comparator, fields, new HeapPool(), memory,
                31, store);
        try {
            for (DataRow row : input) buffer.putRow(row);
            buffer.sort();
            List<DataRow> output = new ArrayList<>();
            for (DataRow row : buffer) output.add(row);
            assertTrue(store.runsCreated() > 1, "test must create multiple spill runs");
            return output;
        } finally {
            buffer.close();
            store.close();
            assertEquals(0, memory.reservedBytes());
            java.io.File[] files = spillDir.toFile().listFiles();
            assertEquals(0, files == null ? 0 : files.length);
        }
    }

    private static String fingerprint(List<DataRow> rows) {
        StringBuilder result = new StringBuilder();
        for (DataRow row : rows) {
            BigDecimal value = (BigDecimal) row.get(0);
            result.append(value.toPlainString()).append('@').append(value.scale())
                    .append('|').append(row.get(1)).append('\n');
        }
        return result.toString();
    }

    private static String source(String file) throws Exception {
        Path cwd = Paths.get("").toAbsolutePath();
        Path path = cwd.resolve("src/main/java/io/rowweave/mem").resolve(file);
        if (!Files.exists(path)) {
            path = cwd.resolve("rowweave-core/src/main/java/io/rowweave/mem").resolve(file);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        for (int index = 0; index < values.length; index++) row.set(index, values[index]);
        return row;
    }
}
