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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncodedCompactDecimalComparatorTest {

    private static final Object[] COMPACT_VALUES = {
            null,
            new BigDecimal("0"), new BigDecimal("0.00"),
            new BigDecimal("1"), new BigDecimal("1.0"), new BigDecimal("1.00"),
            new BigDecimal("-1"), new BigDecimal("-1.0"),
            new BigDecimal("0.1"), new BigDecimal("0.10"),
            new BigDecimal("10"), new BigDecimal("-10"),
            new BigDecimal("999999999999999999"),
            new BigDecimal("-999999999999999999"),
            new BigDecimal(BigInteger.ONE, Integer.MAX_VALUE),
            new BigDecimal(BigInteger.ONE.negate(), Integer.MAX_VALUE),
            new BigDecimal(BigInteger.ONE, Integer.MIN_VALUE),
            new BigDecimal(BigInteger.ONE.negate(), Integer.MIN_VALUE)
    };

    @Test
    void compactDecimalsMatchRowComparatorForScaleExtremesNullPoliciesAndDirections() {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        for (SortField.Direction direction : SortField.Direction.values()) {
            for (SortField.NullOrder nullOrder : SortField.NullOrder.values()) {
                SortField[] fields = {new SortField(0, direction, nullOrder)};
                RowComparator authority = new RowComparator(meta, fields);
                EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                        meta, authority, fields);
                for (Object left : COMPACT_VALUES) {
                    for (Object right : COMPACT_VALUES) {
                        assertCanonicalSign(meta, authority, encoded, row(left), row(right));
                    }
                }
            }
        }
    }

    @Test
    void randomizedCompactDecimalsMatchJavaBigDecimalCompareTo() {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);
        RowSerializer serializer = new RowSerializer(meta);
        Random random = new Random(0xDEC1A1L);
        for (int i = 0; i < 10_000; i++) {
            long leftUnscaled = randomCompactUnscaled(random);
            long rightUnscaled = randomCompactUnscaled(random);
            int leftScale = random.nextInt();
            int rightScale = random.nextInt();
            DataRow left = row(BigDecimal.valueOf(leftUnscaled, leftScale));
            DataRow right = row(BigDecimal.valueOf(rightUnscaled, rightScale));
            int expected = Integer.signum(authority.compare(left, right));
            int actual = Integer.signum(encoded.compare(
                    serializer.serialize(left), serializer.serialize(right)));
            assertEquals(expected, actual,
                    () -> "left=" + left.get(0) + " right=" + right.get(0));
        }
    }

    @Test
    void groupKeyKeepsScaleSensitiveOrderAfterNumericEquality() {
        DataSchema meta = new DataSchema(
                new Field("amount", DataType.BigDecimalType),
                new Field("district", DataType.StringType));
        int[] indices = {0, 1};
        SortField[] fields = {
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        GroupKeyComparator authority = new GroupKeyComparator(meta, indices);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);
        DataRow[] rows = {
                row(new BigDecimal("1"), "AAA"),
                row(new BigDecimal("1.0"), "AAA"),
                row(new BigDecimal("1.00"), "AAA"),
                row(new BigDecimal("1.00"), "BBB"),
                row(new BigDecimal("0.00"), "AAA"),
                row(null, "AAA")
        };
        for (DataRow left : rows) {
            for (DataRow right : rows) {
                assertCanonicalSign(meta, authority, encoded, left, right);
            }
        }
    }

    @Test
    void arbitraryPrecisionRequestsWholeRowFallbackAndCorruptCompactPayloadFailsFast() {
        DataSchema meta = new DataSchema(new Field("amount", DataType.BigDecimalType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                meta, new RowComparator(meta, fields), fields);
        RowSerializer serializer = new RowSerializer(meta);
        byte[] huge = serializer.serialize(row(new BigDecimal("1000000000000000000")));
        byte[] compact = serializer.serialize(row(BigDecimal.ONE));
        assertBothRepresentations(EncodedSortKeyComparator.FALLBACK,
                encoded, huge, compact);

        byte[] unknownTag = compact.clone();
        unknownTag[5] = 2;
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(unknownTag, compact));

        byte[] nonCanonical = compact.clone();
        putLong(nonCanonical, 6, 1_000_000_000_000_000_000L);
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(nonCanonical, compact));

        byte[] truncated = java.util.Arrays.copyOf(compact, compact.length - 1);
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(truncated, compact));
    }

    @Test
    void compactDecimalKeysRemainStableAcrossPointerAndPipelinedDiskRuns(@TempDir Path tempDir) {
        DataSchema meta = new DataSchema(
                new Field("amount", DataType.BigDecimalType),
                new Field("seq", DataType.LongType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        RowSerializer serializer = new RowSerializer(meta);
        List<DataRow> input = new ArrayList<>();
        String[] values = {"0", "0.00", "1", "1.0", "-1.00", "10.5", null};
        for (int i = 0; i < 2_000; i++) {
            String value = values[(i * 31) % values.length];
            input.add(row(value == null ? null : new BigDecimal(value), (long) i));
        }
        List<DataRow> expected = new ArrayList<>();
        for (DataRow value : input) {
            expected.add(serializer.deserialize(serializer.serialize(value)));
        }
        expected.sort(authority);

        List<DataRow> pointer = runDiskSort(meta, fields, authority, input,
                tempDir.resolve("pointer"), false);
        List<DataRow> pipelined = runDiskSort(meta, fields, authority, input,
                tempDir.resolve("pipelined"), true);
        assertEquals(fingerprint(expected), fingerprint(pointer), "Pointer decimal spill order");
        assertEquals(fingerprint(expected), fingerprint(pipelined), "Pipelined decimal spill order");
    }

    @Test
    void compactDecimalFastPathDoesNotConstructNumericObjects() throws Exception {
        String source = source("EncodedSortKeyComparator.java");
        int method = source.indexOf("compareEncodedDecimal");
        assertTrue(method >= 0, "compact decimal comparator must exist");
        String body = source.substring(method, source.indexOf("static boolean encodedNull", method));
        assertTrue(!body.contains("new BigDecimal") && !body.contains("new BigInteger")
                        && !body.contains("Math.pow") && !body.contains("double"),
                "compact decimal comparison must stay primitive and exact");
    }

    private static void assertCanonicalSign(DataSchema meta, Comparator<DataRow> authority,
                                            EncodedSortKeyComparator encoded,
                                            DataRow left, DataRow right) {
        RowSerializer serializer = new RowSerializer(meta);
        DataRow canonicalLeft = serializer.deserialize(serializer.serialize(left));
        DataRow canonicalRight = serializer.deserialize(serializer.serialize(right));
        int expected = Integer.signum(authority.compare(canonicalLeft, canonicalRight));
        int actual = compareBoth(meta, encoded, left, right);
        assertEquals(expected, Integer.signum(actual),
                () -> "left=" + left.get(0) + " right=" + right.get(0) + " actual=" + actual);
    }

    private static int compareBoth(DataSchema meta, EncodedSortKeyComparator encoded,
                                   DataRow left, DataRow right) {
        RowSerializer serializer = new RowSerializer(meta);
        byte[] leftBytes = serializer.serialize(left);
        byte[] rightBytes = serializer.serialize(right);
        int arrayResult = encoded.compare(leftBytes, rightBytes);
        assertBothRepresentations(arrayResult, encoded, leftBytes, rightBytes);
        return arrayResult;
    }

    private static void assertBothRepresentations(int expected, EncodedSortKeyComparator encoded,
                                                  byte[] leftBytes, byte[] rightBytes) {
        assertEquals(expected, encoded.compare(leftBytes, rightBytes));
        HeapPool pool = new HeapPool();
        MemorySegment leftSegment = pool.allocate(Math.max(1, leftBytes.length));
        MemorySegment rightSegment = pool.allocate(Math.max(1, rightBytes.length));
        try {
            leftSegment.putBytes(0, leftBytes);
            rightSegment.putBytes(0, rightBytes);
            assertEquals(expected, encoded.compare(
                    leftSegment, leftBytes.length, rightSegment, rightBytes.length));
        } finally {
            pool.free(leftSegment);
            pool.free(rightSegment);
            pool.trim();
        }
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
            assertTrue(store.runsCreated() > 1, "test must create multiple disk runs");
            return output;
        } finally {
            buffer.close();
            store.close();
            assertEquals(0, memory.reservedBytes(), "sort memory release");
            java.io.File[] files = spillDir.toFile().listFiles();
            assertEquals(0, files == null ? 0 : files.length, "spill payload cleanup");
        }
    }

    private static String fingerprint(List<DataRow> rows) {
        StringBuilder result = new StringBuilder();
        for (DataRow row : rows) {
            BigDecimal value = (BigDecimal) row.get(0);
            result.append(value == null ? "null" : value.toPlainString())
                    .append('@').append(value == null ? "-" : value.scale())
                    .append('|').append(row.get(1)).append('\n');
        }
        return result.toString();
    }

    private static long randomCompactUnscaled(Random random) {
        long value = random.nextLong() % 1_000_000_000_000_000_000L;
        return value == -1_000_000_000_000_000_000L ? 0 : value;
    }

    private static void putLong(byte[] target, int offset, long value) {
        for (int i = 0; i < 8; i++) {
            target[offset + i] = (byte) (value >>> (56 - i * 8));
        }
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        for (int i = 0; i < values.length; i++) row.set(i, values[i]);
        return row;
    }

    private static String source(String file) throws Exception {
        Path cwd = Paths.get("").toAbsolutePath();
        Path path = cwd.resolve("src/main/java/io/rowweave/mem").resolve(file);
        if (!Files.exists(path)) {
            path = cwd.resolve("rowweave-core/src/main/java/io/rowweave/mem").resolve(file);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
