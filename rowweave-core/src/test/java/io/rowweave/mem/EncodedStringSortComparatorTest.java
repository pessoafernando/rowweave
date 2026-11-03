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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncodedStringSortComparatorTest {

    private static final Object[] VALUES = {
            null,
            "",
            "\u0000",
            "?",
            "adaptor-key-000001",
            "adaptor-key-000002",
            "e\u0301",
            "é",
            "\u4E2D\u6587",
            "\uD800\uDC00",       // U+10000, UTF-16 first unit D800
            "\uE000",             // BMP private use; Java orders U+10000 before this
            "😀",
            "😀-adaptor",
            "😁",
            "\uD800",             // isolated surrogate -> RowSerializer canonical '?'
            "\uDC00"
    };

    @Test
    void serializedUtf8ComparisonMatchesCanonicalJavaUtf16ForAllNullPoliciesAndDirections() {
        DataSchema meta = new DataSchema(new Field("k", DataType.StringType));
        for (SortField.Direction direction : SortField.Direction.values()) {
            for (SortField.NullOrder nullOrder : SortField.NullOrder.values()) {
                SortField[] fields = {new SortField(0, direction, nullOrder)};
                RowComparator authority = new RowComparator(meta, fields);
                EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                        meta, authority, fields);
                for (Object left : VALUES) {
                    for (Object right : VALUES) {
                        assertCanonicalSign(meta, authority, encoded, row(left), row(right));
                    }
                }
            }
        }
    }

    @Test
    void usesJavaUtf16CodeUnitOrderRatherThanUnicodeCodePointOrUtf8ByteOrder() {
        DataSchema meta = new DataSchema(new Field("k", DataType.StringType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);

        int compared = compareBoth(meta, encoded, row("\uD800\uDC00"), row("\uE000"));
        assertEquals(-1, Integer.signum(compared), "Java compares high surrogate D800 before E000");
        int supplementaryFirstByte = "\uD800\uDC00".getBytes(StandardCharsets.UTF_8)[0] & 0xFF;
        int bmpFirstByte = "\uE000".getBytes(StandardCharsets.UTF_8)[0] & 0xFF;
        assertTrue(supplementaryFirstByte > bmpFirstByte,
                "UTF-8 byte order must be opposite to Java UTF-16 order for this pair");
    }

    @Test
    void groupKeyAndFollowingFixedFieldUseTheSameCanonicalStringOrder() {
        DataSchema meta = new DataSchema(
                new Field("adaptor", DataType.StringType),
                new Field("period", DataType.LongType));
        int[] indices = {0, 1};
        SortField[] fields = {
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        GroupKeyComparator authority = new GroupKeyComparator(meta, indices);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);
        DataRow[] rows = {
                row("adaptor-😀", 1L), row("adaptor-😀", 2L),
                row("adaptor-\uE000", 0L), row("adaptor-\uD800\uDC00", 0L),
                row(null, 0L), row(null, null)
        };
        for (DataRow left : rows) {
            for (DataRow right : rows) {
                assertCanonicalSign(meta, authority, encoded, left, right);
            }
        }
    }

    @Test
    void randomizedUnicodePairsMatchCanonicalJavaComparator() {
        DataSchema meta = new DataSchema(new Field("k", DataType.StringType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);
        RowSerializer serializer = new RowSerializer(meta);
        Random random = new Random(0xA100L);
        for (int i = 0; i < 5_000; i++) {
            DataRow left = row(randomString(random));
            DataRow right = row(randomString(random));
            DataRow canonicalLeft = serializer.deserialize(serializer.serialize(left));
            DataRow canonicalRight = serializer.deserialize(serializer.serialize(right));
            int expected = Integer.signum(authority.compare(canonicalLeft, canonicalRight));
            int actual = Integer.signum(encoded.compare(
                    serializer.serialize(left), serializer.serialize(right)));
            assertEquals(expected, actual, () -> "left=" + left.get(0) + " right=" + right.get(0));
        }
    }

    @Test
    void commonPrefixUnicodeKeysRemainStableAcrossPointerAndPipelinedDiskRuns(
            @TempDir Path tempDir) {
        DataSchema meta = new DataSchema(
                new Field("adaptor", DataType.StringType),
                new Field("seq", DataType.LongType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        RowSerializer serializer = new RowSerializer(meta);
        List<DataRow> input = new ArrayList<>();
        String[] suffixes = {"0001", "0002", "😀", "\uD800\uDC00", "\uE000", "\u4E2D\u6587", null};
        for (int i = 0; i < 2_000; i++) {
            String suffix = suffixes[(i * 31) % suffixes.length];
            input.add(row(suffix == null ? null : "adaptor-common-prefix-" + suffix, (long) i));
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
        assertEquals(fingerprint(expected), fingerprint(pointer), "Pointer String spill order");
        assertEquals(fingerprint(expected), fingerprint(pipelined), "Pipelined String spill order");
    }

    @Test
    void byteNativeStringPrefixMatchesResidentPrefixIncludingIsolatedSurrogateCanonicalization() {
        DataSchema meta = new DataSchema(
                new Field("pad", DataType.BigDecimalType),
                new Field("k", DataType.StringType));
        RowSerializer serializer = new RowSerializer(meta);
        for (SortField.Direction direction : SortField.Direction.values()) {
            for (SortField.NullOrder nullOrder : SortField.NullOrder.values()) {
                SortField[] fields = {new SortField(1, direction, nullOrder)};
                NormalizedKeyComputer keys = NormalizedKeyComputer.of(meta, fields);
                for (Object value : VALUES) {
                    DataRow original = row(new java.math.BigDecimal("123.45"), value);
                    byte[] bytes = serializer.serialize(original);
                    DataRow canonical = serializer.deserialize(bytes);
                    assertEquals(keys.computePrefix(canonical),
                            keys.computePrefixFromBytes(bytes, meta),
                            () -> "byte prefix mismatch for value=" + value);
                    assertEquals(keys.computePrefix(canonical), keys.computePrefix(original),
                            () -> "resident prefix must use serialized canonical form for value=" + value);
                }
            }
        }
    }

    @Test
    void malformedUtf8AndLengthAreRejectedInsteadOfSilentlyReplaced() {
        DataSchema meta = new DataSchema(new Field("k", DataType.StringType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                meta, new RowComparator(meta, fields), fields);
        byte[] valid = new RowSerializer(meta).serialize(row("valid"));

        assertCorrupt(encoded, valid, stringRow(-1));
        assertCorrupt(encoded, valid, stringRow(2, 0x61));                 // truncated
        assertCorrupt(encoded, valid, stringRow(2, 0xC0, 0x80));           // overlong NUL
        assertCorrupt(encoded, valid, stringRow(3, 0xED, 0xA0, 0x80));     // UTF-8 surrogate
        assertCorrupt(encoded, valid, stringRow(4, 0xF4, 0x90, 0x80, 0x80)); // > U+10FFFF
        assertCorrupt(encoded, valid, stringRow(1, 0x80));                 // stray continuation
    }

    @Test
    void stringFastPathSourceDoesNotConstructDecodedObjects() throws Exception {
        String source = source("SerializedStringOrder.java");
        int decoder = source.indexOf("compareUtf8AsUtf16");
        assertTrue(decoder >= 0, "String fast comparator must exist");
        String tail = source.substring(decoder);
        assertTrue(!tail.contains("new String(") && !tail.contains("new DataRow(")
                        && !tail.contains("getBytes(") && !tail.contains("deserialize("),
                "encoded String comparator must not allocate decoded row/string/byte arrays");
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
                () -> "left=" + left + " right=" + right + " actual=" + actual);
    }

    private static int compareBoth(DataSchema meta, EncodedSortKeyComparator encoded,
                                   DataRow left, DataRow right) {
        RowSerializer serializer = new RowSerializer(meta);
        byte[] leftBytes = serializer.serialize(left);
        byte[] rightBytes = serializer.serialize(right);
        int arrayResult = encoded.compare(leftBytes, rightBytes);

        HeapPool pool = new HeapPool();
        MemorySegment leftSegment = pool.allocate(Math.max(1, leftBytes.length));
        MemorySegment rightSegment = pool.allocate(Math.max(1, rightBytes.length));
        try {
            leftSegment.putBytes(0, leftBytes);
            rightSegment.putBytes(0, rightBytes);
            int segmentResult = encoded.compare(
                    leftSegment, leftBytes.length, rightSegment, rightBytes.length);
            assertEquals(arrayResult, segmentResult, "segment/array String order");
            return arrayResult;
        } finally {
            pool.free(leftSegment);
            pool.free(rightSegment);
            pool.trim();
        }
    }

    private static void assertCorrupt(EncodedSortKeyComparator encoded,
                                      byte[] valid, byte[] corrupt) {
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(corrupt, valid));
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
            assertEquals(0, memory.reservedBytes(), "sort memory must be fully released");
            java.io.File[] files = spillDir.toFile().listFiles();
            assertEquals(0, files == null ? 0 : files.length, "spill payload cleanup");
        }
    }

    private static String fingerprint(List<DataRow> rows) {
        StringBuilder result = new StringBuilder();
        for (DataRow row : rows) {
            result.append(row.get(0)).append('|').append(row.get(1)).append('\n');
        }
        return result.toString();
    }

    private static String randomString(Random random) {
        int units = random.nextInt(20);
        StringBuilder value = new StringBuilder();
        for (int i = 0; i < units; i++) {
            switch (random.nextInt(6)) {
                case 0:
                case 1:
                    value.append((char) random.nextInt(0x80));
                    break;
                case 2:
                    value.append((char) (0x80 + random.nextInt(0x700)));
                    break;
                case 3:
                    value.append((char) (0x4E00 + random.nextInt(0x2000)));
                    break;
                case 4:
                    value.appendCodePoint(0x10000 + random.nextInt(0x100000));
                    break;
                default:
                    value.append(random.nextBoolean() ? '\uD800' : '\uDC00');
                    break;
            }
        }
        return value.toString();
    }

    private static byte[] stringRow(int declaredLength, int... payload) {
        byte[] row = new byte[1 + 4 + payload.length];
        row[1] = (byte) (declaredLength >>> 24);
        row[2] = (byte) (declaredLength >>> 16);
        row[3] = (byte) (declaredLength >>> 8);
        row[4] = (byte) declaredLength;
        for (int i = 0; i < payload.length; i++) {
            row[5 + i] = (byte) payload[i];
        }
        return row;
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
