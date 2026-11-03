package io.rowweave.mem;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegment;
import io.rowweave.ops.GroupKeyComparator;
import io.rowweave.ops.RowComparator;
import io.rowweave.ops.SortField;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncodedSortKeyComparatorTest {

    @Test
    void fixedWidthRowComparatorMatchesAuthoritativeSignForAllNullPoliciesAndDirections() {
        assertSingleFieldMatrix(DataType.IntegerType,
                new Object[]{null, Integer.MIN_VALUE, -1, 0, 1, Integer.MAX_VALUE});
        assertSingleFieldMatrix(DataType.LongType,
                new Object[]{null, Long.MIN_VALUE, -1L, 0L, 1L, Long.MAX_VALUE});
        assertSingleFieldMatrix(DataType.BooleanType,
                new Object[]{null, false, true});
        assertSingleFieldMatrix(DataType.DateType,
                new Object[]{null, new Date(Long.MIN_VALUE), new Date(0), new Date(Long.MAX_VALUE)});
    }

    @Test
    void locatesMultipleFixedKeysPastVariableColumnsWithoutDecodingRows() {
        DataSchema meta = new DataSchema(
                new Field("pad", DataType.StringType),
                new Field("adaptor", DataType.LongType),
                new Field("period", DataType.IntegerType));
        SortField[] fields = {
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                new SortField(2, SortField.Direction.DESC, SortField.NullOrder.DEFAULT)};
        RowComparator authority = new RowComparator(meta, fields);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);

        DataRow[] rows = {
                row("a", 7L, 1),
                row("a much wider prefix", 7L, 2),
                row("", 8L, null),
                row("ignored", null, Integer.MIN_VALUE)
        };
        assertAllPairs(meta, authority, encoded, rows, false);
    }

    @Test
    void groupKeyComparatorMatchesNullLastAndStableFixedKeyOrder() {
        DataSchema meta = new DataSchema(
                new Field("adaptor", DataType.LongType),
                new Field("period", DataType.IntegerType));
        int[] indices = {0, 1};
        SortField[] fields = {
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        GroupKeyComparator authority = new GroupKeyComparator(meta, indices);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);
        DataRow[] rows = {
                row(0L, 0), row(0L, 1), row(1L, Integer.MIN_VALUE),
                row(1L, null), row(null, 0), row(null, null)
        };

        assertAllPairs(meta, authority, encoded, rows, false);
    }

    @Test
    void complexAndCustomComparatorsAlwaysRequestAuthoritativeFallback() {
        assertFallback(DataType.NullType, 1L);

        DataSchema meta = new DataSchema(new Field("k", DataType.LongType));
        SortField[] fields = {new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        Comparator<DataRow> custom = (left, right) -> Long.compare((Long) right.get(0), (Long) left.get(0));
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, custom, fields);
        assertEncodedEquals(EncodedSortKeyComparator.FALLBACK, meta, encoded, row(1L), row(2L));

        DataSchema two = new DataSchema(new Field("a", DataType.LongType), new Field("b", DataType.LongType));
        RowComparator fieldZero = new RowComparator(two,
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST));
        SortField[] mismatched = {new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        EncodedSortKeyComparator mismatch = EncodedSortKeyComparator.create(two, fieldZero, mismatched);
        assertEncodedEquals(EncodedSortKeyComparator.FALLBACK, two, mismatch,
                row(1L, 2L), row(1L, 3L));

        DataSchema untypedPrefix = new DataSchema(
                new Field("dynamic", DataType.NullType), new Field("k", DataType.LongType));
        SortField[] second = {
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator afterUntyped = new RowComparator(untypedPrefix, second);
        EncodedSortKeyComparator unsafeOffset = EncodedSortKeyComparator.create(
                untypedPrefix, afterUntyped, second);
        assertEncodedEquals(EncodedSortKeyComparator.FALLBACK, untypedPrefix, unsafeOffset,
                row("left", 1L), row(7, 2L));
    }

    @Test
    void fixedWidthPrefixFromBytesMatchesRowPathPastVariableColumns() {
        Object[][] cases = {
                {DataType.IntegerType, Integer.MIN_VALUE},
                {DataType.LongType, Long.MAX_VALUE},
                {DataType.BooleanType, true},
                {DataType.DateType, new Date(123456789L)}
        };
        for (Object[] entry : cases) {
            DataType type = (DataType) entry[0];
            for (SortField.Direction direction : SortField.Direction.values()) {
                for (SortField.NullOrder nullOrder : SortField.NullOrder.values()) {
                    DataSchema meta = new DataSchema(
                            new Field("pad", DataType.StringType), new Field("k", type));
                    SortField[] fields = {new SortField(1, direction, nullOrder)};
                    NormalizedKeyComputer keys = NormalizedKeyComputer.of(meta, fields);
                    RowSerializer serializer = new RowSerializer(meta);
                    for (Object value : new Object[]{null, entry[1]}) {
                        DataRow row = row("variable-width-padding", value);
                        assertEquals(keys.computePrefix(row),
                                keys.computePrefixFromBytes(serializer.serialize(row), meta));
                    }
                }
            }
        }
    }

    @Test
    void residentAndMergeTiePathsAttemptEncodedComparisonBeforeRowDeserialization() throws Exception {
        String batch = source("EncodedSortBatch.java");
        int batchFast = batch.indexOf("encodedComparator.compare(");
        int batchFallback = batch.indexOf("compared == EncodedSortKeyComparator.FALLBACK", batchFast);
        int batchDecode = batch.indexOf("serializer.deserialize", batchFallback);
        assertTrue(batchFast >= 0 && batchFast < batchFallback && batchFallback < batchDecode,
                "resident tie path must decode only after encoded FALLBACK");

        String merge = source("SortedRunStore.java");
        int mergeFast = merge.indexOf("encodedComparator.compare(");
        int mergeFallback = merge.indexOf("fc == EncodedSortKeyComparator.FALLBACK", mergeFast);
        int mergeDecode = merge.indexOf("toRow(serializer)", mergeFallback);
        assertTrue(mergeFast >= 0 && mergeFast < mergeFallback && mergeFallback < mergeDecode,
                "merge tie path must decode only after encoded FALLBACK");

        String normalized = source("NormalizedKeyComputer.java");
        int fixedBranch = normalized.indexOf("canReadEncodedField(meta, fieldIndex, type)");
        int fallbackDecode = normalized.indexOf("serializer.deserialize(rowBytes)", fixedBranch);
        assertTrue(fixedBranch >= 0 && fixedBranch < fallbackDecode,
                "fixed prefix branch must precede complex-type row decode fallback");
    }

    private static void assertSingleFieldMatrix(DataType type, Object[] values) {
        DataSchema meta = new DataSchema(new Field("k", type));
        for (SortField.Direction direction : SortField.Direction.values()) {
            for (SortField.NullOrder nullOrder : SortField.NullOrder.values()) {
                SortField[] fields = {new SortField(0, direction, nullOrder)};
                RowComparator authority = new RowComparator(meta, fields);
                EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);
                DataRow[] rows = new DataRow[values.length];
                for (int i = 0; i < values.length; i++) {
                    rows[i] = row(values[i]);
                }
                assertAllPairs(meta, authority, encoded, rows, false);
            }
        }
    }

    private static void assertFallback(DataType type, Object value) {
        DataSchema meta = new DataSchema(new Field("k", type));
        SortField[] fields = {new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, authority, fields);
        assertEncodedEquals(EncodedSortKeyComparator.FALLBACK, meta, encoded, row(value), row(value));
    }

    private static void assertAllPairs(DataSchema meta, Comparator<DataRow> authority,
                                       EncodedSortKeyComparator encoded, DataRow[] rows,
                                       boolean fallbackExpected) {
        for (DataRow left : rows) {
            for (DataRow right : rows) {
                int expected = Integer.signum(authority.compare(left, right));
                int actual = compareBothRepresentations(meta, encoded, left, right);
                if (fallbackExpected) {
                    assertEquals(EncodedSortKeyComparator.FALLBACK, actual);
                } else {
                    assertEquals(expected, Integer.signum(actual),
                            () -> "left=" + left + " right=" + right);
                }
            }
        }
    }

    private static void assertEncodedEquals(int expected, DataSchema meta,
                                            EncodedSortKeyComparator encoded,
                                            DataRow left, DataRow right) {
        assertEquals(expected, compareBothRepresentations(meta, encoded, left, right));
    }

    private static int compareBothRepresentations(DataSchema meta, EncodedSortKeyComparator encoded,
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
            assertEquals(arrayResult, segmentResult, "segment/array comparison must be identical");
            return arrayResult;
        } finally {
            pool.free(leftSegment);
            pool.free(rightSegment);
            pool.trim();
        }
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        for (int i = 0; i < values.length; i++) {
            row.set(i, values[i]);
        }
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
