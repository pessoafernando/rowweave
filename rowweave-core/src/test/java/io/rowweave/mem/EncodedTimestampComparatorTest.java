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
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class EncodedTimestampComparatorTest {

    @Test
    void timestampsMatchAuthorityWithoutFallbackForDirectionsAndNullPolicies() {
        DataSchema meta = new DataSchema(new Field("created_at", DataType.TimestampType));
        Timestamp[] values = {
                null,
                timestamp(-2_000L, 1),
                timestamp(-1_000L, 999_999_999),
                timestamp(0L, 0),
                timestamp(0L, 123_456_000),
                timestamp(0L, 123_456_789),
                timestamp(1_000L, 0),
                timestamp(1_000L, 999_999_999)
        };
        for (SortField.Direction direction : SortField.Direction.values()) {
            for (SortField.NullOrder nullOrder : SortField.NullOrder.values()) {
                SortField[] fields = {new SortField(0, direction, nullOrder)};
                RowComparator authority = new RowComparator(meta, fields);
                EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                        meta, authority, fields);
                for (Timestamp left : values) {
                    for (Timestamp right : values) {
                        DataRow leftRow = row(left);
                        DataRow rightRow = row(right);
                        int actual = compareBoth(meta, encoded, leftRow, rightRow);
                        assertNotEquals(EncodedSortKeyComparator.FALLBACK, actual,
                                "built-in Timestamp order must stay encoded");
                        assertEquals(Integer.signum(authority.compare(leftRow, rightRow)),
                                Integer.signum(actual),
                                () -> "left=" + left + " right=" + right);
                    }
                }
            }
        }
    }

    @Test
    void groupAndMultiFieldComparatorsContinueAfterEqualTimestamp() {
        DataSchema meta = new DataSchema(
                new Field("created_at", DataType.TimestampType),
                new Field("adaptor", DataType.StringType));
        Timestamp instant = timestamp(1_700_000_000_000L, 123_456_789);
        DataRow left = row(instant, "A");
        DataRow right = row(instant, "B");

        SortField[] rowFields = {
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                new SortField(1, SortField.Direction.DESC, SortField.NullOrder.NULLS_LAST)};
        RowComparator rows = new RowComparator(meta, rowFields);
        assertDirectAuthority(meta, rows,
                EncodedSortKeyComparator.create(meta, rows, rowFields), left, right);

        int[] indices = {0, 1};
        SortField[] groupFields = {
                new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST),
                new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        GroupKeyComparator groups = new GroupKeyComparator(meta, indices);
        int actual = compareBoth(meta,
                EncodedSortKeyComparator.create(meta, groups, groupFields), left, right);
        assertNotEquals(EncodedSortKeyComparator.FALLBACK, actual);
        assertEquals(Integer.signum(groups.compare(left, right)), Integer.signum(actual));
    }

    @Test
    void serializedPrefixMatchesRowPrefixAndRejectsNonCanonicalTimestamp() {
        DataSchema meta = new DataSchema(new Field("created_at", DataType.TimestampType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        NormalizedKeyComputer keys = NormalizedKeyComputer.of(meta, fields);
        RowSerializer serializer = new RowSerializer(meta);
        DataRow row = row(timestamp(-2_000L, 987_654_321));
        byte[] encoded = serializer.serialize(row);
        assertEquals(keys.computePrefix(row), keys.computePrefixFromBytes(encoded, meta));

        byte[] inconsistent = encoded.clone();
        putLong(inconsistent, 1, ((Timestamp) row.get(0)).getTime() + 1);
        assertThrows(IllegalArgumentException.class,
                () -> keys.computePrefixFromBytes(inconsistent, meta));
    }

    @Test
    void comparatorRejectsInvalidNanosInconsistentMillisAndTruncation() {
        DataSchema meta = new DataSchema(new Field("created_at", DataType.TimestampType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(
                meta, new RowComparator(meta, fields), fields);
        RowSerializer serializer = new RowSerializer(meta);
        byte[] valid = serializer.serialize(row(timestamp(0L, 123_456_789)));

        byte[] invalidNanos = valid.clone();
        putInt(invalidNanos, 9, 1_000_000_000);
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(invalidNanos, valid));

        byte[] inconsistent = valid.clone();
        putLong(inconsistent, 1, 124L);
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(inconsistent, valid));

        byte[] truncated = java.util.Arrays.copyOf(valid, valid.length - 1);
        assertThrows(IllegalArgumentException.class, () -> encoded.compare(truncated, valid));
    }

    @Test
    void customTimestampComparatorStillRequestsWholeRowFallback() {
        DataSchema meta = new DataSchema(new Field("created_at", DataType.TimestampType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        Comparator<DataRow> reversed = (left, right) -> ((Timestamp) right.get(0))
                .compareTo((Timestamp) left.get(0));
        EncodedSortKeyComparator encoded = EncodedSortKeyComparator.create(meta, reversed, fields);
        assertEquals(EncodedSortKeyComparator.FALLBACK, compareBoth(meta, encoded,
                row(timestamp(0L, 1)), row(timestamp(0L, 2))));
    }

    @Test
    void timestampKeysRemainStableAcrossPointerAndPipelinedDiskRuns(@TempDir Path tempDir) {
        DataSchema meta = new DataSchema(
                new Field("created_at", DataType.TimestampType),
                new Field("seq", DataType.LongType));
        SortField[] fields = {new SortField(
                0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)};
        RowComparator authority = new RowComparator(meta, fields);
        List<DataRow> input = new ArrayList<>();
        for (int index = 0; index < 2_000; index++) {
            long shuffled = (index * 2654435761L) % 1_000_000L;
            long second = index % 11 == 0 ? -2_000L : 1_700_000_000_000L;
            input.add(row(timestamp(second, (int) shuffled), (long) index));
        }
        List<DataRow> expected = new ArrayList<>(input);
        expected.sort(authority);
        assertEquals(fingerprint(expected), fingerprint(runDiskSort(
                meta, fields, authority, input, tempDir.resolve("pointer"), false)));
        assertEquals(fingerprint(expected), fingerprint(runDiskSort(
                meta, fields, authority, input, tempDir.resolve("pipelined"), true)));
    }

    @Test
    void timestampComparatorAndPrefixDoNotConstructTimestampObjects() throws Exception {
        String comparator = source("EncodedSortKeyComparator.java");
        int compareStart = comparator.indexOf("private static <T> int compareTimestamp");
        int compareEnd = comparator.indexOf("private static <T> void validateTimestampPayload",
                compareStart);
        String compareBody = comparator.substring(compareStart, compareEnd);
        assertEquals(-1, compareBody.indexOf("new Timestamp"));
        assertEquals(-1, compareBody.indexOf("deserialize"));

        String normalized = source("NormalizedKeyComputer.java");
        int timestampCase = normalized.indexOf("case TimestampType:");
        int timestampBreak = normalized.indexOf("break;", timestampCase);
        String timestampBody = normalized.substring(timestampCase, timestampBreak);
        assertTrue(timestampBody.contains("encodedTimestampMillis"));
        assertEquals(-1, timestampBody.indexOf("deserialize"));
    }

    private static Timestamp timestamp(long epochSecondMillis, int nanos) {
        Timestamp timestamp = new Timestamp(epochSecondMillis);
        timestamp.setNanos(nanos);
        return timestamp;
    }

    private static void assertDirectAuthority(DataSchema meta, Comparator<DataRow> authority,
                                              EncodedSortKeyComparator encoded,
                                              DataRow left, DataRow right) {
        int actual = compareBoth(meta, encoded, left, right);
        assertNotEquals(EncodedSortKeyComparator.FALLBACK, actual);
        assertEquals(Integer.signum(authority.compare(left, right)), Integer.signum(actual));
    }

    private static void putInt(byte[] target, int offset, int value) {
        target[offset] = (byte) (value >>> 24);
        target[offset + 1] = (byte) (value >>> 16);
        target[offset + 2] = (byte) (value >>> 8);
        target[offset + 3] = (byte) value;
    }

    private static void putLong(byte[] target, int offset, long value) {
        for (int index = 0; index < 8; index++) {
            target[offset + index] = (byte) (value >>> (56 - 8 * index));
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
            Timestamp value = (Timestamp) row.get(0);
            result.append(value.getTime()).append(':').append(value.getNanos())
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

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        for (int index = 0; index < values.length; index++) row.set(index, values[index]);
        return row;
    }
}
