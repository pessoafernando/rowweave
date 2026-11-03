package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.Calendar;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** strict metadata and one-row-at-a-time JDBC mapping. */
class JdbcSnapshotInputExecutorTest {

    private static final String QUERY = repeat('a');
    private static final String PARTITION = repeat('b');
    private static final String SNAPSHOT = repeat('c');

    @AfterEach
    void clearProvider() {
        TestProvider.nextHandle = null;
    }

    @Test
    void streamsSupportedValuesWithoutRoundingAndUsesUtcCalendar() {
        DataSchema meta = new DataSchema(
                new Field("active", DataType.BooleanType),
                new Field("count", DataType.IntegerType),
                new Field("sequence", DataType.LongType),
                new Field("amount", DataType.BigDecimalType),
                new Field("memo", DataType.StringType),
                new Field("business_date", DataType.DateType),
                new Field("created_at", DataType.TimestampType));
        BigDecimal amount = new BigDecimal("1234567890.12345678901234567890");
        java.sql.Date date = java.sql.Date.valueOf("2025-03-09");
        java.sql.Timestamp timestamp = java.sql.Timestamp.valueOf(
                "2025-03-09 01:02:03.123456789");
        FakeResultSet rows = new FakeResultSet(
                new int[]{Types.BOOLEAN, Types.INTEGER, Types.BIGINT,
                        Types.DECIMAL, Types.NVARCHAR, Types.DATE, Types.TIMESTAMP},
                new Object[][]{{true, 42L, Long.MAX_VALUE, amount,
                        "unicode-α", date, timestamp}});
        TestHandle handle = handle(meta, rows);
        TestProvider.nextHandle = handle;

        JdbcSnapshotInputExecutor executor = executor(meta);
        executor.open();
        assertTrue(executor.hasNext());
        DataRow row = executor.next();

        assertEquals(Boolean.TRUE, row.getBoolean(0));
        assertEquals(Integer.valueOf(42), row.getInteger(1));
        assertEquals(Long.valueOf(Long.MAX_VALUE), row.getLong(2));
        assertSame(amount, row.getBigDecimal(3), "source must not round BigDecimal");
        assertEquals("unicode-α", row.getString(4));
        assertSame(date, row.getDate(5));
        assertSame(timestamp, row.getTimestamp(6));
        assertEquals(123456789, row.getTimestamp(6).getNanos());
        assertEquals("UTC", rows.dateCalendarZone);
        assertEquals("UTC", rows.timestampCalendarZone);
        assertFalse(executor.hasNext());
        assertEquals(1, handle.closeCalls);
    }

    @Test
    void preservesSqlNullAndCachesExactlyOneLookaheadWithFreshRows() {
        DataSchema meta = new DataSchema(
                new Field("id", DataType.LongType),
                new Field("name", DataType.StringType));
        FakeResultSet rows = new FakeResultSet(
                new int[]{Types.BIGINT, Types.VARCHAR},
                new Object[][]{{null, null}, {7L, "seven"}});
        TestProvider.nextHandle = handle(meta, rows);
        JdbcSnapshotInputExecutor executor = executor(meta);
        executor.open();

        assertTrue(executor.hasNext());
        assertTrue(executor.hasNext());
        assertEquals(1, rows.nextCalls, "hasNext must not advance twice");
        DataRow first = executor.next();
        assertNull(first.get(0));
        assertNull(first.get(1));

        assertTrue(executor.hasNext());
        DataRow second = executor.next();
        assertNotSame(first, second);
        assertEquals(Long.valueOf(7L), second.getLong(0));
        assertEquals("seven", second.getString(1));
        assertFalse(executor.hasNext());
        assertFalse(executor.hasNext());
        assertEquals(3, rows.nextCalls, "EOF must also be cached");
        assertThrows(NoSuchElementException.class, executor::next);
    }

    @Test
    void rejectsHandleIdentitySchemaAndColumnCountBeforeFirstRow() {
        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));

        FakeResultSet identityRows = rows(Types.BIGINT, 1L);
        TestProvider.nextHandle = new TestHandle(
                "other-provider", 3, QUERY, PARTITION, SNAPSHOT,
                SchemaFingerprint.of(meta), identityRows.resultSet);
        assertOpenFailure(meta, "JDBC_IDENTITY_MISMATCH", identityRows);

        FakeResultSet schemaRows = rows(Types.BIGINT, 1L);
        TestProvider.nextHandle = new TestHandle(
                "test-jdbc", 3, QUERY, PARTITION, SNAPSHOT,
                repeat('d'), schemaRows.resultSet);
        assertOpenFailure(meta, "JDBC_SCHEMA_MISMATCH", schemaRows);

        FakeResultSet countRows = new FakeResultSet(
                new int[]{Types.BIGINT, Types.VARCHAR},
                new Object[][]{{1L, "extra"}});
        TestProvider.nextHandle = handle(meta, countRows);
        assertOpenFailure(meta, "JDBC_SCHEMA_MISMATCH", countRows);
    }

    @Test
    void rejectsUnsupportedJdbcAndAlgoTypesBeforeFirstRow() {
        DataSchema stringMeta = new DataSchema(new Field("payload", DataType.StringType));
        FakeResultSet binary = rows(Types.BLOB, "not-text");
        TestProvider.nextHandle = handle(stringMeta, binary);
        assertOpenFailure(stringMeta, "JDBC_SCHEMA_MISMATCH", binary);

        DataSchema bitBooleanMeta = new DataSchema(
                new Field("flag", DataType.BooleanType));
        FakeResultSet bit = rows(Types.BIT, true);
        TestProvider.nextHandle = handle(bitBooleanMeta, bit);
        assertOpenFailure(bitBooleanMeta, "JDBC_SCHEMA_MISMATCH", bit);

        DataSchema nullMeta = new DataSchema(new Field("unsupported", DataType.NullType));
        FakeResultSet nullType = rows(Types.NULL, null);
        TestProvider.nextHandle = handle(nullMeta, nullType);
        assertOpenFailure(nullMeta, "JDBC_SCHEMA_MISMATCH", nullType);
    }

    @Test
    void rejectsIntegerOverflowWithoutReturningPartialRow() {
        DataSchema meta = new DataSchema(new Field("count", DataType.IntegerType));
        FakeResultSet rows = rows(Types.INTEGER, (long) Integer.MAX_VALUE + 1L);
        TestHandle handle = handle(meta, rows);
        TestProvider.nextHandle = handle;
        JdbcSnapshotInputExecutor executor = executor(meta);
        executor.open();

        IllegalStateException failure = assertThrows(
                IllegalStateException.class, executor::hasNext);
        assertEquals("JDBC_VALUE_OUT_OF_RANGE", failure.getMessage());
        assertEquals(1, handle.closeCalls);
        assertFalse(rows.closed, "handle owns ResultSet closure");
    }

    private static void assertOpenFailure(
            DataSchema meta, String code, FakeResultSet rows) {
        TestHandle handle = TestProvider.nextHandle;
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, () -> executor(meta).open());
        assertEquals(code, failure.getMessage());
        assertEquals(0, rows.nextCalls);
        assertEquals(1, handle.closeCalls);
    }

    private static JdbcSnapshotInputExecutor executor(DataSchema meta) {
        return (JdbcSnapshotInputExecutor) SourceExecutorFactory.create(
                new JdbcSnapshotSource(
                        meta, new TestProvider(), "endpoint", "credential", "dataset",
                        "query", "partition", "snapshot", QUERY, PARTITION, SNAPSHOT,
                        JdbcSnapshotSource.DEFAULT_FETCH_SIZE,
                        JdbcSnapshotSource.DEFAULT_QUERY_TIMEOUT_SECONDS,
                        JdbcSnapshotSource.DEFAULT_MAX_CELL_BYTES));
    }

    private static TestHandle handle(DataSchema meta, FakeResultSet rows) {
        return new TestHandle(
                "test-jdbc", 3, QUERY, PARTITION, SNAPSHOT,
                SchemaFingerprint.of(meta), rows.resultSet);
    }

    private static FakeResultSet rows(int type, Object value) {
        return new FakeResultSet(new int[]{type}, new Object[][]{{value}});
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    public static final class TestProvider implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        static TestHandle nextHandle;
        public TestProvider() { }
        @Override public String providerType() { return "test-jdbc"; }
        @Override public int providerVersion() { return 3; }
        @Override public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            TestHandle result = nextHandle;
            if (result == null) throw new IllegalStateException("test handle missing");
            return result;
        }
    }

    private static final class TestHandle implements JdbcSnapshotReadHandle {
        private final String providerType;
        private final int providerVersion;
        private final String queryDigest;
        private final String partitionDigest;
        private final String snapshotId;
        private final String schemaFingerprint;
        private final ResultSet resultSet;
        private int closeCalls;

        private TestHandle(
                String providerType, int providerVersion, String queryDigest,
                String partitionDigest, String snapshotId,
                String schemaFingerprint, ResultSet resultSet) {
            this.providerType = providerType;
            this.providerVersion = providerVersion;
            this.queryDigest = queryDigest;
            this.partitionDigest = partitionDigest;
            this.snapshotId = snapshotId;
            this.schemaFingerprint = schemaFingerprint;
            this.resultSet = resultSet;
        }

        @Override public String providerType() { return providerType; }
        @Override public int providerVersion() { return providerVersion; }
        @Override public String queryDigest() { return queryDigest; }
        @Override public String partitionDigest() { return partitionDigest; }
        @Override public String snapshotId() { return snapshotId; }
        @Override public String schemaFingerprint() { return schemaFingerprint; }
        @Override public ResultSet resultSet() { return resultSet; }
        @Override public void close() { closeCalls++; }
    }

    private static final class FakeResultSet implements InvocationHandler {
        private final int[] columnTypes;
        private final Object[][] rows;
        private final ResultSet resultSet;
        private int row = -1;
        private int nextCalls;
        private boolean lastWasNull;
        private boolean closed;
        private String dateCalendarZone;
        private String timestampCalendarZone;

        private FakeResultSet(int[] columnTypes, Object[][] rows) {
            this.columnTypes = columnTypes;
            this.rows = rows;
            this.resultSet = (ResultSet) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{ResultSet.class}, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String name = method.getName();
            if (name.equals("getMetaData")) return metadata(columnTypes);
            if (name.equals("next")) {
                nextCalls++;
                row++;
                return row < rows.length;
            }
            if (name.equals("wasNull")) return lastWasNull;
            if (name.equals("close")) {
                closed = true;
                return null;
            }
            if (name.equals("isClosed")) return closed;
            if (name.equals("getBoolean") || name.equals("getLong")
                    || name.equals("getBigDecimal") || name.equals("getString")
                    || name.equals("getDate") || name.equals("getTimestamp")) {
                int column = ((Integer) args[0]) - 1;
                Object value = rows[row][column];
                lastWasNull = value == null;
                if (name.equals("getDate") && args.length == 2) {
                    dateCalendarZone = zone((Calendar) args[1]);
                }
                if (name.equals("getTimestamp") && args.length == 2) {
                    timestampCalendarZone = zone((Calendar) args[1]);
                }
                if (value != null) return value;
                if (name.equals("getBoolean")) return false;
                if (name.equals("getLong")) return 0L;
                return null;
            }
            if (name.equals("toString")) return "FakeResultSet";
            if (name.equals("hashCode")) return System.identityHashCode(proxy);
            if (name.equals("equals")) return proxy == args[0];
            throw new UnsupportedOperationException("unexpected ResultSet method: " + name);
        }

        private static String zone(Calendar calendar) {
            return calendar.getTimeZone().getID();
        }
    }

    private static ResultSetMetaData metadata(final int[] types) {
        return (ResultSetMetaData) Proxy.newProxyInstance(
                JdbcSnapshotInputExecutorTest.class.getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getColumnCount")) return types.length;
                    if (method.getName().equals("getColumnType")) {
                        return types[((Integer) args[0]) - 1];
                    }
                    if (method.getName().equals("toString")) return "FakeMetadata";
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) return proxy == args[0];
                    throw new UnsupportedOperationException(
                            "unexpected metadata method: " + method.getName());
                });
    }
}
