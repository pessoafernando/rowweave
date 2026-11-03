package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded cells, deterministic failures and cancellation lifecycle. */
class JdbcSnapshotInputLifecycleTest {

    private static final String QUERY = digest('a');
    private static final String PARTITION = digest('b');
    private static final String SNAPSHOT = digest('c');

    @AfterEach
    void resetProvider() {
        LifecycleProvider.handle = null;
        LifecycleProvider.openFailure = null;
        LifecycleProvider.lastRequest = null;
    }

    @Test
    void passesHardExecutionHintsAndEarlyCloseIsIdempotent() {
        DataSchema meta = new DataSchema(new Field("name", DataType.StringType));
        TestResultSet rows = resultSet(Types.VARCHAR, new Object[0][0]);
        LifecycleHandle handle = handle(meta, rows);
        LifecycleProvider.handle = handle;
        JdbcSnapshotInputExecutor executor = executor(meta, 777, 29, 2048);

        executor.open();
        assertEquals(777, LifecycleProvider.lastRequest.fetchSize());
        assertEquals(29, LifecycleProvider.lastRequest.queryTimeoutSeconds());
        assertEquals(2048, LifecycleProvider.lastRequest.maxCellBytes());
        executor.close();
        executor.close();

        assertEquals(1, handle.closeCalls);
        IllegalStateException closed = assertThrows(
                IllegalStateException.class, executor::hasNext);
        assertEquals("JDBC_EXECUTOR_CLOSED", closed.getMessage());
    }

    @Test
    void enforcesUtf8AndDecimalCanonicalCellLimitsWithoutRounding() {
        DataSchema stringMeta = new DataSchema(new Field("text", DataType.StringType));
        String exact = repeat("\u754C", 341) + "x";
        TestResultSet exactRows = resultSet(Types.VARCHAR,
                new Object[][]{{exact}});
        LifecycleProvider.handle = handle(stringMeta, exactRows);
        JdbcSnapshotInputExecutor exactExecutor = executor(
                stringMeta, 1, 1, JdbcSnapshotSource.MIN_CELL_BYTES);
        exactExecutor.open();
        assertEquals(exact, exactExecutor.next().getString(0));

        String oversized = repeat("\u754C", 342);
        TestResultSet oversizedRows = resultSet(Types.VARCHAR,
                new Object[][]{{oversized}});
        LifecycleHandle oversizedHandle = handle(stringMeta, oversizedRows);
        LifecycleProvider.handle = oversizedHandle;
        JdbcSnapshotInputExecutor oversizedExecutor = executor(
                stringMeta, 1, 1, JdbcSnapshotSource.MIN_CELL_BYTES);
        oversizedExecutor.open();
        assertFailure("JDBC_CELL_LIMIT_EXCEEDED", oversizedExecutor::hasNext);
        assertEquals(1, oversizedHandle.closeCalls);

        DataSchema decimalMeta = new DataSchema(
                new Field("amount", DataType.BigDecimalType));
        BigDecimal hugeScale = new BigDecimal(BigInteger.ONE, -2048);
        TestResultSet decimalRows = resultSet(Types.DECIMAL,
                new Object[][]{{hugeScale}});
        LifecycleHandle decimalHandle = handle(decimalMeta, decimalRows);
        LifecycleProvider.handle = decimalHandle;
        JdbcSnapshotInputExecutor decimalExecutor = executor(
                decimalMeta, 1, 1, JdbcSnapshotSource.MIN_CELL_BYTES);
        decimalExecutor.open();
        assertFailure("JDBC_CELL_LIMIT_EXCEEDED", decimalExecutor::hasNext);
        assertEquals(1, decimalHandle.closeCalls);
    }

    @Test
    void sanitizesProviderNextAndGetterFailuresAndClosesOnce() {
        DataSchema meta = new DataSchema(new Field("name", DataType.StringType));
        LifecycleProvider.openFailure = new IllegalStateException(
                "jdbc:vendor://secret-host password=secret");
        assertFailure("JDBC_OPEN_FAILED", () -> executor(meta, 1, 1, 1024).open());

        TestResultSet nextFailureRows = resultSet(Types.VARCHAR,
                new Object[][]{{"unused"}});
        nextFailureRows.nextFailure = new SQLException("secret query and token");
        LifecycleHandle nextHandle = handle(meta, nextFailureRows);
        LifecycleProvider.openFailure = null;
        LifecycleProvider.handle = nextHandle;
        JdbcSnapshotInputExecutor nextExecutor = executor(meta, 1, 1, 1024);
        nextExecutor.open();
        assertFailure("JDBC_READ_FAILED", nextExecutor::hasNext);
        assertEquals(1, nextHandle.closeCalls);

        TestResultSet getterFailureRows = resultSet(Types.VARCHAR,
                new Object[][]{{"unused"}});
        getterFailureRows.getterFailure = new SQLException("raw SQL secret");
        LifecycleHandle getterHandle = handle(meta, getterFailureRows);
        LifecycleProvider.handle = getterHandle;
        JdbcSnapshotInputExecutor getterExecutor = executor(meta, 1, 1, 1024);
        getterExecutor.open();
        assertFailure("JDBC_READ_FAILED", getterExecutor::hasNext);
        assertEquals(1, getterHandle.closeCalls);
    }

    @Test
    void reportsStandaloneCloseFailureAndSuppressesItBehindReadFailure() {
        DataSchema meta = new DataSchema(new Field("name", DataType.StringType));
        TestResultSet earlyRows = resultSet(Types.VARCHAR, new Object[0][0]);
        LifecycleHandle earlyHandle = handle(meta, earlyRows);
        earlyHandle.closeFailure = new IllegalStateException("close secret");
        LifecycleProvider.handle = earlyHandle;
        JdbcSnapshotInputExecutor earlyExecutor = executor(meta, 1, 1, 1024);
        earlyExecutor.open();
        assertFailure("JDBC_CLOSE_FAILED", earlyExecutor::close);
        earlyExecutor.close();
        assertEquals(1, earlyHandle.closeCalls);

        TestResultSet failedRows = resultSet(Types.VARCHAR,
                new Object[][]{{"unused"}});
        failedRows.nextFailure = new SQLException("read secret");
        LifecycleHandle failedHandle = handle(meta, failedRows);
        failedHandle.closeFailure = new IllegalStateException("close secret");
        LifecycleProvider.handle = failedHandle;
        JdbcSnapshotInputExecutor failedExecutor = executor(meta, 1, 1, 1024);
        failedExecutor.open();
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, failedExecutor::hasNext);
        assertEquals("JDBC_READ_FAILED", failure.getMessage());
        assertEquals(1, failure.getSuppressed().length);
        assertEquals("JDBC_CLOSE_FAILED", failure.getSuppressed()[0].getMessage());
        assertFalse(failure.toString().contains("secret"));
        assertEquals(1, failedHandle.closeCalls);
    }

    @Test
    void concurrentCloseInterruptsBlockedResultSetNext() throws Exception {
        DataSchema meta = new DataSchema(new Field("name", DataType.StringType));
        TestResultSet blockedRows = resultSet(Types.VARCHAR,
                new Object[][]{{"must-not-escape"}});
        blockedRows.nextEntered = new CountDownLatch(1);
        blockedRows.nextRelease = new CountDownLatch(1);
        blockedRows.failAfterRelease = true;
        LifecycleHandle handle = handle(meta, blockedRows);
        handle.closeAction = blockedRows.nextRelease::countDown;
        LifecycleProvider.handle = handle;
        JdbcSnapshotInputExecutor executor = executor(meta, 1, 1, 1024);
        executor.open();

        ExecutorService reader = Executors.newSingleThreadExecutor();
        try {
            Future<String> outcome = reader.submit(() -> {
                try {
                    executor.hasNext();
                    return "ROW_ESCAPED";
                } catch (IllegalStateException expected) {
                    return expected.getMessage();
                }
            });
            assertTrue(blockedRows.nextEntered.await(5, TimeUnit.SECONDS));
            executor.close();
            assertEquals("JDBC_READ_FAILED", outcome.get(5, TimeUnit.SECONDS));
            assertEquals(1, handle.closeCalls);
        } finally {
            reader.shutdownNow();
            assertTrue(reader.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    private static JdbcSnapshotInputExecutor executor(
            DataSchema meta, int fetchSize, int timeout, int maxCellBytes) {
        return (JdbcSnapshotInputExecutor) SourceExecutorFactory.create(
                new JdbcSnapshotSource(
                        meta, new LifecycleProvider(), "endpoint", "credential",
                        "dataset", "query", "partition", "snapshot",
                        QUERY, PARTITION, SNAPSHOT,
                        fetchSize, timeout, maxCellBytes));
    }

    private static LifecycleHandle handle(DataSchema meta, TestResultSet rows) {
        return new LifecycleHandle(SchemaFingerprint.of(meta), rows.resultSet);
    }

    private static TestResultSet resultSet(int type, Object[][] rows) {
        return new TestResultSet(new int[]{type}, rows);
    }

    private static void assertFailure(String code, Runnable operation) {
        IllegalStateException failure = assertThrows(
                IllegalStateException.class, operation::run);
        assertEquals(code, failure.getMessage());
        assertFalse(failure.toString().contains("secret"));
    }

    private static String digest(char value) {
        return repeat(String.valueOf(value), 64);
    }

    private static String repeat(String value, int count) {
        StringBuilder result = new StringBuilder(value.length() * count);
        for (int i = 0; i < count; i++) result.append(value);
        return result.toString();
    }

    public static final class LifecycleProvider
            implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        static LifecycleHandle handle;
        static RuntimeException openFailure;
        static JdbcSnapshotReadRequest lastRequest;
        public LifecycleProvider() { }
        @Override public String providerType() { return "lifecycle-jdbc"; }
        @Override public int providerVersion() { return 1; }
        @Override public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            lastRequest = request;
            if (openFailure != null) throw openFailure;
            return handle;
        }
    }

    private static final class LifecycleHandle implements JdbcSnapshotReadHandle {
        private final String schemaFingerprint;
        private final ResultSet resultSet;
        private int closeCalls;
        private RuntimeException closeFailure;
        private Runnable closeAction;

        private LifecycleHandle(String schemaFingerprint, ResultSet resultSet) {
            this.schemaFingerprint = schemaFingerprint;
            this.resultSet = resultSet;
        }

        @Override public String providerType() { return "lifecycle-jdbc"; }
        @Override public int providerVersion() { return 1; }
        @Override public String queryDigest() { return QUERY; }
        @Override public String partitionDigest() { return PARTITION; }
        @Override public String snapshotId() { return SNAPSHOT; }
        @Override public String schemaFingerprint() { return schemaFingerprint; }
        @Override public ResultSet resultSet() { return resultSet; }
        @Override public void close() {
            closeCalls++;
            if (closeAction != null) closeAction.run();
            if (closeFailure != null) throw closeFailure;
        }
    }

    private static final class TestResultSet implements InvocationHandler {
        private final int[] types;
        private final Object[][] rows;
        private final ResultSet resultSet;
        private int row = -1;
        private boolean lastWasNull;
        private SQLException nextFailure;
        private SQLException getterFailure;
        private CountDownLatch nextEntered;
        private CountDownLatch nextRelease;
        private boolean failAfterRelease;

        private TestResultSet(int[] types, Object[][] rows) {
            this.types = types;
            this.rows = rows;
            this.resultSet = (ResultSet) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{ResultSet.class}, this);
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            String name = method.getName();
            if (name.equals("getMetaData")) return metadata(types);
            if (name.equals("next")) {
                if (nextEntered != null) nextEntered.countDown();
                if (nextRelease != null) nextRelease.await();
                if (failAfterRelease) throw new SQLException("cancel secret");
                if (nextFailure != null) throw nextFailure;
                row++;
                return row < rows.length;
            }
            if (name.equals("wasNull")) return lastWasNull;
            if (name.equals("getString") || name.equals("getBigDecimal")) {
                if (getterFailure != null) throw getterFailure;
                Object value = rows[row][((Integer) args[0]) - 1];
                lastWasNull = value == null;
                return value;
            }
            if (name.equals("toString")) return "LifecycleResultSet";
            if (name.equals("hashCode")) return System.identityHashCode(proxy);
            if (name.equals("equals")) return proxy == args[0];
            throw new UnsupportedOperationException("unexpected ResultSet method: " + name);
        }
    }

    private static ResultSetMetaData metadata(final int[] types) {
        return (ResultSetMetaData) Proxy.newProxyInstance(
                JdbcSnapshotInputLifecycleTest.class.getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("getColumnCount")) return types.length;
                    if (method.getName().equals("getColumnType")) {
                        return types[((Integer) args[0]) - 1];
                    }
                    if (method.getName().equals("toString")) return "LifecycleMetadata";
                    if (method.getName().equals("hashCode")) {
                        return System.identityHashCode(proxy);
                    }
                    if (method.getName().equals("equals")) return proxy == args[0];
                    throw new UnsupportedOperationException(
                            "unexpected metadata method: " + method.getName());
                });
    }
}
