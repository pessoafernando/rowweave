package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** real in-memory JDBC driver conformance without a vendor support claim. */
class JdbcH2ConformanceTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.IntegerType),
            new Field("amount", DataType.BigDecimalType),
            new Field("memo", DataType.StringType),
            new Field("business_date", DataType.DateType),
            new Field("created_at", DataType.TimestampType));
    private static final String QUERY = "SELECT id, amount, memo, business_date, "
            + "created_at FROM ledger WHERE snapshot_id = ? AND partition_id = ? ORDER BY id";
    private static final String QUERY_DIGEST = sha256(QUERY);
    private static final String PARTITION_DIGEST = sha256("partition=0");
    private static final String SNAPSHOT_ID = sha256("snapshot-v1");

    @AfterEach
    void resetProvider() {
        H2SnapshotProvider.reset();
    }

    @Test
    void realDriverStreamsPreparedExactSnapshotWithNullScaleAndNanos()
            throws Exception {
        configureDatabase();
        JdbcSnapshotInputExecutor executor = executor(
                QUERY_DIGEST, META, 37, 11);

        executor.open();
        assertTrue(executor.hasNext());
        DataRow first = executor.next();
        assertEquals(Integer.valueOf(1), first.getInteger(0));
        assertEquals(new BigDecimal("123.450000000000000000"),
                first.getBigDecimal(1));
        assertEquals(18, first.getBigDecimal(1).scale());
        assertEquals("unicode-α", first.getString(2));
        assertEquals(java.time.LocalDate.of(2025, 3, 9),
                ((java.sql.Date) first.getDate(3)).toLocalDate());
        assertEquals(123456789, first.getTimestamp(4).getNanos());

        assertTrue(executor.hasNext());
        DataRow second = executor.next();
        assertNotSame(first, second);
        assertEquals(Integer.valueOf(2), second.getInteger(0));
        assertNull(second.get(1));
        assertNull(second.get(2));
        assertNull(second.get(3));
        assertNull(second.get(4));
        assertFalse(executor.hasNext());

        assertEquals(37, H2SnapshotProvider.observedFetchSize);
        assertEquals(11, H2SnapshotProvider.observedQueryTimeout);
        assertEquals(Arrays.asList("resultSet", "statement", "connection"),
                H2SnapshotProvider.closeOrder);
        assertEquals(1, H2SnapshotProvider.opens.get());
        assertEquals(1, H2SnapshotProvider.closes.get());
    }

    @Test
    void reopeningAfterNewVersionStillReadsFrozenSnapshotFromFirstRow()
            throws Exception {
        configureDatabase();
        JdbcSnapshotSource frozen = input(QUERY_DIGEST, META, 31, 9);
        JdbcSnapshotInputExecutor firstAttempt =
                (JdbcSnapshotInputExecutor) frozen.createSourceExecutor();
        firstAttempt.open();
        assertEquals(Integer.valueOf(1), firstAttempt.next().getInteger(0));
        firstAttempt.close();

        insertOtherSnapshot();
        H2SnapshotProvider.credentialGeneration.set(2);
        JdbcSnapshotInputExecutor retry =
                (JdbcSnapshotInputExecutor) frozen.createSourceExecutor();
        retry.open();
        List<Integer> ids = new ArrayList<>();
        while (retry.hasNext()) ids.add(retry.next().getInteger(0));

        assertEquals(Arrays.asList(1, 2), ids);
        assertEquals(Arrays.asList(1, 2),
                H2SnapshotProvider.openedCredentialGenerations);
        assertEquals(Arrays.asList(SNAPSHOT_ID, SNAPSHOT_ID),
                H2SnapshotProvider.openedSnapshots);
        assertEquals(2, H2SnapshotProvider.opens.get());
        assertEquals(2, H2SnapshotProvider.closes.get());
    }

    @Test
    void digestOrRealMetadataDriftFailsClosedAndDoesNotLeakDriverDetails()
            throws Exception {
        configureDatabase();
        JdbcSnapshotInputExecutor wrongDigest = executor(
                sha256("other query"), META, 10, 10);
        IllegalStateException digestFailure = assertThrows(
                IllegalStateException.class, wrongDigest::open);
        assertEquals("JDBC_OPEN_FAILED", digestFailure.getMessage());
        assertFalse(digestFailure.toString().contains("jdbc:h2"));
        assertEquals(0, H2SnapshotProvider.opens.get());

        DataSchema wrongMeta = new DataSchema(
                new Field("id", DataType.IntegerType),
                new Field("amount", DataType.StringType),
                new Field("memo", DataType.StringType),
                new Field("business_date", DataType.DateType),
                new Field("created_at", DataType.TimestampType));
        JdbcSnapshotInputExecutor schemaDrift = executor(
                QUERY_DIGEST, wrongMeta, 10, 10);
        IllegalStateException schemaFailure = assertThrows(
                IllegalStateException.class, schemaDrift::open);
        assertEquals("JDBC_SCHEMA_MISMATCH", schemaFailure.getMessage());
        assertEquals(Arrays.asList("resultSet", "statement", "connection"),
                H2SnapshotProvider.closeOrder);
        assertEquals(1, H2SnapshotProvider.opens.get());
        assertEquals(1, H2SnapshotProvider.closes.get());
    }

    private static JdbcSnapshotInputExecutor executor(
            String queryDigest, DataSchema meta, int fetch, int timeout) {
        return (JdbcSnapshotInputExecutor)
                input(queryDigest, meta, fetch, timeout).createSourceExecutor();
    }

    private static JdbcSnapshotSource input(
            String queryDigest, DataSchema meta, int fetch, int timeout) {
        return new JdbcSnapshotSource(
                meta, new H2SnapshotProvider(), "h2-endpoint", "h2-credential",
                "ledger", "ordered-ledger-query", "partition-0", "snapshot-v1",
                queryDigest, PARTITION_DIGEST, SNAPSHOT_ID,
                fetch, timeout, 4096);
    }

    private static void configureDatabase() throws Exception {
        Class.forName("org.h2.Driver");
        String url = "jdbc:h2:mem:rowweave_jdbc_"
                + UUID.randomUUID().toString().replace("-", "")
                + ";DB_CLOSE_DELAY=-1";
        H2SnapshotProvider.endpointUrls.put("h2-endpoint", url);
        H2SnapshotProvider.credentials.put(
                "h2-credential", new String[]{"sa", "test-secret"});
        H2SnapshotProvider.queries.put("ordered-ledger-query", QUERY);
        H2SnapshotProvider.partitions.put("partition-0", 0);
        H2SnapshotProvider.snapshots.put("snapshot-v1", SNAPSHOT_ID);
        H2SnapshotProvider.keepAlive =
                DriverManager.getConnection(url, "sa", "test-secret");
        try (Statement statement = H2SnapshotProvider.keepAlive.createStatement()) {
            statement.execute("CREATE TABLE ledger (snapshot_id VARCHAR(64) NOT NULL, "
                    + "partition_id INT NOT NULL, id INT NOT NULL, amount DECIMAL(38,18), "
                    + "memo VARCHAR(2000), business_date DATE, created_at TIMESTAMP(9), "
                    + "PRIMARY KEY(snapshot_id, partition_id, id))");
        }
        try (PreparedStatement insert = H2SnapshotProvider.keepAlive.prepareStatement(
                "INSERT INTO ledger VALUES (?, ?, ?, ?, ?, ?, ?)")) {
            insert.setString(1, SNAPSHOT_ID);
            insert.setInt(2, 0);
            insert.setInt(3, 1);
            insert.setBigDecimal(4, new BigDecimal("123.450000000000000000"));
            insert.setString(5, "unicode-α");
            insert.setDate(6, java.sql.Date.valueOf("2025-03-09"));
            Timestamp timestamp = Timestamp.valueOf("2025-03-09 01:02:03.123456789");
            insert.setTimestamp(7, timestamp);
            insert.executeUpdate();

            insert.setString(1, SNAPSHOT_ID);
            insert.setInt(2, 0);
            insert.setInt(3, 2);
            insert.setNull(4, java.sql.Types.DECIMAL);
            insert.setNull(5, java.sql.Types.VARCHAR);
            insert.setNull(6, java.sql.Types.DATE);
            insert.setNull(7, java.sql.Types.TIMESTAMP);
            insert.executeUpdate();
        }
    }

    private static void insertOtherSnapshot() throws SQLException {
        String other = sha256("snapshot-v2");
        try (PreparedStatement insert = H2SnapshotProvider.keepAlive.prepareStatement(
                "INSERT INTO ledger VALUES (?, 0, 99, 999.0, 'other', NULL, NULL)")) {
            insert.setString(1, other);
            insert.executeUpdate();
        }
    }

    private static String sha256(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            char[] result = new char[bytes.length * 2];
            char[] digits = "0123456789abcdef".toCharArray();
            for (int index = 0; index < bytes.length; index++) {
                result[index * 2] = digits[(bytes[index] >>> 4) & 0xf];
                result[index * 2 + 1] = digits[bytes[index] & 0xf];
            }
            return new String(result);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    public static final class H2SnapshotProvider
            implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        static final Map<String, String> endpointUrls = new HashMap<>();
        static final Map<String, String[]> credentials = new HashMap<>();
        static final Map<String, String> queries = new HashMap<>();
        static final Map<String, Integer> partitions = new HashMap<>();
        static final Map<String, String> snapshots = new HashMap<>();
        static final AtomicInteger credentialGeneration = new AtomicInteger(1);
        static final AtomicInteger opens = new AtomicInteger();
        static final AtomicInteger closes = new AtomicInteger();
        static final List<Integer> openedCredentialGenerations = new ArrayList<>();
        static final List<String> openedSnapshots = new ArrayList<>();
        static final List<String> closeOrder = new ArrayList<>();
        static Connection keepAlive;
        static int observedFetchSize;
        static int observedQueryTimeout;

        public H2SnapshotProvider() { }

        static void reset() {
            if (keepAlive != null) {
                try {
                    keepAlive.close();
                } catch (SQLException ignored) {
                    // Test cleanup only.
                }
            }
            keepAlive = null;
            endpointUrls.clear();
            credentials.clear();
            queries.clear();
            partitions.clear();
            snapshots.clear();
            credentialGeneration.set(1);
            opens.set(0);
            closes.set(0);
            openedCredentialGenerations.clear();
            openedSnapshots.clear();
            closeOrder.clear();
            observedFetchSize = 0;
            observedQueryTimeout = 0;
        }

        @Override public String providerType() { return "h2-test-conformance"; }
        @Override public int providerVersion() { return 1; }

        @Override
        public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            String query = queries.get(request.queryRef());
            Integer partition = partitions.get(request.partitionRef());
            String snapshot = snapshots.get(request.snapshotRef());
            if (query == null || partition == null || snapshot == null
                    || !sha256(query).equals(request.queryDigest())
                    || !sha256("partition=" + partition)
                    .equals(request.partitionDigest())
                    || !snapshot.equals(request.snapshotId())) {
                throw new IllegalStateException("configuration drift at jdbc:h2:secret");
            }
            String url = endpointUrls.get(request.endpointRef());
            String[] credential = credentials.get(request.credentialRef());
            Connection connection = null;
            PreparedStatement statement = null;
            ResultSet resultSet = null;
            try {
                connection = DriverManager.getConnection(
                        url, credential[0], credential[1]);
                connection.setReadOnly(true);
                statement = connection.prepareStatement(
                        query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                statement.setFetchSize(request.fetchSize());
                statement.setQueryTimeout(request.queryTimeoutSeconds());
                statement.setString(1, snapshot);
                statement.setInt(2, partition);
                resultSet = statement.executeQuery();
                observedFetchSize = statement.getFetchSize();
                observedQueryTimeout = statement.getQueryTimeout();
                opens.incrementAndGet();
                openedCredentialGenerations.add(credentialGeneration.get());
                openedSnapshots.add(snapshot);
                return new H2Handle(request, resultSet, statement, connection);
            } catch (SQLException failure) {
                closeQuietly(resultSet);
                closeQuietly(statement);
                closeQuietly(connection);
                throw new IllegalStateException("driver secret: " + failure.getMessage());
            }
        }

        private static void closeQuietly(AutoCloseable resource) {
            if (resource == null) return;
            try {
                resource.close();
            } catch (Exception ignored) {
                // The executor exposes only its fixed JDBC failure taxonomy.
            }
        }
    }

    private static final class H2Handle implements JdbcSnapshotReadHandle {
        private final JdbcSnapshotReadRequest request;
        private final ResultSet resultSet;
        private final PreparedStatement statement;
        private final Connection connection;
        private boolean closed;

        private H2Handle(
                JdbcSnapshotReadRequest request, ResultSet resultSet,
                PreparedStatement statement, Connection connection) {
            this.request = request;
            this.resultSet = resultSet;
            this.statement = statement;
            this.connection = connection;
        }

        @Override public String providerType() { return "h2-test-conformance"; }
        @Override public int providerVersion() { return 1; }
        @Override public String queryDigest() { return request.queryDigest(); }
        @Override public String partitionDigest() { return request.partitionDigest(); }
        @Override public String snapshotId() { return request.snapshotId(); }
        @Override public String schemaFingerprint() {
            return SchemaFingerprint.of(META);
        }
        @Override public ResultSet resultSet() { return resultSet; }

        @Override
        public synchronized void close() {
            if (closed) return;
            closed = true;
            try {
                resultSet.close();
                H2SnapshotProvider.closeOrder.add("resultSet");
                statement.close();
                H2SnapshotProvider.closeOrder.add("statement");
                connection.close();
                H2SnapshotProvider.closeOrder.add("connection");
                H2SnapshotProvider.closes.incrementAndGet();
            } catch (SQLException failure) {
                throw new IllegalStateException("driver close secret");
            }
        }
    }
}
