package io.rowweave.connector.postgres;

import io.rowweave.DataRow;
import io.rowweave.stream.api.StreamDataRowCodec;
import io.rowweave.stream.contract.StreamUpsertRecord;
import io.rowweave.stream.contract.StreamUpsertRecordCodec;
import io.rowweave.stream.spi.StreamCdcCheckpointCompletion;
import io.rowweave.stream.spi.StreamCdcSourceContext;
import io.rowweave.stream.spi.StreamCdcSourceEvent;
import io.rowweave.stream.spi.StreamCdcSourceReader;
import io.rowweave.stream.spi.StreamSecretAccess;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Opt-in protocol conformance against an isolated real PostgreSQL server. */
class PostgresRealServiceIT {
    private static final String DATABASE = "rowweave_cdc";
    private static final String SLOT = "rowweave_cdc_slot";
    private static final String PUBLICATION = "rowweave_cdc_publication";

    @Test
    void snapshotPgoutputAndCompletedCheckpointOwnExternalAck(
            @TempDir Path temp) throws Exception {
        Path binaries = binaries();
        int port = freePort();
        try (Server server = Server.start(binaries, temp, port)) {
            provision(port);
            try (Connection admin = connect(port, DATABASE)) {
            PostgresConnectorConfiguration configuration =
                    configuration(port);
            byte[] credentials = credentials();
            try {
                PostgresConnectorProvider provider =
                        new PostgresConnectorProvider();
                StreamCdcSourceReader reader = provider.openCdc(context(
                        configuration, StreamCdcSourceContext.Phase.SNAPSHOT,
                        0L, null, credentials));
                byte[] completedPosition;
                long lastSequence;
                try {
                    StreamCdcSourceEvent initial = await(reader,
                            StreamCdcSourceEvent.Kind.SNAPSHOT_AFTER_IMAGE);
                    assertAfterImage(configuration, initial, 1L, "10.00");
                    StreamCdcSourceEvent handoff = await(reader,
                            StreamCdcSourceEvent.Kind
                                    .SNAPSHOT_HANDOFF_COMPLETE);
                    String acknowledgedBefore = confirmedFlush(admin);
                    assertNotNull(acknowledgedBefore);

                    try (Statement change = admin.createStatement()) {
                        admin.setAutoCommit(false);
                        change.executeUpdate("INSERT INTO ledger.entries "
                                + "VALUES (2, 20.00)");
                        change.executeUpdate("UPDATE ledger.entries SET "
                                + "id = 3, amount = 30.00 WHERE id = 2");
                        change.executeUpdate("DELETE FROM ledger.entries "
                                + "WHERE id = 3");
                        admin.commit();
                        admin.setAutoCommit(true);
                    }

                    List<StreamCdcSourceEvent> changes = awaitLog(reader, 4);
                    assertAfterImage(configuration, changes.get(0),
                            2L, "20.00");
                    assertTombstone(configuration, changes.get(1), 2L);
                    assertAfterImage(configuration, changes.get(2),
                            3L, "30.00");
                    assertTombstone(configuration, changes.get(3), 3L);
                    for (int index = 0; index < 3; index++) {
                        assertFalse(changes.get(index)
                                .hasCompletedExternalPosition());
                    }
                    StreamCdcSourceEvent last = changes.get(3);
                    assertTrue(last.hasCompletedExternalPosition());
                    completedPosition = last.completedExternalPositionFrame();
                    lastSequence = last.logSequence();

                    assertEquals(acknowledgedBefore, confirmedFlush(admin),
                            "reading a commit must not acknowledge WAL");
                    reader.notifyCheckpointComplete(
                            new StreamCdcCheckpointCompletion(
                                    1L, lastSequence, completedPosition));
                    awaitConfirmed(admin, PostgresCdcPositionCodec.decode(
                            completedPosition).endLsn());
                } finally {
                    reader.close();
                }

                try (StreamCdcSourceReader restored = provider.openCdc(
                        context(configuration,
                                StreamCdcSourceContext.Phase.LOG,
                                lastSequence, completedPosition,
                                credentials))) {
                    ExecutorService read = Executors.newSingleThreadExecutor();
                    try {
                        Future<StreamCdcSourceEvent> pending =
                                read.submit(restored::next);
                        Thread.sleep(4_000L);
                        assertFalse(pending.isDone(),
                                "an idle replication stream must remain "
                                        + "healthy past wal_sender_timeout");
                        try (Statement change = admin.createStatement()) {
                            change.executeUpdate("INSERT INTO ledger.entries "
                                    + "VALUES (4, 40.00)");
                        }
                        assertAfterImage(configuration,
                                pending.get(15L, TimeUnit.SECONDS),
                                4L, "40.00");
                    } finally {
                        read.shutdownNow();
                    }
                }

                try (Statement drift = admin.createStatement()) {
                    drift.execute("ALTER TABLE ledger.entries "
                            + "ADD COLUMN memo text");
                }
                IllegalArgumentException failure = assertThrows(
                        IllegalArgumentException.class,
                        () -> provider.openCdc(context(configuration,
                                StreamCdcSourceContext.Phase.SNAPSHOT,
                                0L, null, credentials)));
                assertEquals("POSTGRES_CDC_SCHEMA_DRIFT",
                        failure.getMessage());
            } finally {
                Arrays.fill(credentials, (byte) 0);
            }
            }
        }
    }

    @Test
    void disconnectReplaysFromCompletedLsnAndRotatedCredentialWins(
            @TempDir Path temp) throws Exception {
        Path binaries = binaries();
        int port = freePort();
        try (Server server = Server.start(binaries, temp, port)) {
            provision(port);
            PostgresConnectorConfiguration configuration =
                    configuration(port);
            try (Connection admin = connect(port, DATABASE)) {
                requirePasswordAuthentication(admin, server.data,
                        "first-secret");
                byte[] first = credentials("first-secret");
                byte[] second = credentials("second-secret");
                try {
                    PostgresConnectorProvider provider =
                            new PostgresConnectorProvider();
                    byte[] handoff;
                    try (StreamCdcSourceReader initial = provider.openCdc(
                            context(configuration,
                                    StreamCdcSourceContext.Phase.SNAPSHOT,
                                    0L, null, first))) {
                        await(initial, StreamCdcSourceEvent.Kind
                                .SNAPSHOT_AFTER_IMAGE);
                        StreamCdcSourceEvent boundary = await(initial,
                                StreamCdcSourceEvent.Kind
                                        .SNAPSHOT_HANDOFF_COMPLETE);
                        handoff = boundary.completedExternalPositionFrame();
                        initial.notifyCheckpointComplete(
                                new StreamCdcCheckpointCompletion(
                                        1L, 0L, handoff));
                    }

                    try (Statement change = admin.createStatement()) {
                        change.executeUpdate("INSERT INTO ledger.entries "
                                + "VALUES (2, 20.00)");
                    }
                    StreamCdcSourceReader disconnected = provider.openCdc(
                            context(configuration,
                                    StreamCdcSourceContext.Phase.LOG,
                                    0L, handoff, first));
                    terminateReplicationBackend(admin);
                    IllegalStateException readFailure =
                            awaitReadFailure(disconnected);
                    assertEquals("POSTGRES_CDC_LOG_READ_FAILED",
                            readFailure.getMessage());
                    disconnected.close();
                    awaitSlotInactive(admin);

                    rotatePassword(admin, "second-secret");
                    IllegalStateException oldCredential = assertThrows(
                            IllegalStateException.class,
                            () -> provider.openCdc(context(configuration,
                                    StreamCdcSourceContext.Phase.LOG,
                                    0L, handoff, first)));
                    assertEquals("POSTGRES_CDC_OPEN_FAILED",
                            oldCredential.getMessage());

                    try (StreamCdcSourceReader restored = provider.openCdc(
                            context(configuration,
                                    StreamCdcSourceContext.Phase.LOG,
                                    0L, handoff, second))) {
                        StreamCdcSourceEvent replayed = await(restored,
                                StreamCdcSourceEvent.Kind.LOG_RECORD);
                        assertAfterImage(configuration, replayed,
                                2L, "20.00");
                        assertTrue(replayed.hasCompletedExternalPosition());
                        restored.notifyCheckpointComplete(
                                new StreamCdcCheckpointCompletion(
                                        2L, replayed.logSequence(),
                                        replayed
                                                .completedExternalPositionFrame()));
                    }
                } finally {
                    Arrays.fill(first, (byte) 0);
                    Arrays.fill(second, (byte) 0);
                }
            }
        }
    }

    private static Path binaries() {
        String configured = System.getProperty("postgres.bin.dir");
        Assumptions.assumeTrue(configured != null,
                "set -Dpostgres.bin.dir to run real PostgreSQL conformance");
        Path result = Paths.get(configured).toAbsolutePath().normalize();
        Assumptions.assumeTrue(Files.isExecutable(result.resolve("initdb"))
                        && Files.isExecutable(result.resolve("postgres")),
                "PostgreSQL binaries are not executable: " + result);
        return result;
    }

    private static void provision(int port) throws Exception {
        try (Connection connection = connect(port, "postgres");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE DATABASE " + DATABASE);
        }
        try (Connection database = connect(port, DATABASE);
             Statement statement = database.createStatement()) {
            statement.execute("CREATE SCHEMA ledger");
            statement.execute("CREATE TABLE ledger.entries ("
                    + "id bigint PRIMARY KEY, amount numeric(18,2))");
            statement.execute("INSERT INTO ledger.entries VALUES (1, 10.00)");
            statement.execute("CREATE PUBLICATION " + PUBLICATION
                    + " FOR TABLE ledger.entries");
        }
    }

    private static PostgresConnectorConfiguration configuration(int port)
            throws Exception {
        try (Connection connection = connect(port, DATABASE)) {
            String system;
            long databaseOid;
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT system_identifier::text, "
                                 + "(SELECT oid FROM pg_database WHERE "
                                 + "datname = current_database()) "
                                 + "FROM pg_control_system()")) {
                assertTrue(result.next());
                system = result.getString(1);
                databaseOid = result.getLong(2);
            }
            long tableOid;
            String identity;
            List<PostgresColumnBinding> columns = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT c.oid, c.relreplident, a.attnum, a.attname, "
                                 + "a.atttypid, a.atttypmod, NOT a.attnotnull "
                                 + "FROM pg_class c JOIN pg_namespace n ON "
                                 + "n.oid = c.relnamespace JOIN pg_attribute a "
                                 + "ON a.attrelid = c.oid WHERE n.nspname = "
                                 + "'ledger' AND c.relname = 'entries' AND "
                                 + "a.attnum > 0 AND NOT a.attisdropped "
                                 + "ORDER BY a.attnum")) {
                assertTrue(result.next());
                tableOid = result.getLong(1);
                identity = result.getString(2);
                do {
                    columns.add(new PostgresColumnBinding(
                            result.getString(4), result.getString(4),
                            result.getInt(3), result.getLong(5),
                            result.getInt(6), result.getBoolean(7)));
                } while (result.next());
            }
            assertEquals("d", identity);
            return PostgresConnectorConfiguration.builder()
                    .host("127.0.0.1").port(port).database(DATABASE)
                    .slot(SLOT).publication(PUBLICATION)
                    .databaseSystemIdentifier(system)
                    .databaseOid(databaseOid)
                    .tlsMode(PostgresTlsMode.DISABLED)
                    .credentialsReference("test/postgres")
                    .tables(Collections.singletonList(
                            new PostgresTableBinding(
                                    "ledger", "entries", tableOid,
                                    PostgresReplicaIdentity.DEFAULT, 0L,
                                    columns, Collections.singletonList(1))))
                    .maximumTransactionChanges(32)
                    .maximumTransactionBytes(1024 * 1024)
                    .maximumRetainedWalBytes(32L * 1024 * 1024)
                    .snapshotFetchRows(16)
                    .connectTimeoutMillis(2_000)
                    .socketTimeoutMillis(2_000)
                    .statusIntervalMillis(500)
                    .build();
        }
    }

    private static StreamCdcSourceContext context(
            PostgresConnectorConfiguration configuration,
            StreamCdcSourceContext.Phase phase, long lastSequence,
            byte[] position, byte[] credentials) {
        StreamSecretAccess secrets = new StreamSecretAccess() {
            @Override public <T> T withSecret(String alias,
                    Operation<T> operation) throws Exception {
                assertEquals(
                        PostgresConnectorConfiguration.CREDENTIALS_ALIAS,
                        alias);
                byte[] issued = credentials.clone();
                try { return operation.apply(issued); }
                finally { Arrays.fill(issued, (byte) 0); }
            }
        };
        return new StreamCdcSourceContext("postgres-real-source",
                configuration.encode(), configuration.snapshotIdentity(), 0L,
                Collections.singletonList("snapshot-all"),
                phase == StreamCdcSourceContext.Phase.LOG
                        ? Collections.singletonList("snapshot-all")
                        : Collections.<String>emptyList(),
                phase, lastSequence, position, secrets);
    }

    private static byte[] credentials() {
        return credentials("unused");
    }

    private static byte[] credentials(String password) {
        PostgresCredentials value = PostgresCredentials.of(
                System.getProperty("user.name")
                        .getBytes(StandardCharsets.UTF_8),
                password.getBytes(StandardCharsets.UTF_8));
        try { return PostgresCredentialCodec.encode(value); }
        finally { value.destroy(); }
    }

    private static void requirePasswordAuthentication(Connection admin,
            Path data, String password) throws Exception {
        rotatePassword(admin, password);
        String rules = "local all all trust\n"
                + "host replication all 127.0.0.1/32 scram-sha-256\n"
                + "host all all 127.0.0.1/32 scram-sha-256\n"
                + "host replication all ::1/128 scram-sha-256\n"
                + "host all all ::1/128 scram-sha-256\n";
        Files.write(data.resolve("pg_hba.conf"),
                rules.getBytes(StandardCharsets.UTF_8));
        try (Statement statement = admin.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT pg_reload_conf()")) {
            assertTrue(result.next() && result.getBoolean(1));
        }
    }

    private static void rotatePassword(Connection admin, String password)
            throws Exception {
        String user = System.getProperty("user.name");
        assertTrue(user.matches("[A-Za-z_][A-Za-z0-9_]*"));
        assertTrue(password.matches("[A-Za-z0-9_-]+"));
        try (Statement statement = admin.createStatement()) {
            statement.execute("ALTER ROLE \"" + user
                    + "\" PASSWORD '" + password + "'");
        }
    }

    private static void terminateReplicationBackend(Connection admin)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            try (PreparedStatement statement = admin.prepareStatement(
                    "SELECT pg_terminate_backend(active_pid) FROM "
                            + "pg_replication_slots WHERE slot_name = ? "
                            + "AND active_pid IS NOT NULL")) {
                statement.setString(1, SLOT);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next() && result.getBoolean(1)) return;
                }
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("replication backend was not active");
    }

    private static IllegalStateException awaitReadFailure(
            StreamCdcSourceReader reader) throws Exception {
        AtomicReference<IllegalStateException> failure =
                new AtomicReference<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (System.nanoTime() < deadline) {
            try {
                reader.next();
            } catch (IllegalStateException expected) {
                failure.set(expected);
                break;
            }
            Thread.sleep(20L);
        }
        assertNotNull(failure.get(), "reader did not observe disconnect");
        return failure.get();
    }

    private static void awaitSlotInactive(Connection admin) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (System.nanoTime() < deadline) {
            try (PreparedStatement statement = admin.prepareStatement(
                    "SELECT NOT active FROM pg_replication_slots "
                            + "WHERE slot_name = ?")) {
                statement.setString(1, SLOT);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next() && result.getBoolean(1)) return;
                }
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("replication slot remained active");
    }

    private static StreamCdcSourceEvent await(
            StreamCdcSourceReader reader, StreamCdcSourceEvent.Kind kind)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15L);
        while (System.nanoTime() < deadline) {
            StreamCdcSourceEvent event = reader.next();
            if (event != null) {
                assertEquals(kind, event.kind());
                return event;
            }
            Thread.sleep(10L);
        }
        throw new AssertionError("timed out waiting for " + kind);
    }

    private static List<StreamCdcSourceEvent> awaitLog(
            StreamCdcSourceReader reader, int count) throws Exception {
        List<StreamCdcSourceEvent> result = new ArrayList<>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15L);
        while (result.size() < count && System.nanoTime() < deadline) {
            StreamCdcSourceEvent event = reader.next();
            if (event == null) {
                Thread.sleep(10L);
            } else {
                assertEquals(StreamCdcSourceEvent.Kind.LOG_RECORD,
                        event.kind());
                result.add(event);
            }
        }
        assertEquals(count, result.size());
        return result;
    }

    private static void assertAfterImage(
            PostgresConnectorConfiguration configuration,
            StreamCdcSourceEvent event, long id, String amount) {
        StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                event.upsertFrame(), 4 * 1024 * 1024, 4 * 1024 * 1024);
        assertEquals(StreamUpsertRecord.Kind.AFTER_IMAGE, record.kind());
        DataRow value = new StreamDataRowCodec(configuration.streamSchema())
                .decode(record.afterImageFrame());
        assertEquals(id, value.getLong(0));
        assertEquals(0, new java.math.BigDecimal(amount)
                .compareTo(value.getBigDecimal(1)));
    }

    private static void assertTombstone(
            PostgresConnectorConfiguration configuration,
            StreamCdcSourceEvent event, long id) {
        StreamUpsertRecord record = StreamUpsertRecordCodec.decode(
                event.upsertFrame(), 4 * 1024 * 1024, 4 * 1024 * 1024);
        assertEquals(StreamUpsertRecord.Kind.TOMBSTONE, record.kind());
        DataRow key = new StreamDataRowCodec(configuration.stableKeySchema())
                .decode(record.stableKeyFrame());
        assertEquals(id, key.getLong(0));
    }

    private static String confirmedFlush(Connection connection)
            throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT confirmed_flush_lsn::text FROM pg_replication_slots "
                        + "WHERE slot_name = ?")) {
            statement.setString(1, SLOT);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                String lsn = result.getString(1);
                assertFalse(result.next());
                return lsn;
            }
        }
    }

    private static void awaitConfirmed(Connection connection, String lsn)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while (System.nanoTime() < deadline) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT pg_wal_lsn_diff(confirmed_flush_lsn, "
                            + "?::pg_lsn) >= 0 FROM pg_replication_slots "
                            + "WHERE slot_name = ?")) {
                statement.setString(1, lsn);
                statement.setString(2, SLOT);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next() && result.getBoolean(1)) return;
                }
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("slot did not acknowledge " + lsn);
    }

    private static Connection connect(int port, String database)
            throws Exception {
        return connect("jdbc:postgresql://127.0.0.1:" + port
                + '/' + database);
    }

    private static Connection connect(String url) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", System.getProperty("user.name"));
        properties.setProperty("password", "unused");
        properties.setProperty("sslmode", "disable");
        return DriverManager.getConnection(url, properties);
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(
                0, 1, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        }
    }

    private static final class Server implements AutoCloseable {
        private final Process process;
        private final Path log;
        private final Path data;

        private Server(Process process, Path log, Path data) {
            this.process = process;
            this.log = log;
            this.data = data;
        }

        private static Server start(Path binaries, Path temp, int port)
                throws Exception {
            Path data = temp.resolve("data");
            Path initLog = temp.resolve("initdb.log");
            Process init = new ProcessBuilder(
                    binaries.resolve("initdb").toString(),
                    "--pgdata", data.toString(), "--auth=trust",
                    "--encoding=UTF8", "--no-locale")
                    .redirectErrorStream(true)
                    .redirectOutput(initLog.toFile()).start();
            if (!init.waitFor(30L, TimeUnit.SECONDS)
                    || init.exitValue() != 0) {
                throw new AssertionError("initdb failed:\n" + read(initLog));
            }
            Path log = temp.resolve("postgres.log");
            Process process = new ProcessBuilder(
                    binaries.resolve("postgres").toString(),
                    "-D", data.toString(), "-h", "127.0.0.1",
                    "-p", Integer.toString(port),
                    "-c", "wal_level=logical",
                    "-c", "max_replication_slots=4",
                    "-c", "max_wal_senders=4",
                    "-c", "wal_sender_timeout=2000",
                    "-c", "fsync=off",
                    "-c", "synchronous_commit=off")
                    .redirectErrorStream(true)
                    .redirectOutput(log.toFile()).start();
            Server server = new Server(process, log, data);
            try {
                server.awaitReady(port);
                return server;
            } catch (Throwable failure) {
                server.close();
                throw failure;
            }
        }

        private void awaitReady(int port) throws Exception {
            long deadline = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(20L);
            while (System.nanoTime() < deadline) {
                if (!process.isAlive()) {
                    throw new AssertionError(
                            "PostgreSQL exited before ready:\n" + read(log));
                }
                try (Connection ignored = connect(port, "postgres")) {
                    return;
                } catch (Exception notReady) {
                    Thread.sleep(25L);
                }
            }
            throw new AssertionError(
                    "PostgreSQL did not become ready:\n" + read(log));
        }

        @Override public void close() throws Exception {
            process.destroy();
            if (!process.waitFor(10L, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                assertTrue(process.waitFor(5L, TimeUnit.SECONDS),
                        "PostgreSQL process did not stop");
            }
        }
    }

    private static String read(Path path) throws IOException {
        if (!Files.exists(path)) return "<missing log>";
        byte[] bytes = Files.readAllBytes(path);
        int start = Math.max(0, bytes.length - 16 * 1024);
        return new String(bytes, start, bytes.length - start,
                StandardCharsets.UTF_8);
    }
}
