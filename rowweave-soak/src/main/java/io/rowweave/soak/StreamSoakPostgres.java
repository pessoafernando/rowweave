package io.rowweave.soak;

import io.rowweave.connector.postgres.PostgresColumnBinding;
import io.rowweave.connector.postgres.PostgresConnectorConfiguration;
import io.rowweave.connector.postgres.PostgresCredentialCodec;
import io.rowweave.connector.postgres.PostgresCredentials;
import io.rowweave.connector.postgres.PostgresReplicaIdentity;
import io.rowweave.connector.postgres.PostgresTableBinding;
import io.rowweave.connector.postgres.PostgresTlsMode;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

final class StreamSoakPostgres implements AutoCloseable {
    static final int CHANGES_PER_MUTATION = 3;
    static final String DATABASE = "rowweave_stream_soak_soak";
    static final String SLOT = "rowweave_stream_soak_soak_slot";
    static final String PUBLICATION = "rowweave_stream_soak_soak_publication";

    private final Process process;
    private final Path log;
    private final int port;
    private final byte[] credentials;
    private long nextId = 2L;

    private StreamSoakPostgres(Process process, Path log, int port,
            byte[] credentials) {
        this.process = process;
        this.log = log;
        this.port = port;
        this.credentials = credentials;
    }

    static StreamSoakPostgres start(Path binaries, Path root) throws Exception {
        if (binaries == null
                || !Files.isExecutable(binaries.resolve("initdb"))
                || !Files.isExecutable(binaries.resolve("postgres"))) {
            throw new IllegalArgumentException(
                    "PostgreSQL binary directory is invalid");
        }
        Files.createDirectories(root);
        Path data = root.resolve("data");
        Path initLog = root.resolve("initdb.log");
        Process init = new ProcessBuilder(
                binaries.resolve("initdb").toString(),
                "--pgdata", data.toString(), "--auth=trust",
                "--encoding=UTF8", "--no-locale")
                .redirectErrorStream(true).redirectOutput(initLog.toFile())
                .start();
        if (!init.waitFor(30L, TimeUnit.SECONDS) || init.exitValue() != 0) {
            throw new IllegalStateException("initdb failed: " + initLog);
        }
        int port = freePort();
        Path log = root.resolve("postgres.log");
        Process process = new ProcessBuilder(
                binaries.resolve("postgres").toString(),
                "-D", data.toString(), "-h", "127.0.0.1",
                "-p", Integer.toString(port),
                "-c", "wal_level=logical",
                "-c", "max_replication_slots=8",
                "-c", "max_wal_senders=8",
                "-c", "max_slot_wal_keep_size=1024MB",
                "-c", "fsync=off")
                .redirectErrorStream(true).redirectOutput(log.toFile())
                .start();
        PostgresCredentials secret = PostgresCredentials.of(
                System.getProperty("user.name")
                        .getBytes(StandardCharsets.UTF_8),
                "unused".getBytes(StandardCharsets.UTF_8));
        byte[] credentials;
        try { credentials = PostgresCredentialCodec.encode(secret); }
        finally { secret.destroy(); }
        StreamSoakPostgres result = new StreamSoakPostgres(
                process, log, port, credentials);
        try {
            result.awaitReady();
            result.provision();
            return result;
        } catch (Throwable failure) {
            result.close();
            throw failure;
        }
    }

    PostgresConnectorConfiguration configuration() throws Exception {
        try (Connection connection = connect(DATABASE)) {
            String system;
            long databaseOid;
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT system_identifier::text, "
                                 + "(SELECT oid FROM pg_database WHERE "
                                 + "datname=current_database()) "
                                 + "FROM pg_control_system()")) {
                require(result.next(), "PostgreSQL identity missing");
                system = result.getString(1);
                databaseOid = result.getLong(2);
            }
            long tableOid;
            List<PostgresColumnBinding> columns = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT c.oid,a.attnum,a.attname,a.atttypid,"
                                 + "a.atttypmod,NOT a.attnotnull FROM "
                                 + "pg_class c JOIN pg_namespace n ON "
                                 + "n.oid=c.relnamespace JOIN pg_attribute a "
                                 + "ON a.attrelid=c.oid WHERE "
                                 + "n.nspname='ledger' AND "
                                 + "c.relname='entries' AND a.attnum>0 AND "
                                 + "NOT a.attisdropped ORDER BY a.attnum")) {
                require(result.next(), "PostgreSQL table missing");
                tableOid = result.getLong(1);
                do {
                    columns.add(new PostgresColumnBinding(
                            result.getString(3), result.getString(3),
                            result.getInt(2), result.getLong(4),
                            result.getInt(5), result.getBoolean(6)));
                } while (result.next());
            }
            return PostgresConnectorConfiguration.builder()
                    .host("127.0.0.1").port(port).database(DATABASE)
                    .slot(SLOT).publication(PUBLICATION)
                    .databaseSystemIdentifier(system).databaseOid(databaseOid)
                    .tlsMode(PostgresTlsMode.DISABLED)
                    .credentialsReference("vault/postgres")
                    .tables(Collections.singletonList(
                            new PostgresTableBinding(
                                    "ledger", "entries", tableOid,
                                    PostgresReplicaIdentity.DEFAULT, 0L,
                                    columns, Collections.singletonList(1))))
                    .maximumTransactionChanges(1024)
                    .maximumTransactionBytes(8L * 1024L * 1024L)
                    .maximumRetainedWalBytes(1024L * 1024L * 1024L)
                    .snapshotFetchRows(1024)
                    .connectTimeoutMillis(2_000)
                    .socketTimeoutMillis(10_000)
                    .statusIntervalMillis(500).build();
        }
    }

    StreamSoakMain.MutationSession openMutationSession() throws Exception {
        Connection connection = connect(DATABASE);
        try {
            connection.setAutoCommit(false);
            return new JdbcMutationSession(connection,
                    connection.createStatement());
        } catch (Exception failure) {
            try { connection.close(); }
            catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private synchronized long nextMutationId() { return nextId++; }

    private final class JdbcMutationSession
            implements StreamSoakMain.MutationSession {
        private final Connection connection;
        private final Statement statement;

        private JdbcMutationSession(Connection connection,
                Statement statement) {
            this.connection = connection;
            this.statement = statement;
        }

        @Override public List<String> mutate() throws Exception {
            long id = nextMutationId();
            List<String> expected = new ArrayList<>(
                    CHANGES_PER_MUTATION);
            try {
                statement.executeUpdate(
                        "INSERT INTO ledger.entries VALUES ("
                                + id + ", " + id + ".00)");
                expected.add("A|" + id + '|' + id + ".00");
                statement.executeUpdate(
                        "UPDATE ledger.entries SET amount="
                                + id + ".50 WHERE id=" + id);
                expected.add("A|" + id + '|' + id + ".50");
                statement.executeUpdate(
                        "DELETE FROM ledger.entries WHERE id=" + id);
                expected.add("D|" + id);
                connection.commit();
            } catch (Exception failure) {
                try { connection.rollback(); }
                catch (Exception rollbackFailure) {
                    failure.addSuppressed(rollbackFailure);
                }
                throw failure;
            }
            return Collections.unmodifiableList(expected);
        }

        @Override public void close() throws Exception {
            try (Connection ignoredConnection = connection;
                 Statement ignoredStatement = statement) {
                // Resources close in reverse order: statement, then connection.
            }
        }
    }

    String confirmedFlush() throws Exception {
        try (Connection connection = connect(DATABASE);
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT confirmed_flush_lsn::text FROM "
                             + "pg_replication_slots WHERE slot_name=?")) {
            statement.setString(1, SLOT);
            try (ResultSet result = statement.executeQuery()) {
                require(result.next(), "replication slot missing");
                return result.getString(1);
            }
        }
    }

    boolean lsnAtLeast(String earlier, String later) throws Exception {
        try (Connection connection = connect(DATABASE);
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT pg_wal_lsn_diff(?::pg_lsn,?::pg_lsn)>=0")) {
            statement.setString(1, later);
            statement.setString(2, earlier);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        }
    }

    byte[] credentialFrame() { return credentials.clone(); }
    Process process() { return process; }
    Path root() { return log.getParent(); }

    private void provision() throws Exception {
        try (Connection root = connect("postgres");
             Statement statement = root.createStatement()) {
            statement.execute("CREATE DATABASE " + DATABASE);
        }
        try (Connection connection = connect(DATABASE);
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA ledger");
            statement.execute("CREATE TABLE ledger.entries ("
                    + "id bigint PRIMARY KEY, amount numeric(18,2))");
            statement.execute("INSERT INTO ledger.entries VALUES (1,10.00)");
            statement.execute("CREATE PUBLICATION " + PUBLICATION
                    + " FOR TABLE ledger.entries");
        }
    }

    private void awaitReady() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20L);
        while (System.nanoTime() < deadline) {
            if (!process.isAlive()) {
                throw new IllegalStateException("PostgreSQL exited: " + log);
            }
            try (Connection ignored = connect("postgres")) { return; }
            catch (Exception notReady) { Thread.sleep(50L); }
        }
        throw new IllegalStateException("PostgreSQL did not become ready");
    }

    private Connection connect(String database) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", System.getProperty("user.name"));
        properties.setProperty("password", "unused");
        properties.setProperty("sslmode", "disable");
        return DriverManager.getConnection(
                "jdbc:postgresql://127.0.0.1:" + port + '/' + database,
                properties);
    }

    private static int freePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(
                0, 1, InetAddress.getByName("127.0.0.1"))) {
            return socket.getLocalPort();
        }
    }

    private static void require(boolean value, String message) {
        if (!value) throw new IllegalStateException(message);
    }

    @Override public void close() {
        java.util.Arrays.fill(credentials, (byte) 0);
        StreamSoakMinio.stop(process);
    }
}
