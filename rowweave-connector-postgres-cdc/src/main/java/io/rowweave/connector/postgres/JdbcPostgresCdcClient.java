package io.rowweave.connector.postgres;

import org.postgresql.PGConnection;
import org.postgresql.replication.LogSequenceNumber;
import org.postgresql.replication.PGReplicationConnection;
import org.postgresql.replication.PGReplicationStream;
import org.postgresql.replication.ReplicationSlotInfo;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

final class JdbcPostgresCdcClient implements PostgresCdcClient {
    private final PostgresConnectorConfiguration configuration;
    private final Connection control;
    private final Connection snapshotConnection;
    private final Connection replicationConnection;
    private final PGReplicationConnection replicationApi;
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile JdbcPostgresSnapshotCursor snapshot;
    private volatile PGReplicationStream stream;

    JdbcPostgresCdcClient(PostgresConnectorConfiguration configuration,
            Connection control, Connection snapshotConnection,
            Connection replicationConnection) throws Exception {
        this.configuration = configuration;
        this.control = control;
        this.snapshotConnection = snapshotConnection;
        this.replicationConnection = replicationConnection;
        this.replicationApi = replicationConnection.unwrap(
                PGConnection.class).getReplicationAPI();
    }

    @Override public synchronized PostgresSnapshotCursor startSnapshot() {
        requireOpen();
        if (snapshot != null || stream != null) throw lifecycle();
        try {
            validateAuthority();
            resetPreHandoffSlot();
            ReplicationSlotInfo slot = replicationApi
                    .createReplicationSlot().logical()
                    .withSlotName(configuration.slot())
                    .withOutputPlugin("pgoutput").make();
            if (slot.getConsistentPoint() == null
                    || slot.getSnapshotName() == null
                    || slot.getSnapshotName().isEmpty()
                    || !"pgoutput".equals(slot.getOutputPlugin())) {
                throw lifecycle();
            }
            configureSnapshot(slot.getSnapshotName());
            PostgresCdcPosition handoff =
                    PostgresCdcPosition.snapshotHandoff(
                            configuration.databaseSystemIdentifier(),
                            configuration.databaseOid(),
                            slot.getConsistentPoint().asString());
            snapshot = new JdbcPostgresSnapshotCursor(
                    configuration, snapshotConnection, handoff);
            return snapshot;
        } catch (Throwable failure) {
            if (failure instanceof IllegalArgumentException
                    && "POSTGRES_CDC_SCHEMA_DRIFT".equals(
                    failure.getMessage())) {
                throw (IllegalArgumentException) failure;
            }
            throw new IllegalStateException("POSTGRES_CDC_SNAPSHOT_FAILED");
        }
    }

    @Override public synchronized void startLog(PostgresCdcPosition position) {
        requireOpen();
        if (stream != null) throw lifecycle();
        try {
            JdbcPostgresSnapshotCursor current = snapshot;
            snapshot = null;
            if (current != null) current.close();
            stream = replicationApi.replicationStream().logical()
                    .withSlotName(configuration.slot())
                    .withStartPosition(LogSequenceNumber.valueOf(
                            position.endLsn()))
                    .withSlotOption("proto_version", 1)
                    .withSlotOption("publication_names",
                            configuration.publication())
                    .withStatusInterval(
                            configuration.statusIntervalMillis(),
                            TimeUnit.MILLISECONDS)
                    .withAutomaticFlush(false)
                    .start();
        } catch (Throwable failure) {
            throw new IllegalStateException("POSTGRES_CDC_LOG_OPEN_FAILED");
        }
    }

    @Override public ByteBuffer readPending() {
        requireOpen();
        PGReplicationStream current = stream;
        if (current == null) throw lifecycle();
        try {
            return current.readPending();
        } catch (Throwable failure) {
            throw new IllegalStateException("POSTGRES_CDC_LOG_READ_FAILED");
        }
    }

    @Override public long retainedWalBytes() {
        requireOpen();
        try (PreparedStatement statement = control.prepareStatement(
                "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn) "
                        + "FROM pg_replication_slots WHERE slot_name = ? "
                        + "AND database = current_database()")) {
            statement.setString(1, configuration.slot());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) throw lifecycle();
                BigDecimal bytes = result.getBigDecimal(1);
                if (bytes == null || bytes.signum() < 0
                        || bytes.compareTo(BigDecimal.valueOf(Long.MAX_VALUE))
                        > 0 || result.next()) throw lifecycle();
                return bytes.longValueExact();
            }
        } catch (Throwable failure) {
            if (failure instanceof IllegalStateException
                    && "POSTGRES_CDC_CLIENT_LIFECYCLE".equals(
                    failure.getMessage())) throw (IllegalStateException) failure;
            throw new IllegalStateException("POSTGRES_CDC_WAL_QUERY_FAILED");
        }
    }

    @Override public synchronized void acknowledge(
            PostgresCdcPosition position) {
        requireOpen();
        PGReplicationStream current = stream;
        if (current == null) throw lifecycle();
        try {
            LogSequenceNumber lsn = LogSequenceNumber.valueOf(
                    position.endLsn());
            current.setAppliedLSN(lsn);
            current.setFlushedLSN(lsn);
            current.forceUpdateStatus();
        } catch (Throwable failure) {
            throw new IllegalStateException("POSTGRES_CDC_ACK_FAILED");
        }
    }

    @Override public void wakeUp() {
        // readPending is nonblocking by construction; no socket close is
        // needed to wake the task loop.
    }

    @Override public void cancel() { closeQuietly(); }

    @Override public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) return;
        Throwable failure = null;
        PGReplicationStream currentStream = stream;
        stream = null;
        if (currentStream != null) {
            try { currentStream.close(); } catch (Throwable close) {
                failure = close;
            }
        }
        JdbcPostgresSnapshotCursor currentSnapshot = snapshot;
        snapshot = null;
        if (currentSnapshot != null) {
            try { currentSnapshot.close(); } catch (Throwable close) {
                failure = append(failure, close);
            }
        }
        failure = close(replicationConnection, failure);
        failure = close(snapshotConnection, failure);
        failure = close(control, failure);
        if (failure != null) {
            throw new IllegalStateException("POSTGRES_CDC_CLOSE_FAILED");
        }
    }

    private void validateAuthority() throws Exception {
        try (Statement statement = control.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT system_identifier::text FROM pg_control_system()")) {
            if (!result.next() || !configuration.databaseSystemIdentifier()
                    .equals(result.getString(1)) || result.next()) {
                throw drift();
            }
        }
        try (PreparedStatement statement = control.prepareStatement(
                "SELECT oid FROM pg_database WHERE datname = current_database()")) {
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || configuration.databaseOid()
                        != result.getLong(1) || result.next()) throw drift();
            }
        }
        for (PostgresTableBinding table : configuration.tables()) {
            validateTable(table);
        }
    }

    private void validateTable(PostgresTableBinding table) throws Exception {
        try (PreparedStatement publication = control.prepareStatement(
                "SELECT 1 FROM pg_publication_tables WHERE pubname = ? "
                        + "AND schemaname = ? AND tablename = ?")) {
            publication.setString(1, configuration.publication());
            publication.setString(2, table.schemaName());
            publication.setString(3, table.tableName());
            try (ResultSet result = publication.executeQuery()) {
                if (!result.next() || result.next()) throw drift();
            }
        }
        String metadata = "SELECT n.nspname, c.relname, c.oid, "
                + "c.relreplident, COALESCE(ri.indexrelid, 0), "
                + "a.attnum, a.attname, a.atttypid, a.atttypmod, "
                + "NOT a.attnotnull FROM pg_class c "
                + "JOIN pg_namespace n ON n.oid = c.relnamespace "
                + "JOIN pg_attribute a ON a.attrelid = c.oid "
                + "LEFT JOIN pg_index ri ON ri.indrelid = c.oid "
                + "AND ri.indisreplident WHERE c.oid = ? "
                + "AND a.attnum > 0 AND NOT a.attisdropped "
                + "ORDER BY a.attnum";
        try (PreparedStatement statement = control.prepareStatement(metadata)) {
            statement.setLong(1, table.tableOid());
            try (ResultSet result = statement.executeQuery()) {
                int index = 0;
                while (result.next()) {
                    if (index >= table.columns().size()) throw drift();
                    PostgresColumnBinding column = table.columns().get(index++);
                    if (!table.schemaName().equals(result.getString(1))
                            || !table.tableName().equals(result.getString(2))
                            || table.tableOid() != result.getLong(3)
                            || replicaIdentity(result.getString(4))
                            != table.replicaIdentity()
                            || table.replicaIdentityIndexOid()
                            != result.getLong(5)
                            || column.attributeNumber() != result.getInt(6)
                            || !column.columnName().equals(result.getString(7))
                            || column.typeOid() != result.getLong(8)
                            || column.typeModifier() != result.getInt(9)
                            || column.nullable() != result.getBoolean(10)) {
                        throw drift();
                    }
                }
                if (index != table.columns().size()) throw drift();
            }
        }
        List<Integer> primary = new ArrayList<>();
        try (PreparedStatement statement = control.prepareStatement(
                "SELECT key.attnum FROM pg_index i "
                        + "CROSS JOIN LATERAL unnest(i.indkey) "
                        + "WITH ORDINALITY AS key(attnum, ord) "
                        + "WHERE i.indrelid = ? AND i.indisprimary "
                        + "ORDER BY key.ord")) {
            statement.setLong(1, table.tableOid());
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) primary.add(result.getInt(1));
            }
        }
        if (!primary.equals(table.primaryKeyAttributeNumbers())) throw drift();
    }

    private void resetPreHandoffSlot() throws Exception {
        boolean present = false;
        boolean active = false;
        try (PreparedStatement statement = control.prepareStatement(
                "SELECT active FROM pg_replication_slots "
                        + "WHERE slot_name = ? AND slot_type = 'logical' "
                        + "AND database = current_database()")) {
            statement.setString(1, configuration.slot());
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    present = true;
                    active = result.getBoolean(1);
                    if (result.next()) throw lifecycle();
                }
            }
        }
        if (active) {
            throw new IllegalStateException("POSTGRES_CDC_SLOT_ACTIVE");
        }
        if (present) {
            try (PreparedStatement statement = control.prepareStatement(
                    "SELECT pg_drop_replication_slot(?)")) {
                statement.setString(1, configuration.slot());
                statement.execute();
            }
        }
    }

    private void configureSnapshot(String snapshotName) throws Exception {
        snapshotConnection.setReadOnly(true);
        snapshotConnection.setTransactionIsolation(
                Connection.TRANSACTION_REPEATABLE_READ);
        snapshotConnection.setAutoCommit(false);
        String escaped = snapshotConnection.unwrap(PGConnection.class)
                .escapeLiteral(snapshotName);
        try (Statement statement = snapshotConnection.createStatement()) {
            statement.execute("SET TRANSACTION SNAPSHOT '" + escaped + "'");
        }
    }

    private static PostgresReplicaIdentity replicaIdentity(String value) {
        if ("d".equals(value)) return PostgresReplicaIdentity.DEFAULT;
        if ("i".equals(value)) return PostgresReplicaIdentity.USING_INDEX;
        if ("f".equals(value)) return PostgresReplicaIdentity.FULL;
        throw drift();
    }

    private void requireOpen() {
        if (closed.get()) throw lifecycle();
    }

    private void closeQuietly() {
        try { close(); } catch (Throwable ignored) { }
    }

    private static Throwable close(Connection connection, Throwable failure) {
        if (connection != null) {
            try { connection.close(); } catch (Throwable close) {
                return append(failure, close);
            }
        }
        return failure;
    }

    private static Throwable append(Throwable failure, Throwable close) {
        if (failure == null) return close;
        failure.addSuppressed(close);
        return failure;
    }

    private static IllegalArgumentException drift() {
        return new IllegalArgumentException("POSTGRES_CDC_SCHEMA_DRIFT");
    }

    private static IllegalStateException lifecycle() {
        return new IllegalStateException("POSTGRES_CDC_CLIENT_LIFECYCLE");
    }
}
