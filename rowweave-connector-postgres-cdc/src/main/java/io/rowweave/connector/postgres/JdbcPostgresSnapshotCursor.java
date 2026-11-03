package io.rowweave.connector.postgres;

import io.rowweave.DataType;
import io.rowweave.DataRow;
import org.postgresql.PGConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.atomic.AtomicBoolean;

final class JdbcPostgresSnapshotCursor implements PostgresSnapshotCursor {
    private final PostgresConnectorConfiguration configuration;
    private final Connection connection;
    private final PostgresCdcPosition handoff;
    private final AtomicBoolean closed = new AtomicBoolean();
    private int tableIndex;
    private PreparedStatement statement;
    private ResultSet rows;

    JdbcPostgresSnapshotCursor(
            PostgresConnectorConfiguration configuration,
            Connection connection, PostgresCdcPosition handoff) {
        this.configuration = configuration;
        this.connection = connection;
        this.handoff = handoff;
    }

    @Override public PostgresCdcPosition handoffPosition() {
        return handoff;
    }

    @Override public DataRow next() {
        if (closed.get()) return null;
        try {
            while (true) {
                if (rows == null && tableIndex < configuration.tables().size()) {
                    open(configuration.tables().get(tableIndex));
                }
                if (rows == null) return null;
                if (rows.next()) {
                    return decode(configuration.tables().get(tableIndex));
                }
                closeQuery();
                tableIndex++;
            }
        } catch (Throwable failure) {
            throw new IllegalStateException("POSTGRES_CDC_SNAPSHOT_FAILED");
        }
    }

    private void open(PostgresTableBinding table) throws Exception {
        PGConnection pg = connection.unwrap(PGConnection.class);
        StringBuilder sql = new StringBuilder("SELECT ");
        for (int index = 0; index < table.columns().size(); index++) {
            if (index > 0) sql.append(',');
            sql.append(pg.escapeIdentifier(
                    table.columns().get(index).columnName()));
        }
        sql.append(" FROM ").append(pg.escapeIdentifier(table.schemaName()))
                .append('.').append(pg.escapeIdentifier(table.tableName()))
                .append(" ORDER BY ");
        for (int index = 0;
                index < table.primaryKeyAttributeNumbers().size(); index++) {
            if (index > 0) sql.append(',');
            int attribute = table.primaryKeyAttributeNumbers().get(index);
            sql.append(pg.escapeIdentifier(column(table, attribute)
                    .columnName()));
        }
        statement = connection.prepareStatement(sql.toString(),
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        statement.setFetchSize(configuration.snapshotFetchRows());
        rows = statement.executeQuery();
    }

    private DataRow decode(PostgresTableBinding table) throws Exception {
        DataRow row = new DataRow(table.columns().size());
        for (int index = 0; index < table.columns().size(); index++) {
            PostgresColumnBinding column = table.columns().get(index);
            Object value = value(index + 1,
                    PostgresTypeMapping.dataType(column.typeOid()));
            if (value == null && !column.nullable()) {
                throw new IllegalArgumentException(
                        "POSTGRES_CDC_VALUE_INVALID");
            }
            row.set(index, value);
        }
        return row;
    }

    private Object value(int index, DataType type) throws Exception {
        Object value;
        switch (type) {
            case BooleanType: value = rows.getBoolean(index); break;
            case IntegerType: value = rows.getInt(index); break;
            case LongType: value = rows.getLong(index); break;
            case BigDecimalType: value = rows.getBigDecimal(index); break;
            case StringType: value = rows.getString(index); break;
            case DateType: value = rows.getDate(index); break;
            case TimestampType: value = rows.getTimestamp(index); break;
            default: throw new IllegalArgumentException(
                    "POSTGRES_CDC_TYPE_UNSUPPORTED");
        }
        return rows.wasNull() ? null : value;
    }

    @Override public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) return;
        Throwable failure = null;
        try { closeQuery(); } catch (Throwable close) { failure = close; }
        try { connection.rollback(); } catch (Throwable close) {
            if (failure == null) failure = close;
            else failure.addSuppressed(close);
        }
        if (failure != null) {
            throw new IllegalStateException("POSTGRES_CDC_SNAPSHOT_CLOSE_FAILED");
        }
    }

    private void closeQuery() throws Exception {
        Throwable failure = null;
        ResultSet currentRows = rows;
        rows = null;
        if (currentRows != null) {
            try { currentRows.close(); } catch (Throwable close) {
                failure = close;
            }
        }
        PreparedStatement currentStatement = statement;
        statement = null;
        if (currentStatement != null) {
            try { currentStatement.close(); } catch (Throwable close) {
                if (failure == null) failure = close;
                else failure.addSuppressed(close);
            }
        }
        if (failure != null) throw new Exception(failure);
    }

    private static PostgresColumnBinding column(
            PostgresTableBinding table, int attribute) {
        for (PostgresColumnBinding column : table.columns()) {
            if (column.attributeNumber() == attribute) return column;
        }
        throw new IllegalArgumentException("POSTGRES_CDC_SCHEMA_DRIFT");
    }
}
