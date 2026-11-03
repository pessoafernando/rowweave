package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;
import java.util.NoSuchElementException;
import java.util.TimeZone;

/** Strict one-row-at-a-time mapper for one exact JDBC snapshot split. */
public final class JdbcSnapshotInputExecutor
        extends SourceExecutor<JdbcSnapshotSource> {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");

    private volatile JdbcSnapshotReadHandle handle;
    private volatile ResultSet resultSet;
    private volatile DataRow nextRow;
    private volatile boolean opened;
    private volatile boolean eof;
    private volatile boolean closed;

    public JdbcSnapshotInputExecutor(JdbcSnapshotSource input) {
        super(input, input.getSchema());
    }

    @Override
    public void open() {
        if (opened && !closed) return;
        if (closed) throw failure("JDBC_EXECUTOR_CLOSED");

        JdbcSnapshotReadHandle openedHandle;
        try {
            openedHandle = input.provider().open(input.readRequest());
        } catch (RuntimeException providerFailure) {
            throw failure("JDBC_OPEN_FAILED");
        }
        if (openedHandle == null) throw failure("JDBC_INVALID_HANDLE");
        boolean cancelled;
        synchronized (this) {
            cancelled = closed;
            if (!cancelled) handle = openedHandle;
        }
        if (cancelled) {
            closeDetached(openedHandle, null);
            throw failure("JDBC_EXECUTOR_CLOSED");
        }
        try {
            validateHandle(openedHandle);
            ResultSet openedResultSet = openedHandle.resultSet();
            if (openedResultSet == null) throw failure("JDBC_INVALID_HANDLE");
            validateMetadata(openedResultSet);
            synchronized (this) {
                if (closed || handle != openedHandle) {
                    throw failure("JDBC_EXECUTOR_CLOSED");
                }
                resultSet = openedResultSet;
                opened = true;
            }
        } catch (Throwable invalid) {
            Throwable primary = sanitize(invalid, "JDBC_OPEN_FAILED");
            closeAfterFailure(primary);
            throwUnchecked(primary);
        }
    }

    private void validateHandle(JdbcSnapshotReadHandle candidate) {
        JdbcSnapshotReadRequest expected = input.readRequest();
        if (!input.providerType().equals(candidate.providerType())
                || input.providerVersion() != candidate.providerVersion()
                || !expected.queryDigest().equals(candidate.queryDigest())
                || !expected.partitionDigest().equals(candidate.partitionDigest())
                || !expected.snapshotId().equals(candidate.snapshotId())) {
            throw failure("JDBC_IDENTITY_MISMATCH");
        }
        if (!SchemaFingerprint.of(dataSchema).equals(candidate.schemaFingerprint())) {
            throw failure("JDBC_SCHEMA_MISMATCH");
        }
    }

    private void validateMetadata(ResultSet candidate) {
        try {
            ResultSetMetaData metadata = candidate.getMetaData();
            if (metadata == null || metadata.getColumnCount() != dataSchema.getFieldCount()) {
                throw failure("JDBC_SCHEMA_MISMATCH");
            }
            for (int index = 0; index < dataSchema.getFieldCount(); index++) {
                int jdbcType = metadata.getColumnType(index + 1);
                if (!supports(dataSchema.getField(index).getDataType(), jdbcType)) {
                    throw failure("JDBC_SCHEMA_MISMATCH");
                }
            }
        } catch (SQLException metadataFailure) {
            throw failure("JDBC_SCHEMA_MISMATCH");
        }
    }

    private static boolean supports(DataType dataType, int jdbcType) {
        if (dataType == null) return false;
        switch (dataType) {
            case BooleanType:
                return jdbcType == Types.BOOLEAN;
            case IntegerType:
                return jdbcType == Types.TINYINT || jdbcType == Types.SMALLINT
                        || jdbcType == Types.INTEGER;
            case LongType:
                return jdbcType == Types.TINYINT || jdbcType == Types.SMALLINT
                        || jdbcType == Types.INTEGER || jdbcType == Types.BIGINT;
            case BigDecimalType:
                return jdbcType == Types.DECIMAL || jdbcType == Types.NUMERIC;
            case StringType:
                return jdbcType == Types.CHAR || jdbcType == Types.VARCHAR
                        || jdbcType == Types.NCHAR || jdbcType == Types.NVARCHAR;
            case DateType:
                return jdbcType == Types.DATE;
            case TimestampType:
                return jdbcType == Types.TIMESTAMP;
            case NullType:
            default:
                return false;
        }
    }

    @Override
    public boolean hasNext() {
        if (nextRow != null) return true;
        if (eof) return false;
        if (closed) throw failure("JDBC_EXECUTOR_CLOSED");
        if (!opened) {
            throw failure("JDBC_EXECUTOR_NOT_OPEN");
        }
        try {
            if (!resultSet.next()) {
                eof = true;
                closeResource(null);
                return false;
            }
            nextRow = mapCurrentRow();
            return true;
        } catch (Throwable readFailure) {
            Throwable primary = sanitize(readFailure, "JDBC_READ_FAILED");
            closeAfterFailure(primary);
            throwUnchecked(primary);
            return false;
        }
    }

    private DataRow mapCurrentRow() throws SQLException {
        DataRow row = new DataRow(dataSchema.getFieldCount());
        for (int index = 0; index < row.size(); index++) {
            row.set(index, readCell(index, dataSchema.getField(index).getDataType()));
        }
        return row;
    }

    private Object readCell(int zeroBasedIndex, DataType type) throws SQLException {
        int column = zeroBasedIndex + 1;
        switch (type) {
            case BooleanType: {
                boolean value = resultSet.getBoolean(column);
                return resultSet.wasNull() ? null : value;
            }
            case IntegerType: {
                long value = resultSet.getLong(column);
                if (resultSet.wasNull()) return null;
                if (value < Integer.MIN_VALUE || value > Integer.MAX_VALUE) {
                    throw failure("JDBC_VALUE_OUT_OF_RANGE");
                }
                return (int) value;
            }
            case LongType: {
                long value = resultSet.getLong(column);
                return resultSet.wasNull() ? null : value;
            }
            case BigDecimalType: {
                BigDecimal value = resultSet.getBigDecimal(column);
                if (resultSet.wasNull()) return null;
                enforceDecimalLimit(value, input.readRequest().maxCellBytes());
                return value;
            }
            case StringType: {
                String value = resultSet.getString(column);
                if (resultSet.wasNull()) return null;
                enforceUtf8Limit(value, input.readRequest().maxCellBytes());
                return value;
            }
            case DateType: {
                java.sql.Date value = resultSet.getDate(column, utcCalendar());
                return resultSet.wasNull() ? null : value;
            }
            case TimestampType: {
                java.sql.Timestamp value = resultSet.getTimestamp(column, utcCalendar());
                return resultSet.wasNull() ? null : value;
            }
            case NullType:
            default:
                throw failure("JDBC_SCHEMA_MISMATCH");
        }
    }

    private static Calendar utcCalendar() {
        return Calendar.getInstance(UTC);
    }

    private static void enforceUtf8Limit(String value, int maxBytes) {
        long bytes = 0L;
        for (int index = 0; index < value.length(); index++) {
            char current = value.charAt(index);
            if (current <= 0x7f) {
                bytes++;
            } else if (current <= 0x7ff) {
                bytes += 2L;
            } else if (Character.isHighSurrogate(current)) {
                if (index + 1 >= value.length()
                        || !Character.isLowSurrogate(value.charAt(index + 1))) {
                    throw failure("JDBC_READ_FAILED");
                }
                index++;
                bytes += 4L;
            } else if (Character.isLowSurrogate(current)) {
                throw failure("JDBC_READ_FAILED");
            } else {
                bytes += 3L;
            }
            if (bytes > maxBytes) throw failure("JDBC_CELL_LIMIT_EXCEEDED");
        }
    }

    private static void enforceDecimalLimit(BigDecimal value, int maxBytes) {
        if (value == null) throw failure("JDBC_READ_FAILED");
        long sign = value.signum() < 0 ? 1L : 0L;
        long scale = value.scale();
        long precision = value.precision();
        long canonicalBytes;
        if (value.signum() == 0) {
            canonicalBytes = scale > 0L ? scale + 2L : 1L;
        } else if (scale <= 0L) {
            canonicalBytes = sign + precision - scale;
        } else {
            canonicalBytes = sign + Math.max(precision - scale, 1L)
                    + 1L + scale;
        }
        if (canonicalBytes > maxBytes) {
            throw failure("JDBC_CELL_LIMIT_EXCEEDED");
        }
    }

    @Override
    public DataRow next() {
        if (!hasNext()) throw new NoSuchElementException("jdbc source is exhausted");
        DataRow result = nextRow;
        nextRow = null;
        return result;
    }

    @Override
    public void close() {
        if (closed) return;
        closeResource(null);
    }

    private void closeAfterFailure(Throwable primary) {
        closeResource(primary);
    }

    private void closeResource(Throwable primary) {
        JdbcSnapshotReadHandle current;
        synchronized (this) {
            current = handle;
            handle = null;
            resultSet = null;
            nextRow = null;
            closed = true;
        }
        if (current == null) return;
        closeDetached(current, primary);
    }

    private static void closeDetached(
            JdbcSnapshotReadHandle current, Throwable primary) {
        try {
            current.close();
        } catch (Throwable closeFailure) {
            JdbcFailure sanitized = failure("JDBC_CLOSE_FAILED");
            if (primary != null) {
                primary.addSuppressed(sanitized);
            } else {
                throw sanitized;
            }
        }
    }

    private static Throwable sanitize(Throwable failure, String fallbackCode) {
        if (failure instanceof Error) return failure;
        if (failure instanceof JdbcFailure) return failure;
        return failure(fallbackCode);
    }

    private static JdbcFailure failure(String code) {
        return new JdbcFailure(code);
    }

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        throw failure("JDBC_READ_FAILED");
    }

    /** Marker keeps already-sanitized public failures from being reclassified. */
    private static final class JdbcFailure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private JdbcFailure(String code) {
            super(code);
        }
    }
}
