package io.rowweave.connector.postgres;

import io.rowweave.DataRow;

interface PostgresSnapshotCursor extends AutoCloseable {
    PostgresCdcPosition handoffPosition();
    DataRow next() throws Exception;
    @Override void close() throws Exception;
}
