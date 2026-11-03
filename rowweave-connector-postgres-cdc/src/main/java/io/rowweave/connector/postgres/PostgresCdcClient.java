package io.rowweave.connector.postgres;

import java.nio.ByteBuffer;

interface PostgresCdcClient extends AutoCloseable {
    PostgresSnapshotCursor startSnapshot() throws Exception;
    void startLog(PostgresCdcPosition position) throws Exception;
    ByteBuffer readPending() throws Exception;
    long retainedWalBytes() throws Exception;
    void acknowledge(PostgresCdcPosition position) throws Exception;
    void wakeUp();
    void cancel();
    @Override void close() throws Exception;
}
