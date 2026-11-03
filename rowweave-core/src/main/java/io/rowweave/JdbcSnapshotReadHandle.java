package io.rowweave;

import java.sql.ResultSet;

/** Exclusive response handle for one ordered JDBC snapshot split. */
public interface JdbcSnapshotReadHandle extends AutoCloseable {

    String providerType();

    int providerVersion();

    String queryDigest();

    String partitionDigest();

    String snapshotId();

    String schemaFingerprint();

    ResultSet resultSet();

    @Override
    void close();
}
