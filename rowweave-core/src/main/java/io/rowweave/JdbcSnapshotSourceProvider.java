package io.rowweave;

import java.io.Serializable;

/** Stateless, signed deployment extension for opening one exact JDBC snapshot split. */
public interface JdbcSnapshotSourceProvider extends Serializable {

    /** Stable provider family identifier. */
    String providerType();

    /** Positive execution contract version. */
    int providerVersion();

    /** Resolve local references and open the exact snapshot split on a Worker. */
    JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request);
}
