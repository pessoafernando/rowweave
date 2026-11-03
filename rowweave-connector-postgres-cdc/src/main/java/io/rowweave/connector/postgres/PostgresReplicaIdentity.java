package io.rowweave.connector.postgres;

/** Supported PostgreSQL replica identity modes for exact update/delete keys. */
public enum PostgresReplicaIdentity {
    DEFAULT,
    USING_INDEX,
    FULL
}
