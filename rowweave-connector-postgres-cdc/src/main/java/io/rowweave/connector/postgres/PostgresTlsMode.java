package io.rowweave.connector.postgres;

/** PostgreSQL JDBC TLS verification mode frozen into connector config. */
public enum PostgresTlsMode {
    DISABLED,
    REQUIRE,
    VERIFY_CA,
    VERIFY_FULL
}
