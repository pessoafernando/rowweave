package io.rowweave.connector.postgres;

import org.postgresql.PGProperty;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.sql.Connection;
import java.sql.Driver;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Properties;

final class JdbcPostgresCdcClientFactory
        implements PostgresCdcClientFactory {
    @Override public PostgresCdcClient create(
            PostgresConnectorConfiguration configuration,
            PostgresCredentials credentials, byte[] tlsTrust)
            throws Exception {
        byte[] username = credentials.username();
        byte[] password = credentials.password();
        Path trustFile = null;
        Connection control = null;
        Connection snapshot = null;
        Connection replication = null;
        Properties regular = new Properties();
        Properties replicationProperties = new Properties();
        try {
            String user = new String(username, StandardCharsets.UTF_8);
            String secret = new String(password, StandardCharsets.UTF_8);
            configureCommon(regular, configuration, user, secret);
            if (tlsTrust != null) {
                trustFile = writeTrust(tlsTrust);
                PGProperty.SSL_ROOT_CERT.set(
                        regular, trustFile.toAbsolutePath().toString());
            }
            replicationProperties.putAll(regular);
            // 42.7.13 has an open blocking-read/socket-timeout regression.
            // Nonblocking polling plus explicit completed-cut status updates
            // avoids both that path and any implicit acknowledgement.
            PGProperty.REPLICATION.set(replicationProperties, "database");
            PGProperty.PREFER_QUERY_MODE.set(replicationProperties, "simple");
            PGProperty.ASSUME_MIN_SERVER_VERSION.set(
                    replicationProperties, "10");
            PGProperty.SOCKET_TIMEOUT.set(replicationProperties, 0);
            String url = jdbcUrl(configuration);
            Driver driver = new org.postgresql.Driver();
            control = connect(driver, url, regular);
            snapshot = connect(driver, url, regular);
            replication = connect(driver, url, replicationProperties);
            return new JdbcPostgresCdcClient(configuration,
                    control, snapshot, replication);
        } catch (Throwable failure) {
            close(replication);
            close(snapshot);
            close(control);
            throw new IllegalStateException("POSTGRES_CDC_OPEN_FAILED");
        } finally {
            regular.clear();
            replicationProperties.clear();
            Arrays.fill(username, (byte) 0);
            Arrays.fill(password, (byte) 0);
            if (tlsTrust != null) Arrays.fill(tlsTrust, (byte) 0);
            if (trustFile != null) Files.deleteIfExists(trustFile);
        }
    }

    private static void configureCommon(Properties properties,
            PostgresConnectorConfiguration configuration,
            String username, String password) {
        PGProperty.USER.set(properties, username);
        PGProperty.PASSWORD.set(properties, password);
        PGProperty.APPLICATION_NAME.set(properties, "rowweave-postgres-cdc");
        PGProperty.CONNECT_TIMEOUT.set(properties,
                seconds(configuration.connectTimeoutMillis()));
        PGProperty.SOCKET_TIMEOUT.set(properties,
                seconds(configuration.socketTimeoutMillis()));
        PGProperty.TCP_KEEP_ALIVE.set(properties, true);
        PGProperty.LOG_SERVER_ERROR_DETAIL.set(properties, false);
        PGProperty.SSL_MODE.set(properties, sslMode(configuration.tlsMode()));
    }

    private static int seconds(int millis) {
        return Math.max(1, (millis + 999) / 1000);
    }

    private static Connection connect(Driver driver, String url,
            Properties properties) throws Exception {
        Connection connection = driver.connect(url, properties);
        if (connection == null) {
            throw new IllegalStateException("POSTGRES_CDC_OPEN_FAILED");
        }
        return connection;
    }

    private static String sslMode(PostgresTlsMode mode) {
        switch (mode) {
            case DISABLED: return "disable";
            case REQUIRE: return "require";
            case VERIFY_CA: return "verify-ca";
            case VERIFY_FULL: return "verify-full";
            default: throw new IllegalArgumentException(
                    "POSTGRES_CDC_CONFIGURATION_INVALID");
        }
    }

    private static String jdbcUrl(PostgresConnectorConfiguration config)
            throws Exception {
        String database = URLEncoder.encode(config.database(), "UTF-8")
                .replace("+", "%20");
        String host = config.host();
        if (host.indexOf(':') >= 0 && !host.startsWith("[")) {
            host = '[' + host + ']';
        }
        return "jdbc:postgresql://" + host + ':' + config.port()
                + '/' + database;
    }

    private static Path writeTrust(byte[] trust) throws Exception {
        if (trust.length < 1 || trust.length > 1024 * 1024) {
            throw new IllegalArgumentException(
                    "POSTGRES_CDC_TLS_TRUST_INVALID");
        }
        Path path = Files.createTempFile("rowweave-pg-ca-", ".pem");
        try {
            try {
                Files.setPosixFilePermissions(path, EnumSet.of(
                        PosixFilePermission.OWNER_READ,
                        PosixFilePermission.OWNER_WRITE));
            } catch (UnsupportedOperationException ignored) {
                // ACL semantics are platform-specific; the file remains a
                // private createTempFile and is deleted before open returns.
            }
            Files.write(path, trust);
            return path;
        } catch (Throwable failure) {
            Files.deleteIfExists(path);
            throw failure;
        }
    }

    private static void close(Connection connection) {
        if (connection != null) {
            try { connection.close(); } catch (Throwable ignored) { }
        }
    }
}
