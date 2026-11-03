package io.rowweave.connector.postgres;

import java.util.Arrays;

/** Owned Worker-local PostgreSQL credential bytes. */
public final class PostgresCredentials implements AutoCloseable {
    private byte[] username;
    private byte[] password;
    private boolean destroyed;

    private PostgresCredentials(byte[] username, byte[] password) {
        this.username = field(username,
                PostgresCredentialCodec.MAX_USERNAME_BYTES, false);
        this.password = field(password,
                PostgresCredentialCodec.MAX_PASSWORD_BYTES, true);
    }

    public static PostgresCredentials of(byte[] username, byte[] password) {
        return new PostgresCredentials(username, password);
    }

    private static byte[] field(byte[] value, int maximum,
            boolean allowControls) {
        if (value == null || value.length < 1 || value.length > maximum) {
            throw invalid();
        }
        String decoded = PostgresCodecSupport.decodeUtf8(value,
                "POSTGRES_CDC_CREDENTIAL_INVALID");
        PostgresCodecSupport.utf8(decoded, maximum, allowControls,
                "POSTGRES_CDC_CREDENTIAL_INVALID");
        return value.clone();
    }

    public synchronized byte[] username() {
        requireLive();
        return username.clone();
    }

    public synchronized byte[] password() {
        requireLive();
        return password.clone();
    }

    public synchronized boolean destroyed() { return destroyed; }

    public synchronized void destroy() {
        if (destroyed) return;
        Arrays.fill(username, (byte) 0);
        Arrays.fill(password, (byte) 0);
        destroyed = true;
    }

    @Override public void close() { destroy(); }

    private void requireLive() {
        if (destroyed) {
            throw new IllegalStateException(
                    "PostgreSQL credentials were destroyed");
        }
    }

    @Override public String toString() {
        return "PostgresCredentials{destroyed=" + destroyed + '}';
    }

    private static IllegalArgumentException invalid() {
        return PostgresCodecSupport.invalid(
                "POSTGRES_CDC_CREDENTIAL_INVALID");
    }
}
