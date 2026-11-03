package io.rowweave.connector.s3;

import java.util.Arrays;

/** Owned ephemeral credential bytes; callers must destroy after client close. */
public final class S3Credentials implements AutoCloseable {
    private byte[] accessKeyId;
    private byte[] secretAccessKey;
    private byte[] sessionToken;
    private boolean destroyed;

    private S3Credentials(byte[] accessKeyId, byte[] secretAccessKey,
            byte[] sessionToken) {
        this.accessKeyId = field(accessKeyId,
                S3CredentialCodec.MAX_ACCESS_KEY_BYTES);
        this.secretAccessKey = field(secretAccessKey,
                S3CredentialCodec.MAX_SECRET_KEY_BYTES);
        this.sessionToken = sessionToken == null ? null
                : field(sessionToken,
                S3CredentialCodec.MAX_SESSION_TOKEN_BYTES);
    }

    public static S3Credentials basic(byte[] accessKeyId,
            byte[] secretAccessKey) {
        return new S3Credentials(accessKeyId, secretAccessKey, null);
    }

    public static S3Credentials session(byte[] accessKeyId,
            byte[] secretAccessKey, byte[] sessionToken) {
        return new S3Credentials(accessKeyId, secretAccessKey, sessionToken);
    }

    private static byte[] field(byte[] value, int maximum) {
        if (value == null || value.length < 1 || value.length > maximum) {
            throw invalid();
        }
        String decoded = S3CodecSupport.decodeUtf8(value,
                "S3_CREDENTIAL_INVALID");
        S3CodecSupport.utf8(decoded, maximum, false,
                "S3_CREDENTIAL_INVALID");
        return value.clone();
    }

    public synchronized byte[] accessKeyId() {
        requireLive(); return accessKeyId.clone();
    }
    public synchronized byte[] secretAccessKey() {
        requireLive(); return secretAccessKey.clone();
    }
    public synchronized boolean hasSessionToken() {
        requireLive(); return sessionToken != null;
    }
    public synchronized byte[] sessionToken() {
        requireLive();
        if (sessionToken == null) {
            throw new IllegalStateException("S3 credentials have no session token");
        }
        return sessionToken.clone();
    }
    public synchronized boolean destroyed() { return destroyed; }

    public synchronized void destroy() {
        if (destroyed) return;
        Arrays.fill(accessKeyId, (byte) 0);
        Arrays.fill(secretAccessKey, (byte) 0);
        if (sessionToken != null) Arrays.fill(sessionToken, (byte) 0);
        destroyed = true;
    }

    @Override public void close() { destroy(); }

    private void requireLive() {
        if (destroyed) throw new IllegalStateException(
                "S3 credentials were destroyed");
    }

    @Override public String toString() {
        return "S3Credentials{session=" + (sessionToken != null) + '}';
    }

    private static IllegalArgumentException invalid() {
        return S3CodecSupport.invalid("S3_CREDENTIAL_INVALID");
    }
}
