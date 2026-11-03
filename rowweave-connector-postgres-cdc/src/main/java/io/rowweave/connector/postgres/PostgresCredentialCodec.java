package io.rowweave.connector.postgres;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

/** Canonical checksummed codec for Worker-local PostgreSQL credentials. */
public final class PostgresCredentialCodec {
    public static final int MAX_USERNAME_BYTES = 256;
    public static final int MAX_PASSWORD_BYTES = 4096;
    public static final int MAX_FRAME_BYTES = 8 * 1024;
    private static final String CODE = "POSTGRES_CDC_CREDENTIAL_INVALID";
    private static final byte[] MAGIC =
            "RWXPGCR1".getBytes(StandardCharsets.US_ASCII);

    private PostgresCredentialCodec() { }

    public static byte[] encode(PostgresCredentials credentials) {
        if (credentials == null || credentials.destroyed()) throw invalid();
        byte[] username = credentials.username();
        byte[] password = credentials.password();
        try {
            return PostgresCodecSupport.encode(MAGIC, MAX_FRAME_BYTES,
                    output -> {
                        write(output, username, MAX_USERNAME_BYTES);
                        write(output, password, MAX_PASSWORD_BYTES);
                    }, CODE);
        } finally {
            Arrays.fill(username, (byte) 0);
            Arrays.fill(password, (byte) 0);
        }
    }

    public static PostgresCredentials decode(byte[] frame) {
        PostgresCredentials result = PostgresCodecSupport.decode(
                frame, MAGIC, MAX_FRAME_BYTES, input -> {
                    byte[] username = read(input, MAX_USERNAME_BYTES);
                    byte[] password = read(input, MAX_PASSWORD_BYTES);
                    try {
                        return PostgresCredentials.of(username, password);
                    } finally {
                        Arrays.fill(username, (byte) 0);
                        Arrays.fill(password, (byte) 0);
                    }
                }, CODE);
        if (!MessageDigest.isEqual(frame, encode(result))) {
            result.destroy();
            throw invalid();
        }
        return result;
    }

    private static void write(java.io.DataOutputStream output, byte[] bytes,
            int maximum) throws java.io.IOException {
        if (bytes.length < 1 || bytes.length > maximum) throw invalid();
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static byte[] read(java.io.DataInputStream input, int maximum)
            throws java.io.IOException {
        int length = input.readInt();
        if (length < 1 || length > maximum) throw invalid();
        byte[] bytes = new byte[length];
        input.readFully(bytes);
        return bytes;
    }

    private static IllegalArgumentException invalid() {
        return PostgresCodecSupport.invalid(CODE);
    }
}
