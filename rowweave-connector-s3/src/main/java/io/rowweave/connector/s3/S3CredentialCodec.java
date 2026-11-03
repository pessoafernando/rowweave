package io.rowweave.connector.s3;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/** Canonical checksummed codec for Worker-local S3 credential payloads. */
public final class S3CredentialCodec {
    public static final int MAX_ACCESS_KEY_BYTES = 256;
    public static final int MAX_SECRET_KEY_BYTES = 1024;
    public static final int MAX_SESSION_TOKEN_BYTES = 8192;
    public static final int MAX_FRAME_BYTES = 16 * 1024;
    private static final String CODE = "S3_CREDENTIAL_INVALID";
    private static final byte[] MAGIC =
            "RWXS3CR1".getBytes(StandardCharsets.US_ASCII);

    private S3CredentialCodec() { }

    public static byte[] encode(S3Credentials credentials) {
        if (credentials == null || credentials.destroyed()) throw invalid();
        byte[] access = credentials.accessKeyId();
        byte[] secret = credentials.secretAccessKey();
        byte[] token = credentials.hasSessionToken()
                ? credentials.sessionToken() : null;
        try {
            return S3CodecSupport.encode(MAGIC, MAX_FRAME_BYTES, output -> {
                output.writeBoolean(token != null);
                write(output, access, MAX_ACCESS_KEY_BYTES);
                write(output, secret, MAX_SECRET_KEY_BYTES);
                if (token != null) {
                    write(output, token, MAX_SESSION_TOKEN_BYTES);
                }
            }, CODE);
        } finally {
            java.util.Arrays.fill(access, (byte) 0);
            java.util.Arrays.fill(secret, (byte) 0);
            if (token != null) java.util.Arrays.fill(token, (byte) 0);
        }
    }

    public static S3Credentials decode(byte[] frame) {
        S3Credentials result = S3CodecSupport.decode(
                frame, MAGIC, MAX_FRAME_BYTES, input -> {
                    boolean session = input.readBoolean();
                    byte[] access = read(input, MAX_ACCESS_KEY_BYTES);
                    byte[] secret = read(input, MAX_SECRET_KEY_BYTES);
                    byte[] token = session
                            ? read(input, MAX_SESSION_TOKEN_BYTES) : null;
                    try {
                        return session
                                ? S3Credentials.session(access, secret, token)
                                : S3Credentials.basic(access, secret);
                    } finally {
                        java.util.Arrays.fill(access, (byte) 0);
                        java.util.Arrays.fill(secret, (byte) 0);
                        if (token != null) java.util.Arrays.fill(token, (byte) 0);
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
        return S3CodecSupport.invalid(CODE);
    }
}
