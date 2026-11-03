package io.rowweave.connector.s3;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class S3CredentialCodecTest {
    @Test
    void credentialFrameIsBoundedCanonicalAndDestroyable() {
        byte[] access = "access-key".getBytes(StandardCharsets.UTF_8);
        byte[] secret = "secret-key".getBytes(StandardCharsets.UTF_8);
        byte[] token = "session-token".getBytes(StandardCharsets.UTF_8);
        S3Credentials credentials = S3Credentials.session(
                access, secret, token);

        byte[] frame = S3CredentialCodec.encode(credentials);
        S3Credentials restored = S3CredentialCodec.decode(frame);
        assertArrayEquals(access, restored.accessKeyId());
        assertArrayEquals(secret, restored.secretAccessKey());
        assertArrayEquals(token, restored.sessionToken());
        assertArrayEquals(frame, S3CredentialCodec.encode(restored));
        assertFalse(restored.toString().contains("access-key"));
        assertFalse(restored.toString().contains("secret-key"));
        assertFalse(restored.toString().contains("session-token"));

        byte[] leaked = restored.secretAccessKey();
        restored.destroy();
        assertTrue(restored.destroyed());
        assertArrayEquals(secret, leaked,
                "defensive copies must not alias the destroyed owner");
        assertThrows(IllegalStateException.class, restored::accessKeyId);
        assertThrows(IllegalStateException.class, restored::secretAccessKey);
        assertThrows(IllegalStateException.class, restored::sessionToken);
        restored.destroy();

        byte[] corrupt = frame.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertInvalid(() -> S3CredentialCodec.decode(corrupt));
        assertInvalid(() -> S3CredentialCodec.decode(
                Arrays.copyOf(frame, frame.length - 1)));
        byte[] trailing = Arrays.copyOf(frame, frame.length + 1);
        assertInvalid(() -> S3CredentialCodec.decode(trailing));

        assertInvalid(() -> S3Credentials.basic(
                new byte[S3CredentialCodec.MAX_ACCESS_KEY_BYTES + 1],
                new byte[] {1}));
        assertInvalid(() -> S3Credentials.session(
                new byte[] {1}, new byte[] {2},
                new byte[S3CredentialCodec.MAX_SESSION_TOKEN_BYTES + 1]));
    }

    private static void assertInvalid(Runnable action) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, action::run);
        assertTrue(failure.getMessage().startsWith("S3_CREDENTIAL_"),
                failure.getMessage());
    }
}
