package io.rowweave.connector.postgres;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresCredentialCodecTest {
    @Test
    void credentialsAreBoundedCanonicalDefensivelyOwnedAndDestroyable() {
        byte[] username = "rowweave_reader".getBytes(StandardCharsets.UTF_8);
        byte[] password = "correct horse battery staple"
                .getBytes(StandardCharsets.UTF_8);
        PostgresCredentials credentials =
                PostgresCredentials.of(username, password);

        byte[] frame = PostgresCredentialCodec.encode(credentials);
        assertArrayEquals(hex("52575850474352310000003700000001"
                        + "0000000f726f7777656176655f7265616465720000001c63"
                        + "6f727265637420686f727365206261747465727920737461"
                        + "706c6540d512c9a4ab5e45321343ac8a3b725953e066bdd5"
                        + "e2a1315ad33cc05794b4c0"), frame);
        PostgresCredentials restored = PostgresCredentialCodec.decode(frame);
        assertArrayEquals(username, restored.username());
        assertArrayEquals(password, restored.password());
        assertArrayEquals(frame, PostgresCredentialCodec.encode(restored));
        assertFalse(restored.toString().contains("rowweave_reader"));
        assertFalse(restored.toString().contains("correct horse"));

        byte[] leaked = restored.password();
        restored.destroy();
        assertTrue(restored.destroyed());
        assertArrayEquals(password, leaked);
        assertThrows(IllegalStateException.class, restored::username);
        assertThrows(IllegalStateException.class, restored::password);
        restored.destroy();

        assertBoundary(PostgresCredentialCodec.MAX_USERNAME_BYTES, true);
        assertBoundary(PostgresCredentialCodec.MAX_PASSWORD_BYTES, false);
        assertInvalid(() -> PostgresCredentials.of(
                new byte[PostgresCredentialCodec.MAX_USERNAME_BYTES + 1],
                new byte[] {'p'}));
        assertInvalid(() -> PostgresCredentials.of(new byte[] {'u'},
                new byte[PostgresCredentialCodec.MAX_PASSWORD_BYTES + 1]));
        assertInvalid(() -> PostgresCredentials.of(
                new byte[] {(byte) 0xc3, (byte) 0x28}, new byte[] {'p'}));

        byte[] corrupt = frame.clone();
        corrupt[corrupt.length / 2] ^= 1;
        assertInvalid(() -> PostgresCredentialCodec.decode(corrupt));
        assertInvalid(() -> PostgresCredentialCodec.decode(
                Arrays.copyOf(frame, frame.length - 1)));
        byte[] trailing = Arrays.copyOf(frame, frame.length + 1);
        assertInvalid(() -> PostgresCredentialCodec.decode(trailing));
    }

    private static void assertBoundary(int limit, boolean username) {
        byte[] below = bytes(limit - 1);
        byte[] exact = bytes(limit);
        if (username) {
            PostgresCredentials.of(below, new byte[] {'p'}).destroy();
            PostgresCredentials.of(exact, new byte[] {'p'}).destroy();
        } else {
            PostgresCredentials.of(new byte[] {'u'}, below).destroy();
            PostgresCredentials.of(new byte[] {'u'}, exact).destroy();
        }
    }

    private static byte[] bytes(int size) {
        byte[] value = new byte[size];
        Arrays.fill(value, (byte) 'x');
        return value;
    }

    private static void assertInvalid(Runnable action) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, action::run);
        assertTrue(failure.getMessage().startsWith(
                "POSTGRES_CDC_CREDENTIAL_"), failure.getMessage());
    }

    private static byte[] hex(String value) {
        byte[] result = new byte[value.length() / 2];
        for (int index = 0; index < result.length; index++) {
            result[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return result;
    }
}
