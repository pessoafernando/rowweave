package io.rowweave.connector.postgres;

import io.rowweave.stream.spi.StreamSecretResolver;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Test-only process service proving Worker-local PostgreSQL secret loading. */
public final class PostgresTestSecretResolver
        implements StreamSecretResolver {
    static final String SECRET_PATH_PROPERTY =
            "rowweave.test.postgres.secret.path";
    static final String MARKER_PATH_PROPERTY =
            "rowweave.test.postgres.secret.marker";

    public PostgresTestSecretResolver() { }

    @Override public byte[] resolve(String reference) {
        final byte[] value;
        if ("vault/postgres".equals(reference)) {
            value = readCredential();
        } else if ("vault/sink".equals(reference)) {
            value = new byte[] {1};
        } else {
            throw new IllegalArgumentException("unknown test secret");
        }
        mark(reference);
        return value;
    }

    private static byte[] readCredential() {
        String configured = System.getProperty(SECRET_PATH_PROPERTY);
        if (configured == null || configured.trim().isEmpty()) {
            throw new IllegalArgumentException("unknown test secret");
        }
        try {
            return Files.readAllBytes(Paths.get(configured));
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "test secret could not be read", failure);
        }
    }

    private static void mark(String reference) {
        String configured = System.getProperty(MARKER_PATH_PROPERTY);
        if (configured == null || configured.trim().isEmpty()) return;
        try {
            Path marker = Paths.get(configured);
            Files.write(marker, (reference + '\n').getBytes(
                    StandardCharsets.US_ASCII),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "test secret resolution could not be marked", failure);
        }
    }
}
