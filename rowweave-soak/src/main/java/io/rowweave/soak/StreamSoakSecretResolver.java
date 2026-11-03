package io.rowweave.soak;

import io.rowweave.stream.spi.StreamSecretResolver;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Worker-local resolver used only by the explicit STREAM SOAK qualification harness. */
public final class StreamSoakSecretResolver implements StreamSecretResolver {
    public static final String S3_SECRET_PATH =
            "rowweave.soak.s3.secret.path";
    public static final String POSTGRES_SECRET_PATH =
            "rowweave.soak.postgres.secret.path";
    public static final String RESOLUTION_LOG_PATH =
            "rowweave.soak.secret.resolution.log";

    @Override public byte[] resolve(String reference) {
        String property;
        if ("vault/s3".equals(reference)) {
            property = S3_SECRET_PATH;
        } else if ("vault/postgres".equals(reference)) {
            property = POSTGRES_SECRET_PATH;
        } else if ("vault/soak-sink".equals(reference)) {
            mark(reference);
            return new byte[] {1};
        } else {
            throw new IllegalArgumentException("unknown soak secret");
        }
        String configured = System.getProperty(property);
        if (configured == null || configured.trim().isEmpty()) {
            throw new IllegalArgumentException("unknown soak secret");
        }
        try {
            byte[] result = Files.readAllBytes(Paths.get(configured));
            mark(reference);
            return result;
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "soak secret could not be read", failure);
        }
    }

    private static void mark(String reference) {
        String configured = System.getProperty(RESOLUTION_LOG_PATH);
        if (configured == null || configured.trim().isEmpty()) return;
        try {
            Path path = Paths.get(configured);
            Files.write(path, (reference + '\n').getBytes(
                            StandardCharsets.US_ASCII),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "soak secret resolution could not be marked", failure);
        }
    }
}
