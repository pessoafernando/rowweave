package io.rowweave.connector.s3;

import io.rowweave.stream.spi.StreamSecretResolver;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/** Test-only classpath service proving Worker-local process resolver injection. */
public final class S3TestSecretResolver implements StreamSecretResolver {
    static final String SECRET_PATH_PROPERTY =
            "rowweave.test.s3.secret.path";
    static final String MARKER_PATH_PROPERTY =
            "rowweave.test.s3.secret.marker";
    private static byte[] configured;
    private static final AtomicInteger RESOLUTIONS = new AtomicInteger();

    public S3TestSecretResolver() { }

    static synchronized void configure(byte[] value) {
        reset();
        configured = value.clone();
    }

    static int resolutions() {
        return RESOLUTIONS.get();
    }

    static synchronized void reset() {
        if (configured != null) Arrays.fill(configured, (byte) 0);
        configured = null;
        RESOLUTIONS.set(0);
    }

    @Override public synchronized byte[] resolve(String reference) {
        if (!"vault/s3".equals(reference)) {
            throw new IllegalArgumentException("unknown test secret");
        }
        byte[] value = configured == null ? readProcessSecret()
                : configured.clone();
        RESOLUTIONS.incrementAndGet();
        markProcessResolution();
        return value;
    }

    private static byte[] readProcessSecret() {
        String path = System.getProperty(SECRET_PATH_PROPERTY);
        if (path == null || path.trim().isEmpty()) {
            throw new IllegalArgumentException("unknown test secret");
        }
        try {
            return Files.readAllBytes(Paths.get(path));
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "test secret could not be read", failure);
        }
    }

    private static void markProcessResolution() {
        String path = System.getProperty(MARKER_PATH_PROPERTY);
        if (path == null || path.trim().isEmpty()) return;
        try {
            Path marker = Paths.get(path);
            Files.write(marker, "resolved\n".getBytes(StandardCharsets.US_ASCII),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception failure) {
            throw new IllegalStateException(
                    "test secret resolution could not be marked", failure);
        }
    }
}
