package io.rowweave.stream.runtime;

import io.rowweave.internal.ProviderActionSupervisor;
import io.rowweave.stream.api.StreamConnectorConfiguration;
import io.rowweave.stream.spi.StreamSecretAccess;
import io.rowweave.stream.spi.StreamSecretResolver;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/** Owns and clears Worker-local secret bytes for precisely one provider open. */
final class StreamConnectorSecretScope
        implements StreamSecretAccess, AutoCloseable {
    static final int MAX_SECRET_BYTES = 16 * 1024;
    private static final long MAX_LOOKUP_MILLIS = 30_000L;
    private final Map<String, byte[]> values = new LinkedHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    private StreamConnectorSecretScope() { }

    static StreamConnectorSecretScope resolve(byte[] configuration,
            StreamSecretResolver resolver,
            ProviderActionSupervisor supervisor, long deadlineNanos) {
        StreamConnectorConfiguration decoded =
                StreamConnectorConfiguration.decode(configuration);
        if (!decoded.secretReferences().isEmpty() && resolver == null) {
            throw new IllegalStateException(
                    "STREAM_CONNECTOR_SECRET_RESOLVER_REQUIRED");
        }
        StreamConnectorSecretScope scope =
                new StreamConnectorSecretScope();
        long lookupDeadline = Math.min(deadlineNanos,
                Math.addExact(System.nanoTime(),
                        TimeUnit.MILLISECONDS.toNanos(
                                MAX_LOOKUP_MILLIS)));
        try {
            for (Map.Entry<String, String> reference
                    : decoded.secretReferences().entrySet()) {
                final byte[] secret;
                try {
                    secret = supervisor.call("stream-connector-secret-resolve",
                            lookupDeadline,
                            () -> resolver.resolve(reference.getValue()));
                } catch (RuntimeException failure) {
                    throw new IllegalStateException(
                            "STREAM_CONNECTOR_SECRET_RESOLUTION_FAILED");
                }
                if (secret == null || secret.length < 1
                        || secret.length > MAX_SECRET_BYTES) {
                    if (secret != null) Arrays.fill(secret, (byte) 0);
                    throw new IllegalStateException(
                            "STREAM_CONNECTOR_SECRET_INVALID");
                }
                scope.values.put(reference.getKey(), secret);
            }
            return scope;
        } catch (RuntimeException | Error failure) {
            scope.close();
            throw failure;
        }
    }

    @Override public <T> T withSecret(
            String alias, Operation<T> operation) throws Exception {
        if (closed.get() || alias == null || operation == null) {
            throw new IllegalStateException(
                    "STREAM_CONNECTOR_SECRET_UNAVAILABLE");
        }
        byte[] secret = values.get(alias);
        if (secret == null) {
            throw new IllegalStateException(
                    "STREAM_CONNECTOR_SECRET_UNKNOWN");
        }
        if (closed.get()) {
            throw new IllegalStateException(
                    "STREAM_CONNECTOR_SECRET_UNAVAILABLE");
        }
        return operation.apply(secret);
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        for (byte[] secret : values.values()) {
            Arrays.fill(secret, (byte) 0);
        }
    }
}
