package io.rowweave.stream.spi;

/** Ephemeral provider access available only during connector open. */
public interface StreamSecretAccess {
    <T> T withSecret(String alias, Operation<T> operation) throws Exception;

    @FunctionalInterface
    interface Operation<T> {
        T apply(byte[] secret) throws Exception;
    }

    static StreamSecretAccess unavailable() {
        return Unavailable.INSTANCE;
    }

    final class Unavailable implements StreamSecretAccess {
        private static final Unavailable INSTANCE = new Unavailable();
        private Unavailable() { }
        @Override public <T> T withSecret(
                String alias, Operation<T> operation) {
            throw new IllegalStateException(
                    "STREAM_CONNECTOR_SECRET_UNAVAILABLE");
        }
    }
}
