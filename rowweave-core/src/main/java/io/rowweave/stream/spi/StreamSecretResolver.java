package io.rowweave.stream.spi;

/** Worker-local lookup; ownership of the returned byte array transfers to RowWeave. */
@FunctionalInterface
public interface StreamSecretResolver {
    byte[] resolve(String reference) throws Exception;
}
