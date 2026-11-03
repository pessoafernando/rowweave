package io.rowweave.stream.spi;

/** Public Worker-local provider for checkpoint-transactional visibility. */
public interface StreamTransactionalSinkProvider {
    String providerKey();
    String contractDigest();
    StreamTransactionalSinkWriter open(
            StreamTransactionalSinkContext context) throws Exception;
}
