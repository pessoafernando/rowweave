package io.rowweave.stream.runtime;

/** Explicit provider SPI for checkpoint-transactional Stream visibility. */
public interface TransactionalStreamSinkProvider {
    String providerKey();
    String contractDigest();
    TransactionalStreamSinkWriter open(StreamSinkTransactionOpenRequest request);
}
