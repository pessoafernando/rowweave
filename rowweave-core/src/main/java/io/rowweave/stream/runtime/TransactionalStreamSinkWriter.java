package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;

/** Provider-owned writer whose transaction actions are exact-idempotent by identity. */
public interface TransactionalStreamSinkWriter extends AutoCloseable {
    void write(byte[] recordFrame);
    byte[] prepare(String transactionIdentity);
    TransactionStatus query(String transactionIdentity);
    void commit(String transactionIdentity, CheckpointDecision commitDecision);
    void finalizeTransaction(String transactionIdentity);
    void abort(String transactionIdentity);
    @Override void close();
}
