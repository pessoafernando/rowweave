package io.rowweave.stream.spi;

/** Exact-idempotent transaction writer owned by one Sink mailbox thread. */
public interface StreamTransactionalSinkWriter extends AutoCloseable {
    void write(byte[] recordFrame) throws Exception;
    byte[] prepare(String transactionIdentity) throws Exception;
    StreamTransactionStatus query(String transactionIdentity) throws Exception;
    void commit(String transactionIdentity) throws Exception;
    void finalizeTransaction(String transactionIdentity) throws Exception;
    void abort(String transactionIdentity) throws Exception;
    @Override void close() throws Exception;
}
