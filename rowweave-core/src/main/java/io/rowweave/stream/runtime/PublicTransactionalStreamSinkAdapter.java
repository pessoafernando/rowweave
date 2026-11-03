package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;
import io.rowweave.stream.spi.StreamSecretAccess;

/** Internal bridge from the public connector SDK to the checkpoint kernel. */
public final class PublicTransactionalStreamSinkAdapter {
    private PublicTransactionalStreamSinkAdapter() { }

    public static TransactionalStreamSinkProvider adapt(
            StreamTransactionalSinkProvider provider) {
        return adapt(provider, StreamSecretAccess.unavailable());
    }

    public static TransactionalStreamSinkProvider adapt(
            StreamTransactionalSinkProvider provider,
            StreamSecretAccess secrets) {
        if (provider == null || empty(provider.providerKey())
                || empty(provider.contractDigest()) || secrets == null) {
            throw new IllegalArgumentException(
                    "public transactional Stream Sink provider is invalid");
        }
        return new TransactionalStreamSinkProvider() {
            @Override public String providerKey() {
                return provider.providerKey();
            }
            @Override public String contractDigest() {
                return provider.contractDigest();
            }
            @Override public TransactionalStreamSinkWriter open(
                    StreamSinkTransactionOpenRequest request) {
                StreamTransactionalSinkWriter writer;
                try {
                    writer = provider.open(new StreamTransactionalSinkContext(
                            request.operatorUid(), request.subtaskIndex(),
                            request.runId(), request.attemptId(),
                            request.fenceToken(), request.assignmentGeneration(),
                            request.binding().configurationFrame(),
                            secrets));
                } catch (Exception failure) {
                    throw providerFailure("open", failure);
                }
                if (writer == null) {
                    throw new IllegalArgumentException(
                            "public transactional Stream Sink returned no writer");
                }
                return bridge(writer);
            }
        };
    }

    private static TransactionalStreamSinkWriter bridge(
            StreamTransactionalSinkWriter writer) {
        return new TransactionalStreamSinkWriter() {
            @Override public void write(byte[] recordFrame) {
                invoke("write", () -> writer.write(recordFrame.clone()));
            }
            @Override public byte[] prepare(String transactionIdentity) {
                byte[] receipt = invokeResult("prepare",
                        () -> writer.prepare(transactionIdentity));
                return receipt == null ? null : receipt.clone();
            }
            @Override public TransactionStatus query(String transactionIdentity) {
                StreamTransactionStatus status = invokeResult("query",
                        () -> writer.query(transactionIdentity));
                if (status == null) return null;
                switch (status) {
                    case UNKNOWN: return TransactionStatus.UNKNOWN;
                    case PREPARED: return TransactionStatus.PREPARED;
                    case VISIBLE: return TransactionStatus.VISIBLE;
                    case ABORTED: return TransactionStatus.ABORTED;
                    default: throw new AssertionError(status);
                }
            }
            @Override public void commit(String transactionIdentity,
                    CheckpointDecision ignoredVerifiedDecision) {
                invoke("commit", () -> writer.commit(transactionIdentity));
            }
            @Override public void finalizeTransaction(String transactionIdentity) {
                invoke("finalize", () ->
                        writer.finalizeTransaction(transactionIdentity));
            }
            @Override public void abort(String transactionIdentity) {
                invoke("abort", () -> writer.abort(transactionIdentity));
            }
            @Override public void close() {
                invoke("close", writer::close);
            }
        };
    }

    private static void invoke(String action, CheckedAction callback) {
        try {
            callback.run();
        } catch (Exception failure) {
            throw providerFailure(action, failure);
        }
    }

    private static <T> T invokeResult(
            String action, CheckedResult<T> callback) {
        try {
            return callback.run();
        } catch (Exception failure) {
            throw providerFailure(action, failure);
        }
    }

    private static IllegalStateException providerFailure(
            String action, Exception failure) {
        return new IllegalStateException(
                "public transactional Stream Sink " + action + " failed",
                failure);
    }

    private static boolean empty(String value) {
        return value == null || value.isEmpty();
    }

    private interface CheckedAction { void run() throws Exception; }
    private interface CheckedResult<T> { T run() throws Exception; }
}
