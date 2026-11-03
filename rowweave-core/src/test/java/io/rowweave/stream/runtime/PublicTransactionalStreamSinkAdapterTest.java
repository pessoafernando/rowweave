package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.spi.StreamTransactionStatus;
import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;
import io.rowweave.stream.spi.StreamTransactionalSinkWriter;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** public transactional SPI reaches the proven kernel ABI. */
class PublicTransactionalStreamSinkAdapterTest {
    private static final String CONTRACT = digestUnchecked(new byte[] {3});

    @Test
    void mapsPublicContextFramesAndDurableStatuses() throws Exception {
        AtomicReference<StreamTransactionalSinkContext> opened =
                new AtomicReference<>();
        AtomicReference<byte[]> written = new AtomicReference<>();
        StreamTransactionalSinkProvider publicProvider =
                new StreamTransactionalSinkProvider() {
            @Override public String providerKey() { return "sink-v1"; }
            @Override public String contractDigest() { return CONTRACT; }
            @Override public StreamTransactionalSinkWriter open(
                    StreamTransactionalSinkContext context) {
                opened.set(context);
                return new StreamTransactionalSinkWriter() {
                    @Override public void write(byte[] frame) {
                        written.set(frame);
                    }
                    @Override public byte[] prepare(String identity) {
                        return new byte[] {4};
                    }
                    @Override public StreamTransactionStatus query(String identity) {
                        return StreamTransactionStatus.VISIBLE;
                    }
                    @Override public void commit(String identity) { }
                    @Override public void finalizeTransaction(String identity) { }
                    @Override public void abort(String identity) { }
                    @Override public void close() { }
                };
            }
        };

        TransactionalStreamSinkProvider internal =
                PublicTransactionalStreamSinkAdapter.adapt(publicProvider);
        byte[] configuration = new byte[] {1, 2};
        StreamSinkBinding binding = new StreamSinkBinding(
                "sink-v1", CONTRACT, digest(configuration), configuration,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
        TransactionalStreamSinkWriter writer = internal.open(
                new StreamSinkTransactionOpenRequest(
                        "sink", 3, "run", "attempt", 5L, 7L, binding));

        byte[] frame = new byte[] {8, 9};
        writer.write(frame);
        frame[0] = 0;
        assertArrayEquals(new byte[] {8, 9}, written.get());
        assertArrayEquals(new byte[] {4}, writer.prepare("transaction"));
        assertEquals(TransactionStatus.VISIBLE, writer.query("transaction"));
        writer.commit("transaction", null);
        writer.finalizeTransaction("transaction");
        writer.abort("transaction");
        writer.close();

        StreamTransactionalSinkContext context = opened.get();
        assertEquals("sink", context.operatorUid());
        assertEquals(3, context.subtaskIndex());
        assertEquals("run", context.runId());
        assertEquals("attempt", context.attemptId());
        assertEquals(5L, context.fenceToken());
        assertEquals(7L, context.assignmentGeneration());
        assertArrayEquals(configuration, context.configurationFrame());
    }

    private static String digest(byte[] value) throws Exception {
        byte[] bytes = java.security.MessageDigest.getInstance("SHA-256")
                .digest(value);
        StringBuilder result = new StringBuilder(64);
        for (byte one : bytes) result.append(String.format("%02x", one));
        return result.toString();
    }

    private static String digestUnchecked(byte[] value) {
        try {
            return digest(value);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
