package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamCheckpointHandle;
import io.rowweave.StreamJobHandle;
import io.rowweave.StreamSavepointHandle;
import io.rowweave.StreamSavepointRequest;
import io.rowweave.StreamSession;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamCommandId;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamOperationStatus;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamSubmissionId;
import io.rowweave.stream.contract.CheckpointDecision;
import io.rowweave.stream.runtime.DurableStreamSourceProvider;
import io.rowweave.stream.runtime.StreamSinkTransactionOpenRequest;
import io.rowweave.stream.runtime.StreamSourceOpenRequest;
import io.rowweave.stream.runtime.StreamSourceReader;
import io.rowweave.stream.runtime.StreamSourceRecord;
import io.rowweave.stream.runtime.TransactionStatus;
import io.rowweave.stream.runtime.TransactionalStreamSinkProvider;
import io.rowweave.stream.runtime.TransactionalStreamSinkWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Local pseudo-deployment must enforce the same absolute cut deadline. */
class InProcessStreamCheckpointDeadlineTest {
    private static final String SOURCE = "deadline-source-v1";
    private static final String SINK = "deadline-sink-v1";
    private static final String SOURCE_CONTRACT = digest("deadline-source");
    private static final String SINK_CONTRACT = digest("deadline-sink");
    /** Bounds only the commit that blocks forever, not the path that reaches it. */
    private static final long CUT_DEADLINE_MILLIS = 5_000L;

    @Test
    void transactionalCompletionCannotBlockPublicCallPastDeadline(
            @TempDir Path root) throws Exception {
        BlockingTransaction transaction = new BlockingTransaction();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(SOURCE, new OneRecordSource()),
                Collections.emptyMap(),
                Collections.singletonMap(SINK, transaction),
                reference -> {
                    throw new UnsupportedOperationException(
                            "deadline test has no functions");
                }, Collections.emptyList())) {
            StreamJobHandle job = session(cluster).submit(
                    StreamSubmissionId.of("local-deadline-submit"));
            StreamCheckpointHandle checkpoint = returnsBeforeCommitCompletes(
                    transaction, () -> job.triggerCheckpoint(
                            StreamCommandId.of("local-deadline-cut")));
            assertTrue(checkpoint.status() == StreamOperationStatus.RUNNING
                            || checkpoint.status()
                            == StreamOperationStatus.SUCCEEDED,
                    "a durable decision must never be exposed as FAILED");
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    checkpoint.awaitCompletion(5_000L));
            assertTrue(transaction.visible.get());
        }
    }

    @Test
    void savepointCannotPublishFailedAfterDurableCommitDecision(
            @TempDir Path root) throws Exception {
        BlockingTransaction transaction = new BlockingTransaction();
        try (InProcessStreamCluster cluster = new InProcessStreamCluster(
                root,
                Collections.singletonMap(SOURCE, new OneRecordSource()),
                Collections.emptyMap(),
                Collections.singletonMap(SINK, transaction),
                reference -> {
                    throw new UnsupportedOperationException(
                            "deadline test has no functions");
                }, Collections.emptyList())) {
            StreamJobHandle job = session(cluster).submit(
                    StreamSubmissionId.of("local-savepoint-deadline-submit"));
            StreamSavepointHandle savepoint = returnsBeforeCommitCompletes(
                    transaction, () -> job.stopWithSavepoint(
                            StreamCommandId.of(
                                    "local-savepoint-deadline-cut"),
                            new StreamSavepointRequest(
                                    "file-savepoint-v1", new byte[0])));
            assertTrue(savepoint.status() == StreamOperationStatus.RUNNING
                            || savepoint.status()
                            == StreamOperationStatus.SUCCEEDED,
                    "a durable savepoint decision must never be exposed as FAILED");
            assertEquals(StreamOperationStatus.SUCCEEDED,
                    savepoint.awaitCompletion(5_000L));
            assertTrue(transaction.visible.get());
        }
    }

    private static <T> T returnsBeforeCommitCompletes(
            BlockingTransaction transaction, Callable<T> operation)
            throws Exception {
        ExecutorService caller = Executors.newSingleThreadExecutor();
        try {
            // The cut only blocks on a transactional sink that already holds a
            // record, so the source-to-sink flow is awaited before the control
            // call is issued rather than racing against it.
            assertTrue(transaction.recordWritten.await(30L, TimeUnit.SECONDS),
                    "the record must reach the transactional sink before the cut");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted while awaiting sink write",
                    interrupted);
        }
        Future<T> result = caller.submit(operation);
        try {
            // Reaching the transactional cut is normal Worker work; the deadline
            // under test only has to bound the commit that never returns on its
            // own, so the budget is set well above scheduling latency.
            assertTrue(transaction.commitStarted.await(30L, TimeUnit.SECONDS),
                    "checkpoint must reach transactional completion; "
                            + "controlDone=" + result.isDone());
            try {
                T value = result.get(30L, TimeUnit.SECONDS);
                assertTrue(transaction.commitInterrupted.await(
                                10L, TimeUnit.SECONDS),
                        "the configured Worker deadline must cancel the blocked RPC");
                assertTrue(!transaction.visible.get(),
                        "the public call must return before commit is visible");
                return value;
            } catch (TimeoutException timeout) {
                throw new AssertionError(
                        "public checkpoint control waited for an uncancellable "
                                + "transactional commit",
                        timeout);
            }
        } finally {
            transaction.allowCommit.release();
            caller.shutdownNow();
        }
    }

    private static StreamSession session(InProcessStreamCluster cluster) {
        StreamSession session = RowWeave.createStreamSession(
                "local-deadline", "local-deadline", cluster);
        session.setParallelism(1);
        session.setMaxParallelism(1);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                60_000L, CUT_DEADLINE_MILLIS, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 0L, 0, 0,
                65_536L, 8, 32, 0L));
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("value", "value", "value",
                        DataType.StringType, false)));
        session.fromSource(StreamSourceSpec.exactOrdered(
                        schema, SOURCE, SOURCE_CONTRACT, new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("deadline-split"), "sequence-v1",
                                new byte[] {2}))))
                .uid("source")
                .sinkTo("sink", StreamSinkSpec.checkpointTransactional(
                        SINK, SINK_CONTRACT, new byte[] {3}));
        return session;
    }

    private static final class OneRecordSource
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return SOURCE; }
        @Override public String contractDigest() { return SOURCE_CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next;
                @Override public StreamSourceRecord next() throws Exception {
                    if (next++ == 0) {
                        return StreamSourceRecord.of(
                                "one".getBytes(StandardCharsets.UTF_8),
                                ByteBuffer.allocate(4).putInt(1).array());
                    }
                    wakeups.acquire();
                    return null;
                }
                @Override public byte[] checkpointPositionFrame() {
                    return ByteBuffer.allocate(4).putInt(next).array();
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class BlockingTransaction
            implements TransactionalStreamSinkProvider {
        private final AtomicBoolean visible = new AtomicBoolean();
        private final CountDownLatch recordWritten = new CountDownLatch(1);
        private final CountDownLatch commitInterrupted = new CountDownLatch(1);
        private final CountDownLatch commitStarted = new CountDownLatch(1);
        private final Semaphore allowCommit = new Semaphore(0);
        @Override public String providerKey() { return SINK; }
        @Override public String contractDigest() { return SINK_CONTRACT; }
        @Override public TransactionalStreamSinkWriter open(
                StreamSinkTransactionOpenRequest request) {
            return new TransactionalStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    recordWritten.countDown();
                }
                @Override public byte[] prepare(String identity) {
                    return new byte[] {1};
                }
                @Override public TransactionStatus query(String identity) {
                    return visible.get()
                            ? TransactionStatus.VISIBLE
                            : TransactionStatus.PREPARED;
                }
                @Override public void commit(
                        String identity, CheckpointDecision decision) {
                    commitStarted.countDown();
                    boolean acquired = false;
                    while (!acquired) {
                        try {
                            allowCommit.acquire();
                            acquired = true;
                        } catch (InterruptedException ignored) {
                            // Simulates a provider call that cannot be canceled.
                            commitInterrupted.countDown();
                        }
                    }
                    visible.set(true);
                }
                @Override public void finalizeTransaction(String identity) { }
                @Override public void abort(String identity) { }
                @Override public void close() { }
            };
        }
    }

    private static String digest(String text) {
        try {
            byte[] value = MessageDigest.getInstance("SHA-256").digest(
                    text.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(value.length * 2);
            for (byte current : value) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
