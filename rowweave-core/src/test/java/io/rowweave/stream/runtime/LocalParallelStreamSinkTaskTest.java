package io.rowweave.stream.runtime;

import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortablePlanCompiler;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamChangeRecord;
import io.rowweave.stream.contract.StreamChangeRecordCodec;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamSinkBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Parallel Sink tasks must retain their physical task identity end to end. */
class LocalParallelStreamSinkTaskTest {

    @Test
    void eagerSinkPublishesAndOpensWithItsActualSubtask() throws Exception {
        LocalStreamChannel channel = channel("eager-lane");
        RecordingEagerProvider provider = new RecordingEagerProvider();
        LocalStreamSinkTask task = new LocalStreamSinkTask(
                "sink-3", "sink", 3, eagerBinding(), provider, channel,
                ignored -> { });
        task.start();

        java.util.concurrent.CompletableFuture<StreamCheckpointMember> cut =
                task.triggerCheckpoint(checkpoint());
        channel.send(StreamEnvelope.checkpointBarrier(
                channel.channelIdentity(), 1L, checkpoint()));
        StreamCheckpointMember member = cut.get(2L, TimeUnit.SECONDS);

        assertEquals(3, member.subtaskIndex());
        assertEquals(3, provider.openRequest.get().subtaskIndex());
        task.cancel();
        task.awaitCompletion(2_000L);
        assertNull(task.failure());
    }

    @Test
    void transactionalSinkUsesItsActualSubtaskForTransactionAndMember()
            throws Exception {
        LocalStreamChannel channel = channel("transactional-lane");
        RecordingTransactionalProvider provider =
                new RecordingTransactionalProvider();
        LocalTransactionalStreamSinkTask task =
                new LocalTransactionalStreamSinkTask(
                        "sink-5", "sink", 5, transactionalBinding(),
                        provider, channel, ignored -> { }, ignored -> { });
        task.start();

        java.util.concurrent.CompletableFuture<StreamCheckpointMember> cut =
                task.triggerCheckpoint(checkpoint());
        channel.send(StreamEnvelope.checkpointBarrier(
                channel.channelIdentity(), 1L, checkpoint()));
        StreamCheckpointMember member = cut.get(2L, TimeUnit.SECONDS);

        task.awaitCompletion(2_000L);
        assertEquals(5, member.subtaskIndex());
        assertEquals(5, provider.openRequest.get().subtaskIndex());
        assertNull(task.failure());
    }

    @Test
    void transactionalSinkAcceptsTriggerWhileBarrierIsPreparing()
            throws Exception {
        LocalStreamChannel channel = channel("prepare-race-lane");
        RecordingTransactionalProvider provider =
                new RecordingTransactionalProvider();
        provider.prepareEntered = new CountDownLatch(1);
        provider.allowPrepare = new CountDownLatch(1);
        LocalTransactionalStreamSinkTask task =
                new LocalTransactionalStreamSinkTask(
                        "prepare-race", "sink", 0, transactionalBinding(),
                        provider, channel, ignored -> { }, ignored -> { });
        task.start();
        try {
            channel.send(StreamEnvelope.checkpointBarrier(
                    channel.channelIdentity(), 1L, checkpoint()));
            assertTrue(provider.prepareEntered.await(2L, TimeUnit.SECONDS));
            java.util.concurrent.CompletableFuture<StreamCheckpointMember> cut =
                    task.triggerCheckpoint(checkpoint());
            provider.allowPrepare.countDown();
            assertNotNull(cut.get(2L, TimeUnit.SECONDS));
            assertEquals(checkpoint(), task.preparedCheckpoint());
            task.awaitCompletion(2_000L);
            assertNull(task.failure());
        } finally {
            provider.allowPrepare.countDown();
            task.cancel();
        }
    }

    @Test
    void transactionalSinkPreparesTerminalInputAtItsNextCheckpoint()
            throws Exception {
        LocalStreamChannel channel = channel("terminal-transaction-lane");
        RecordingTransactionalProvider provider =
                new RecordingTransactionalProvider();
        LocalTransactionalStreamSinkTask task =
                new LocalTransactionalStreamSinkTask(
                        "terminal-transaction", "sink", 0,
                        transactionalBinding(), provider, channel,
                        ignored -> { }, ignored -> { });
        task.start();
        try {
            channel.send(StreamEnvelope.record(
                    channel.channelIdentity(), 1L, new byte[] {7}));
            channel.send(StreamEnvelope.endOfInput(
                    channel.channelIdentity(), 2L));
            task.awaitCompletion(200L);
            assertFalse(task.isCompleted(),
                    "terminal input must retain its transaction until a cut");
            assertNotNull(task.triggerCheckpoint(checkpoint())
                    .get(2L, TimeUnit.SECONDS));
            task.awaitCompletion(2_000L);
            assertTrue(task.isCompleted());
            assertEquals(1, provider.writes.size());
            assertEquals(checkpoint(), task.preparedCheckpoint());
            assertNull(task.failure());
        } finally {
            task.cancel();
        }
    }

    @Test
    void transactionalSinkPreparesExactRetractSequenceWithCheckpoint()
            throws Exception {
        LocalStreamChannel channel = channel("retract-transaction-lane");
        RecordingTransactionalProvider provider =
                new RecordingTransactionalProvider();
        PortableRowSchema schema = new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id",
                        PortableDataType.STRING, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, false)));
        StreamChangeKeyValidator validator = new StreamChangeKeyValidator(
                schema, Collections.singletonList("id"), 4_096);
        LocalTransactionalStreamSinkTask task =
                new LocalTransactionalStreamSinkTask(
                        "retract-sink", "sink", 0,
                        retractTransactionalBinding(), provider, channel,
                        ignored -> { }, validator::validate);
        byte[] insert = StreamChangeRecordCodec.encode(
                StreamChangeRecord.insert(row(schema, "A", "1.25")));
        byte[] delete = StreamChangeRecordCodec.encode(
                StreamChangeRecord.delete(key("A"),
                        row(schema, "A", "1.25")));
        task.start();
        java.util.concurrent.CompletableFuture<StreamCheckpointMember> cut =
                task.triggerCheckpoint(checkpoint());
        channel.send(StreamEnvelope.record(
                channel.channelIdentity(), 1L, insert));
        channel.send(StreamEnvelope.record(
                channel.channelIdentity(), 2L, delete));
        channel.send(StreamEnvelope.checkpointBarrier(
                channel.channelIdentity(), 3L, checkpoint()));
        cut.get(2L, TimeUnit.SECONDS);
        task.awaitCompletion(2_000L);

        assertEquals(2, provider.writes.size());
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                insert, provider.writes.get(0));
        org.junit.jupiter.api.Assertions.assertArrayEquals(
                delete, provider.writes.get(1));
        assertEquals(2, provider.writesObservedAtPrepare);
        assertNull(task.failure());
    }

    private static LocalStreamChannel channel(String lane) {
        return new LocalStreamChannel(new StreamChannelIdentity(
                "run-1", "attempt-1", 7L, 3L, lane, 1L),
                4, 4_096);
    }

    private static CheckpointIdentity checkpoint() {
        return new CheckpointIdentity(
                "definition-1", repeat('a'), "run-1", "attempt-1", 7L,
                4L, "coordinator-1", 1L, repeat('b'), repeat('c'), 1, 1);
    }

    private static StreamSinkBinding eagerBinding() {
        return binding(StreamSinkBinding.VisibilityProfile.EAGER_AT_LEAST_ONCE);
    }

    private static StreamSinkBinding transactionalBinding() {
        return binding(
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL);
    }

    private static StreamSinkBinding retractTransactionalBinding() {
        byte[] configuration = new byte[] {1};
        return new StreamSinkBinding(
                "sink-provider", repeat('d'), sha256(configuration),
                configuration,
                StreamSinkBinding.VisibilityProfile.CHECKPOINT_TRANSACTIONAL,
                Arrays.asList(PortableStreamPlan.ChangelogMode.APPEND,
                        PortableStreamPlan.ChangelogMode.UPSERT,
                        PortableStreamPlan.ChangelogMode.RETRACT));
    }

    private static byte[] row(PortableRowSchema schema,
            String id, String amount) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, new java.math.BigDecimal(amount));
        return new RowSerializer(PortablePlanCompiler.toDataSchema(schema))
                .serialize(row);
    }

    private static byte[] key(String id) {
        PortableRowSchema schema = new PortableRowSchema(
                Collections.singletonList(new PortableField(
                        "id", "id", "id",
                        PortableDataType.STRING, false)));
        DataRow row = new DataRow(1);
        row.set(0, id);
        return new RowSerializer(PortablePlanCompiler.toDataSchema(schema))
                .serialize(row);
    }

    private static StreamSinkBinding binding(
            StreamSinkBinding.VisibilityProfile visibility) {
        byte[] configuration = new byte[] {1};
        return new StreamSinkBinding(
                "sink-provider", repeat('d'), sha256(configuration),
                configuration, visibility);
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder hex = new StringBuilder(64);
            for (byte one : digest) {
                hex.append(String.format("%02x", one & 0xff));
            }
            return hex.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String repeat(char value) {
        char[] values = new char[64];
        java.util.Arrays.fill(values, value);
        return new String(values);
    }

    private static final class RecordingEagerProvider
            implements EagerStreamSinkProvider {
        private final AtomicReference<StreamSinkOpenRequest> openRequest =
                new AtomicReference<>();
        @Override public String providerKey() { return "sink-provider"; }
        @Override public String contractDigest() { return repeat('d'); }
        @Override public EagerStreamSinkWriter open(
                StreamSinkOpenRequest request) {
            openRequest.set(request);
            return new EagerStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) { }
                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class RecordingTransactionalProvider
            implements TransactionalStreamSinkProvider {
        private final AtomicReference<StreamSinkTransactionOpenRequest>
                openRequest = new AtomicReference<>();
        private final List<byte[]> writes = new ArrayList<>();
        private volatile int writesObservedAtPrepare;
        private volatile CountDownLatch prepareEntered;
        private volatile CountDownLatch allowPrepare;
        @Override public String providerKey() { return "sink-provider"; }
        @Override public String contractDigest() { return repeat('d'); }
        @Override public TransactionalStreamSinkWriter open(
                StreamSinkTransactionOpenRequest request) {
            openRequest.set(request);
            return new TransactionalStreamSinkWriter() {
                @Override public void write(byte[] recordFrame) {
                    writes.add(recordFrame.clone());
                }
                @Override public byte[] prepare(String transactionIdentity) {
                    if (prepareEntered != null) {
                        prepareEntered.countDown();
                        try {
                            if (!allowPrepare.await(2L, TimeUnit.SECONDS)) {
                                throw new IllegalStateException(
                                        "test prepare was not released");
                            }
                        } catch (InterruptedException interrupted) {
                            Thread.currentThread().interrupt();
                            throw new IllegalStateException(interrupted);
                        }
                    }
                    writesObservedAtPrepare = writes.size();
                    return ("receipt:" + transactionIdentity).getBytes(
                            StandardCharsets.UTF_8);
                }
                @Override public TransactionStatus query(
                        String transactionIdentity) {
                    return TransactionStatus.PREPARED;
                }
                @Override public void commit(String transactionIdentity,
                        io.rowweave.stream.contract.CheckpointDecision decision) {
                }
                @Override public void finalizeTransaction(
                        String transactionIdentity) { }
                @Override public void abort(String transactionIdentity) { }
                @Override public void close() { }
            };
        }
    }
}
