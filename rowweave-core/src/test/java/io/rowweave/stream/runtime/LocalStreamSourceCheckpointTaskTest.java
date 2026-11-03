package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.CheckpointIdentity;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamCheckpointMember;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceDiscoveryDescriptor;
import io.rowweave.stream.contract.StreamSourceSplit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** source mailbox checkpoint injection and exact reopen. */
class LocalStreamSourceCheckpointTaskTest {
    private static final String ARTIFACT = repeat('a');
    private static final String DEPLOYMENT = repeat('d');
    private static final String CONTRACT = repeat('c');
    private static final String SPLIT_ID = repeat('b');

    @TempDir
    Path temporaryDirectory;

    @Test
    void sourceMailboxPersistsPositionBeforeOrderedBarrier() throws Exception {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("payloads"), 64 * 1024L);
        TestProvider provider = new TestProvider(
                bytes("initial"), StreamSourceRecord.of(
                        bytes("record-1"), bytes("position-1")));
        LocalStreamChannel channel = new LocalStreamChannel(
                channelIdentity("attempt-1", 1L), 4, 4096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-0", "source", binding(), split(), provider, channel,
                new StreamSourceCheckpointRuntime(
                        ARTIFACT, 0, temporaryDirectory.resolve("attempt-1"),
                        payloads, null));

        source.start();
        StreamEnvelope record = channel.receive();
        assertEquals(StreamEnvelope.Kind.RECORD, record.kind());
        assertArrayEquals(bytes("record-1"), record.recordFrame());

        CheckpointIdentity checkpoint = checkpoint("attempt-1", 1L, 1L);
        CompletableFuture<StreamCheckpointMember> acknowledged =
                source.triggerCheckpoint(checkpoint);
        StreamEnvelope barrier = channel.receive();
        StreamCheckpointMember member = acknowledged.get(2, TimeUnit.SECONDS);

        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER, barrier.kind());
        assertEquals(checkpoint, barrier.checkpointIdentity());
        assertEquals(record.sequence() + 1L, barrier.sequence());
        assertEquals(1L, member.logicalPosition());
        assertTrue(provider.reader.positionCalls.get() > 0);
        source.cancel();
        source.awaitCompletion(2_000L);
        assertNull(source.failure());

        StreamSourcePositionCheckpoint verifier =
                new StreamSourcePositionCheckpoint(
                        ARTIFACT, "source", 0, binding(), split(),
                        temporaryDirectory.resolve("verify"), payloads,
                        0L, bytes("initial"));
        verifier.restore(member);
        assertArrayEquals(bytes("position-1"),
                verifier.currentPositionFrame());
    }

    @Test
    void replacementValidatesPayloadBeforeProviderOpenAndContinuesOrdinal()
            throws Exception {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("restore-payloads"), 64 * 1024L);
        StreamSourcePositionCheckpoint seed =
                new StreamSourcePositionCheckpoint(
                        ARTIFACT, "source", 0, binding(), split(),
                        temporaryDirectory.resolve("seed"), payloads, 0L,
                        bytes("initial"));
        seed.recordEmitted(bytes("position-1"));
        StreamCheckpointMember restoreMember = seed.snapshot();

        TestProvider replacementProvider = new TestProvider(
                bytes("unused"), StreamSourceRecord.of(
                        bytes("record-2"), bytes("position-2")));
        LocalStreamChannel replacementChannel = new LocalStreamChannel(
                channelIdentity("attempt-2", 2L), 4, 4096);
        LocalStreamSourceTask replacement = new LocalStreamSourceTask(
                "source-0", "source", binding(), split(), replacementProvider,
                replacementChannel, new StreamSourceCheckpointRuntime(
                        ARTIFACT, 0, temporaryDirectory.resolve("attempt-2"),
                        payloads, restoreMember));
        replacement.start();

        assertEquals(StreamEnvelope.Kind.RECORD,
                replacementChannel.receive().kind());
        assertTrue(replacementProvider.openRequest.get().hasRestoredPosition());
        assertEquals(1L, replacementProvider.openRequest.get()
                .restoredLogicalPosition());
        assertArrayEquals(bytes("position-1"), replacementProvider.openRequest
                .get().restoredPositionFrame());
        StreamCheckpointMember next = replacement.triggerCheckpoint(
                checkpoint("attempt-2", 2L, 2L)).get(2, TimeUnit.SECONDS);
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                replacementChannel.receive().kind());
        assertEquals(2L, next.logicalPosition());
        replacement.cancel();
        replacement.awaitCompletion(2_000L);
        assertNull(replacement.failure());

        Files.write(payloads.payloadPath(restoreMember.contentDigest()),
                new byte[] {0}, StandardOpenOption.TRUNCATE_EXISTING);
        TestProvider corruptProvider = new TestProvider(bytes("unused"));
        LocalStreamSourceTask corrupt = new LocalStreamSourceTask(
                "source-0", "source", binding(), split(), corruptProvider,
                new LocalStreamChannel(channelIdentity("attempt-3", 3L),
                        2, 4096),
                new StreamSourceCheckpointRuntime(
                        ARTIFACT, 0, temporaryDirectory.resolve("attempt-3"),
                        payloads, restoreMember));
        corrupt.start();
        corrupt.awaitCompletion(2_000L);
        assertTrue(corrupt.isCompleted());
        assertEquals(0, corruptProvider.openCount.get());
        assertTrue(corrupt.failure() instanceof StreamCheckpointException);
        assertEquals(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                ((StreamCheckpointException) corrupt.failure()).code());
    }

    @Test
    void wrongAttemptCheckpointIsRejectedWithoutBarrierOrPositionMutation() {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("scope-payloads"), 64 * 1024L);
        TestProvider provider = new TestProvider(bytes("initial"));
        LocalStreamChannel channel = new LocalStreamChannel(
                channelIdentity("attempt-1", 1L), 2, 4096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-0", "source", binding(), split(), provider, channel,
                new StreamSourceCheckpointRuntime(
                        ARTIFACT, 0, temporaryDirectory.resolve("scope"),
                        payloads, null));

        CompletableFuture<StreamCheckpointMember> rejected =
                source.triggerCheckpoint(checkpoint("attempt-x", 1L, 1L));
        assertTrue(rejected.isCompletedExceptionally());
        assertFalse(source.isCompleted());
        assertEquals(0, provider.openCount.get());
        assertEquals(0L, channel.throughput());
    }

    @Test
    void cleanBoundedEndRetainsItsTerminalCheckpointCut() throws Exception {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("bounded-payloads"), 64 * 1024L);
        LocalStreamChannel channel = new LocalStreamChannel(
                channelIdentity("attempt-1", 1L), 4, 4096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-0", "source", binding(), split(),
                new BoundedProvider(), channel,
                new StreamSourceCheckpointRuntime(
                        ARTIFACT, 0,
                        temporaryDirectory.resolve("bounded-working"),
                        payloads, null), 0L, false, true);

        source.start();
        assertEquals(StreamEnvelope.Kind.RECORD, channel.receive().kind());
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                channel.receive().kind());
        source.awaitCompletion(2_000L);
        assertTrue(source.isCompleted());
        assertNull(source.failure());

        StreamCheckpointMember first = source.triggerCheckpoint(
                checkpoint("attempt-1", 1L, 1L)).get(2, TimeUnit.SECONDS);
        StreamCheckpointMember second = source.triggerCheckpoint(
                checkpoint("attempt-1", 1L, 2L)).get(2, TimeUnit.SECONDS);
        assertEquals(1L, first.logicalPosition());
        assertEquals(first, second);
        assertTrue(source.triggerCheckpoint(
                checkpoint("attempt-1", 1L, 2L))
                .isCompletedExceptionally());
        assertEquals(0, channel.queuedEvents());
    }

    @Test
    void subtaskWithoutFrozenSplitEndsAndRetainsEmptyCheckpointCut()
            throws Exception {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("empty-lane-payloads"),
                64 * 1024L);
        TestProvider provider = new TestProvider(bytes("unused"));
        LocalStreamChannel channel = new LocalStreamChannel(
                channelIdentity("attempt-1", 1L), 2, 4096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-1", "source", binding(), null, provider, channel,
                new StreamSourceCheckpointRuntime(
                        ARTIFACT, 1,
                        temporaryDirectory.resolve("empty-lane-working"),
                        payloads, null));

        source.start();
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                channel.receive().kind());
        source.awaitCompletion(2_000L);
        assertTrue(source.isCompleted());
        assertNull(source.failure());
        assertEquals(0, provider.openCount.get());

        StreamCheckpointMember member = source.triggerCheckpoint(
                checkpoint("attempt-1", 1L, 1L))
                .get(2, TimeUnit.SECONDS);
        assertEquals(1, member.subtaskIndex());
        assertEquals(0L, member.logicalPosition());
        assertEquals(0, channel.queuedEvents());
    }

    @Test
    void revokingRestoredSplitBeforeStartDoesNotRestoreItsPositionIntoReplacement()
            throws Exception {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("dynamic-payloads"), 64 * 1024L);
        StreamSourceBinding dynamic = dynamicBinding();
        StreamSourcePositionCheckpoint seed =
                new StreamSourcePositionCheckpoint(ARTIFACT, "source", 0,
                        dynamic, split(), temporaryDirectory.resolve("seed-a"),
                        payloads, 0L, bytes("initial-a"));
        seed.recordEmitted(bytes("position-a"));
        StreamSourceSplit replacement = new StreamSourceSplit(
                repeat('e'), "test-position-v1", new byte[] {9});
        TestProvider provider = new TestProvider(bytes("initial-b"),
                StreamSourceRecord.of(bytes("record-b"), bytes("position-b")));
        LocalStreamChannel channel = new LocalStreamChannel(
                channelIdentity("attempt-2", 2L), 4, 4096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-0", "source", dynamic, split(), provider, channel,
                new StreamSourceCheckpointRuntime(ARTIFACT, 0,
                        temporaryDirectory.resolve("dynamic-working"),
                        payloads, seed.snapshot()), 0L, true);

        assertTrue(source.revokeDynamicSplit(split().splitId()).isDone(),
                "an unopened reader must revoke without waiting for start");
        source.assignDynamicSplit(replacement);
        source.start();
        assertEquals(StreamEnvelope.Kind.RECORD,
                channel.receive().kind());
        assertNull(source.failure());
        assertEquals(replacement, provider.openRequest.get().split());
        assertFalse(provider.openRequest.get().hasRestoredPosition());
        source.cancel();
        source.awaitCompletion(2_000L);
        assertNull(source.failure());
    }

    @Test
    void repeatedPreStartRevocationOfSameSplitIdRemovesNewGeneration()
            throws Exception {
        StreamSourceSplit nextGeneration = new StreamSourceSplit(
                SPLIT_ID, "test-position-v1", new byte[] {9});
        StreamSourceSplit replacement = new StreamSourceSplit(
                repeat('f'), "test-position-v1", new byte[] {10});
        TestProvider provider = new TestProvider(bytes("initial-c"),
                StreamSourceRecord.of(bytes("record-c"), bytes("position-c")));
        LocalStreamChannel channel = new LocalStreamChannel(
                channelIdentity("attempt-1", 1L), 4, 4096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-0", "source", dynamicBinding(), split(), provider,
                channel, null, 0L, true);

        assertTrue(source.revokeDynamicSplit(SPLIT_ID).isDone());
        source.assignDynamicSplit(nextGeneration);
        assertTrue(source.revokeDynamicSplit(SPLIT_ID).isDone());
        source.assignDynamicSplit(replacement);
        source.start();
        assertEquals(StreamEnvelope.Kind.RECORD,
                channel.receive().kind());
        assertEquals(replacement, provider.openRequest.get().split());
        source.cancel();
        source.awaitCompletion(2_000L);
        assertNull(source.failure());
    }

    @Test
    void checkpointBarrierBeforeDelayedReaderWakeupKeepsSourceAlive()
            throws Exception {
        FileCheckpointPayloadStore payloads = new FileCheckpointPayloadStore(
                temporaryDirectory.resolve("delayed-wakeup-payloads"),
                64 * 1024L);
        LocalStreamChannel channel = new LocalStreamChannel(
                channelIdentity("attempt-1", 1L), 4, 4096);
        BlockingFirstRecordOutput output =
                new BlockingFirstRecordOutput(channel);
        DelayedWakeupProvider provider = new DelayedWakeupProvider();
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-0", "source", binding(), split(), provider, output,
                new StreamSourceCheckpointRuntime(ARTIFACT, 0,
                        temporaryDirectory.resolve("delayed-wakeup-working"),
                        payloads, null));

        source.start();
        assertTrue(output.firstRecordEntered.await(2L, TimeUnit.SECONDS));
        CompletableFuture<StreamCheckpointMember> cut =
                source.triggerCheckpoint(checkpoint("attempt-1", 1L, 1L));
        output.releaseFirstRecord.countDown();
        assertEquals(StreamEnvelope.Kind.RECORD, channel.receive().kind());
        assertEquals(StreamEnvelope.Kind.CHECKPOINT_BARRIER,
                channel.receive().kind());
        assertEquals(1L, cut.get(2L, TimeUnit.SECONDS).logicalPosition());
        assertEquals(StreamEnvelope.Kind.RECORD, channel.receive().kind(),
                "the delayed wakeup must not terminate a resident reader");
        source.cancel();
        source.awaitCompletion(2_000L);
        assertNull(source.failure());
    }

    private static StreamSourceBinding binding() {
        byte[] configuration = new byte[] {7};
        return new StreamSourceBinding(
                "test-log-v1", CONTRACT, sha256Hex(configuration),
                configuration, Collections.singletonList(split()),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
    }

    private static StreamSourceBinding dynamicBinding() {
        byte[] configuration = new byte[] {7};
        return new StreamSourceBinding(
                "test-log-v1", CONTRACT, sha256Hex(configuration),
                configuration,
                new StreamSourceDiscoveryDescriptor(10L, 1_000L,
                        4, 4, 2, 64, 64),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
    }

    private static StreamSourceSplit split() {
        return new StreamSourceSplit(
                SPLIT_ID, "test-position-v1", new byte[] {8});
    }

    private static StreamChannelIdentity channelIdentity(
            String attemptId, long generation) {
        return new StreamChannelIdentity(
                "run-1", attemptId, 7L, generation,
                "source-sink-0", 1L);
    }

    private static CheckpointIdentity checkpoint(
            String attemptId, long generation, long sequence) {
        return new CheckpointIdentity(
                "definition-1", ARTIFACT, "run-1", attemptId, 7L,
                4L, "coordinator-1", sequence, ARTIFACT, DEPLOYMENT, 1, 1);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static final class TestProvider
            implements DurableStreamSourceProvider {
        private final AtomicInteger openCount = new AtomicInteger();
        private final AtomicReference<StreamSourceOpenRequest> openRequest =
                new AtomicReference<>();
        private final TestReader reader;

        private TestProvider(byte[] initialPosition,
                             StreamSourceRecord... records) {
            reader = new TestReader(initialPosition, records);
        }

        @Override public String providerKey() { return "test-log-v1"; }
        @Override public String contractDigest() { return CONTRACT; }
        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            openCount.incrementAndGet();
            openRequest.set(request);
            return reader;
        }
    }

    private static final class BlockingFirstRecordOutput
            implements StreamChannelOutput {
        private final LocalStreamChannel delegate;
        private final CountDownLatch firstRecordEntered =
                new CountDownLatch(1);
        private final CountDownLatch releaseFirstRecord =
                new CountDownLatch(1);
        private boolean firstRecord = true;

        private BlockingFirstRecordOutput(LocalStreamChannel delegate) {
            this.delegate = delegate;
        }

        @Override public StreamChannelIdentity channelIdentity() {
            return delegate.channelIdentity();
        }

        @Override public void send(StreamEnvelope envelope) {
            if (envelope.kind() == StreamEnvelope.Kind.RECORD
                    && firstRecord) {
                firstRecord = false;
                firstRecordEntered.countDown();
                try {
                    if (!releaseFirstRecord.await(5L, TimeUnit.SECONDS)) {
                        throw new IllegalStateException(
                                "first record was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException(interrupted);
                }
            }
            delegate.send(envelope);
        }

        @Override public void close() { delegate.close(); }
        @Override public void cancel() { delegate.cancel(); }
        @Override public void fail(Throwable cause) { delegate.fail(cause); }
    }

    private static final class DelayedWakeupProvider
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return "test-log-v1"; }
        @Override public String contractDigest() { return CONTRACT; }

        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private final Semaphore wakeups = new Semaphore(0);
                private int next;

                @Override public StreamSourceRecord next() throws Exception {
                    if (next++ == 0) {
                        return StreamSourceRecord.of(bytes("record-1"),
                                bytes("position-1"));
                    }
                    if (next == 2) {
                        wakeups.acquire();
                        return null;
                    }
                    if (next == 3) {
                        return StreamSourceRecord.of(bytes("record-2"),
                                bytes("position-2"));
                    }
                    wakeups.acquire();
                    return null;
                }

                @Override public byte[] checkpointPositionFrame() {
                    return bytes("initial");
                }
                @Override public void wakeUp() { wakeups.release(); }
                @Override public void cancel() { wakeups.release(); }
                @Override public void close() { wakeups.release(); }
            };
        }
    }

    private static final class BoundedProvider
            implements DurableStreamSourceProvider {
        @Override public String providerKey() { return "test-log-v1"; }
        @Override public String contractDigest() { return CONTRACT; }

        @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
            return new StreamSourceReader() {
                private int next;

                @Override public StreamSourceRecord next() {
                    throw new AssertionError("bounded reader uses nextEvent");
                }

                @Override public StreamSourceEvent nextEvent() {
                    if (next++ == 0) {
                        return StreamSourceEvent.record(StreamSourceRecord.of(
                                bytes("record-1"), bytes("position-1")));
                    }
                    return StreamSourceEvent.endOfInput();
                }

                @Override public byte[] checkpointPositionFrame() {
                    return bytes("initial");
                }

                @Override public void cancel() { }
                @Override public void close() { }
            };
        }
    }

    private static final class TestReader implements StreamSourceReader {
        private final byte[] initialPosition;
        private final Queue<StreamSourceRecord> records = new ArrayDeque<>();
        private final Semaphore signal = new Semaphore(0);
        private final AtomicInteger positionCalls = new AtomicInteger();

        private TestReader(byte[] initialPosition,
                           StreamSourceRecord... records) {
            this.initialPosition = initialPosition.clone();
            this.records.addAll(Arrays.asList(records));
        }

        @Override public byte[] checkpointPositionFrame() {
            positionCalls.incrementAndGet();
            return initialPosition.clone();
        }

        @Override public StreamSourceRecord next() throws Exception {
            StreamSourceRecord record = records.poll();
            if (record != null) return record;
            signal.acquire();
            return null;
        }

        @Override public void wakeUp() { signal.release(); }
        @Override public void cancel() { signal.release(); }
        @Override public void close() { signal.release(); }
    }
}
