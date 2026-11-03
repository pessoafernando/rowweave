package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamSourceBinding;
import io.rowweave.stream.contract.StreamSourceSplit;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** resident replayable source and single-thread mailbox ownership. */
class LocalStreamSourceTaskTest {
    private static final String CONTRACT = repeat('a');
    private static final String SPLIT_ID = repeat('b');

    @Test
    void residentSourcePushesOnItsTaskThreadAndMailboxSerializesHandler()
            throws Exception {
        StreamSourceBinding binding = binding();
        StreamSourceSplit split = binding.fixedSplits().get(0);
        RecordingProvider provider = new RecordingProvider(
                "test-log-v1", CONTRACT,
                StreamSourceRecord.of(new byte[]{1}, new byte[]{11}),
                StreamSourceRecord.of(new byte[]{2}, new byte[]{12}),
                StreamSourceRecord.of(new byte[]{3}, new byte[]{13}));
        LocalStreamChannel channel = new LocalStreamChannel(
                identity("source-sink-0"), 1, 4096);
        List<byte[]> received = Collections.synchronizedList(new ArrayList<>());
        List<String> handlerThreads = Collections.synchronizedList(new ArrayList<>());
        StreamTask sink = new StreamTask("sink", channel, envelope -> {
            received.add(envelope.recordFrame());
            handlerThreads.add(Thread.currentThread().getName());
            try {
                Thread.sleep(20L);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
            }
        });
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source", "source", binding, split, provider, channel);

        sink.start();
        source.start();
        eventually(() -> received.size() == 3 && channel.parkCount() > 0L);
        source.cancel();
        sink.cancel();
        source.awaitCompletion(2000L);
        sink.awaitCompletion(2000L);

        assertTrue(source.isCompleted());
        assertTrue(sink.isCompleted());
        assertNull(source.failure());
        assertNull(sink.failure());
        assertEquals(1, provider.openCount.get());
        assertEquals(1, provider.reader.cancelCount.get());
        assertEquals(1, provider.reader.closeCount.get());
        assertTrue(provider.openThread.get().startsWith("rowweave-stream-source-"));
        assertEquals(Collections.singletonList("rowweave-stream-task-sink"),
                unique(handlerThreads));
        assertArrayEquals(new byte[]{1}, received.get(0));
        assertArrayEquals(new byte[]{2}, received.get(1));
        assertArrayEquals(new byte[]{3}, received.get(2));
        assertEquals(3L, channel.throughput());
    }

    @Test
    void sourceControlEventsShareTheOrderedDataLane() throws Exception {
        StreamSourceBinding binding = binding();
        StreamSourceSplit split = binding.fixedSplits().get(0);
        Queue<StreamSourceEvent> events = new ArrayDeque<>();
        events.add(StreamSourceEvent.record(
                StreamSourceRecord.of(new byte[] {1}, new byte[] {11}, 10L)));
        events.add(StreamSourceEvent.watermark(10L));
        events.add(StreamSourceEvent.watermarkStatus(
                StreamEnvelope.WatermarkStatus.IDLE, 1L));
        events.add(StreamSourceEvent.watermark(20L));
        events.add(StreamSourceEvent.watermarkStatus(
                StreamEnvelope.WatermarkStatus.ACTIVE, 2L));
        CountDownLatch cancelled = new CountDownLatch(1);
        DurableStreamSourceProvider provider = new DurableStreamSourceProvider() {
            @Override public String providerKey() { return "test-log-v1"; }
            @Override public String contractDigest() { return CONTRACT; }
            @Override public StreamSourceReader open(StreamSourceOpenRequest request) {
                return new StreamSourceReader() {
                    @Override public StreamSourceRecord next() {
                        throw new AssertionError("event-aware source must use nextEvent");
                    }
                    @Override public StreamSourceEvent nextEvent()
                            throws Exception {
                        StreamSourceEvent event = events.poll();
                        if (event != null) return event;
                        cancelled.await();
                        return null;
                    }
                    @Override public void cancel() { cancelled.countDown(); }
                    @Override public void close() { }
                };
            }
        };
        StreamChannelIdentity identity = identity("source-control");
        LocalStreamChannel channel = new LocalStreamChannel(identity, 8, 4096);
        List<StreamEnvelope> received =
                Collections.synchronizedList(new ArrayList<>());
        StreamTask sink = new StreamTask("source-control", channel, received::add);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-control", "source", binding, split, provider, channel);

        sink.start();
        source.start();
        eventually(() -> received.size() == 5);
        source.cancel();
        sink.cancel();
        source.awaitCompletion(2_000L);
        sink.awaitCompletion(2_000L);

        assertEquals(Arrays.asList(
                StreamEnvelope.Kind.RECORD,
                StreamEnvelope.Kind.WATERMARK,
                StreamEnvelope.Kind.WATERMARK_STATUS,
                StreamEnvelope.Kind.WATERMARK,
                StreamEnvelope.Kind.WATERMARK_STATUS),
                Arrays.asList(received.get(0).kind(), received.get(1).kind(),
                        received.get(2).kind(), received.get(3).kind(),
                        received.get(4).kind()));
        for (int i = 0; i < received.size(); i++) {
            assertEquals(i + 1L, received.get(i).sequence());
            assertEquals(identity, received.get(i).channelIdentity());
        }
        assertEquals(10L, received.get(0).timestamp());
        assertEquals(10L, received.get(1).timestamp());
        assertEquals(StreamEnvelope.WatermarkStatus.IDLE,
                received.get(2).watermarkStatus());
        assertEquals(1L, received.get(2).watermarkStatusGeneration());
        assertEquals(20L, received.get(3).timestamp());
        assertEquals(StreamEnvelope.WatermarkStatus.ACTIVE,
                received.get(4).watermarkStatus());
        assertEquals(2L, received.get(4).watermarkStatusGeneration());
        assertNull(source.failure());
    }

    @Test
    void boundedSourceCanEndItsOrderedLaneWithoutCancellation()
            throws Exception {
        StreamSourceBinding binding = binding();
        Queue<StreamSourceEvent> events = new ArrayDeque<>();
        events.add(StreamSourceEvent.record(StreamSourceRecord.of(
                new byte[] {1}, new byte[] {11})));
        events.add(StreamSourceEvent.endOfInput());
        DurableStreamSourceProvider provider = new DurableStreamSourceProvider() {
            @Override public String providerKey() { return "test-log-v1"; }
            @Override public String contractDigest() { return CONTRACT; }
            @Override public StreamSourceReader open(
                    StreamSourceOpenRequest request) {
                return new StreamSourceReader() {
                    @Override public StreamSourceRecord next() {
                        throw new AssertionError(
                                "bounded source must use nextEvent");
                    }
                    @Override public StreamSourceEvent nextEvent() {
                        return events.remove();
                    }
                    @Override public void cancel() { }
                    @Override public void close() { }
                };
            }
        };
        LocalStreamChannel channel = new LocalStreamChannel(
                identity("bounded-source"), 4, 4_096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "bounded-source", "source", binding,
                binding.fixedSplits().get(0), provider, channel, null,
                0L, false, true);

        source.start();
        source.awaitCompletion(2_000L);

        assertTrue(source.isCompleted());
        assertNull(source.failure());
        assertEquals(StreamEnvelope.Kind.RECORD, channel.receive().kind());
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                channel.receive().kind());
    }

    @Test
    void boundedSourceTreatsNullAsTemporaryUntilExplicitEnd()
            throws Exception {
        StreamSourceBinding binding = binding();
        AtomicInteger polls = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        DurableStreamSourceProvider provider = new DurableStreamSourceProvider() {
            @Override public String providerKey() { return "test-log-v1"; }
            @Override public String contractDigest() { return CONTRACT; }
            @Override public StreamSourceReader open(
                    StreamSourceOpenRequest request) {
                return new StreamSourceReader() {
                    @Override public StreamSourceRecord next() {
                        throw new AssertionError("event-aware reader");
                    }
                    @Override public StreamSourceEvent nextEvent() {
                        return polls.incrementAndGet() <= 2 ? null
                                : StreamSourceEvent.endOfInput();
                    }
                    @Override public void cancel() { }
                    @Override public void close() { closes.incrementAndGet(); }
                };
            }
        };
        LocalStreamChannel channel = new LocalStreamChannel(
                identity("bounded-null-poll"), 4, 4_096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "bounded-null-poll", "source", binding,
                binding.fixedSplits().get(0), provider, channel, null,
                0L, false, true);

        source.start();
        source.awaitCompletion(2_000L);

        assertTrue(source.isCompleted());
        assertNull(source.failure());
        assertEquals(3, polls.get());
        assertEquals(1, closes.get());
        assertEquals(StreamEnvelope.Kind.END_OF_INPUT,
                channel.receive().kind());
    }

    @Test
    void nullPollingSourceCanBeCancelledWithoutPublishingEnd()
            throws Exception {
        StreamSourceBinding binding = binding();
        CountDownLatch polled = new CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        DurableStreamSourceProvider provider = new DurableStreamSourceProvider() {
            @Override public String providerKey() { return "test-log-v1"; }
            @Override public String contractDigest() { return CONTRACT; }
            @Override public StreamSourceReader open(
                    StreamSourceOpenRequest request) {
                return new StreamSourceReader() {
                    @Override public StreamSourceRecord next() {
                        throw new AssertionError("event-aware reader");
                    }
                    @Override public StreamSourceEvent nextEvent() {
                        polled.countDown();
                        return null;
                    }
                    @Override public void cancel() { }
                    @Override public void close() { closes.incrementAndGet(); }
                };
            }
        };
        LocalStreamChannel channel = new LocalStreamChannel(
                identity("bounded-null-cancel"), 4, 4_096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "bounded-null-cancel", "source", binding,
                binding.fixedSplits().get(0), provider, channel);

        source.start();
        assertTrue(polled.await(2, TimeUnit.SECONDS));
        source.cancel();
        source.awaitCompletion(2_000L);

        assertTrue(source.isCompleted());
        assertNull(source.failure());
        assertEquals(1, closes.get());
        assertEquals(0L, channel.throughput());
    }

    @Test
    void undeclaredTerminalEventFailsBeforeEndIsPublished() throws Exception {
        StreamSourceBinding binding = binding();
        DurableStreamSourceProvider provider = new DurableStreamSourceProvider() {
            @Override public String providerKey() { return "test-log-v1"; }
            @Override public String contractDigest() { return CONTRACT; }
            @Override public StreamSourceReader open(
                    StreamSourceOpenRequest request) {
                return new StreamSourceReader() {
                    @Override public StreamSourceRecord next() {
                        throw new AssertionError("event-aware reader");
                    }
                    @Override public StreamSourceEvent nextEvent() {
                        return StreamSourceEvent.endOfInput();
                    }
                    @Override public void cancel() { }
                    @Override public void close() { }
                };
            }
        };
        LocalStreamChannel channel = new LocalStreamChannel(
                identity("undeclared-terminal"), 4, 4_096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "undeclared-terminal", "source", binding,
                binding.fixedSplits().get(0), provider, channel);

        source.start();
        source.awaitCompletion(2_000L);

        assertTrue(source.failure() instanceof IllegalStateException);
        assertTrue(source.failure().getMessage().contains(
                "undeclared END_OF_INPUT"));
        assertThrows(StreamChannelFailedException.class, channel::receive);
    }

    @Test
    void providerAndSplitPreflightRejectBeforeOpen() {
        StreamSourceBinding binding = binding();
        RecordingProvider wrongContract = new RecordingProvider(
                "test-log-v1", repeat('f'));
        LocalStreamChannel channel = new LocalStreamChannel(
                identity("source-sink-0"), 1, 4096);

        assertThrows(IllegalArgumentException.class, () -> new LocalStreamSourceTask(
                "source", "source", binding, binding.fixedSplits().get(0),
                wrongContract, channel));
        assertEquals(0, wrongContract.openCount.get());

        RecordingProvider valid = new RecordingProvider("test-log-v1", CONTRACT);
        StreamSourceSplit unknown = new StreamSourceSplit(
                repeat('c'), "test-position-v1", new byte[]{9});
        assertThrows(IllegalArgumentException.class, () -> new LocalStreamSourceTask(
                "source", "source", binding, unknown, valid, channel));
        assertEquals(0, valid.openCount.get());
    }

    @Test
    void sourceAndHandlerFailuresPoisonTheLaneWithTheOriginalCause()
            throws Exception {
        RuntimeException sourceBoom = new RuntimeException("source boom");
        RecordingProvider provider = new RecordingProvider("test-log-v1", CONTRACT);
        provider.reader.failure = sourceBoom;
        LocalStreamChannel sourceChannel = new LocalStreamChannel(
                identity("source-failure"), 1, 4096);
        LocalStreamSourceTask source = new LocalStreamSourceTask(
                "source-failure", "source", binding(),
                binding().fixedSplits().get(0), provider, sourceChannel);

        source.start();
        source.awaitCompletion(2000L);
        assertSame(sourceBoom, source.failure());
        StreamChannelFailedException downstream = assertThrows(
                StreamChannelFailedException.class, sourceChannel::receive);
        assertSame(sourceBoom, downstream.getCause());

        StreamChannelIdentity handlerIdentity = identity("handler-failure");
        LocalStreamChannel handlerChannel = new LocalStreamChannel(
                handlerIdentity, 1, 4096);
        RuntimeException handlerBoom = new RuntimeException("handler boom");
        StreamTask task = new StreamTask("handler-failure", handlerChannel,
                ignored -> { throw handlerBoom; });
        task.start();
        handlerChannel.send(StreamEnvelope.record(
                handlerIdentity, 1, new byte[]{1}));
        task.awaitCompletion(2000L);

        assertSame(handlerBoom, task.failure());
        StreamChannelFailedException upstream = assertThrows(
                StreamChannelFailedException.class,
                () -> handlerChannel.send(StreamEnvelope.record(
                        handlerIdentity, 2, new byte[]{2})));
        assertSame(handlerBoom, upstream.getCause());
    }

    private static StreamSourceBinding binding() {
        byte[] configuration = new byte[]{7};
        return new StreamSourceBinding(
                "test-log-v1", CONTRACT, sha256Hex(configuration), configuration,
                Collections.singletonList(new StreamSourceSplit(
                        SPLIT_ID, "test-position-v1", new byte[]{8})),
                StreamSourceBinding.ReplayGuarantee.EXACT_ORDERED);
    }

    private static StreamChannelIdentity identity(String channelId) {
        return new StreamChannelIdentity(
                "run-1", "attempt-1", 7L, 3L, channelId, 1L);
    }

    private static List<String> unique(List<String> values) {
        List<String> unique = new ArrayList<>();
        synchronized (values) {
            for (String value : values) if (!unique.contains(value)) unique.add(value);
        }
        return unique;
    }

    private static void eventually(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3L);
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true");
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String sha256Hex(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) result.append(String.format("%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static final class RecordingProvider
            implements DurableStreamSourceProvider {
        private final String providerKey;
        private final String contractDigest;
        private final AtomicInteger openCount = new AtomicInteger();
        private final AtomicReference<String> openThread = new AtomicReference<>();
        private final RecordingReader reader;

        private RecordingProvider(
                String providerKey, String contractDigest,
                StreamSourceRecord... records) {
            this.providerKey = providerKey;
            this.contractDigest = contractDigest;
            this.reader = new RecordingReader(records);
        }

        @Override public String providerKey() { return providerKey; }
        @Override public String contractDigest() { return contractDigest; }

        @Override
        public StreamSourceReader open(StreamSourceOpenRequest request) {
            openCount.incrementAndGet();
            openThread.set(Thread.currentThread().getName());
            assertEquals("source", request.sourceOperatorUid());
            assertEquals(SPLIT_ID, request.split().splitId());
            return reader;
        }
    }

    private static final class RecordingReader implements StreamSourceReader {
        private final Queue<StreamSourceRecord> records = new ArrayDeque<>();
        private final CountDownLatch cancelled = new CountDownLatch(1);
        private final AtomicInteger cancelCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();
        private volatile RuntimeException failure;

        private RecordingReader(StreamSourceRecord... records) {
            this.records.addAll(Arrays.asList(records));
        }

        @Override
        public StreamSourceRecord next() throws Exception {
            if (failure != null) throw failure;
            StreamSourceRecord record = records.poll();
            if (record != null) return record;
            cancelled.await();
            return null;
        }

        @Override
        public void cancel() {
            cancelCount.incrementAndGet();
            cancelled.countDown();
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            cancelled.countDown();
        }

    }
}
