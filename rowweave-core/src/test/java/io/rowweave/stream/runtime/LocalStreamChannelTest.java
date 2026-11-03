package io.rowweave.stream.runtime;

import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamEnvelopeCodec;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded local Stream data lane and cancel control lane. */
class LocalStreamChannelTest {

    @Test
    void exactChannelSequenceAndNormalClosePreserveOrderedDrain() throws Exception {
        StreamChannelIdentity identity = identity("lane-a");
        LocalStreamChannel channel = new LocalStreamChannel(identity, 2, 4096);
        StreamEnvelope first = StreamEnvelope.record(identity, 1, new byte[]{1});
        StreamEnvelope second = StreamEnvelope.record(identity, 2, new byte[]{2});

        channel.send(first);
        channel.send(second);
        channel.close();

        assertEquals(first, channel.receive());
        assertEquals(second, channel.receive());
        assertNull(channel.receive());
        assertEquals(2L, channel.throughput());
        assertEquals(2, channel.maximumObservedEvents());
        assertTrue(channel.maximumObservedBytes() <= 4096L);

        LocalStreamChannel gap = new LocalStreamChannel(identity, 2, 4096);
        assertStreamFailure("CHANNEL_SEQUENCE_GAP", () -> gap.send(second));
        assertEquals(0, gap.queuedEvents());
        StreamEnvelope stale = StreamEnvelope.record(identity("lane-b"), 1,
                new byte[]{3});
        assertStreamFailure("STALE_CHANNEL", () -> gap.send(stale));
        assertEquals(0, gap.queuedEvents());
    }

    @Test
    void countAndEncodedByteBoundsBackpressureWithoutOvershoot() throws Exception {
        StreamChannelIdentity identity = identity("lane-count");
        StreamEnvelope first = StreamEnvelope.record(identity, 1, new byte[]{1});
        StreamEnvelope second = StreamEnvelope.record(identity, 2, new byte[]{2});
        int oneFrame = StreamEnvelopeCodec.encode(first).length;
        LocalStreamChannel channel = new LocalStreamChannel(identity, 1, oneFrame);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                channel.send(first);
                channel.send(second);
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        }, "stream-channel-producer");

        producer.start();
        eventually(() -> channel.parkCount() == 1L);
        assertEquals(1, channel.queuedEvents());
        assertEquals(oneFrame, channel.queuedBytes());
        assertEquals(first, channel.receive());
        producer.join(2000L);

        assertTrue(!producer.isAlive());
        assertNull(failure.get());
        assertEquals(second, channel.receive());
        assertEquals(1, channel.maximumObservedEvents());
        assertEquals(oneFrame, channel.maximumObservedBytes());
        StreamChannelMetrics metrics = channel.metrics();
        assertTrue(metrics.available());
        assertEquals(0L, metrics.currentEvents());
        assertEquals(0L, metrics.currentBytes());
        assertEquals(1L, metrics.highWaterEvents());
        assertEquals(oneFrame, metrics.highWaterBytes());
        assertTrue(metrics.blockedNanos() > 0L);

        LocalStreamChannel tooSmall = new LocalStreamChannel(
                identity("lane-small"), 1, oneFrame - 1L);
        StreamEnvelope oversized = StreamEnvelope.record(
                identity("lane-small"), 1, new byte[]{1});
        assertThrows(IllegalArgumentException.class, () -> tooSmall.send(oversized));
        assertEquals(0, tooSmall.queuedEvents());
    }

    @Test
    void cancelBypassesFullDataLaneAndWakesBlockedProducerAndConsumer()
            throws Exception {
        StreamChannelIdentity identity = identity("lane-cancel-send");
        LocalStreamChannel full = new LocalStreamChannel(identity, 1, 4096);
        full.send(StreamEnvelope.record(identity, 1, new byte[]{1}));
        AtomicReference<Throwable> sendFailure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            try {
                full.send(StreamEnvelope.record(identity, 2, new byte[]{2}));
            } catch (Throwable thrown) {
                sendFailure.set(thrown);
            }
        }, "stream-cancel-producer");
        producer.start();
        eventually(() -> full.parkCount() == 1L);

        full.cancel();
        producer.join(2000L);
        assertTrue(!producer.isAlive());
        assertTrue(sendFailure.get() instanceof StreamChannelCancelledException);
        assertThrows(StreamChannelCancelledException.class, full::receive);
        assertEquals(0, full.queuedEvents());
        assertEquals(0L, full.queuedBytes());

        LocalStreamChannel empty = new LocalStreamChannel(
                identity("lane-cancel-receive"), 1, 4096);
        AtomicReference<Throwable> receiveFailure = new AtomicReference<>();
        Thread consumer = new Thread(() -> {
            try {
                empty.receive();
            } catch (Throwable thrown) {
                receiveFailure.set(thrown);
            }
        }, "stream-cancel-consumer");
        consumer.start();
        eventually(() -> consumer.getState() == Thread.State.WAITING);

        empty.cancel();
        consumer.join(2000L);
        assertTrue(!consumer.isAlive());
        assertTrue(receiveFailure.get() instanceof StreamChannelCancelledException);
    }

    private static StreamChannelIdentity identity(String channelId) {
        return new StreamChannelIdentity(
                "run-1", "attempt-1", 7L, 3L, channelId, 1L);
    }

    private static void eventually(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true");
    }

    private static void assertStreamFailure(
            String code, org.junit.jupiter.api.function.Executable action) {
        StreamContractException failure = assertThrows(StreamContractException.class, action);
        assertEquals(code, failure.code(), failure.getMessage());
    }
}
