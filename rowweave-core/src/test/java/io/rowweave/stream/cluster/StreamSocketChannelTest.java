package io.rowweave.stream.cluster;

import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.stream.contract.StreamChannelIdentity;
import io.rowweave.stream.contract.StreamContractException;
import io.rowweave.stream.contract.StreamEnvelope;
import io.rowweave.stream.contract.StreamEnvelopeCodec;
import io.rowweave.stream.runtime.StreamChannelCancelledException;
import io.rowweave.stream.runtime.StreamChannelMetrics;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The socket channel must order and bound like the local lane, with cancel independent of data credit. */
class StreamSocketChannelTest {

    @Test
    void workerDataLaneUsesRoleAuthenticatedTransport() throws Exception {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        StreamChannelIdentity identity = identity("secured-worker-lane");
        StreamEnvelope record = StreamEnvelope.record(
                identity, 1L, new byte[] {9});
        AtomicReference<StreamSocketChannel.Receiver> accepted =
                new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        try (StreamSocketChannel.Listener listener = StreamSocketChannel.bind(
                security, identity, 2, 4_096, 8_192L)) {
            Thread accept = new Thread(() -> {
                try {
                    accepted.set(listener.accept());
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            });
            accept.start();
            try (StreamSocketChannel.Sender sender = StreamSocketChannel.connect(
                    security, security.advertisedHost(), listener.localPort(),
                    identity, 2, 4_096, 8_192L)) {
                accept.join(2_000L);
                assertNull(failure.get());
                sender.send(record);
                sender.close();
                assertEquals(record, accepted.get().receive());
                assertNull(accepted.get().receive());
            } finally {
                if (accepted.get() != null) accepted.get().close();
            }
        }
    }

    @Test
    void workerDataLaneRejectsDeploymentIdentityMismatchBeforeLaneHello()
            throws Exception {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        StreamChannelIdentity identity = identity("deployment-mismatch");
        RpcSecurityMetadata expected = RpcSecurityMetadata.of(
                repeat('a'), repeat('b'));
        RpcSecurityMetadata unexpected = RpcSecurityMetadata.of(
                repeat('c'), repeat('b'));
        AtomicReference<Throwable> acceptFailure = new AtomicReference<>();
        try (StreamSocketChannel.Listener listener = StreamSocketChannel.bind(
                security, expected, identity, 1, 4_096, 4_096L)) {
            Thread accept = new Thread(() -> {
                try {
                    listener.accept();
                } catch (Throwable thrown) {
                    acceptFailure.set(thrown);
                }
            });
            accept.start();
            assertThrows(java.io.IOException.class,
                    () -> StreamSocketChannel.connect(
                            security, unexpected,
                            security.advertisedHost(), listener.localPort(),
                            identity, 1, 4_096, 4_096L));
            accept.join(2_000L);
            assertTrue(acceptFailure.get() instanceof java.io.IOException);
        }
    }

    @Test
    void realSocketUsesIndependentWireAndPreservesOrderedDrain() throws Exception {
        StreamChannelIdentity identity = identity("socket-ordered");
        StreamEnvelope record = StreamEnvelope.record(
                identity, 1L, 19L, new byte[] {1, 2});
        StreamEnvelope watermark = StreamEnvelope.watermark(identity, 2L, 19L);
        try (StreamSocketChannel.Pair pair = StreamSocketChannel.openLoopback(
                identity, 2, 4_096, 8_192L)) {
            assertTrue(pair.localPort() > 0);
            assertArrayEquals("RWXXSDV1".getBytes(StandardCharsets.US_ASCII),
                    StreamSocketChannel.protocolMagic());
            List<Integer> kindIds = new ArrayList<>();
            for (StreamSocketChannel.FrameKind kind
                    : StreamSocketChannel.FrameKind.values()) {
                kindIds.add(kind.wireId());
            }
            assertEquals(Arrays.asList(1, 2, 3, 4, 5), kindIds);

            pair.sender().send(record);
            pair.sender().send(watermark);
            pair.sender().close();

            assertEquals(record, pair.receiver().receive());
            assertEquals(watermark, pair.receiver().receive());
            assertNull(pair.receiver().receive());
            eventually(() -> pair.sender().outstandingEvents() == 0
                    && pair.sender().outstandingBytes() == 0L);
            assertEquals(0, pair.sender().outstandingEvents());
            assertEquals(0L, pair.sender().outstandingBytes());
        }

        StreamChannelIdentity expected = identity("socket-stale");
        try (StreamSocketChannel.Pair pair = StreamSocketChannel.openLoopback(
                expected, 1, 4_096, 4_096L)) {
            StreamEnvelope stale = StreamEnvelope.record(
                    identity("other-lane"), 1L, new byte[] {1});
            StreamContractException failure = assertThrows(
                    StreamContractException.class,
                    () -> pair.sender().send(stale));
            assertEquals("STALE_CHANNEL", failure.code());
            assertEquals(0, pair.sender().outstandingEvents());
        }
    }

    @Test
    void generalizedAddressEntryPointsPreserveTheSameLaneProtocol()
            throws Exception {
        StreamChannelIdentity identity = identity("generalized");
        StreamSocketChannel.Listener listener = StreamSocketChannel.bind(
                java.net.InetAddress.getLoopbackAddress(), identity,
                4, 4_096, 16_384L);
        Thread accept = new Thread(() -> {
            try {
                listener.accept();
            } catch (Exception ignored) {
                // listener closes after the handshake
            }
        });
        accept.start();
        try (StreamSocketChannel.Sender sender =
                StreamSocketChannel.connect("127.0.0.1",
                listener.localPort(), identity, 4, 4_096, 16_384L)) {
            assertEquals(identity, sender.channelIdentity());
        } finally {
            listener.close();
            accept.join(2_000L);
        }
    }

    @Test
    void eventAndByteCreditBackpressureAndCancelWakeBothDirections()
            throws Exception {
        StreamChannelIdentity identity = identity("socket-credit");
        StreamEnvelope first = StreamEnvelope.record(identity, 1L, new byte[] {1});
        StreamEnvelope second = StreamEnvelope.record(identity, 2L, new byte[] {2});
        int frameBytes = StreamEnvelopeCodec.encode(first).length;
        try (StreamSocketChannel.Pair pair = StreamSocketChannel.openLoopback(
                identity, 1, frameBytes, frameBytes)) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread producer = producer(pair.sender(), first, second, failure);
            producer.start();

            eventually(() -> pair.sender().parkCount() == 1L);
            assertEquals(1, pair.sender().outstandingEvents());
            assertEquals(frameBytes, pair.sender().outstandingBytes());
            assertEquals(first, pair.receiver().receive());
            producer.join(2_000L);

            assertTrue(!producer.isAlive());
            assertNull(failure.get());
            assertEquals(second, pair.receiver().receive());
            assertEquals(1, pair.sender().maximumObservedEvents());
            assertEquals(frameBytes, pair.sender().maximumObservedBytes());
            eventually(() -> pair.sender().metrics().currentEvents() == 0L);
            StreamChannelMetrics metrics = pair.sender().metrics();
            assertTrue(metrics.available());
            assertEquals(0L, metrics.currentEvents());
            assertEquals(0L, metrics.currentBytes());
            assertEquals(1L, metrics.highWaterEvents());
            assertEquals(frameBytes, metrics.highWaterBytes());
            assertTrue(metrics.blockedNanos() > 0L);
        }

        StreamChannelIdentity cancelSend = identity("socket-cancel-send");
        StreamEnvelope held = StreamEnvelope.record(
                cancelSend, 1L, new byte[] {1});
        int heldBytes = StreamEnvelopeCodec.encode(held).length;
        try (StreamSocketChannel.Pair pair = StreamSocketChannel.openLoopback(
                cancelSend, 1, heldBytes, heldBytes)) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread producer = producer(pair.sender(), held,
                    StreamEnvelope.record(cancelSend, 2L, new byte[] {2}), failure);
            producer.start();
            eventually(() -> pair.sender().parkCount() == 1L);

            pair.receiver().cancel();
            producer.join(2_000L);
            assertTrue(!producer.isAlive());
            assertTrue(failure.get() instanceof StreamChannelCancelledException);
        }

        StreamChannelIdentity cancelReceive = identity("socket-cancel-receive");
        try (StreamSocketChannel.Pair pair = StreamSocketChannel.openLoopback(
                cancelReceive, 1, 4_096, 4_096L)) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread consumer = new Thread(() -> {
                try {
                    pair.receiver().receive();
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            }, "stream-socket-cancel-consumer");
            consumer.start();
            eventually(() -> consumer.getState() == Thread.State.RUNNABLE
                    || consumer.getState() == Thread.State.WAITING);

            pair.sender().cancel();
            consumer.join(2_000L);
            assertTrue(!consumer.isAlive());
            assertTrue(failure.get() instanceof StreamChannelCancelledException);
        }
    }

    @Test
    void localCancelStillWakesBlockedReceiverAfterReverseCancelWinsRace()
            throws Exception {
        StreamChannelIdentity identity = identity("socket-cancel-race");
        try (StreamSocketChannel.Pair pair = StreamSocketChannel.openLoopback(
                identity, 1, 4_096, 4_096L)) {
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread consumer = new Thread(() -> {
                try {
                    pair.receiver().receive();
                } catch (Throwable thrown) {
                    failure.set(thrown);
                }
            }, "stream-socket-cancel-race-consumer");
            consumer.start();
            eventually(() -> consumer.getState() == Thread.State.RUNNABLE
                    || consumer.getState() == Thread.State.WAITING);

            pair.receiver().cancel();
            eventually(() -> senderCancelled(pair.sender()));
            pair.sender().cancel();

            consumer.join(2_000L);
            assertTrue(!consumer.isAlive(),
                    "local cancel must emit the forward cancel frame even "
                            + "after reverse cancel was observed");
            assertTrue(failure.get() instanceof StreamChannelCancelledException);
        }
    }

    private static Thread producer(
            StreamSocketChannel.Sender sender,
            StreamEnvelope first, StreamEnvelope second,
            AtomicReference<Throwable> failure) {
        return new Thread(() -> {
            try {
                sender.send(first);
                sender.send(second);
            } catch (Throwable thrown) {
                failure.set(thrown);
            }
        }, "stream-socket-producer");
    }

    private static StreamChannelIdentity identity(String channelId) {
        return new StreamChannelIdentity(
                "run-1", "attempt-1", 7L, 3L, channelId, 1L);
    }

    private static boolean senderCancelled(StreamSocketChannel.Sender sender) {
        try {
            Field field = StreamSocketChannel.Sender.class
                    .getDeclaredField("cancelled");
            field.setAccessible(true);
            return field.getBoolean(sender);
        } catch (ReflectiveOperationException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String repeat(char value) {
        char[] result = new char[64];
        Arrays.fill(result, value);
        return new String(result);
    }

    private static void eventually(BooleanSupplier condition) throws Exception {
        long deadline = System.nanoTime() + 2_000_000_000L;
        while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(5L);
        }
        assertTrue(condition.getAsBoolean(), "condition did not become true");
    }
}
