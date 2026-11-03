package io.rowweave.sched.net;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.mem.RowSerializer;
import io.rowweave.sched.MultiProducerChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.DataOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Shuffle ingress must keep its connection, thread and heap bounds even against a peer that ignores the credit and target contract. */
class ShuffleIngressAdmissionTest {

    private static final DataSchema META = new DataSchema(new Field("v", DataType.IntegerType));
    private static final RowSerializer SERIALIZER = new RowSerializer(META);

    @Test
    void productionConfigurationRejectsEveryInvalidAdmissionDimension() {
        long heap = 1024L * 1024 * 1024;
        assertInvalid("rowweave.shuffle.maxConnections", "0", heap);
        assertInvalid("rowweave.shuffle.maxIngressHandlers", "4097", heap);
        assertInvalid("rowweave.shuffle.maxTargetsPerConnection", "100001", heap);
        assertInvalid("rowweave.shuffle.maxPendingFramesPerTarget", "0", heap);
        assertInvalid("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(FrameCodec.MAX_FRAME_BYTES - 1L), heap);
        long minimumSingleFrame = FrameCodec.MAX_FRAME_BYTES
                + FrameCodec.HEADER_BYTES + 128L;
        assertInvalid("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(minimumSingleFrame - 1L), heap);
        assertInvalid("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(heap / 2L + 1L), heap);
        assertInvalid("rowweave.shuffle.idleReadTimeoutMs", "999", heap);
        assertInvalid("rowweave.shuffle.maxConnections", "not-an-int", heap);

        Properties valid = new Properties();
        valid.setProperty("rowweave.shuffle.ingressResidentBytes",
                String.valueOf(minimumSingleFrame));
        ShuffleIngressAdmissionConfig parsed =
                ShuffleIngressAdmissionConfig.fromProperties(valid, heap);
        assertEquals(256, parsed.maxConnections());
        assertEquals(256, parsed.maxIngressHandlers());
        assertEquals(4096, parsed.maxTargetsPerConnection());
        assertEquals(65_536, parsed.maxPendingFramesPerTarget(),
                "the default pending cap holds enough frames to spend the 1 MiB initial credit");
        assertTrue((long) parsed.maxPendingFramesPerTarget() * FrameCodec.HEADER_BYTES
                        >= (1L << 20),
                "a valid sender is bounded only by the 1 MiB byte credit, so the frame cap refuses nothing first");
        assertEquals(60_000L, parsed.idleReadTimeoutMillis());
    }

    @Test
    @Timeout(10)
    void randomTargetsCannotCreateMoreWaitingHandlersThanAdmissionLimit() throws Exception {
        DataInboundRegistry registry = new DataInboundRegistry();
        ShuffleIngressAdmissionConfig config = ShuffleIngressAdmissionConfig.forTesting(
                2, 2, 8, 4, FrameCodec.MAX_FRAME_BYTES, 5_000L);
        WorkerDataServer server = new WorkerDataServer(registry, 60_000L, 0, config);
        try (Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            writeFrame(out, FrameCodec.FRAME_OPEN, UUID.randomUUID(), new byte[0]);
            writeFrame(out, FrameCodec.FRAME_OPEN, UUID.randomUUID(), new byte[0]);
            awaitSnapshot(server, snapshot -> snapshot.activeHandlers() == 2);

            writeFrame(out, FrameCodec.FRAME_OPEN, UUID.randomUUID(), new byte[0]);
            assertPeerClosed(socket, "a third unknown target is refused by handler admission and the connection closes");

            WorkerDataServer.AdmissionSnapshot saturated = server.admissionSnapshot();
            assertTrue(saturated.peakHandlers() <= 2);
            assertTrue(saturated.activeConnections() <= 2);
        } finally {
            server.close();
        }
        WorkerDataServer.AdmissionSnapshot closed = server.admissionSnapshot();
        assertEquals(0, closed.activeHandlers());
        assertEquals(0, closed.activeConnections());
        assertEquals(0L, closed.residentBytes());
    }

    @Test
    @Timeout(10)
    void connectionAndPendingFrameCapsCloseOnlyTheOffendingConnection() throws Exception {
        DataInboundRegistry registry = new DataInboundRegistry();
        ShuffleIngressAdmissionConfig config = ShuffleIngressAdmissionConfig.forTesting(
                1, 1, 1, 1, FrameCodec.MAX_FRAME_BYTES, 5_000L);
        try (WorkerDataServer server = new WorkerDataServer(registry, 60_000L, 0, config);
             Socket admitted = new Socket("127.0.0.1", server.port())) {
            admitted.setSoTimeout(2_000);
            DataOutputStream admittedOut = new DataOutputStream(admitted.getOutputStream());
            UUID firstTarget = UUID.randomUUID();
            writeFrame(admittedOut, FrameCodec.FRAME_OPEN, firstTarget, new byte[0]);
            awaitSnapshot(server, snapshot -> snapshot.activeHandlers() == 1);

            try (Socket excessConnection = new Socket("127.0.0.1", server.port())) {
                excessConnection.setSoTimeout(2_000);
                assertPeerClosed(excessConnection,
                        "a child channel beyond the active connection cap closes immediately");
            }
            assertTrue(server.admissionSnapshot().activeConnections() <= 1);

            writeFrame(admittedOut, FrameCodec.FRAME_RECORD, firstTarget, new byte[]{1});
            writeFrame(admittedOut, FrameCodec.FRAME_RECORD, firstTarget, new byte[]{2});
            assertPeerClosed(admitted,
                    "while the handler is blocked a second pending frame beyond the credit trips the count cap");
            assertTrue(server.admissionSnapshot().peakPendingFramesPerTarget() <= 1);
            awaitSnapshot(server, snapshot -> snapshot.activeHandlers() == 0
                    && snapshot.residentBytes() == 0L);
        }
    }

    @Test
    @Timeout(10)
    void perConnectionTargetCapRejectsBeforeCreatingAnotherIngress() throws Exception {
        DataInboundRegistry registry = new DataInboundRegistry();
        ShuffleIngressAdmissionConfig config = ShuffleIngressAdmissionConfig.forTesting(
                1, 2, 1, 4, FrameCodec.MAX_FRAME_BYTES, 5_000L);
        try (WorkerDataServer server = new WorkerDataServer(registry, 60_000L, 0, config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            socket.setSoTimeout(2_000);
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            writeFrame(out, FrameCodec.FRAME_OPEN, UUID.randomUUID(), new byte[0]);
            awaitSnapshot(server, snapshot -> snapshot.activeHandlers() == 1);

            writeFrame(out, FrameCodec.FRAME_OPEN, UUID.randomUUID(), new byte[0]);
            assertPeerClosed(socket, "the second target closes the connection before the per-connection cap");
            assertTrue(server.admissionSnapshot().peakTargetsPerConnection() <= 1);
            assertTrue(server.admissionSnapshot().peakHandlers() <= 1,
                    "a target beyond the limit creates no second ingress handler");
            awaitSnapshot(server, snapshot -> snapshot.activeHandlers() == 0
                    && snapshot.residentBytes() == 0L);
        }
    }

    @Test
    @Timeout(10)
    void idleConnectionIsClosedAndReleasesItsAdmissionSlot() throws Exception {
        DataInboundRegistry registry = new DataInboundRegistry();
        ShuffleIngressAdmissionConfig config = ShuffleIngressAdmissionConfig.forTesting(
                1, 1, 1, 1, FrameCodec.MAX_FRAME_BYTES, 50L);
        try (WorkerDataServer server = new WorkerDataServer(registry, 60_000L, 0, config);
             Socket idle = new Socket("127.0.0.1", server.port())) {
            idle.setSoTimeout(2_000);
            awaitSnapshot(server, snapshot -> snapshot.activeConnections() == 1);
            assertPeerClosed(idle, "the idle read deadline closes a connection that sent no frame");
            awaitSnapshot(server, snapshot -> snapshot.activeConnections() == 0);
        }
    }

    @Test
    @Timeout(10)
    void boundedTwoHandlerIngressPreservesSlowFastTargetIsolation() throws Exception {
        DataInboundRegistry registry = new DataInboundRegistry();
        UUID slowTarget = UUID.randomUUID();
        UUID fastTarget = UUID.randomUUID();
        MultiProducerChannel slow = new MultiProducerChannel(1, 1, 4096);
        MultiProducerChannel fast = new MultiProducerChannel(8, 1, 4096);
        registry.register(slowTarget, "job", slow, META, 1, 4096);
        registry.register(fastTarget, "job", fast, META, 1, 4096);
        ShuffleIngressAdmissionConfig config = ShuffleIngressAdmissionConfig.forTesting(
                1, 2, 2, 4, FrameCodec.MAX_FRAME_BYTES, 5_000L);

        try (WorkerDataServer server = new WorkerDataServer(registry, 5_000L, 0, config);
             Socket socket = new Socket("127.0.0.1", server.port())) {
            DataOutputStream out = new DataOutputStream(socket.getOutputStream());
            writeFrame(out, FrameCodec.FRAME_RECORD, slowTarget, rowBytes(1));
            writeFrame(out, FrameCodec.FRAME_RECORD, slowTarget, rowBytes(2));
            writeFrame(out, FrameCodec.FRAME_RECORD, fastTarget, rowBytes(99));
            writeFrame(out, FrameCodec.FRAME_EOS, fastTarget, new byte[0]);

            assertEquals(99, fast.poll().row().get(0));
            assertEquals(null, fast.poll(), "a fast target completes on its own while a slow target stays blocked");
            assertEquals(1, slow.poll().row().get(0));
            assertEquals(2, slow.poll().row().get(0));
            writeFrame(out, FrameCodec.FRAME_EOS, slowTarget, new byte[0]);
            assertEquals(null, slow.poll());
            awaitSnapshot(server, snapshot -> snapshot.residentBytes() == 0L);
            assertTrue(server.admissionSnapshot().peakHandlers() <= 2);
        }
    }

    @Test
    void residentBudgetRejectsBeforePayloadAllocationAndSuccessfulFrameReleasesExactlyOnce()
            throws Exception {
        ShuffleIngressResidentBudget budget = ShuffleIngressResidentBudget.forTesting(128L);
        AtomicInteger allocations = new AtomicInteger();
        EmbeddedChannel rejected = new EmbeddedChannel(new FrameDecoder(budget, length -> {
            allocations.incrementAndGet();
            return new byte[length];
        }));
        byte[] tooLarge = wireFrame(FrameCodec.FRAME_RECORD, UUID.randomUUID(), new byte[128]);
        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> rejected.writeInbound(Unpooled.wrappedBuffer(tooLarge)));
        assertTrue(rootCause(failure).getMessage().contains("shuffle ingress resident budget"));
        assertEquals(0, allocations.get(), "the budget refusal happens before the byte array is allocated");
        assertEquals(0L, budget.snapshot().usedBytes());
        rejected.finishAndReleaseAll();

        ShuffleIngressResidentBudget successBudget = ShuffleIngressResidentBudget.forTesting(512L);
        EmbeddedChannel accepted = new EmbeddedChannel(new FrameDecoder(successBudget));
        byte[] small = wireFrame(FrameCodec.FRAME_RECORD, UUID.randomUUID(), new byte[8]);
        assertTrue(accepted.writeInbound(Unpooled.wrappedBuffer(small)));
        Frame decoded = accepted.readInbound();
        assertNotNull(decoded);
        assertEquals(FrameCodec.HEADER_BYTES + 8L + 128L,
                successBudget.snapshot().usedBytes(),
                "the resident budget includes the conservative heap cost of the Frame, UUID, reservation and queue references");
        decoded.releaseIngressReservation();
        decoded.releaseIngressReservation();
        assertEquals(0L, successBudget.snapshot().usedBytes(), "releasing twice is idempotent and never underflows");
        accepted.finishAndReleaseAll();
    }

    @Test
    void productionMuxIngressCannotUseCachedPoolAndMustWireAllAdmissionDimensions()
            throws Exception {
        String server = source("WorkerDataServer.java");
        String handler = source("DataDemuxHandler.java");
        String decoder = source("FrameDecoder.java");
        assertFalse(server.contains("newCachedThreadPool"));
        assertTrue(server.contains("ShuffleIngressAdmissionConfig"));
        assertTrue(server.contains("maxConnections"));
        assertTrue(handler.contains("maxTargetsPerConnection"));
        assertTrue(handler.contains("maxPendingFramesPerTarget"));
        assertTrue(decoder.contains("reserveBeforePayloadCopy"));
    }

    private static void writeFrame(DataOutputStream out, byte type, UUID targetId, byte[] payload)
            throws Exception {
        out.writeByte(type);
        out.writeLong(targetId.getMostSignificantBits());
        out.writeLong(targetId.getLeastSignificantBits());
        out.writeInt(payload.length);
        out.write(payload);
        out.flush();
    }

    private static void assertInvalid(String key, String value, long heap) {
        Properties properties = new Properties();
        properties.setProperty(key, value);
        assertThrows(IllegalArgumentException.class,
                () -> ShuffleIngressAdmissionConfig.fromProperties(properties, heap), key);
    }

    private static byte[] wireFrame(byte type, UUID targetId, byte[] payload) throws Exception {
        java.io.ByteArrayOutputStream bytes = new java.io.ByteArrayOutputStream();
        writeFrame(new DataOutputStream(bytes), type, targetId, payload);
        return bytes.toByteArray();
    }

    private static byte[] rowBytes(int value) {
        DataRow row = new DataRow(1);
        row.set(0, value);
        return SERIALIZER.serialize(row);
    }

    private static void assertPeerClosed(Socket socket, String message) throws Exception {
        try {
            assertEquals(-1, socket.getInputStream().read(), message);
        } catch (SocketTimeoutException timeout) {
            throw new AssertionError(message + " (peer remained open)", timeout);
        } catch (java.net.SocketException reset) {
            // Both a reset and an orderly end of file fail closed.
        }
    }

    private static void awaitSnapshot(WorkerDataServer server,
                                      java.util.function.Predicate<WorkerDataServer.AdmissionSnapshot> condition)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (true) {
            WorkerDataServer.AdmissionSnapshot snapshot = server.admissionSnapshot();
            if (condition.test(snapshot)) {
                return;
            }
            if (System.nanoTime() >= deadline) {
                WorkerDataServer.AdmissionSnapshot finalSnapshot = server.admissionSnapshot();
                if (condition.test(finalSnapshot)) {
                    return;
                }
                throw new AssertionError("admission state did not converge: " + finalSnapshot);
            }
            Thread.sleep(5L);
        }
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    private static String source(String fileName) throws Exception {
        Path cwd = Paths.get(System.getProperty("user.dir"));
        Path path = cwd.resolve("src/main/java/io/rowweave/sched/net").resolve(fileName);
        if (!Files.exists(path)) {
            path = cwd.resolve("rowweave-core/src/main/java/io/rowweave/sched/net")
                    .resolve(fileName);
        }
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
