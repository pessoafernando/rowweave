package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** V1 reserves from the header and never retains read buffers. */
class ReservedV1FrameDecoderTest {
    private static final UUID TARGET = new UUID(401L, 409L);

    @Test
    void headerReservesWholeFrameBeforeFragmentedBodyAndReadBuffersAreReleased() {
        byte[] payload = new byte[]{1, 2, 3, 4, 5};
        long frameCharge = ShuffleIngressResidentBudget.accountedFrameBytes(
                FrameCodec.V1_HEADER_BYTES + payload.length);
        long scratch = ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES;
        ShuffleIngressResidentBudget budget = ShuffleIngressResidentBudget.forTesting(
                scratch + frameCharge);
        EmbeddedChannel channel = new EmbeddedChannel(
                new ReservedV1FrameDecoder(budget));
        assertEquals(scratch, budget.snapshot().usedBytes());

        ByteBuf header = header(FrameCodec.FRAME_RECORD, 7, payload.length);
        assertFalse(channel.writeInbound(header));
        assertEquals(0, header.refCnt());
        assertEquals(scratch + frameCharge, budget.snapshot().usedBytes(),
                "the complete frame must be charged after only its header arrives");

        ByteBuf first = Unpooled.wrappedBuffer(payload, 0, 2);
        ByteBuf second = Unpooled.wrappedBuffer(payload, 2, payload.length - 2);
        assertFalse(channel.writeInbound(first));
        assertEquals(0, first.refCnt());
        channel.writeInbound(second);
        assertEquals(0, second.refCnt());
        Frame decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(7, decoded.producerOrdinal());
        assertArrayEquals(payload, decoded.payload());
        decoded.releaseIngressReservation();
        assertEquals(scratch, budget.snapshot().usedBytes());

        channel.finishAndReleaseAll();
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void insufficientFrameBudgetRejectsAfterHeaderBeforePayloadAllocation() {
        long scratch = ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES;
        ShuffleIngressResidentBudget budget =
                ShuffleIngressResidentBudget.forTesting(scratch);
        AtomicInteger allocations = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(new ReservedV1FrameDecoder(
                budget, length -> {
                    allocations.incrementAndGet();
                    return new byte[length];
                }));
        ByteBuf header = header(FrameCodec.FRAME_RECORD, 1, 1);

        assertFalse(channel.writeInbound(header));
        assertEquals(0, header.refCnt());
        assertEquals(0, allocations.get());
        assertFalse(channel.isActive());
        assertEquals(0L, budget.snapshot().usedBytes());
        Frame rejection = channel.readOutbound();
        assertNotNull(rejection, "validated V1 stream must receive a fixed resource rejection");
        assertEquals(FrameCodec.FRAME_ERROR, rejection.type());
        assertEquals(TARGET, rejection.targetId());
        assertEquals(1, rejection.producerOrdinal());
        assertEquals("shuffle ingress resource rejected",
                new String(rejection.payload(), StandardCharsets.UTF_8));
        channel.finishAndReleaseAll();
    }

    @Test
    void midBodyCloseReleasesFrameAndConnectionScratchExactlyOnce() {
        int length = 1024;
        long frameCharge = ShuffleIngressResidentBudget.accountedFrameBytes(
                FrameCodec.V1_HEADER_BYTES + length);
        long scratch = ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES;
        ShuffleIngressResidentBudget budget = ShuffleIngressResidentBudget.forTesting(
                scratch + frameCharge);
        EmbeddedChannel channel = new EmbeddedChannel(
                new ReservedV1FrameDecoder(budget));
        channel.writeInbound(header(FrameCodec.FRAME_RECORD, 3, length));
        ByteBuf partial = Unpooled.wrappedBuffer(new byte[17]);
        channel.writeInbound(partial);
        assertEquals(0, partial.refCnt());
        assertEquals(scratch + frameCharge, budget.snapshot().usedBytes());

        channel.close();
        channel.runPendingTasks();
        assertEquals(0L, budget.snapshot().usedBytes());
        channel.finishAndReleaseAll();
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void maximumFrameMinusOneByteAcrossFixedReadsHasNoRetainedInputCumulation() {
        int length = FrameCodec.MAX_FRAME_BYTES;
        long frameCharge = ShuffleIngressResidentBudget.accountedFrameBytes(
                FrameCodec.V1_HEADER_BYTES + (long) length);
        long scratch = ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES;
        ShuffleIngressResidentBudget budget = ShuffleIngressResidentBudget.forTesting(
                scratch + frameCharge);
        EmbeddedChannel channel = new EmbeddedChannel(
                new ReservedV1FrameDecoder(budget));

        ByteBuf first = Unpooled.buffer(FrameCodec.V1_HEADER_BYTES + 1)
                .writeBytes(headerBytes(FrameCodec.FRAME_RECORD, 4, length))
                .writeByte(7);
        assertFalse(channel.writeInbound(first));
        assertEquals(0, first.refCnt());
        assertEquals(scratch + frameCharge, budget.snapshot().usedBytes());

        int remaining = length - 2;
        while (remaining > 0) {
            int bytes = Math.min(16 * 1024, remaining);
            ByteBuf fragment = Unpooled.buffer(bytes).writeZero(bytes);
            assertFalse(channel.writeInbound(fragment));
            assertEquals(0, fragment.refCnt(),
                    "every plaintext input buffer must be released in the same read callback");
            assertEquals(scratch + frameCharge, budget.snapshot().usedBytes());
            remaining -= bytes;
        }
        assertNull(channel.readInbound(), "one body byte is intentionally missing");
        assertEquals(scratch + frameCharge, budget.snapshot().usedBytes());

        channel.close();
        channel.runPendingTasks();
        assertEquals(0L, budget.snapshot().usedBytes());
        channel.finishAndReleaseAll();
    }

    @Test
    void nextFrameReservationRejectsBeforeItsBodyAllocationInSamePlaintextChunk() {
        byte[] firstPayload = new byte[]{11, 12, 13};
        long firstCharge = ShuffleIngressResidentBudget.accountedFrameBytes(
                FrameCodec.V1_HEADER_BYTES + firstPayload.length);
        long scratch = ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES;
        ShuffleIngressResidentBudget budget = ShuffleIngressResidentBudget.forTesting(
                scratch + firstCharge);
        AtomicInteger allocations = new AtomicInteger();
        ReservedV1FrameDecoder decoder = new ReservedV1FrameDecoder(budget, length -> {
            assertTrue(budget.snapshot().usedBytes() >= scratch
                            + ShuffleIngressResidentBudget.accountedFrameBytes(
                                    FrameCodec.V1_HEADER_BYTES + (long) length),
                    "payload allocation must happen only after the exact frame reservation");
            allocations.incrementAndGet();
            return new byte[length];
        });
        EmbeddedChannel channel = new EmbeddedChannel(decoder);
        byte[] firstWire = wireFrame(FrameCodec.FRAME_RECORD, 5, firstPayload);
        byte[] secondHeader = headerBytes(FrameCodec.FRAME_RECORD, 6, 1);
        ByteBuf plaintext = Unpooled.buffer(firstWire.length + secondHeader.length + 1)
                .writeBytes(firstWire)
                .writeBytes(secondHeader)
                .writeByte(99);

        assertTrue(channel.writeInbound(plaintext),
                "the first complete frame is still delivered before the second is rejected");
        assertEquals(0, plaintext.refCnt());
        assertEquals(1, allocations.get(),
                "the rejected second frame must not allocate or copy its payload");
        assertFalse(channel.isActive());
        assertEquals(firstCharge, budget.snapshot().usedBytes(),
                "the already-emitted first Frame remains its downstream owner's responsibility");

        Frame first = channel.readInbound();
        assertNotNull(first);
        assertArrayEquals(firstPayload, first.payload());
        Frame rejection = channel.readOutbound();
        assertNotNull(rejection);
        assertEquals(FrameCodec.FRAME_ERROR, rejection.type());
        assertEquals(6, rejection.producerOrdinal());
        assertEquals("shuffle ingress resource rejected",
                new String(rejection.payload(), StandardCharsets.UTF_8));
        first.releaseIngressReservation();
        assertEquals(0L, budget.snapshot().usedBytes());
        channel.finishAndReleaseAll();
    }

    @Test
    void malformedHeaderAndHandlerRemovalReleaseScratchWithoutTargetDiagnostic() {
        long scratch = ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES;
        ShuffleIngressResidentBudget malformedBudget =
                ShuffleIngressResidentBudget.forTesting(scratch);
        EmbeddedChannel malformed = new EmbeddedChannel(
                new ReservedV1FrameDecoder(malformedBudget));
        ByteBuf invalid = header((byte) 127, 1, 0);

        RuntimeException failure = assertThrows(RuntimeException.class,
                () -> malformed.writeInbound(invalid));
        assertEquals(0, invalid.refCnt());
        assertFalse(rootCause(failure).getMessage().contains(TARGET.toString()));
        assertEquals(0L, malformedBudget.snapshot().usedBytes());
        malformed.finishAndReleaseAll();

        ShuffleIngressResidentBudget removalBudget =
                ShuffleIngressResidentBudget.forTesting(scratch);
        ReservedV1FrameDecoder decoder = new ReservedV1FrameDecoder(removalBudget);
        EmbeddedChannel removed = new EmbeddedChannel(decoder);
        assertEquals(scratch, removalBudget.snapshot().usedBytes());
        removed.pipeline().remove(decoder);
        removed.runPendingTasks();
        assertFalse(removed.isOpen());
        assertEquals(0L, removalBudget.snapshot().usedBytes());
        removed.finishAndReleaseAll();
    }

    private static ByteBuf header(byte type, int ordinal, int payloadLength) {
        return Unpooled.wrappedBuffer(headerBytes(type, ordinal, payloadLength));
    }

    private static byte[] headerBytes(byte type, int ordinal, int payloadLength) {
        ByteBuf header = Unpooled.buffer(FrameCodec.V1_HEADER_BYTES)
                .writeByte(type)
                .writeLong(TARGET.getMostSignificantBits())
                .writeLong(TARGET.getLeastSignificantBits())
                .writeInt(ordinal)
                .writeInt(payloadLength);
        try {
            byte[] bytes = new byte[header.readableBytes()];
            header.readBytes(bytes);
            return bytes;
        } finally {
            header.release();
        }
    }

    private static byte[] wireFrame(byte type, int ordinal, byte[] payload) {
        byte[] header = headerBytes(type, ordinal, payload.length);
        byte[] wire = new byte[header.length + payload.length];
        System.arraycopy(header, 0, wire, 0, header.length);
        System.arraycopy(payload, 0, wire, header.length, payload.length);
        return wire;
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }
}
