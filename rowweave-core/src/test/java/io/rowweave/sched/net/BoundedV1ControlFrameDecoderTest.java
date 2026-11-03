package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** authenticated server replies cannot use a cumulating 64 MiB decoder. */
class BoundedV1ControlFrameDecoderTest {
    private static final UUID TARGET = new UUID(809L, 811L);

    @Test
    void fragmentedCreditUsesFixedStateAndReleasesEveryInputBuffer() {
        AtomicInteger allocations = new AtomicInteger();
        EmbeddedChannel channel = new EmbeddedChannel(
                new BoundedV1ControlFrameDecoder(length -> {
                    allocations.incrementAndGet();
                    return new byte[length];
                }));
        byte[] wire = wire(FrameCodec.FRAME_CREDIT, 3, new byte[]{0, 0, 0, 7});
        for (int index = 0; index < wire.length; index++) {
            ByteBuf fragment = Unpooled.wrappedBuffer(new byte[]{wire[index]});
            channel.writeInbound(fragment);
            assertEquals(0, fragment.refCnt());
        }

        Frame decoded = channel.readInbound();
        assertNotNull(decoded);
        assertEquals(FrameCodec.FRAME_CREDIT, decoded.type());
        assertEquals(TARGET, decoded.targetId());
        assertEquals(3, decoded.producerOrdinal());
        assertArrayEquals(new byte[]{0, 0, 0, 7}, decoded.payload());
        assertEquals(1, allocations.get());
        channel.finishAndReleaseAll();
    }

    @Test
    void oversizedErrorAndBusinessDirectionFramesRejectBeforePayloadAllocation() {
        AtomicInteger allocations = new AtomicInteger();
        EmbeddedChannel oversized = new EmbeddedChannel(
                new BoundedV1ControlFrameDecoder(length -> {
                    allocations.incrementAndGet();
                    return new byte[length];
                }));
        ByteBuf errorHeader = header(FrameCodec.FRAME_ERROR, 1, 65);
        assertThrows(RuntimeException.class, () -> oversized.writeInbound(errorHeader));
        assertEquals(0, errorHeader.refCnt());
        assertEquals(0, allocations.get());
        assertFalse(oversized.isActive());
        oversized.finishAndReleaseAll();

        EmbeddedChannel business = new EmbeddedChannel(
                new BoundedV1ControlFrameDecoder(length -> {
                    allocations.incrementAndGet();
                    return new byte[length];
                }));
        ByteBuf recordHeader = header(FrameCodec.FRAME_RECORD, 1, 0);
        assertThrows(RuntimeException.class, () -> business.writeInbound(recordHeader));
        assertEquals(0, recordHeader.refCnt());
        assertEquals(0, allocations.get());
        assertFalse(business.isActive());
        business.finishAndReleaseAll();
    }

    @Test
    void muxV1SelectsBoundedDecoderAndV0KeepsLegacyPath() {
        EmbeddedChannel v1 = new EmbeddedChannel();
        MuxConnectionPool.configureInboundFrameAllocation(
                v1, DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1);
        ChannelHandler v1Decoder = MuxConnectionPool.newInboundFrameDecoder(
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1);
        assertInstanceOf(BoundedV1ControlFrameDecoder.class, v1Decoder);
        assertEquals(16 * 1024, nextReadCapacity(v1));
        v1.pipeline().addLast(v1Decoder);
        v1.finishAndReleaseAll();

        EmbeddedChannel v0 = new EmbeddedChannel();
        RecvByteBufAllocator original = v0.config().getRecvByteBufAllocator();
        MuxConnectionPool.configureInboundFrameAllocation(
                v0, DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0);
        ChannelHandler v0Decoder = MuxConnectionPool.newInboundFrameDecoder(
                DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0);
        assertInstanceOf(FrameDecoder.class, v0Decoder);
        assertSame(original, v0.config().getRecvByteBufAllocator());
        v0.pipeline().addLast(v0Decoder);
        v0.finishAndReleaseAll();
    }

    private static int nextReadCapacity(EmbeddedChannel channel) {
        RecvByteBufAllocator.Handle handle =
                channel.config().getRecvByteBufAllocator().newHandle();
        ByteBuf buffer = handle.allocate(UnpooledByteBufAllocator.DEFAULT);
        try {
            return buffer.capacity();
        } finally {
            buffer.release();
        }
    }

    private static ByteBuf header(byte type, int ordinal, int payloadLength) {
        return Unpooled.buffer(FrameCodec.V1_HEADER_BYTES)
                .writeByte(type)
                .writeLong(TARGET.getMostSignificantBits())
                .writeLong(TARGET.getLeastSignificantBits())
                .writeInt(ordinal)
                .writeInt(payloadLength);
    }

    private static byte[] wire(byte type, int ordinal, byte[] payload) {
        ByteBuf encodedHeader = header(type, ordinal, payload.length);
        ByteBuf buffer = Unpooled.buffer(FrameCodec.V1_HEADER_BYTES + payload.length);
        try {
            buffer.writeBytes(encodedHeader).writeBytes(payload);
            byte[] result = new byte[buffer.readableBytes()];
            buffer.readBytes(result);
            return result;
        } finally {
            encodedHeader.release();
            buffer.release();
        }
    }
}
