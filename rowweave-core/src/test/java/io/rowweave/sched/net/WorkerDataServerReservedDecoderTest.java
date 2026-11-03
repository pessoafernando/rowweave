package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.RecvByteBufAllocator;
import io.netty.channel.embedded.EmbeddedChannel;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** production protocol selection must install the reserved V1 decoder. */
class WorkerDataServerReservedDecoderTest {

    @Test
    void mtlsV1SelectsReservedDecoderReservesScratchAndFixesReadAllocation() {
        ShuffleIngressResidentBudget budget = ShuffleIngressResidentBudget.forTesting(
                ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES
                        + ShuffleIngressResidentBudget.accountedFrameBytes(
                                FrameCodec.V1_HEADER_BYTES));
        EmbeddedChannel channel = new EmbeddedChannel();

        WorkerDataServer.configureInboundFrameAllocation(
                channel, DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1);
        ChannelHandler decoder = WorkerDataServer.newInboundFrameDecoder(
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1, budget);
        channel.pipeline().addLast(decoder);

        assertInstanceOf(ReservedV1FrameDecoder.class, decoder);
        assertEquals(ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES,
                budget.snapshot().usedBytes(),
                "handlerAdded must reserve scratch before the authenticated hello lease is released");
        assertEquals(16 * 1024, nextReadCapacity(channel),
                "V1 reads must not let Netty grow a plaintext cumulation-sized receive buffer");

        channel.finishAndReleaseAll();
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void loopbackV0KeepsLegacyDecoderAndReceiveAllocator() {
        ShuffleIngressResidentBudget budget = ShuffleIngressResidentBudget.forTesting(
                ShuffleIngressResidentBudget.minimumSingleFrameBytes());
        EmbeddedChannel channel = new EmbeddedChannel();
        RecvByteBufAllocator original = channel.config().getRecvByteBufAllocator();

        WorkerDataServer.configureInboundFrameAllocation(
                channel, DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0);
        ChannelHandler decoder = WorkerDataServer.newInboundFrameDecoder(
                DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0, budget);
        channel.pipeline().addLast(decoder);

        assertInstanceOf(FrameDecoder.class, decoder);
        assertSame(original, channel.config().getRecvByteBufAllocator());
        assertEquals(0L, budget.snapshot().usedBytes(),
                "V0 compatibility path must not acquire V1 connection scratch");
        channel.finishAndReleaseAll();
    }

    @Test
    void budgetRejectionTraversesProductionEncoderBeforeConnectionClose() {
        ShuffleIngressResidentBudget budget = ShuffleIngressResidentBudget.forTesting(
                ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES);
        EmbeddedChannel channel = new EmbeddedChannel();
        ChannelHandler decoder = WorkerDataServer.newInboundFrameDecoder(
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1, budget);
        DelayedOutboundWrite delayed = new DelayedOutboundWrite();
        channel.pipeline().addLast(decoder);
        channel.pipeline().addLast(new FrameEncoder(
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1));
        channel.pipeline().addLast(delayed);
        UUID target = new UUID(701L, 709L);
        ByteBuf header = Unpooled.buffer(FrameCodec.V1_HEADER_BYTES)
                .writeByte(FrameCodec.FRAME_RECORD)
                .writeLong(target.getMostSignificantBits())
                .writeLong(target.getLeastSignificantBits())
                .writeInt(17)
                .writeInt(1);

        channel.writeInbound(header);
        assertEquals(0, header.refCnt());
        assertTrue(channel.isActive(), "connection remains live while rejection flush is pending");
        assertEquals(ShuffleIngressResidentBudget.CONNECTION_SCRATCH_BYTES,
                budget.snapshot().usedBytes(),
                "connection scratch must remain owned until rejection flush closes the channel");
        delayed.forward();
        channel.runPendingTasks();
        ByteBuf encoded = channel.readOutbound();
        try {
            assertEquals(FrameCodec.FRAME_ERROR, encoded.readByte());
            assertEquals(target.getMostSignificantBits(), encoded.readLong());
            assertEquals(target.getLeastSignificantBits(), encoded.readLong());
            assertEquals(17, encoded.readInt());
            int payloadLength = encoded.readInt();
            byte[] payload = new byte[payloadLength];
            encoded.readBytes(payload);
            assertEquals("shuffle ingress resource rejected",
                    new String(payload, StandardCharsets.UTF_8));
            assertEquals(0, encoded.readableBytes());
        } finally {
            encoded.release();
        }
        assertEquals(0L, budget.snapshot().usedBytes());
        channel.finishAndReleaseAll();
    }

    private static final class DelayedOutboundWrite extends ChannelOutboundHandlerAdapter {
        private ChannelHandlerContext context;
        private Object message;
        private ChannelPromise promise;

        @Override
        public void write(ChannelHandlerContext context, Object message,
                          ChannelPromise promise) {
            this.context = context;
            this.message = message;
            this.promise = promise;
        }

        private void forward() {
            context.write(message, promise);
            context.flush();
            context = null;
            message = null;
            promise = null;
        }
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
}
