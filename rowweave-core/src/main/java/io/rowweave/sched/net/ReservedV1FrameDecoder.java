package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.function.IntFunction;

/**
 * MTLS V1 decoder that consumes every plaintext read synchronously. Only the fixed
 * header scratch and the already-reserved exact payload array survive across reads;
 * no Netty {@code ByteBuf} is retained or cumulated.
 */
final class ReservedV1FrameDecoder extends ChannelInboundHandlerAdapter {
    private static final int HEADER_BYTES = FrameCodec.V1_HEADER_BYTES;
    private static final byte[] RESOURCE_REJECTION =
            "shuffle ingress resource rejected".getBytes(StandardCharsets.UTF_8);

    private final ShuffleIngressResidentBudget residentBudget;
    private final IntFunction<byte[]> payloadAllocator;
    private final byte[] header = new byte[HEADER_BYTES];
    private int headerLength;
    private byte type;
    private UUID targetId;
    private int producerOrdinal;
    private byte[] payload;
    private int payloadLength;
    private ShuffleIngressResidentBudget.Reservation frameReservation;
    private ShuffleIngressResidentBudget.Reservation scratchReservation;
    private boolean rejecting;

    ReservedV1FrameDecoder(ShuffleIngressResidentBudget residentBudget) {
        this(residentBudget, byte[]::new);
    }

    ReservedV1FrameDecoder(ShuffleIngressResidentBudget residentBudget,
                           IntFunction<byte[]> payloadAllocator) {
        if (residentBudget == null || payloadAllocator == null) {
            throw new IllegalArgumentException(
                    "resident budget and payload allocator are required");
        }
        this.residentBudget = residentBudget;
        this.payloadAllocator = payloadAllocator;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext context) {
        try {
            scratchReservation = residentBudget.reserveConnectionScratch();
        } catch (RuntimeException failure) {
            context.close();
            throw failure;
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (rejecting) {
            ReferenceCountUtil.release(message);
            return;
        }
        if (!(message instanceof ByteBuf)) {
            ReferenceCountUtil.release(message);
            fail(context, new CorruptedFrameException(
                    "authenticated frame decoder requires plaintext bytes"));
            return;
        }
        ByteBuf input = (ByteBuf) message;
        try {
            while (input.isReadable() && context.channel().isOpen()) {
                if (headerLength < HEADER_BYTES) {
                    int copied = Math.min(input.readableBytes(), HEADER_BYTES - headerLength);
                    input.readBytes(header, headerLength, copied);
                    headerLength += copied;
                    if (headerLength < HEADER_BYTES) return;
                    prepareFrame();
                    if (payload.length == 0) emit(context);
                }
                if (headerLength == HEADER_BYTES && input.isReadable()) {
                    int copied = Math.min(input.readableBytes(), payload.length - payloadLength);
                    input.readBytes(payload, payloadLength, copied);
                    payloadLength += copied;
                    if (payloadLength == payload.length) emit(context);
                }
            }
        } catch (IngressBudgetRejected rejected) {
            rejectResource(context);
        } catch (Throwable failure) {
            fail(context, failure);
        } finally {
            input.release();
        }
    }

    private void prepareFrame() {
        ByteBuffer fields = ByteBuffer.wrap(header);
        type = fields.get();
        long most = fields.getLong();
        long least = fields.getLong();
        targetId = new UUID(most, least);
        producerOrdinal = fields.getInt();
        int declaredPayloadLength = fields.getInt();
        try {
            FrameCodec.validateHeaderForProtocol(
                    io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                    type, targetId, producerOrdinal, declaredPayloadLength);
        } catch (IllegalArgumentException invalid) {
            throw new CorruptedFrameException(
                    "invalid authenticated frame header", invalid);
        }
        try {
            frameReservation = residentBudget.reserveBeforePayloadCopy(
                    (long) HEADER_BYTES + declaredPayloadLength);
        } catch (IllegalStateException rejected) {
            throw new IngressBudgetRejected(rejected);
        }
        try {
            payload = payloadAllocator.apply(declaredPayloadLength);
            if (payload == null || payload.length != declaredPayloadLength) {
                throw new IllegalStateException("payload allocator returned wrong length");
            }
        } catch (Throwable failure) {
            releaseFrameReservation();
            throw failure;
        }
        payloadLength = 0;
    }

    private void emit(ChannelHandlerContext context) {
        Frame frame = new Frame(type, targetId, producerOrdinal, payload, frameReservation);
        frameReservation = null;
        headerLength = 0;
        targetId = null;
        payload = null;
        payloadLength = 0;
        context.fireChannelRead(frame);
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        releaseAll();
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext context) throws Exception {
        releaseAll();
        if (context.channel().isOpen()) context.close();
        super.handlerRemoved(context);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        fail(context, cause);
    }

    private void fail(ChannelHandlerContext context, Throwable failure) {
        releaseAll();
        context.close();
        context.fireExceptionCaught(failure == null
                ? new CorruptedFrameException("authenticated frame decode failed")
                : failure);
    }

    private void rejectResource(ChannelHandlerContext context) {
        rejecting = true;
        UUID rejectedTarget = targetId;
        int rejectedOrdinal = producerOrdinal;
        context.channel().config().setAutoRead(false);
        releaseFrameState();
        context.channel().writeAndFlush(new Frame(
                        FrameCodec.FRAME_ERROR, rejectedTarget, rejectedOrdinal,
                        RESOURCE_REJECTION.clone()))
                .addListener(ChannelFutureListener.CLOSE);
    }

    private void releaseAll() {
        releaseFrameState();
        ShuffleIngressResidentBudget.Reservation scratch = scratchReservation;
        scratchReservation = null;
        if (scratch != null) scratch.release();
    }

    private void releaseFrameState() {
        releaseFrameReservation();
        payload = null;
        targetId = null;
        headerLength = 0;
        payloadLength = 0;
    }

    private void releaseFrameReservation() {
        ShuffleIngressResidentBudget.Reservation reservation = frameReservation;
        frameReservation = null;
        if (reservation != null) reservation.release();
    }

    private static final class IngressBudgetRejected extends RuntimeException {
        private IngressBudgetRejected(Throwable cause) {
            super(cause);
        }
    }
}
