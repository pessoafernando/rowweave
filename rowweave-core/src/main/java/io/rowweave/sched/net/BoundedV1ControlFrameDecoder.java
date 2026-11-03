package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.util.ReferenceCountUtil;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.function.IntFunction;

/** Non-cumulating decoder for bounded server-to-producer V1 control replies. */
final class BoundedV1ControlFrameDecoder extends ChannelInboundHandlerAdapter {
    static final int MAX_ERROR_BYTES = 64;

    private final IntFunction<byte[]> payloadAllocator;
    private final byte[] header = new byte[FrameCodec.V1_HEADER_BYTES];
    private int headerLength;
    private byte type;
    private UUID targetId;
    private int producerOrdinal;
    private byte[] payload;
    private int payloadLength;

    BoundedV1ControlFrameDecoder() {
        this(byte[]::new);
    }

    BoundedV1ControlFrameDecoder(IntFunction<byte[]> payloadAllocator) {
        if (payloadAllocator == null) {
            throw new IllegalArgumentException("control payload allocator is required");
        }
        this.payloadAllocator = payloadAllocator;
    }

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) {
        if (!(message instanceof ByteBuf)) {
            ReferenceCountUtil.release(message);
            fail(context, new CorruptedFrameException(
                    "authenticated control decoder requires plaintext bytes"));
            return;
        }
        ByteBuf input = (ByteBuf) message;
        try {
            while (input.isReadable() && context.channel().isOpen()) {
                if (headerLength < FrameCodec.V1_HEADER_BYTES) {
                    int copied = Math.min(input.readableBytes(),
                            FrameCodec.V1_HEADER_BYTES - headerLength);
                    input.readBytes(header, headerLength, copied);
                    headerLength += copied;
                    if (headerLength < FrameCodec.V1_HEADER_BYTES) return;
                    prepareFrame();
                    if (payload.length == 0) emit(context);
                }
                if (headerLength == FrameCodec.V1_HEADER_BYTES && input.isReadable()) {
                    int copied = Math.min(input.readableBytes(),
                            payload.length - payloadLength);
                    input.readBytes(payload, payloadLength, copied);
                    payloadLength += copied;
                    if (payloadLength == payload.length) emit(context);
                }
            }
        } catch (Throwable failure) {
            fail(context, failure);
        } finally {
            input.release();
        }
    }

    private void prepareFrame() {
        ByteBuffer fields = ByteBuffer.wrap(header);
        type = fields.get();
        targetId = new UUID(fields.getLong(), fields.getLong());
        producerOrdinal = fields.getInt();
        int declaredPayloadLength = fields.getInt();
        try {
            FrameCodec.validateHeaderForProtocol(
                    io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                    type, targetId, producerOrdinal, declaredPayloadLength);
            validateControlDirection(type, declaredPayloadLength);
        } catch (IllegalArgumentException invalid) {
            throw new CorruptedFrameException(
                    "invalid authenticated control frame header", invalid);
        }
        payload = payloadAllocator.apply(declaredPayloadLength);
        if (payload == null || payload.length != declaredPayloadLength) {
            throw new IllegalStateException("control payload allocator returned wrong length");
        }
        payloadLength = 0;
    }

    private static void validateControlDirection(byte type, int payloadLength) {
        switch (type) {
            case FrameCodec.FRAME_KEEPALIVE:
                if (payloadLength != 0) {
                    throw new IllegalArgumentException(
                            "authenticated keepalive payload must be empty");
                }
                return;
            case FrameCodec.FRAME_CREDIT:
                if (payloadLength != Integer.BYTES) {
                    throw new IllegalArgumentException(
                            "authenticated credit payload must be four bytes");
                }
                return;
            case FrameCodec.FRAME_ERROR:
                if (payloadLength > MAX_ERROR_BYTES) {
                    throw new IllegalArgumentException(
                            "authenticated error payload exceeds control bound");
                }
                return;
            default:
                throw new IllegalArgumentException(
                        "business frame is invalid in the control reply direction");
        }
    }

    private void emit(ChannelHandlerContext context) {
        Frame frame = new Frame(type, targetId, producerOrdinal, payload);
        clearFrameState();
        context.fireChannelRead(frame);
    }

    @Override
    public void channelInactive(ChannelHandlerContext context) throws Exception {
        clearFrameState();
        super.channelInactive(context);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext context) throws Exception {
        clearFrameState();
        if (context.channel().isOpen()) context.close();
        super.handlerRemoved(context);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext context, Throwable cause) {
        fail(context, cause);
    }

    private void fail(ChannelHandlerContext context, Throwable failure) {
        clearFrameState();
        context.close();
        context.fireExceptionCaught(failure == null
                ? new CorruptedFrameException("authenticated control frame decode failed")
                : failure);
    }

    private void clearFrameState() {
        headerLength = 0;
        targetId = null;
        payload = null;
        payloadLength = 0;
    }
}
