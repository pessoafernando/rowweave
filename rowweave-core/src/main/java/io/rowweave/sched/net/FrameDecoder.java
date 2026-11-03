package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.CorruptedFrameException;

import java.util.List;
import java.util.function.IntFunction;

/**
 * Turns wire bytes into {@link Frame} objects by decoding the record frame, target id, length
 * and payload. TCP fragmentation and coalescing are handled by the accumulate-and-reenter
 * semantics of Netty's ByteToMessageDecoder. A length below 0 or above MAX_FRAME_BYTES fails
 * loudly, which is the OOM guard.<p>The payload is copied into a heap byte[] instead of being
 * held as a pooled ByteBuf reference, which keeps ownership simple and removes a class of
 * lifetime bugs. Downstream the frame goes to the application layer, where RowSerializer
 * deserializes it into the existing queue.
 */
public final class FrameDecoder extends ByteToMessageDecoder {

    private final io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol;
    private final ShuffleIngressResidentBudget residentBudget;
    private final IntFunction<byte[]> payloadAllocator;

    public FrameDecoder() {
        this(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0,
                null, byte[]::new);
    }

    public FrameDecoder(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol) {
        this(protocol, null, byte[]::new);
    }

    FrameDecoder(ShuffleIngressResidentBudget residentBudget) {
        this(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0,
                residentBudget, byte[]::new);
    }

    FrameDecoder(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol,
                 ShuffleIngressResidentBudget residentBudget) {
        this(protocol, residentBudget, byte[]::new);
    }

    FrameDecoder(ShuffleIngressResidentBudget residentBudget,
                 IntFunction<byte[]> payloadAllocator) {
        this(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0,
                residentBudget, payloadAllocator);
    }

    FrameDecoder(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol,
                 ShuffleIngressResidentBudget residentBudget,
                 IntFunction<byte[]> payloadAllocator) {
        if (protocol == null) throw new IllegalArgumentException("data protocol is required");
        if (payloadAllocator == null) {
            throw new IllegalArgumentException("payloadAllocator must not be null");
        }
        this.protocol = protocol;
        this.residentBudget = residentBudget;
        this.payloadAllocator = payloadAllocator;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        if (!in.isReadable()) {
            return;
        }
        byte candidateType = in.getByte(in.readerIndex());
        if (!FrameCodec.isKnownType(candidateType)) {
            int unsignedType = Byte.toUnsignedInt(candidateType);
            throw new CorruptedFrameException("unknown frame type " + unsignedType
                    + " (0x" + String.format(java.util.Locale.ROOT, "%02X", unsignedType) + ")");
        }
        int headerBytes = FrameCodec.headerBytes(protocol);
        if (in.readableBytes() < headerBytes) {
            return;  // header not complete, wait for more bytes
        }
        in.markReaderIndex();
        byte type = in.readByte();
        long tidMsb = in.readLong();  // targetId UUID (16 bytes: msb then lsb)
        long tidLsb = in.readLong();
        int producerOrdinal = protocol
                == io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1
                ? in.readInt() : 0;
        int len = in.readInt();
        if (len < 0 || len > FrameCodec.MAX_FRAME_BYTES) {
            String context = protocol == io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol
                    .SHUFFLE_MTLS_V1
                    ? "authenticated frame"
                    : "frame targetId=" + new java.util.UUID(tidMsb, tidLsb);
            throw new CorruptedFrameException("malformed/oversized " + context + " length: type="
                    + FrameCodec.typeName(type) + " length=" + len
                    + " max=" + FrameCodec.MAX_FRAME_BYTES + " (OOM guard)");
        }
        try {
            FrameCodec.validateHeaderForProtocol(protocol, type,
                    new java.util.UUID(tidMsb, tidLsb), producerOrdinal, len);
        } catch (IllegalArgumentException invalid) {
            throw new CorruptedFrameException(invalid.getMessage(), invalid);
        }
        if (in.readableBytes() < len) {
            in.resetReaderIndex();  // body not complete: rewind to the frame header and wait
            return;
        }
        ShuffleIngressResidentBudget.Reservation reservation = null;
        try {
            if (residentBudget != null) {
                try {
                    reservation = residentBudget.reserveBeforePayloadCopy(
                            (long) headerBytes + len);
                } catch (RuntimeException rejected) {
                    // This complete frame has already been refused, so its payload is consumed here.
                    // Leaving it buffered would let channel close or decodeLast read it as a second
                    // frame and hide the original admission cause.
                    in.skipBytes(len);
                    throw rejected;
                }
            }
            byte[] payload = payloadAllocator.apply(len);
            if (payload == null || payload.length != len) {
                throw new IllegalStateException("payload allocator returned wrong length");
            }
            in.readBytes(payload);  // copied into a heap byte[] only after the budget is granted
            out.add(new Frame(type, new java.util.UUID(tidMsb, tidLsb), producerOrdinal, payload,
                    reservation));
            reservation = null; // ownership transferred to Frame/DataDemuxHandler.
        } finally {
            if (reservation != null) reservation.release();
        }
    }
}
