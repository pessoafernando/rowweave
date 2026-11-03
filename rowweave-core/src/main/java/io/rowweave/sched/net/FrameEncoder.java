package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

/**
 * Turns a {@link Frame} into wire bytes by writing the type, target id, big-endian length and
 * payload. Netty's ByteBuf is big-endian by default, which matches DataOutputStream.writeInt
 * and writeLong. The targetId is the multiplexing routing key: a 16-byte UUID, the same width
 * as a Flink InputChannelID.
 */
public final class FrameEncoder extends MessageToByteEncoder<Frame> {

    private final io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol;

    public FrameEncoder() {
        this(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0);
    }

    public FrameEncoder(io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol protocol) {
        if (protocol == null) throw new IllegalArgumentException("data protocol is required");
        this.protocol = protocol;
    }

    @Override
    protected void encode(ChannelHandlerContext ctx, Frame frame, ByteBuf out) {
        FrameCodec.validateForProtocol(protocol, frame);
        byte[] payload = frame.payload();
        java.util.UUID tid = frame.targetId();
        out.writeByte(frame.type());
        out.writeLong(tid.getMostSignificantBits());  // targetId UUID (16 bytes: msb then lsb)
        out.writeLong(tid.getLeastSignificantBits());
        if (protocol == io.rowweave.cluster.rpc.DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1) {
            out.writeInt(frame.producerOrdinal());
        }
        out.writeInt(payload.length);  // big-endian (Netty default)
        out.writeBytes(payload);
    }
}
