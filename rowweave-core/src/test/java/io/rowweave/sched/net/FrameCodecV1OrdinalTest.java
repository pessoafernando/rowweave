package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import io.netty.handler.codec.EncoderException;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** MTLS V1 has a fixed 25-byte header and stable logical-producer ordinal. */
class FrameCodecV1OrdinalTest {

    @Test
    void mtlsV1RoundTripsOrdinalInExactlyTwentyFiveHeaderBytes() {
        UUID target = new UUID(11L, 13L);
        byte[] payload = new byte[]{1, 2, 3};
        EmbeddedChannel encoder = new EmbeddedChannel(new FrameEncoder(
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1));
        encoder.writeOutbound(new Frame(FrameCodec.FRAME_RECORD, target, 37, payload));
        ByteBuf wire = encoder.readOutbound();
        assertEquals(FrameCodec.V1_HEADER_BYTES + payload.length, wire.readableBytes());

        EmbeddedChannel decoder = new EmbeddedChannel(new FrameDecoder(
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1));
        decoder.writeInbound(wire.retain());
        Frame decoded = decoder.readInbound();
        assertEquals(FrameCodec.FRAME_RECORD, decoded.type());
        assertEquals(target, decoded.targetId());
        assertEquals(37, decoded.producerOrdinal());
        assertArrayEquals(payload, decoded.payload());
        wire.release();
        encoder.finishAndReleaseAll();
        decoder.finishAndReleaseAll();
    }

    @Test
    void loopbackV0WireRemainsTwentyOneBytesAndHasNoOrdinal() {
        EmbeddedChannel encoder = new EmbeddedChannel(new FrameEncoder());
        encoder.writeOutbound(new Frame(FrameCodec.FRAME_RECORD,
                new UUID(1L, 2L), new byte[0]));
        ByteBuf wire = encoder.readOutbound();
        assertEquals(FrameCodec.V0_HEADER_BYTES, wire.readableBytes());
        wire.release();
        encoder.finishAndReleaseAll();
    }

    @Test
    void protocolSpecificOrdinalRulesFailBeforeWritingBytes() {
        UUID target = new UUID(5L, 8L);
        EmbeddedChannel v1 = new EmbeddedChannel(new FrameEncoder(
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1));
        assertThrows(EncoderException.class, () -> v1.writeOutbound(
                new Frame(FrameCodec.FRAME_RECORD, target, 0, new byte[0])));
        assertThrows(EncoderException.class, () -> v1.writeOutbound(
                new Frame(FrameCodec.FRAME_KEEPALIVE, target, 1, new byte[0])));
        v1.finishAndReleaseAll();

        EmbeddedChannel v0 = new EmbeddedChannel(new FrameEncoder());
        assertThrows(EncoderException.class, () -> v0.writeOutbound(
                new Frame(FrameCodec.FRAME_RECORD, target, 1, new byte[0])));
        v0.finishAndReleaseAll();
    }

    @Test
    void mtlsV1MalformedFrameDiagnosticDoesNotExposeTargetIdentity() {
        UUID target = new UUID(0x1122334455667788L, 0x1020304050607080L);
        ByteBuf header = Unpooled.buffer(FrameCodec.V1_HEADER_BYTES)
                .writeByte(FrameCodec.FRAME_RECORD)
                .writeLong(target.getMostSignificantBits())
                .writeLong(target.getLeastSignificantBits())
                .writeInt(1)
                .writeInt(FrameCodec.MAX_FRAME_BYTES + 1);
        EmbeddedChannel decoder = new EmbeddedChannel(new FrameDecoder(
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1));

        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> decoder.writeInbound(header));
        Throwable root = rootCause(thrown);
        assertInstanceOf(CorruptedFrameException.class, root);
        assertFalse(allMessages(thrown).contains(target.toString()));
        decoder.finishAndReleaseAll();
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable root = failure;
        while (root.getCause() != null && root.getCause() != root) root = root.getCause();
        return root;
    }

    private static String allMessages(Throwable failure) {
        StringBuilder messages = new StringBuilder();
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current.getMessage() != null) messages.append(current.getMessage()).append('\n');
        }
        return messages.toString();
    }
}
