package io.rowweave.sched.net;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.CorruptedFrameException;
import java.util.UUID;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the FrameCodec frame encoder and decoder. A frame is
 * `[type:byte][targetId:long][len:int big-endian][payload]`, where targetId is the mux
 * routing key. Handwritten frame bytes must agree with FrameEncoder, and the pipeline is
 * driven through a Netty EmbeddedChannel.
 */
class FrameCodecTest {

    /** Handwritten frame bytes, the comparison baseline: DataOutputStream writeByte, then writeLong for the targetId, then writeInt for the length, then write. */
    private static byte[] wireFrame(byte type, UUID targetId, byte[] payload) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream o = new DataOutputStream(bos);
        o.writeByte(type);
        o.writeLong(targetId.getMostSignificantBits());
        o.writeLong(targetId.getLeastSignificantBits());
        o.writeInt(payload.length);
        o.write(payload);
        o.flush();
        return bos.toByteArray();
    }

    private static byte[] wireHeader(byte type, UUID targetId, int length) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream o = new DataOutputStream(bos);
        o.writeByte(type);
        o.writeLong(targetId.getMostSignificantBits());
        o.writeLong(targetId.getLeastSignificantBits());
        o.writeInt(length);
        o.flush();
        return bos.toByteArray();
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null && current.getCause() != current) {
            current = current.getCause();
        }
        return current;
    }

    /** Encoding a Frame to wire bytes must match the handwritten new format, targetId included, byte for byte. */
    @Test
    void encodeMatchesWire() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new FrameEncoder());
        byte[] payload = {1, 2, 3, 4, 5};
        UUID tid = new UUID(0L,0x1122334455667788L);
        assertTrue(ch.writeOutbound(new Frame(FrameCodec.FRAME_RECORD, tid, payload)));
        ByteBuf enc = ch.readOutbound();
        byte[] got = new byte[enc.readableBytes()];
        enc.readBytes(got);
        enc.release();
        assertArrayEquals(wireFrame(FrameCodec.FRAME_RECORD, tid, payload), got, "the encoded bytes match the hand-written frame including the targetId");
        assertFalse(ch.finish());
    }

    /** Frames with an empty payload, such as EOS and KEEPALIVE, agree byte for byte and carry the targetId. */
    @Test
    void encodeEmptyPayloadFrame() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new FrameEncoder());
        assertTrue(ch.writeOutbound(new Frame(FrameCodec.FRAME_EOS, new UUID(0L,42L), new byte[0])));
        ByteBuf enc = ch.readOutbound();
        byte[] got = new byte[enc.readableBytes()];
        enc.readBytes(got);
        enc.release();
        assertArrayEquals(wireFrame(FrameCodec.FRAME_EOS, new UUID(0L,42L), new byte[0]), got, "an EOS frame with an empty payload matches");
        assertFalse(ch.finish());
    }

    /** OPEN and a CREDIT carrying a targetId share one frame protocol. */
    @Test
    void roundTripsOpenAndTargetCreditFrames() {
        UUID target = new UUID(90L, 42L);
        EmbeddedChannel channel = new EmbeddedChannel(new FrameEncoder(), new FrameDecoder());
        assertTrue(channel.writeOutbound(new Frame(FrameCodec.FRAME_OPEN, target, new byte[0])));
        ByteBuf openWire = channel.readOutbound();
        assertTrue(channel.writeInbound(openWire));
        Frame open = channel.readInbound();
        assertEquals(FrameCodec.FRAME_OPEN, open.type());
        assertEquals(target, open.targetId());

        byte[] credit = {0, 0, 4, 0};
        assertTrue(channel.writeOutbound(new Frame(FrameCodec.FRAME_CREDIT, target, credit)));
        ByteBuf creditWire = channel.readOutbound();
        assertTrue(channel.writeInbound(creditWire));
        Frame decoded = channel.readInbound();
        assertEquals(FrameCodec.FRAME_CREDIT, decoded.type());
        assertEquals(target, decoded.targetId());
        assertArrayEquals(credit, decoded.payload());
        channel.finishAndReleaseAll();
    }

    /** The legacy two-argument constructor leaves the targetId field at 0. */
    @Test
    void encodeLegacyTargetIdZero() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new FrameEncoder());
        byte[] payload = {7, 7};
        assertTrue(ch.writeOutbound(new Frame(FrameCodec.FRAME_RECORD, payload))); // 2-arg → targetId=0
        ByteBuf enc = ch.readOutbound();
        byte[] got = new byte[enc.readableBytes()];
        enc.readBytes(got);
        enc.release();
        assertArrayEquals(wireFrame(FrameCodec.FRAME_RECORD, new UUID(0L,0L), payload), got, "the constructor without a target encodes targetId 0");
        assertFalse(ch.finish());
    }

    /** Decoding wire bytes back into a Frame restores the type, targetId and payload correctly. */
    @Test
    void decodeWire() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new FrameDecoder());
        byte[] payload = {9, 8, 7, 6};
        UUID tid = new UUID(0L,0xDEADBEEFL);
        ch.writeInbound(Unpooled.wrappedBuffer(wireFrame(FrameCodec.FRAME_ERROR, tid, payload)));
        Frame f = ch.readInbound();
        assertNotNull(f, "one frame decodes");
        assertEquals(FrameCodec.FRAME_ERROR, f.type());
        assertEquals(tid, f.targetId(), "the targetId survives");
        assertArrayEquals(payload, f.payload());
        assertFalse(ch.finish());
    }

    /** An unknown type is a complete protocol refusal: one byte is enough to reject it, without waiting for a fake header. */
    @Test
    void rejectsUnknownTypeBeforeWaitingForHeader() {
        EmbeddedChannel ch = new EmbeddedChannel(new FrameDecoder());
        RuntimeException thrown = assertThrows(RuntimeException.class,
                () -> ch.writeInbound(Unpooled.wrappedBuffer(new byte[]{0x52})));
        Throwable root = rootCause(thrown);
        assertInstanceOf(CorruptedFrameException.class, root);
        assertTrue(root.getMessage().contains("82"), "the type is reported as unsigned decimal: " + root.getMessage());
        assertTrue(root.getMessage().contains("0x52"), "the type is reported in hexadecimal as well: " + root.getMessage());
        // ByteToMessageDecoder keeps the unconsumed first byte after an exception, so finishing the EmbeddedChannel triggers the same fail-fast again.
        assertThrows(CorruptedFrameException.class, ch::finishAndReleaseAll);
    }

    /** An out-of-range length under a valid type must keep type, target, length and maximum in the message, rather than degrading to a bare OOM guard. */
    @Test
    void oversizedLengthReportsFrameContext() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new FrameDecoder());
        UUID target = new UUID(0x1122334455667788L, 0x1020304050607080L);
        int asciiRequ = 0x52657175;
        RuntimeException thrown = assertThrows(RuntimeException.class, () -> ch.writeInbound(
                Unpooled.wrappedBuffer(wireHeader(FrameCodec.FRAME_RECORD, target, asciiRequ))));
        Throwable root = rootCause(thrown);
        assertInstanceOf(CorruptedFrameException.class, root);
        assertTrue(root.getMessage().contains("RECORD"), "the frame type is reported: " + root.getMessage());
        assertTrue(root.getMessage().contains(target.toString()), "the targetId is reported: " + root.getMessage());
        assertTrue(root.getMessage().contains(String.valueOf(asciiRequ)), "the actual length is reported: " + root.getMessage());
        assertTrue(root.getMessage().contains(String.valueOf(FrameCodec.MAX_FRAME_BYTES)),
                "the length limit is reported: " + root.getMessage());
        ch.finishAndReleaseAll();
    }

    /** Split arrival, that is a partial TCP write: feeding the bytes in two batches, the decoder must wait for the whole frame, which is the LengthField semantics. */
    @Test
    void decodeHandlesFragmentedArrival() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new FrameDecoder());
        byte[] payload = {10, 20, 30, 40, 50, 60};
        byte[] wire = wireFrame(FrameCodec.FRAME_RECORD, new UUID(0L,5L), payload);
        // Feed the first 3 bytes, which is less than a header plus body, so no frame should be produced.
        ch.writeInbound(Unpooled.wrappedBuffer(wire, 0, 3));
        assertNull(ch.readInbound(), "a partial frame yields nothing");
        // Then feed the rest, which yields the complete frame.
        ch.writeInbound(Unpooled.wrappedBuffer(wire, 3, wire.length - 3));
        Frame f = ch.readInbound();
        assertNotNull(f, "the frame arrives once the bytes are complete");
        assertEquals(FrameCodec.FRAME_RECORD, f.type());
        assertEquals(new UUID(0L,5L), f.targetId());
        assertArrayEquals(payload, f.payload());
        assertFalse(ch.finish());
    }

    /** Several frames in one batch, that is coalesced writes: two frames with different targetIds must decode as two frames, each keeping its own targetId. */
    @Test
    void decodeHandlesMultipleFramesInOneBuffer() throws Exception {
        EmbeddedChannel ch = new EmbeddedChannel(new FrameDecoder());
        byte[] f1 = wireFrame(FrameCodec.FRAME_RECORD, new UUID(0L,100L), new byte[]{1, 1});
        byte[] f2 = wireFrame(FrameCodec.FRAME_EOS, new UUID(0L,200L), new byte[0]);
        byte[] both = new byte[f1.length + f2.length];
        System.arraycopy(f1, 0, both, 0, f1.length);
        System.arraycopy(f2, 0, both, f1.length, f2.length);
        ch.writeInbound(Unpooled.wrappedBuffer(both));

        List<Frame> frames = new ArrayList<>();
        Frame f;
        while ((f = ch.readInbound()) != null) {
            frames.add(f);
        }
        assertEquals(2, frames.size(), "a coalesced read decodes 2 frames");
        assertEquals(FrameCodec.FRAME_RECORD, frames.get(0).type());
        assertEquals(new UUID(0L,100L), frames.get(0).targetId());
        assertEquals(FrameCodec.FRAME_EOS, frames.get(1).type());
        assertEquals(new UUID(0L,200L), frames.get(1).targetId(), "different targetIds stay separate, which is what multiplexed demultiplexing relies on");
        assertFalse(ch.finish());
    }

    /** A large payload round-trips, keeping both the payload and the targetId faithful through encode then decode. */
    @Test
    void roundTripLargePayload() {
        byte[] payload = new byte[100_000];
        for (int i = 0; i < payload.length; i++) payload[i] = (byte) (i * 31);
        EmbeddedChannel enc = new EmbeddedChannel(new FrameEncoder());
        enc.writeOutbound(new Frame(FrameCodec.FRAME_RECORD, new UUID(0L,999L), payload));
        ByteBuf wire = enc.readOutbound();

        EmbeddedChannel dec = new EmbeddedChannel(new FrameDecoder());
        dec.writeInbound(wire);  // hand ownership of the buffer to the decoder
        Frame f = dec.readInbound();
        assertNotNull(f);
        assertEquals(new UUID(0L,999L), f.targetId());
        assertArrayEquals(payload, f.payload(), "a large payload survives encoding and decoding");
        assertFalse(enc.finish());
        assertFalse(dec.finish());
    }
}
