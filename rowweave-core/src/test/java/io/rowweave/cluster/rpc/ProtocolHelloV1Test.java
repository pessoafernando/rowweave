package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolHelloV1Test {

    @Test
    void everyControlMessageHasOneExplicitStableWireId() {
        Set<Integer> ids = new LinkedHashSet<>();
        for (RpcMessage.Type type : RpcMessage.Type.values()) {
            int id = ControlMessageId.wireId(type);
            assertTrue(id > 0);
            assertTrue(ids.add(id), type.name());
            assertEquals(type, ControlMessageId.fromWireId(id));
        }
        assertEquals(RpcMessage.Type.values().length,
                ControlMessageId.allWireIds().size());
        assertThrows(IllegalArgumentException.class,
                () -> ControlMessageId.fromWireId(Integer.MAX_VALUE));
    }

    @Test
    void helloFrameIsCanonicalBoundedAndDefensivelyOwned() {
        byte[] optional = new byte[]{7, 8, 9};
        ProtocolHelloV1 hello = hello("a", 4,
                set(7, 3, 5), set("runtime.z", "runtime.a"),
                Collections.singleton(91), Collections.singletonMap(92, optional));
        optional[0] = 99;

        byte[] encoded = ProtocolHelloV1Codec.encode(hello);
        ProtocolHelloV1 decoded = ProtocolHelloV1Codec.decode(encoded);
        assertEquals(hello, decoded);
        assertArrayEquals(new byte[]{7, 8, 9}, decoded.optionalExtensions().get(92));
        assertEquals(Arrays.asList(3, 5, 7), decoded.messageIds());
        assertEquals(Arrays.asList("runtime.a", "runtime.z"),
                decoded.runtimeCapabilities());

        byte[] reordered = ProtocolHelloV1Codec.encode(hello("a", 4,
                set(5, 7, 3), set("runtime.a", "runtime.z"),
                Collections.singleton(91),
                Collections.singletonMap(92, new byte[]{7, 8, 9})));
        assertArrayEquals(encoded, reordered);

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolHelloV1Codec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolHelloV1Codec.decode(Arrays.copyOf(encoded,
                        encoded.length - 1)));
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolHelloV1Codec.decode(new byte[
                        ProtocolHelloV1Codec.MAX_FRAME_BYTES + 1]));
    }

    @Test
    void negotiationComputesOnlyTheSafeIntersection() {
        ProtocolHelloV1 current = hello("a", 4, set(3, 5, 7),
                set("runtime.batch", "runtime.cdc"),
                Collections.<Integer>emptySet(), Collections.singletonMap(92,
                        new byte[]{1}));
        ProtocolHelloV1 previous = hello("a", 3, set(3, 9),
                set("runtime.batch", "runtime.old"),
                Collections.<Integer>emptySet(), Collections.singletonMap(93,
                        new byte[]{2}));

        NegotiatedProtocol negotiated = ProtocolNegotiator.negotiate(
                current, previous);
        assertEquals(1, negotiated.protocolEpoch());
        assertEquals(0, negotiated.minimumWriterRevision());
        assertEquals(3, negotiated.maximumWriterRevision());
        assertEquals(Collections.singletonList(3), negotiated.messageIds());
        assertEquals(Collections.singletonList("runtime.batch"),
                negotiated.runtimeCapabilities());
        assertEquals(1024, negotiated.maximumFrameBytes());
        assertEquals(2048L, negotiated.maximumResidentBytes());
        assertFalse(negotiated.optionalExtensionIds().contains(92));
        assertFalse(negotiated.optionalExtensionIds().contains(93));
    }

    @Test
    void requiredMismatchAndRevisionDriftFailBeforeUse() {
        ProtocolHelloV1 current = hello("a", 4, set(3, 5),
                set("runtime.batch"), Collections.singleton(91),
                Collections.<Integer, byte[]>emptyMap());
        ProtocolHelloV1 withoutRequired = hello("a", 3, set(3, 5),
                set("runtime.batch"), Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap());
        assertThrows(ProtocolNegotiationException.class,
                () -> ProtocolNegotiator.negotiate(current, withoutRequired));

        assertThrows(ProtocolNegotiationException.class,
                () -> ProtocolNegotiator.negotiate(current,
                        hello("b", 3, set(3, 5), set("runtime.batch"),
                                Collections.singleton(91),
                                Collections.<Integer, byte[]>emptyMap())));
        assertThrows(ProtocolNegotiationException.class,
                () -> ProtocolNegotiator.negotiate(current,
                        new ProtocolHelloV1(repeat('a', 64), 2,
                                new ProtocolRevisionRange(0, 3),
                                new ProtocolRevisionRange(0, 3),
                                set(3, 5), set(3), set(2), set(1), set(1),
                                set("runtime.batch"), Collections.singleton(91),
                                Collections.<Integer, byte[]>emptyMap(), 1024, 2048L)));
    }

    @Test
    void negotiatedRpcFramesUseStableIdsAndRejectUnadvertisedMessages()
            throws Exception {
        int ackId = ControlMessageId.wireId(RpcMessage.Type.ACK);
        ProtocolHelloV1 ackOnly = hello("a", 4, set(ackId),
                set("runtime.batch"), Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap());
        NegotiatedProtocol negotiated = ProtocolNegotiator.negotiate(
                ackOnly, ackOnly);

        byte[] encoded = RpcCodec.encodeV1(RpcMessage.ack(), negotiated);
        assertEquals(ackId, ByteBuffer.wrap(encoded).getInt(Integer.BYTES));
        assertEquals(RpcMessage.Type.ACK, RpcCodec.decodeV1(
                new DataInputStream(new ByteArrayInputStream(encoded)),
                negotiated).type());

        byte[] legacy = RpcCodec.encode(RpcMessage.ack());
        assertEquals(RpcMessage.Type.ACK.ordinal(),
                ByteBuffer.wrap(legacy).getInt(Integer.BYTES));
        assertThrows(ProtocolNegotiationException.class,
                () -> RpcCodec.encodeV1(
                        RpcMessage.jobResult(true, 0, null), negotiated));
    }

    private static ProtocolHelloV1 hello(
            String digestChar, int maximumRevision, Set<Integer> messageIds,
            Set<String> runtimeCapabilities, Set<Integer> requiredExtensions,
            java.util.Map<Integer, byte[]> optionalExtensions) {
        return new ProtocolHelloV1(repeat(digestChar.charAt(0), 64), 1,
                new ProtocolRevisionRange(0, maximumRevision),
                new ProtocolRevisionRange(0, maximumRevision),
                messageIds, set(3), set(2), set(1), set(1),
                runtimeCapabilities, requiredExtensions, optionalExtensions,
                1024, 2048L);
    }

    private static Set<Integer> set(Integer... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    private static Set<String> set(String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    private static String repeat(char value, int count) {
        char[] values = new char[count];
        Arrays.fill(values, value);
        return new String(values);
    }
}
