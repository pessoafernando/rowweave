package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class NegotiatedRpcConnectionTest {

    @Test
    void negotiatedResidentLimitRejectsOversizedOutboundBeforeDispatch() {
        ProtocolHelloV1 hello = hello('a', 32L,
                RpcMessage.Type.SUBMIT_JOB, RpcMessage.Type.ACK);
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = RpcServer.negotiated(request -> {
                 handled.incrementAndGet();
                 return RpcMessage.ack();
             }, hello);
             RpcClient client = RpcClient.negotiated(
                     "127.0.0.1", server.port(), hello)) {
            ProtocolNegotiationException rejected = assertThrows(
                    ProtocolNegotiationException.class,
                    () -> client.call(RpcMessage.submitJob(new byte[64])));
            assertEquals("RESIDENT_LIMIT_EXCEEDED", rejected.code());
            assertEquals(0, handled.get());
        }
    }

    @Test
    void negotiatedResidentLimitRejectsOversizedInboundBeforeDispatch()
            throws Exception {
        ProtocolHelloV1 hello = hello('a', 32L,
                RpcMessage.Type.SUBMIT_JOB, RpcMessage.Type.ACK);
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = RpcServer.negotiated(request -> {
                 handled.incrementAndGet();
                 return RpcMessage.ack();
             }, hello);
             java.net.Socket socket = new java.net.Socket(
                     "127.0.0.1", server.port())) {
            java.io.DataInputStream input = new java.io.DataInputStream(
                    socket.getInputStream());
            ProtocolHelloV1Codec.encodeTo(hello, socket.getOutputStream());
            socket.getOutputStream().flush();
            ProtocolHelloV1 remote = ProtocolHelloV1Codec.decodeFrom(input);
            NegotiatedProtocol negotiated = ProtocolNegotiator.negotiate(
                    hello, remote);
            RpcCodec.encodeToV1(RpcMessage.submitJob(new byte[64]),
                    socket.getOutputStream(), RpcAdmissionConfig.fromSystemProperties(),
                    negotiated);
            socket.getOutputStream().flush();

            try {
                assertEquals(-1, input.read());
            } catch (java.net.SocketException reset) {
                // TCP may surface the fail-closed peer shutdown as EOF or RST.
            }
            assertEquals(0, handled.get());
        }
    }

    @Test
    void negotiatedResidentLimitRejectsOversizedResponse() {
        ProtocolHelloV1 hello = hello('a', 32L,
                RpcMessage.Type.HEARTBEAT, RpcMessage.Type.JOB_RESULT);
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = RpcServer.negotiated(request -> {
                 handled.incrementAndGet();
                 return RpcMessage.jobResult(false, 0L,
                         "response-resident-budget-is-deliberately-too-large");
             }, hello);
             RpcClient client = RpcClient.negotiated(
                     "127.0.0.1", server.port(), hello)) {
            assertThrows(RuntimeException.class,
                    () -> client.call(RpcMessage.heartbeat("w")));
            assertEquals(1, handled.get());
        }
    }

    @Test
    void matchingPeersExchangeHelloBeforeStableIdBusinessFrames() {
        ProtocolHelloV1 hello = hello('a', RpcMessage.Type.HEARTBEAT,
                RpcMessage.Type.ACK);
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = RpcServer.negotiated(request -> {
                 handled.incrementAndGet();
                 return RpcMessage.ack();
             }, hello);
             RpcClient client = RpcClient.negotiated(
                     "127.0.0.1", server.port(), hello)) {
            assertNotNull(client.negotiatedProtocol());
            assertEquals(RpcMessage.Type.ACK,
                    client.call(RpcMessage.heartbeat("worker")).type());
            assertEquals(1, handled.get());
        }
    }

    @Test
    void negotiatedIntersectionIsBoundToEveryRequestContext() {
        ProtocolHelloV1 hello = hello('a', RpcMessage.Type.HEARTBEAT,
                RpcMessage.Type.ACK);
        AtomicReference<NegotiatedProtocol> observed = new AtomicReference<>();
        AtomicReference<ProtocolHelloV1> observedRemote = new AtomicReference<>();
        try (RpcServer server = RpcServer.negotiated((request, context) -> {
                 observed.set(context.negotiatedProtocol());
                 observedRemote.set(context.remoteProtocolHello());
                 return RpcMessage.ack();
             }, hello);
             RpcClient client = RpcClient.negotiated(
                     "127.0.0.1", server.port(), hello)) {
            assertEquals(RpcMessage.Type.ACK,
                    client.call(RpcMessage.heartbeat("worker")).type());
            assertNotNull(observed.get());
            assertEquals(client.negotiatedProtocol().messageIds(),
                    observed.get().messageIds());
            assertEquals(hello, observedRemote.get());
        }
    }

    @Test
    void digestMismatchAndLegacyTrafficAreRejectedBeforeHandler() {
        ProtocolHelloV1 serverHello = hello('a', RpcMessage.Type.HEARTBEAT,
                RpcMessage.Type.ACK);
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = RpcServer.negotiated(request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, serverHello)) {
            assertThrows(RuntimeException.class, () -> {
                try (RpcClient ignored = RpcClient.negotiated(
                        "127.0.0.1", server.port(),
                        hello('b', RpcMessage.Type.HEARTBEAT,
                                RpcMessage.Type.ACK))) {
                    // constructor performs the handshake
                }
            });
            try (RpcClient legacy = new RpcClient("127.0.0.1", server.port())) {
                assertThrows(RuntimeException.class,
                        () -> legacy.call(RpcMessage.heartbeat("legacy")));
            }
            assertEquals(0, handled.get());
        }
    }

    @Test
    void unadvertisedBusinessMessageFailsLocallyWithoutServerDispatch() {
        ProtocolHelloV1 hello = hello('a', RpcMessage.Type.HEARTBEAT,
                RpcMessage.Type.ACK);
        AtomicInteger handled = new AtomicInteger();
        try (RpcServer server = RpcServer.negotiated(request -> {
                 handled.incrementAndGet();
                 return RpcMessage.ack();
             }, hello);
             RpcClient client = RpcClient.negotiated(
                     "127.0.0.1", server.port(), hello)) {
            assertThrows(ProtocolNegotiationException.class,
                    () -> client.call(RpcMessage.queryRun("run-1")));
            assertEquals(0, handled.get());
        }
    }

    private static ProtocolHelloV1 hello(char digest,
                                         RpcMessage.Type... types) {
        return hello(digest, 8L * 1024 * 1024, types);
    }

    private static ProtocolHelloV1 hello(char digest, long maximumResidentBytes,
                                         RpcMessage.Type... types) {
        LinkedHashSet<Integer> messageIds = new LinkedHashSet<>();
        for (RpcMessage.Type type : types) {
            messageIds.add(ControlMessageId.wireId(type));
        }
        char[] digestChars = new char[64];
        Arrays.fill(digestChars, digest);
        return new ProtocolHelloV1(new String(digestChars), 1,
                new ProtocolRevisionRange(0, 4),
                new ProtocolRevisionRange(0, 4), messageIds,
                Collections.singleton(3), Collections.singleton(2),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton("runtime.batch"),
                Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap(),
                1024 * 1024, maximumResidentBytes);
    }
}
