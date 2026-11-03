package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class InProcessRpcRegistryTest {

    @Test
    void activeEndpointRejectsDifferentHandlerWithoutReplacingPublishedBinding() {
        String host = "inproc-identity-" + java.util.UUID.randomUUID();
        int port = 1;
        AtomicInteger firstCalls = new AtomicInteger();
        AtomicInteger newcomerCalls = new AtomicInteger();
        InProcessRpcRegistry.Registration endpointRegistration = InProcessRpcRegistry.register(host, port, request -> {
            firstCalls.incrementAndGet();
            return RpcMessage.ack();
        });
        RpcTransport published = InProcessRpcRegistry.lookup(host, port);
        try {
            assertThrows(IllegalStateException.class,
                    () -> InProcessRpcRegistry.register(host, port, request -> {
                        newcomerCalls.incrementAndGet();
                        return RpcMessage.ack();
                    }),
                    "an active endpoint is not silently replaced by another handler");

            published.call(RpcMessage.heartbeat("published"));
            InProcessRpcRegistry.lookup(host, port).call(RpcMessage.heartbeat("future"));
            assertEquals(2, firstCalls.get(), "published and future lookups observe the same first handler");
            assertEquals(0, newcomerCalls.get());
        } finally {
            endpointRegistration.close();
        }
    }

    @Test
    void staleLeaseCannotRemoveReplacementGeneration() {
        String host = "inproc-generation-" + java.util.UUID.randomUUID();
        int port = 1;
        InProcessRpcRegistry.Registration first = InProcessRpcRegistry.register(
                host, port, request -> RpcMessage.ack());
        first.close();

        AtomicInteger replacementCalls = new AtomicInteger();
        InProcessRpcRegistry.Registration replacement = InProcessRpcRegistry.register(host, port, request -> {
            replacementCalls.incrementAndGet();
            return RpcMessage.ack();
        });
        try {
            first.close();
            assertTrue(InProcessRpcRegistry.isRegistered(host, port),
                    "a repeated close of an old lease does not delete the newer generation");
            assertEquals(RpcMessage.Type.ACK,
                    InProcessRpcRegistry.lookup(host, port)
                            .call(RpcMessage.heartbeat("replacement")).type());
            assertEquals(1, replacementCalls.get());
        } finally {
            replacement.close();
        }
        assertFalse(InProcessRpcRegistry.isRegistered(host, port));
        assertNull(InProcessRpcRegistry.lookup(host, port));
    }

    @Test
    void ownerCloseUnpublishesEndpointAndShutsItsPool() {
        String host = "inproc-owner-close-" + java.util.UUID.randomUUID();
        int port = 1;
        InProcessRpcRegistry.Registration registration = InProcessRpcRegistry.register(
                host, port, request -> RpcMessage.ack());
        RpcTransport published = InProcessRpcRegistry.lookup(host, port);

        registration.close();

        assertNull(InProcessRpcRegistry.lookup(host, port));
        IllegalStateException closed = assertThrows(IllegalStateException.class,
                () -> published.call(RpcMessage.heartbeat("stale")),
                "owner close shuts down its own shared handler pool");
        assertEquals("in-process RPC endpoint is closed", closed.getMessage());
        assertDoesNotThrow(registration::close, "closing a lease twice is harmless");
    }
}
