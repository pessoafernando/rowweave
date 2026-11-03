package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** socket and in-process paths share exact set fencing. */
class RpcSecurityFenceTest {
    private static final String SET_A = repeat('a', 64);
    private static final String SET_B = repeat('b', 64);
    private static final String SET_C = repeat('c', 64);
    private static final RpcSecurityMetadata LOCAL = RpcSecurityMetadata.of(SET_A, SET_B);

    @Test
    void secureOutboundAttachesAndInboundRequiresExactIdentity() {
        RpcMessage plain = RpcMessage.ack();
        RpcMessage secured = RpcSecurityFence.outbound(plain, LOCAL, true);
        assertEquals(SET_A, secured.securityMetadata().deploymentSetId());
        assertSame(secured, RpcSecurityFence.inbound(secured, LOCAL, true));

        assertThrows(SecurityException.class,
                () -> RpcSecurityFence.inbound(plain, LOCAL, true));
        assertThrows(SecurityException.class,
                () -> RpcSecurityFence.inbound(
                        plain.withSecurityMetadata(RpcSecurityMetadata.of(SET_C, SET_B)),
                        LOCAL, true));
        assertThrows(SecurityException.class,
                () -> RpcSecurityFence.outbound(
                        plain.withSecurityMetadata(RpcSecurityMetadata.of(SET_A, SET_C)),
                        LOCAL, true));
    }

    @Test
    void loopbackLegacyRemainsUnmodified() {
        RpcMessage plain = RpcMessage.ack();
        assertSame(plain, RpcSecurityFence.outbound(plain, null, false));
        assertSame(plain, RpcSecurityFence.inbound(plain, null, false));
        assertNull(plain.securityMetadata());
    }

    @Test
    void inProcessSecureTransportUsesSameFenceBeforeHandler() {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        try (InProcessRpcTransport transport = InProcessRpcTransport.secure(request -> {
            calls.incrementAndGet();
            assertEquals(SET_A, request.securityMetadata().deploymentSetId());
            return RpcMessage.ack();
        }, LOCAL)) {
            RpcMessage response = transport.call(RpcMessage.heartbeat("w", 1L, "inc"));
            assertEquals(SET_B, response.securityMetadata().authzSetId());
            assertEquals(1, calls.get());

            assertThrows(SecurityException.class, () -> transport.call(
                    RpcMessage.heartbeat("w", 1L, "inc").withSecurityMetadata(
                            RpcSecurityMetadata.of(SET_C, SET_B))));
            assertEquals(1, calls.get(), "mismatch must be rejected before handler");
        }

        try (InProcessRpcTransport transport = InProcessRpcTransport.secure(request ->
                RpcMessage.ack().withSecurityMetadata(RpcSecurityMetadata.of(SET_C, SET_B)),
                LOCAL)) {
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> transport.call(RpcMessage.ack()));
            assertTrue(hasCause(failure, SecurityException.class));
        }
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (type.isInstance(current)) return true;
            if (current == current.getCause()) break;
        }
        return false;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}
