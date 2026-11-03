package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** logical role endpoints are asynchronous and never fall back to sockets. */
class RpcRoleEndpointTest {

    @Test
    void inProcessEndpointUsesUnroutableIdentityAndFailsClosedAfterLeaseEnds() {
        String host = "127.0.0.1";
        AtomicReference<Thread> handlerThread = new AtomicReference<>();
        RpcRoleEndpoint endpoint = RpcRoleEndpoints.inProcess(host, request -> {
            handlerThread.set(Thread.currentThread());
            return RpcMessage.ack();
        }, 2);
        Thread caller = Thread.currentThread();

        assertTrue(InProcessRpcRegistry.isLogicalPort(endpoint.port()));
        assertFalse(endpoint.hasNetworkListener());
        assertFalse(endpoint.acceptThreadAlive());
        endpoint.activate();
        try (RpcTransport transport = RpcTransports.connect(host, endpoint.port())) {
            assertEquals(RpcMessage.Type.ACK,
                    transport.call(RpcMessage.heartbeat("worker")).type());
        }

        assertNotEquals(caller, handlerThread.get());
        assertEquals(1, endpoint.handlerThreadCount());
        endpoint.close();
        IllegalStateException stale = assertThrows(IllegalStateException.class,
                () -> RpcTransports.connect(host, endpoint.port()));
        assertTrue(stale.getMessage().contains("closed"), stale.getMessage());
        endpoint.close();
    }
}
