package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RpcServerActivationTest {

    @Test
    void dormantServerMayBacklogARequestButCannotInvokeHandlerBeforeActivation() throws Exception {
        AtomicInteger handled = new AtomicInteger();
        RpcServer server = RpcServer.bindDormant(request -> {
            handled.incrementAndGet();
            return RpcMessage.ack();
        }, 0, ControlSecurityConfig.fromSystemProperties(),
                RpcAdmissionConfig.fromSystemProperties(), RpcRequestPolicy.GENERAL);
        try (Socket queued = new Socket("127.0.0.1", server.port())) {
            queued.setSoTimeout(2_000);
            RpcCodec.encodeTo(RpcMessage.heartbeat("queued"), queued.getOutputStream());
            queued.getOutputStream().flush();

            Thread.sleep(100L);
            assertEquals(0, handled.get(), "a bound dormant server must not accept from the backlog");
            assertFalse(server.acceptThreadAlive());

            server.activate();
            assertEquals(RpcMessage.Type.ACK,
                    RpcCodec.decode(new DataInputStream(queued.getInputStream())).type());
            assertEquals(1, handled.get());
            assertThrows(IllegalStateException.class, server::activate,
                    "activation is a one-shot ownership transition");
        } finally {
            server.close();
        }
    }

    @Test
    void closingDormantServerRejectsBacklogAndIsIdempotent() throws Exception {
        RpcServer server = RpcServer.bindDormant(request -> RpcMessage.ack(), 0,
                ControlSecurityConfig.fromSystemProperties(),
                RpcAdmissionConfig.fromSystemProperties(), RpcRequestPolicy.GENERAL);
        int port = server.port();
        try (Socket queued = new Socket("127.0.0.1", port)) {
            queued.setSoTimeout(2_000);
            server.close();
            server.close();
            try {
                assertEquals(-1, queued.getInputStream().read());
            } catch (IOException reset) {
                // Both FIN/EOF and RST are deterministic terminal outcomes for a queued socket.
            }
            assertFalse(server.acceptThreadAlive());
            assertThrows(RuntimeException.class,
                    () -> new RpcClient("127.0.0.1", port, 300, 300));
        }
    }

    @Test
    void activationStartFailureClosesBoundPortAndPreservesPrimary() {
        RuntimeException primary = new RuntimeException("injected thread start failure");
        RpcServer.ThreadStarter failingStarter = new RpcServer.ThreadStarter() {
            @Override
            public Thread create(Runnable target, String name) {
                return new Thread(target, name);
            }

            @Override
            public void start(Thread thread) {
                throw primary;
            }
        };
        RpcServer server = RpcServer.bindDormant(request -> RpcMessage.ack(), 0,
                ControlSecurityConfig.fromSystemProperties(),
                RpcAdmissionConfig.fromSystemProperties(), RpcRequestPolicy.GENERAL,
                ControlResidentBudget.processWide(), failingStarter);
        int port = server.port();

        RuntimeException actual = assertThrows(RuntimeException.class, server::activate);

        assertSame(primary, actual);
        assertFalse(server.acceptThreadAlive());
        assertThrows(RuntimeException.class,
                () -> new RpcClient("127.0.0.1", port, 300, 300));
        server.close();
    }
}
