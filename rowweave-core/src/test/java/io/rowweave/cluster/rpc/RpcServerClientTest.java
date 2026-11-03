package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class RpcServerClientTest {

    @Test
    void requestResponseRoundTrip() {
        try (RpcServer server = new RpcServer(req -> {
            if (req.type() == RpcMessage.Type.SUBMIT_JOB) {
                return RpcMessage.jobResult(true, 99L, null);
            }
            return RpcMessage.ack();
        })) {
            try (RpcClient client = new RpcClient("127.0.0.1", server.port())) {
                RpcMessage resp = client.call(RpcMessage.submitJob(new byte[]{7, 7}));
                assertEquals(RpcMessage.Type.JOB_RESULT, resp.type());
                assertTrue(resp.success());
                assertEquals(99L, resp.rowCount());
            }
        }
    }

    @Test
    void defaultServerBindsLoopbackInsteadOfWildcard() {
        try (RpcServer server = new RpcServer(req -> RpcMessage.ack())) {
            assertTrue(server.boundAddress().isLoopbackAddress(),
                    "LOOPBACK profile must not expose plaintext RPC on wildcard/non-loopback: "
                            + server.boundAddress());
        }
    }

    @Test
    void multipleCallsOnSameConnection() {
        ConcurrentLinkedQueue<String> seen = new ConcurrentLinkedQueue<>();
        try (RpcServer server = new RpcServer(req -> {
            if (req.type() == RpcMessage.Type.REGISTER_WORKER) {
                seen.add(req.workerId());
            }
            return RpcMessage.ack();
        })) {
            try (RpcClient client = new RpcClient("127.0.0.1", server.port())) {
                for (int i = 0; i < 5; i++) {
                    RpcMessage resp = client.call(RpcMessage.registerWorker("w" + i, "h", 1000 + i, 4));
                    assertEquals(RpcMessage.Type.ACK, resp.type());
                }
            }
        }
        assertEquals(5, seen.size(), "all 5 registrations were received");
    }

    @Test
    void connectToDeadPortFailsFast() {
        assertThrows(RuntimeException.class,
                () -> new RpcClient("127.0.0.1", 1, 500, 500));
    }

    @Test
    void serverCloseUnblocksGracefully() {
        RpcServer server = new RpcServer(req -> RpcMessage.ack());
        int port = server.port();
        server.close();
        assertThrows(RuntimeException.class, () -> {
            try (RpcClient c = new RpcClient("127.0.0.1", port, 500, 500)) {
                c.call(RpcMessage.heartbeat("x"));
            }
        });
    }

    @Test
    void wildcardBindCannotAliasExistingLoopbackListener() throws Exception {
        try (ServerSocket loopback = new ServerSocket()) {
            loopback.bind(new InetSocketAddress("127.0.0.1", 0));
            AtomicReference<RpcServer> unexpected = new AtomicReference<>();
            try {
                assertThrows(RuntimeException.class,
                        () -> unexpected.set(new RpcServer(
                                request -> RpcMessage.ack(), loopback.getLocalPort())));
            } finally {
                if (unexpected.get() != null) {
                    unexpected.get().close();
                }
            }
        }
    }

    @Test
    void closeOwnsAcceptedIdleConnectionAndStopsAcceptLoop() throws Exception {
        RpcServer server = new RpcServer(req -> RpcMessage.ack());
        Socket idle = new Socket("127.0.0.1", server.port());
        idle.setSoTimeout(1_000);
        try {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
            while (server.liveConnectionCount() != 1 && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertEquals(1, server.liveConnectionCount(), "accepted socket must be owned before handler handoff");

            server.close();

            assertFalse(server.acceptThreadAlive(), "close must join the accept loop");
            assertEquals(0, server.liveConnectionCount());
            assertEquals(-1, idle.getInputStream().read(), "server close must unblock idle client with EOF");
        } finally {
            idle.close();
            server.close();
        }
    }

    @Test
    void corruptResponseCarriesEndpointRequestAndOperationSequence() throws Exception {
        AtomicReference<Throwable> serverFailure = new AtomicReference<>();
        List<RpcMessage.Type> seen = java.util.Collections.synchronizedList(new ArrayList<>());
        try (ServerSocket fake = new ServerSocket(0)) {
            Thread responder = new Thread(() -> {
                try (Socket accepted = fake.accept();
                     DataInputStream in = new DataInputStream(new BufferedInputStream(accepted.getInputStream()))) {
                    seen.add(RpcCodec.decode(in).type());
                    seen.add(RpcCodec.decode(in).type());
                    accepted.getOutputStream().write(new byte[]{'H', 'T', 'T', 'P'});
                    accepted.getOutputStream().flush();
                } catch (Throwable failure) {
                    serverFailure.set(failure);
                }
            }, "rpc-http-corruption-fixture");
            responder.setDaemon(true);
            responder.start();

            try (RpcClient client = new RpcClient("127.0.0.1", fake.getLocalPort(), 1_000, 1_000)) {
                client.send(RpcMessage.heartbeat("w"));
                RuntimeException failure = assertThrows(RuntimeException.class,
                        () -> client.call(RpcMessage.assignStage("w", 7L, new byte[]{1})));
                String message = failure.getMessage();
                assertTrue(message.contains("remote=127.0.0.1:" + fake.getLocalPort()), message);
                assertTrue(message.contains("local=127.0.0.1:"), message);
                assertTrue(message.contains("request=ASSIGN_STAGE"), message);
                assertTrue(message.contains("op=2"), message);
                assertNotNull(failure.getCause(), "the codec cause must be preserved");
                assertTrue(failure.getCause().getMessage().contains("0x48545450"),
                        "the cause must keep the raw prefix: " + failure.getCause());
            }

            responder.join(2_000L);
            assertFalse(responder.isAlive());
            assertNull(serverFailure.get());
            assertEquals(java.util.Arrays.asList(
                    RpcMessage.Type.HEARTBEAT, RpcMessage.Type.ASSIGN_STAGE), seen);
        }
    }
}

