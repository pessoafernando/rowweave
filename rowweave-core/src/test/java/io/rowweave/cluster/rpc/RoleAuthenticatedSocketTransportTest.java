package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Stream sockets reuse the Batch control identity boundary. */
class RoleAuthenticatedSocketTransportTest {

    @Test
    void loopbackProfileUsesTheSameRoleAwareBindAndConnectPath()
            throws Exception {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        try (ServerSocket server = RoleAuthenticatedSocketTransport.bind(
                security, ControlRole.MASTER, 0, 4)) {
            CompletableFuture<Integer> received = CompletableFuture.supplyAsync(
                    () -> acceptInt(server, security));
            try (Socket client = RoleAuthenticatedSocketTransport.connect(
                    security, ControlRole.CLIENT, ControlRole.MASTER,
                    "127.0.0.1", server.getLocalPort(), 5_000, 5_000)) {
                DataOutputStream output = new DataOutputStream(
                        client.getOutputStream());
                output.writeInt(42);
                output.flush();
            }
            assertEquals(42, received.get(5, TimeUnit.SECONDS));
            assertTrue(server.getInetAddress().isLoopbackAddress());
        }
    }

    @Test
    void invalidRoleOrEndpointFailsBeforeSocketCreation() {
        ControlSecurityConfig security = ControlSecurityConfig.fromProperties(
                new Properties());
        assertThrows(IllegalArgumentException.class,
                () -> RoleAuthenticatedSocketTransport.bind(
                        security, null, 0, 4));
        assertThrows(IllegalArgumentException.class,
                () -> RoleAuthenticatedSocketTransport.connect(
                        security, ControlRole.CLIENT, ControlRole.MASTER,
                        "", 1, 1, 1));
    }

    private static int acceptInt(ServerSocket server,
            ControlSecurityConfig security) {
        try (Socket socket = server.accept()) {
            RoleAuthenticatedSocketTransport.authenticateAccepted(
                    security, ControlRole.CLIENT, socket, 5_000);
            return new DataInputStream(socket.getInputStream()).readInt();
        } catch (Exception failure) {
            throw new IllegalStateException(failure);
        }
    }
}
