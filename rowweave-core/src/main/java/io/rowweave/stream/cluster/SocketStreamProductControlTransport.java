package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamControlException;
import io.rowweave.cluster.rpc.ControlRole;
import io.rowweave.cluster.rpc.ControlSecurityConfig;
import io.rowweave.cluster.rpc.RoleAuthenticatedSocketTransport;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

/** Bounded request/response transport for a remote Stream product Master. */
final class SocketStreamProductControlTransport
        implements StreamProductControlTransport {
    private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
    private static final int READ_TIMEOUT_MILLIS = 30_000;

    private final String masterHost;
    private final int masterPort;
    private final ControlSecurityConfig security;

    SocketStreamProductControlTransport(String masterHost, int masterPort) {
        this(masterHost, masterPort, loopbackSecurity());
    }

    SocketStreamProductControlTransport(String masterHost, int masterPort,
            ControlSecurityConfig security) {
        if (masterHost == null || masterHost.trim().isEmpty()
                || masterPort <= 0 || masterPort > 65_535
                || security == null) {
            throw new IllegalArgumentException(
                    "Stream Master host and port are required");
        }
        this.masterHost = masterHost;
        this.masterPort = masterPort;
        this.security = security;
    }

    @Override
    public StreamProductJobView call(
            StreamProductControlWire.Request request) {
        try (Socket socket = RoleAuthenticatedSocketTransport.connect(
                security, ControlRole.CLIENT, ControlRole.MASTER,
                masterHost, masterPort, CONNECT_TIMEOUT_MILLIS,
                READ_TIMEOUT_MILLIS)) {
            DataOutputStream output = new DataOutputStream(
                    new BufferedOutputStream(socket.getOutputStream()));
            DataInputStream input = new DataInputStream(
                    new BufferedInputStream(socket.getInputStream()));
            StreamProductControlWire.writeRequest(output, request);
            StreamProductControlWire.Response response =
                    StreamProductControlWire.readResponse(input);
            return requireSuccess(response);
        } catch (StreamControlException failure) {
            throw failure;
        } catch (IOException | RuntimeException failure) {
            throw new StreamControlException(
                    StreamControlException.Code.TRANSPORT_FAILURE,
                    "Cluster Stream control RPC failed", failure);
        }
    }

    private static ControlSecurityConfig loopbackSecurity() {
        return ControlSecurityConfig.fromProperties(new Properties());
    }

    static StreamProductJobView requireSuccess(
            StreamProductControlWire.Response response) {
        if (!response.success()) {
            throw new StreamControlException(
                    response.errorCode, response.errorMessage);
        }
        return response.view;
    }
}
