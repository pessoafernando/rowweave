package io.rowweave.cluster.rpc;

import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.EnumSet;

/**
 * Role-authenticated socket boundary shared by non-RPC framed protocols.
 *
 * <p>LOOPBACK remains plaintext but is pinned to loopback addresses. MTLS uses
 * the same certificates, cipher policy, hostname verification and authz roles
 * as the Batch control plane; there is no plaintext fallback.
 */
public final class RoleAuthenticatedSocketTransport {
    private RoleAuthenticatedSocketTransport() { }

    public static ServerSocket bind(ControlSecurityConfig security,
            ControlRole localRole, int port, int backlog) {
        requireSecurityAndRole(security, localRole);
        if (port < 0 || port > 65_535 || backlog < 1) {
            throw new IllegalArgumentException(
                    "control socket bind parameters are invalid");
        }
        ServerSocket server = null;
        try {
            server = security.profile() == ControlSecurityProfile.MTLS
                    ? security.tlsContext().newServerSocket()
                    : new ServerSocket();
            server.setReuseAddress(false);
            server.bind(new InetSocketAddress(
                    security.bindAddress(), port), backlog);
            return server;
        } catch (Throwable failure) {
            if (server != null) {
                try {
                    server.close();
                } catch (Throwable closeFailure) {
                    if (closeFailure != failure) {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            }
            if (failure instanceof Error) throw (Error) failure;
            throw new IllegalStateException(
                    "failed to bind authenticated control socket", failure);
        }
    }

    public static Socket connect(ControlSecurityConfig security,
            ControlRole localRole, ControlRole expectedRemoteRole,
            String host, int port, int connectTimeoutMillis,
            int readTimeoutMillis) throws IOException {
        requireSecurityAndRole(security, localRole);
        if (expectedRemoteRole == null || host == null
                || host.trim().isEmpty() || port < 1 || port > 65_535
                || connectTimeoutMillis < 1 || readTimeoutMillis < 1) {
            throw new IllegalArgumentException(
                    "authenticated control endpoint is invalid");
        }
        InetAddress target = ControlEndpointPolicy.resolveTarget(
                security.profile(), host);
        Socket socket = null;
        try {
            socket = security.profile() == ControlSecurityProfile.MTLS
                    ? security.tlsContext().newClientSocket()
                    : new Socket();
            socket.connect(new InetSocketAddress(target, port),
                    connectTimeoutMillis);
            socket.setSoTimeout(readTimeoutMillis);
            socket.setTcpNoDelay(true);
            if (security.profile() == ControlSecurityProfile.MTLS) {
                SSLSocket tls = (SSLSocket) socket;
                tls.startHandshake();
                requirePeerRole(security.tlsContext().peerRoles(tls),
                        expectedRemoteRole);
            } else if (!socket.getInetAddress().isLoopbackAddress()) {
                throw new IOException(
                        "LOOPBACK control transport reached non-loopback peer");
            }
            return socket;
        } catch (IOException | RuntimeException failure) {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    public static void authenticateAccepted(ControlSecurityConfig security,
            ControlRole expectedRemoteRole, Socket socket,
            int handshakeTimeoutMillis) throws IOException {
        if (security == null || expectedRemoteRole == null || socket == null
                || handshakeTimeoutMillis < 1) {
            throw new IllegalArgumentException(
                    "accepted control socket authentication is invalid");
        }
        socket.setSoTimeout(handshakeTimeoutMillis);
        socket.setTcpNoDelay(true);
        if (security.profile() == ControlSecurityProfile.MTLS) {
            if (!(socket instanceof SSLSocket)) {
                throw new IOException("MTLS listener accepted plaintext socket");
            }
            SSLSocket tls = (SSLSocket) socket;
            tls.startHandshake();
            requirePeerRole(security.tlsContext().peerRoles(tls),
                    expectedRemoteRole);
        } else if (!socket.getInetAddress().isLoopbackAddress()) {
            throw new IOException(
                    "LOOPBACK listener rejected non-loopback peer");
        }
    }

    private static void requireSecurityAndRole(
            ControlSecurityConfig security, ControlRole localRole) {
        if (security == null || localRole == null) {
            throw new IllegalArgumentException(
                    "control security and local role are required");
        }
        security.requireImplementedTransport();
        security.requireLocalRole(localRole);
    }

    private static void requirePeerRole(EnumSet<ControlRole> roles,
            ControlRole expected) throws IOException {
        if (roles == null || !roles.contains(expected)) {
            throw new IOException(
                    "mTLS peer lacks required " + expected + " role");
        }
    }
}
