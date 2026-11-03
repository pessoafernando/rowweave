package io.rowweave.cluster.rpc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.function.Function;

/**
 * Minimal RPC client that connects to a host and port: {@link #call} sends one {@link
 * RpcMessage} and blocks for the response. A long-lived connection can carry any number of
 * calls over the same socket. The connect and read timeouts fail fast rather than hanging.
 */
public final class RpcClient implements RpcTransport {

    private static final int CONNECT_TIMEOUT_MS = 5_000;
    private static final int READ_TIMEOUT_MS = 30_000;

    private final Socket socket;
    private final DataInputStream in;
    private final OutputStream out;
    private final String remoteEndpoint;
    private final String localEndpoint;
    private final RpcAdmissionConfig admission;
    private final ControlResidentBudget residentBudget;
    private final ControlSecurityProfile securityProfile;
    private final java.util.EnumSet<ControlRole> peerRoles;
    private final RpcSecurityMetadata localSecurityMetadata;
    private final NegotiatedProtocol negotiatedProtocol;
    private long operationSequence;

    public RpcClient(String host, int port) {
        this(host, port, CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS,
                ControlSecurityConfig.fromSystemProperties());
    }

    public RpcClient(String host, int port, int connectTimeoutMs, int readTimeoutMs) {
        this(host, port, connectTimeoutMs, readTimeoutMs,
                ControlSecurityConfig.fromSystemProperties());
    }

    public static RpcClient negotiated(
            String host, int port, ProtocolHelloV1 localHello) {
        return new RpcClient(host, port, CONNECT_TIMEOUT_MS, READ_TIMEOUT_MS,
                ControlSecurityConfig.fromSystemProperties(),
                ControlResidentBudget.processWide(), localHello);
    }

    /** Validates and pins the target before a socket is created; a secure endpoint must never fall back to plaintext. */
    RpcClient(String host, int port, int connectTimeoutMs, int readTimeoutMs,
              ControlSecurityConfig securityConfig) {
        this(host, port, connectTimeoutMs, readTimeoutMs, securityConfig,
                ControlResidentBudget.processWide());
    }

    RpcClient(String host, int port, int connectTimeoutMs, int readTimeoutMs,
              ControlSecurityConfig securityConfig, ControlResidentBudget residentBudget) {
        this(host, port, connectTimeoutMs, readTimeoutMs, securityConfig,
                residentBudget, null);
    }

    RpcClient(String host, int port, int connectTimeoutMs, int readTimeoutMs,
              ControlSecurityConfig securityConfig, ControlResidentBudget residentBudget,
              ProtocolHelloV1 localHello) {
        if (securityConfig == null) {
            throw new IllegalArgumentException("securityConfig must not be null");
        }
        if (residentBudget == null) {
            throw new IllegalArgumentException("residentBudget must not be null");
        }
        securityConfig.requireImplementedTransport();
        this.admission = RpcAdmissionConfig.fromSystemProperties();
        this.residentBudget = residentBudget;
        this.securityProfile = securityConfig.profile();
        this.localSecurityMetadata = securityConfig.securityMetadata();
        InetAddress target = ControlEndpointPolicy.resolveTarget(securityConfig.profile(), host);
        Socket connected = null;
        try {
            connected = securityProfile == ControlSecurityProfile.MTLS
                    ? securityConfig.tlsContext().newClientSocket() : new Socket();
            connected.connect(new InetSocketAddress(target, port), connectTimeoutMs);
            connected.setSoTimeout(readTimeoutMs);  // fail fast on the read timeout
            if (securityProfile == ControlSecurityProfile.MTLS) {
                javax.net.ssl.SSLSocket tls = (javax.net.ssl.SSLSocket) connected;
                tls.startHandshake();
                this.peerRoles = securityConfig.tlsContext().peerRoles(tls);
            } else {
                this.peerRoles = null;
            }
            // Control plane RPC is a synchronous round trip of small messages: stage assignment, port
            // report and stage report. Nagle on by default with a delayed acknowledgement downstream costs
            // about 40ms per round trip, and a cluster submit makes dozens of them, which adds up to a
            // fixed 1.5 to 2.8 second overhead for assign plus barrier. Nagle is off here, matching the
            // TCP_NODELAY of the Netty data plane.
            connected.setTcpNoDelay(true);
            DataInputStream connectedInput = new DataInputStream(
                    new BufferedInputStream(connected.getInputStream()));
            OutputStream connectedOutput = new BufferedOutputStream(
                    connected.getOutputStream());
            NegotiatedProtocol protocol = null;
            if (localHello != null) {
                ProtocolHelloV1Codec.encodeTo(localHello, connectedOutput);
                connectedOutput.flush();
                ProtocolHelloV1 remoteHello = ProtocolHelloV1Codec.decodeFrom(
                        connectedInput);
                protocol = ProtocolNegotiator.negotiate(localHello, remoteHello);
            }
            this.socket = connected;
            this.in = connectedInput;
            this.out = connectedOutput;
            this.negotiatedProtocol = protocol;
            this.remoteEndpoint = host + ":" + port;
            this.localEndpoint = connected.getLocalAddress().getHostAddress() + ":" + connected.getLocalPort();
        } catch (IOException | RuntimeException e) {
            if (connected != null) {
                try {
                    connected.close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw new RuntimeException("RPC connect failed to " + host + ":" + port, e);
        }
    }

    /** Sends the request and blocks for the response, throwing on a read timeout or an unexpectedly closed response stream. */
    public synchronized RpcMessage call(RpcMessage request) {
        return callScoped(request, response -> {
            if (response.hasRetainedBinaryPayload()) {
                throw new IllegalStateException(
                        "binary RPC response requires callScoped consumption: " + response.type());
            }
            return response;
        });
    }

    @Override
    public synchronized <T> T callScoped(RpcMessage request,
                                         Function<? super RpcMessage, ? extends T> consumer) {
        if (consumer == null) throw new IllegalArgumentException("response consumer must not be null");
        long operation = ++operationSequence;
        ControlResidentLease lease = openResidentLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            RpcMessage securedRequest = RpcSecurityFence.outbound(request, localSecurityMetadata,
                    securityProfile == ControlSecurityProfile.MTLS);
            requireAuthorizedRemote(securedRequest);
            lease.reserve(owner, securedRequest.encodeResidentBytes());
            if (negotiatedProtocol == null) {
                RpcCodec.encodeTo(securedRequest, out, admission);
            } else {
                RpcCodec.encodeToV1(
                        securedRequest, out, admission, negotiatedProtocol);
            }
            out.flush();
            lease.release(owner);
            lease = openResidentLease();
            owner = lease.ownerToken();
            RpcMessage resp = negotiatedProtocol == null
                    ? RpcCodec.decode(in, lease, owner)
                    : RpcCodec.decodeV1(in, negotiatedProtocol, lease, owner);
            if (resp == null) {
                throw new java.io.EOFException("RPC connection closed before response");
            }
            resp = RpcSecurityFence.inbound(resp, localSecurityMetadata,
                    securityProfile == ControlSecurityProfile.MTLS);
            return consumer.apply(resp);
        } catch (ProtocolNegotiationException rejected) {
            throw rejected;
        } catch (IOException | RuntimeException failure) {
            throw operationFailure("RPC call failed", request, operation, failure);
        } finally {
            lease.release(owner);
        }
    }

    /** One-way notification with no response awaited, such as a heartbeat. */
    public synchronized void send(RpcMessage request) {
        long operation = ++operationSequence;
        ControlResidentLease lease = openResidentLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            RpcMessage securedRequest = RpcSecurityFence.outbound(request, localSecurityMetadata,
                    securityProfile == ControlSecurityProfile.MTLS);
            requireAuthorizedRemote(securedRequest);
            lease.reserve(owner, securedRequest.encodeResidentBytes());
            if (negotiatedProtocol == null) {
                RpcCodec.encodeTo(securedRequest, out, admission);
            } else {
                RpcCodec.encodeToV1(
                        securedRequest, out, admission, negotiatedProtocol);
            }
            out.flush();
        } catch (ProtocolNegotiationException rejected) {
            throw rejected;
        } catch (IOException | RuntimeException failure) {
            throw operationFailure("RPC send failed", request, operation, failure);
        } finally {
            lease.release(owner);
        }
    }

    private RuntimeException operationFailure(String summary, RpcMessage request, long operation,
                                              Throwable cause) {
        return new RuntimeException(summary + " remote=" + remoteEndpoint + " local=" + localEndpoint
                + " request=" + (request == null ? "null" : request.type()) + " op=" + operation, cause);
    }

    private void requireAuthorizedRemote(RpcMessage request) throws IOException {
        if (securityProfile == ControlSecurityProfile.MTLS) {
            ControlRequestAuthorization.requireRemoteEndpoint(
                    peerRoles, request == null ? null : request.type());
        }
    }

    private ControlResidentLease openResidentLease() {
        return negotiatedProtocol == null
                ? residentBudget.openLease()
                : residentBudget.openLease(negotiatedProtocol.maximumResidentBytes());
    }

    String negotiatedTlsProtocol() {
        return socket instanceof javax.net.ssl.SSLSocket
                ? ((javax.net.ssl.SSLSocket) socket).getSession().getProtocol() : null;
    }

    NegotiatedProtocol negotiatedProtocol() { return negotiatedProtocol; }

    String negotiatedTlsCipherSuite() {
        return socket instanceof javax.net.ssl.SSLSocket
                ? ((javax.net.ssl.SSLSocket) socket).getSession().getCipherSuite() : null;
    }

    @Override
    public void close() {
        try {
            socket.close();
        } catch (IOException ignored) {
            // best-effort
        }
    }
}
