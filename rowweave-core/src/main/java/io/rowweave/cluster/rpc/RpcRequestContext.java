package io.rowweave.cluster.rpc;

/**
 * Non-wire request context created by {@link RpcServer} after TLS authentication.
 * The absolute deadline starts before frame decoding and is never reset for the handler.
 */
public final class RpcRequestContext {
    private final long requestDeadlineNanos;
    private final AuthenticatedRpcPeer authenticatedPeer;
    private final NegotiatedProtocol negotiatedProtocol;
    private final ProtocolHelloV1 remoteProtocolHello;

    RpcRequestContext(long requestDeadlineNanos,
                      AuthenticatedRpcPeer authenticatedPeer,
                      NegotiatedProtocol negotiatedProtocol,
                      ProtocolHelloV1 remoteProtocolHello) {
        this.requestDeadlineNanos = requestDeadlineNanos;
        this.authenticatedPeer = authenticatedPeer;
        this.negotiatedProtocol = negotiatedProtocol;
        this.remoteProtocolHello = remoteProtocolHello;
    }

    public long requestDeadlineNanos() {
        return requestDeadlineNanos;
    }

    /** Null only for the LOOPBACK control profile. */
    public AuthenticatedRpcPeer authenticatedPeer() {
        return authenticatedPeer;
    }

    /** Null only on the pre-v1 legacy transport. */
    public NegotiatedProtocol negotiatedProtocol() {
        return negotiatedProtocol;
    }

    /** Authenticated by negotiation; null only on the pre-v1 legacy transport. */
    public ProtocolHelloV1 remoteProtocolHello() {
        return remoteProtocolHello;
    }
}
