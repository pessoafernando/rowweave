package io.rowweave.cluster.rpc;

/**
 * Factory for RPC transports, the single entry point for connecting to an endpoint. {@link
 * InProcessRpcRegistry} is consulted first, so an endpoint in the same JVM gets an in-process
 * transport; an unregistered endpoint falls back to a socket {@link RpcClient}.<p>On a cluster
 * the endpoint is not in the registry, so a socket is used and behaves exactly like a plain
 * {@code new RpcClient(host, port)}. In the local path the master and worker handlers are
 * registered in process, so traffic goes through {@link InProcessRpcTransport} without sockets
 * or serialization.
 */
public final class RpcTransports {

    private RpcTransports() {
    }

    /** Connects with the default timeout: in process on a registry hit, otherwise a socket. */
    public static RpcTransport connect(String host, int port) {
        RpcTransport inproc = InProcessRpcRegistry.lookup(host, port);
        if (inproc != null) {
            return inproc;
        }
        rejectStaleLogicalEndpoint(port);
        return new RpcClient(host, port);
    }

    /** Connects with an explicit timeout, which only a socket honours; an in-process transport has no timeout concept. */
    public static RpcTransport connect(String host, int port, int connectTimeoutMs, int readTimeoutMs) {
        RpcTransport inproc = InProcessRpcRegistry.lookup(host, port);
        if (inproc != null) {
            return ((InProcessRpcTransport) inproc).withResponseTimeout(readTimeoutMs);
        }
        rejectStaleLogicalEndpoint(port);
        return new RpcClient(host, port, connectTimeoutMs, readTimeoutMs);
    }

    /** Connect with an already verified process security identity (including deploymentSetId). */
    public static RpcTransport connect(String host, int port,
                                       ControlSecurityConfig securityConfig) {
        return connect(host, port, 5_000, 30_000, securityConfig);
    }

    public static RpcTransport connect(String host, int port, int connectTimeoutMs,
                                       int readTimeoutMs,
                                       ControlSecurityConfig securityConfig) {
        return connect(host, port, connectTimeoutMs, readTimeoutMs,
                securityConfig, null);
    }

    /** Public-v1 connection; an in-process endpoint may not bypass the hello boundary. */
    public static RpcTransport connect(String host, int port, int connectTimeoutMs,
                                       int readTimeoutMs,
                                       ControlSecurityConfig securityConfig,
                                       ProtocolHelloV1 protocolHello) {
        if (securityConfig == null) {
            throw new IllegalArgumentException("securityConfig must not be null");
        }
        RpcTransport inproc = InProcessRpcRegistry.lookup(host, port);
        if (inproc == null) {
            rejectStaleLogicalEndpoint(port);
        }
        if (protocolHello != null && inproc != null) {
            throw new ProtocolNegotiationException(
                    "IN_PROCESS_NEGOTIATION_REQUIRED",
                    "public-v1 transport cannot bypass hello negotiation");
        }
        RpcTransport delegate = inproc == null
                ? new RpcClient(host, port, connectTimeoutMs, readTimeoutMs,
                        securityConfig, ControlResidentBudget.processWide(),
                        protocolHello)
                : ((InProcessRpcTransport) inproc).withResponseTimeout(readTimeoutMs);
        return new SecurityBoundTransport(delegate, securityConfig.securityMetadata(),
                securityConfig.profile() == ControlSecurityProfile.MTLS);
    }

    private static void rejectStaleLogicalEndpoint(int port) {
        if (InProcessRpcRegistry.isLogicalPort(port)) {
            throw new IllegalStateException("in-process RPC endpoint is closed");
        }
    }

    private static final class SecurityBoundTransport implements RpcTransport {
        private final RpcTransport delegate;
        private final RpcSecurityMetadata identity;
        private final boolean required;

        private SecurityBoundTransport(RpcTransport delegate, RpcSecurityMetadata identity,
                                       boolean required) {
            this.delegate = delegate;
            this.identity = identity;
            this.required = required;
        }

        @Override
        public RpcMessage call(RpcMessage request) {
            return callScoped(request, response -> {
                if (response.hasRetainedBinaryPayload()) {
                    throw new IllegalStateException(
                            "binary RPC response requires callScoped consumption: "
                                    + response.type());
                }
                return response;
            });
        }

        @Override
        public <T> T callScoped(RpcMessage request,
                                java.util.function.Function<? super RpcMessage, ? extends T> consumer) {
            RpcMessage secured = RpcSecurityFence.outbound(request, identity, required);
            return delegate.callScoped(secured, response -> consumer.apply(
                    RpcSecurityFence.inbound(response, identity, required)));
        }

        @Override
        public void send(RpcMessage request) {
            delegate.send(RpcSecurityFence.outbound(request, identity, required));
        }

        @Override
        public void close() {
            delegate.close();
        }
    }
}
