package io.rowweave.cluster.rpc;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.function.Function;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Minimal RPC server that listens on a ServerSocket with one thread per connection, reading a
 * {@link RpcMessage}, handing it to the {@code handler} and writing the response back. One
 * connection serves any number of request-response pairs. The handler runs on the connection
 * thread, separate from the stage execution threads, so heartbeats and registrations are never
 * blocked by a parked stage.<p>Shutdown: {@link #close()} closes the ServerSocket, which makes
 * accept throw and the accept thread exit, and it also closes every live connection socket. A
 * peer blocked reading a response on one of those connections, such as a client waiting for a
 * job result, then sees EOF or a reset right away instead of waiting out its socket timeout.
 * The threads are daemon threads.
 */
public final class RpcServer implements AutoCloseable {

    @FunctionalInterface
    public interface Handler {
        RpcMessage handle(RpcMessage request, RpcRequestContext context);
    }

    /** Compile-time test seam; production always uses {@link #DEFAULT_THREAD_STARTER}. */
    interface ThreadStarter {
        Thread create(Runnable target, String name);

        void start(Thread thread);
    }

    private enum Lifecycle {
        DORMANT,
        ACTIVE,
        CLOSED
    }

    private static final ThreadStarter DEFAULT_THREAD_STARTER = new ThreadStarter() {
        @Override
        public Thread create(Runnable target, String name) {
            return new Thread(target, name);
        }

        @Override
        public void start(Thread thread) {
            thread.start();
        }
    };

    private final ServerSocket serverSocket;
    private final Handler handler;
    private final ControlSecurityProfile securityProfile;
    private final ControlTlsContext tlsContext;
    private final RpcAdmissionConfig admission;
    private final RpcRequestPolicy requestPolicy;
    private final RpcFrameBudget frameBudget;
    private final ControlResidentBudget residentBudget;
    private final RpcSecurityMetadata localSecurityMetadata;
    private final ThreadStarter threadStarter;
    private final AtomicInteger connectionThreadStarts = new AtomicInteger();
    private ProtocolHelloV1 localProtocolHello;
    private final Object lifecycleLock = new Object();
    private volatile Thread acceptThread;
    private volatile boolean running;
    private Lifecycle lifecycle = Lifecycle.DORMANT;
    // Tracks every accepted socket, linearizing registration against close. Otherwise a peer could miss the
    // FIN or reset when the server shuts down and wait out its socket timeout, or a socket returned by accept
    // but not yet registered could slip past close and end up unmanaged.
    private final RpcConnectionRegistry connections = new RpcConnectionRegistry();

    public RpcServer(Function<RpcMessage, RpcMessage> handler) {
        this(handler, 0, ControlSecurityConfig.fromSystemProperties());
    }

    /**
     * port 0 picks a random port, as before; a port greater than zero binds exactly that, so
     * separate processes can know it in advance.
     */
    public RpcServer(Function<RpcMessage, RpcMessage> handler, int port) {
        this(handler, port, ControlSecurityConfig.fromSystemProperties());
    }

    public static RpcServer negotiated(
            Function<RpcMessage, RpcMessage> handler,
            ProtocolHelloV1 localHello) {
        return negotiated(adapt(handler), localHello);
    }

    public static RpcServer negotiated(
            Handler handler, ProtocolHelloV1 localHello) {
        if (localHello == null) {
            throw new IllegalArgumentException("local protocol hello is required");
        }
        RpcServer server = new RpcServer(handler, true, 0,
                ControlSecurityConfig.fromSystemProperties(),
                RpcAdmissionConfig.fromSystemProperties(), RpcRequestPolicy.GENERAL,
                ControlResidentBudget.processWide(), false, DEFAULT_THREAD_STARTER);
        server.localProtocolHello = localHello;
        server.activate();
        return server;
    }

    /**
     * An explicit security configuration is fully validated before any socket or thread is
     * created.
     */
    public RpcServer(Function<RpcMessage, RpcMessage> handler, int port,
                     ControlSecurityConfig securityConfig) {
        this(handler, port, securityConfig, RpcAdmissionConfig.fromSystemProperties(),
                RpcRequestPolicy.GENERAL, ControlResidentBudget.processWide());
    }

    public RpcServer(Function<RpcMessage, RpcMessage> handler, int port,
                     ControlSecurityConfig securityConfig, RpcAdmissionConfig admission,
                     RpcRequestPolicy requestPolicy) {
        this(handler, port, securityConfig, admission, requestPolicy,
                ControlResidentBudget.processWide());
    }

    RpcServer(Function<RpcMessage, RpcMessage> handler, int port,
              ControlSecurityConfig securityConfig, RpcAdmissionConfig admission,
              RpcRequestPolicy requestPolicy, ControlResidentBudget residentBudget) {
        this(adapt(handler), false, port, securityConfig, admission, requestPolicy,
                residentBudget, true, DEFAULT_THREAD_STARTER);
    }

    public RpcServer(Handler handler, int port,
                     ControlSecurityConfig securityConfig, RpcAdmissionConfig admission,
                     RpcRequestPolicy requestPolicy) {
        this(handler, true, port, securityConfig, admission, requestPolicy,
                ControlResidentBudget.processWide(), true, DEFAULT_THREAD_STARTER);
    }

    RpcServer(Handler handler, int port,
              ControlSecurityConfig securityConfig, RpcAdmissionConfig admission,
              RpcRequestPolicy requestPolicy, ControlResidentBudget residentBudget) {
        this(handler, true, port, securityConfig, admission, requestPolicy, residentBudget,
                true, DEFAULT_THREAD_STARTER);
    }

    private RpcServer(Handler handler, boolean contextAware, int port,
                      ControlSecurityConfig securityConfig, RpcAdmissionConfig admission,
                      RpcRequestPolicy requestPolicy, ControlResidentBudget residentBudget,
                      boolean autoActivate, ThreadStarter threadStarter) {
        if (handler == null || securityConfig == null || admission == null || requestPolicy == null) {
            throw new IllegalArgumentException("RPC server dependencies must not be null");
        }
        if (residentBudget == null) throw new IllegalArgumentException("resident budget must not be null");
        if (threadStarter == null) throw new IllegalArgumentException("thread starter must not be null");
        if (!contextAware
                && securityConfig.profile() == ControlSecurityProfile.MTLS
                && requestPolicy.allows(RpcMessage.Type.REGISTER_WORKER)) {
            throw new IllegalArgumentException(
                    "MTLS REGISTER_WORKER endpoint requires a context-aware handler");
        }
        securityConfig.requireImplementedTransport();
        this.handler = handler;
        this.securityProfile = securityConfig.profile();
        this.tlsContext = securityConfig.tlsContext();
        this.admission = admission;
        this.requestPolicy = requestPolicy;
        this.frameBudget = new RpcFrameBudget(admission.inFlightFrameBytes());
        this.residentBudget = residentBudget;
        this.localSecurityMetadata = securityConfig.securityMetadata();
        this.threadStarter = threadStarter;
        ServerSocket bound = null;
        try {
            bound = securityProfile == ControlSecurityProfile.MTLS
                    ? tlsContext.newServerSocket() : new ServerSocket();
            // macOS permits a wildcard listener with SO_REUSEADDR to alias an existing
            // loopback-only listener. A random RPC bind can then publish the aliased port
            // while 127.0.0.1 connections reach the unrelated service instead.
            bound.setReuseAddress(false);
            bound.bind(new InetSocketAddress(securityConfig.bindAddress(), port), admission.listenBacklog());
        } catch (Throwable failure) {
            if (bound != null) {
                try {
                    bound.close();
                } catch (Throwable closeFailure) {
                    if (closeFailure != failure) failure.addSuppressed(closeFailure);
                }
            }
            if (failure instanceof IOException) {
                throw new RuntimeException("failed to bind RPC server on "
                        + securityConfig.bindAddress().getHostAddress() + ":" + port, failure);
            }
            rethrow(failure);
            throw new AssertionError("unreachable");
        }
        this.serverSocket = bound;
        if (autoActivate) {
            activate();
        }
    }

    /**
     * Bind the endpoint without creating an accept thread. Role constructors use this boundary so
     * a handler capturing the role cannot observe a partially constructed object.
     */
    public static RpcServer bindDormant(Function<RpcMessage, RpcMessage> handler, int port,
                                        ControlSecurityConfig securityConfig,
                                        RpcAdmissionConfig admission,
                                        RpcRequestPolicy requestPolicy) {
        return new RpcServer(adapt(handler), false, port, securityConfig, admission, requestPolicy,
                ControlResidentBudget.processWide(), false, DEFAULT_THREAD_STARTER);
    }

    /** Context-aware dormant endpoint for Master/Worker role construction. */
    public static RpcServer bindDormant(Handler handler, int port,
                                        ControlSecurityConfig securityConfig,
                                        RpcAdmissionConfig admission,
                                        RpcRequestPolicy requestPolicy) {
        return new RpcServer(handler, true, port, securityConfig, admission, requestPolicy,
                ControlResidentBudget.processWide(), false, DEFAULT_THREAD_STARTER);
    }

    /**
     * Context-aware role endpoint with optional public-v1 negotiation. A null hello preserves the
     * legacy framing used by tests and compatibility constructors; otherwise negotiation completes
     * before role activation dispatch.
     */
    public static RpcServer bindDormant(Handler handler, int port,
                                        ControlSecurityConfig securityConfig,
                                        RpcAdmissionConfig admission,
                                        RpcRequestPolicy requestPolicy,
                                        ProtocolHelloV1 localHello) {
        RpcServer server = new RpcServer(handler, true, port, securityConfig, admission,
                requestPolicy, ControlResidentBudget.processWide(), false,
                DEFAULT_THREAD_STARTER);
        server.localProtocolHello = localHello;
        return server;
    }

    static RpcServer bindDormant(Function<RpcMessage, RpcMessage> handler, int port,
                                 ControlSecurityConfig securityConfig, RpcAdmissionConfig admission,
                                 RpcRequestPolicy requestPolicy, ControlResidentBudget residentBudget,
                                 ThreadStarter threadStarter) {
        return new RpcServer(adapt(handler), false, port, securityConfig, admission, requestPolicy,
                residentBudget, false, threadStarter);
    }

    /** One-shot transition from a bound endpoint to an externally active endpoint. */
    public void activate() {
        Throwable activationFailure = null;
        synchronized (lifecycleLock) {
            if (lifecycle != Lifecycle.DORMANT) {
                throw new IllegalStateException("RPC server is not dormant: " + lifecycle);
            }
            try {
                Thread thread = threadStarter.create(this::acceptLoop, "rowweave-rpc-accept");
                if (thread == null) {
                    throw new IllegalStateException("RPC accept thread starter returned null");
                }
                thread.setDaemon(true);
                acceptThread = thread;
                running = true;
                lifecycle = Lifecycle.ACTIVE;
                threadStarter.start(thread);
                return;
            } catch (Throwable failure) {
                running = false;
                lifecycle = Lifecycle.CLOSED;
                activationFailure = failure;
            }
        }
        closeAfterActivationFailure(activationFailure);
        rethrow(activationFailure);
    }

    public int port() {
        return serverSocket.getLocalPort();
    }

    /**
     * For tests and diagnostics: the address actually listened on, not a guess from the
     * configuration.
     */
    InetAddress boundAddress() {
        return serverSocket.getInetAddress();
    }

    private void acceptLoop() {
        while (running) {
            Socket socket;
            try {
                socket = serverSocket.accept();
            } catch (IOException e) {
                return;  // close() shuts the ServerSocket, accept throws, and the thread exits cleanly
            }
            if (securityProfile == ControlSecurityProfile.LOOPBACK
                    && !socket.getInetAddress().isLoopbackAddress()) {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // Best effort on the rejection path; a peer that is not loopback must not reach the handler.
                }
                continue;
            }
            // Take ownership after accept and before handing off to the handler; if close is already linearized, registration closes the late socket.
            if (!connections.register(socket, admission.maxConnections())) {
                if (connections.isClosed()) return;
                continue;
            }
            try {
                socket.setTcpNoDelay(true);  // turns Nagle off, so the reply leg of a small control message does not eat a 40ms delayed-ack stall; matches RpcClient
            } catch (IOException optionFailure) {
                connections.unregister(socket);
                try {
                    socket.close();
                } catch (IOException closeFailure) {
                    optionFailure.addSuppressed(closeFailure);
                }
                if (!running) {
                    return;
                }
                continue;
            }
            Thread conn = new Thread(() -> handleConnection(socket), "rowweave-rpc-conn");
            conn.setDaemon(true);
            try {
                connectionThreadStarts.incrementAndGet();
                conn.start();
            } catch (RuntimeException | Error startFailure) {
                connections.unregister(socket);
                try {
                    socket.close();
                } catch (IOException closeFailure) {
                    startFailure.addSuppressed(closeFailure);
                }
                throw startFailure;
            }
        }
    }

    private void handleConnection(Socket socket) {
        try (Socket s = socket) {
            java.util.EnumSet<ControlRole> peerRoles = null;
            AuthenticatedRpcPeer authenticatedPeer = null;
            if (securityProfile == ControlSecurityProfile.MTLS) {
                javax.net.ssl.SSLSocket tls = (javax.net.ssl.SSLSocket) s;
                tls.setSoTimeout((int) admission.frameDeadlineMillis());
                tls.startHandshake();
                authenticatedPeer = tlsContext.authenticatedPeer(tls);
                peerRoles = authenticatedPeer.roles();
            }
            FrameDeadlineInputStream deadlineInput = new FrameDeadlineInputStream(
                    s, s.getInputStream(), admission.frameDeadlineMillis());
            DataInputStream in = new DataInputStream(new BufferedInputStream(deadlineInput));
            OutputStream rawOut = new BufferedOutputStream(s.getOutputStream());
            NegotiatedProtocol negotiated = null;
            ProtocolHelloV1 remoteProtocolHello = null;
            if (localProtocolHello != null) {
                deadlineInput.beginFrame();
                ProtocolHelloV1 remoteHello = ProtocolHelloV1Codec.decodeFrom(in);
                negotiated = ProtocolNegotiator.negotiate(
                        localProtocolHello, remoteHello);
                remoteProtocolHello = remoteHello;
                ProtocolHelloV1Codec.encodeTo(localProtocolHello, rawOut);
                rawOut.flush();
            }
            while (true) {
                deadlineInput.beginFrame();
                RpcCodec.InboundFrame frame;
                try {
                    frame = negotiated == null
                            ? RpcCodec.decodeRequest(in, admission, frameBudget,
                                    requestPolicy, residentBudget, peerRoles)
                            : RpcCodec.decodeRequestV1(in, admission, frameBudget,
                                    requestPolicy, residentBudget, peerRoles,
                                    negotiated);
                } catch (ControlResidentBudgetExceededException exhausted) {
                    writeBudgetRejection(rawOut, exhausted);
                    return;
                }
                if (frame == null) return;
                try (RpcCodec.InboundFrame owned = frame) {
                    RpcMessage request = RpcSecurityFence.inbound(
                            owned.message(), localSecurityMetadata,
                            securityProfile == ControlSecurityProfile.MTLS);
                    ControlResidentLease responseLease = negotiated == null
                            ? residentBudget.openLease()
                            : residentBudget.openLease(
                                    negotiated.maximumResidentBytes());
                    ControlResidentLease.OwnerToken responseOwner = responseLease.ownerToken();
                    try {
                        request.attachResponseResidentLease(responseLease, responseOwner);
                        RpcMessage resp;
                        try {
                            resp = handler.handle(request, new RpcRequestContext(
                                    deadlineInput.frameDeadlineNanos(), authenticatedPeer,
                                    negotiated, remoteProtocolHello));
                        } catch (ControlResidentBudgetExceededException exhausted) {
                            writeBudgetRejection(rawOut, exhausted);
                            continue;
                        }
                        if (resp != null) {
                            try {
                                resp = RpcSecurityFence.outbound(resp, localSecurityMetadata,
                                        securityProfile == ControlSecurityProfile.MTLS);
                                long encodeResident = resp.encodeResidentBytes();
                                long producerReserved = request.responseResidentReservedBytes();
                                if (encodeResident > producerReserved) {
                                    responseLease.reserve(responseOwner,
                                            encodeResident - producerReserved);
                                }
                                if (negotiated == null) {
                                    RpcCodec.encodeTo(resp, rawOut, admission);
                                } else {
                                    RpcCodec.encodeToV1(
                                            resp, rawOut, admission, negotiated);
                                }
                                rawOut.flush();
                            } catch (ControlResidentBudgetExceededException exhausted) {
                                writeBudgetRejection(rawOut, exhausted);
                            }
                        }
                    } finally {
                        responseLease.release(responseOwner);
                    }
                }
            }
        } catch (ProtocolNegotiationException rejectedProtocol) {
            // Public-v1 negotiation failed before business dispatch: close the connection.
        } catch (SecurityException rejectedIdentity) {
            // Authenticated peer belongs to another deployment/authz set: close this connection.
        } catch (IOException e) {
            // A dropped or corrupt connection ends this connection thread; fail-fast on the control plane is left to heartbeat and job result handling.
        } finally {
            connections.unregister(socket);
        }
    }

    /**
     * Admission rejection is the bounded emergency path: the exhausted request/response lease cannot
     * carry its own error frame. The codec's normal-frame cap still bounds this fixed-shape response.
     */
    private void writeBudgetRejection(OutputStream out,
                                      ControlResidentBudgetExceededException exhausted)
            throws IOException {
        RpcMessage response = RpcSecurityFence.outbound(
                RpcMessage.jobResult(false, 0, exhausted.getMessage()), localSecurityMetadata,
                securityProfile == ControlSecurityProfile.MTLS);
        RpcCodec.encodeTo(response, out, admission);
        out.flush();
    }

    @Override
    public void close() {
        synchronized (lifecycleLock) {
            if (lifecycle == Lifecycle.CLOSED) {
                return;
            }
            lifecycle = Lifecycle.CLOSED;
            running = false;
        }
        try {
            serverSocket.close();
        } catch (IOException ignored) {
            // best-effort
        }
        // The registry linearizes against a late register, so once close returns no accepted socket is unmanaged.
        RuntimeException closeFailure = null;
        try {
            connections.close();
        } catch (RuntimeException failure) {
            closeFailure = failure;
        }
        joinAcceptThread();
        if (closeFailure != null) {
            throw closeFailure;
        }
    }

    private void joinAcceptThread() {
        Thread thread = acceptThread;
        if (thread == null || Thread.currentThread() == thread) {
            return;
        }
        try {
            thread.join(5_000L);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        if (thread.isAlive()) {
            thread.interrupt();
        }
    }

    private void closeAfterActivationFailure(Throwable primary) {
        try {
            serverSocket.close();
        } catch (Throwable closeFailure) {
            primary.addSuppressed(closeFailure);
        }
        try {
            connections.close();
        } catch (Throwable closeFailure) {
            primary.addSuppressed(closeFailure);
        }
        try {
            joinAcceptThread();
        } catch (Throwable closeFailure) {
            primary.addSuppressed(closeFailure);
        }
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    /**
     * For test observability: accepted sockets currently held by the server.
     */
    int liveConnectionCount() {
        return connections.size();
    }

    /**
     * For test observability: the assertion that close converges the accept loop.
     */
    boolean acceptThreadAlive() {
        Thread thread = acceptThread;
        return thread != null && thread.isAlive();
    }

    int connectionThreadStartCount() { return connectionThreadStarts.get(); }

    boolean isClosedForDiagnostics() {
        synchronized (lifecycleLock) {
            return lifecycle == Lifecycle.CLOSED;
        }
    }

    long inFlightFrameBytes() { return frameBudget.used(); }

    long residentBytes() { return residentBudget.snapshot().usedBytes(); }

    private static Handler adapt(Function<RpcMessage, RpcMessage> handler) {
        if (handler == null) {
            throw new IllegalArgumentException("RPC server handler must not be null");
        }
        return (request, context) -> handler.apply(request);
    }
}
