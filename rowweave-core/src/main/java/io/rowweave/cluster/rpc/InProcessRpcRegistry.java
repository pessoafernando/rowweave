package io.rowweave.cluster.rpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Registry of in-process RPC endpoints. The local path registers the handlers of the master and
 * worker servers living in the same JVM under their pseudo {@code host:port}; {@link
 * RpcTransports#connect} consults this table first and, on a hit, calls in process without a
 * socket or serialization, falling back to a socket otherwise.<p>A later registration is
 * visible: this is a ConcurrentHashMap, so a master registered first is seen by a worker whose
 * constructor calls the master. Each endpoint has one shared handler pool, which is the
 * in-process equivalent of a target server having one inbound pool and of a socket using one
 * thread per connection, so a transport reuses it by delivering the call there and waiting on a
 * Future rather than running the handler on the caller's thread.
 */
public final class InProcessRpcRegistry {

    private static final class Endpoint {
        final Function<RpcMessage, RpcMessage> handler;
        final BoundedInProcessExecutor executor;
        Endpoint(Function<RpcMessage, RpcMessage> handler, BoundedInProcessExecutor executor) {
            this.handler = handler;
            this.executor = executor;
        }
    }

    private static final Map<String, Endpoint> ENDPOINTS = new ConcurrentHashMap<>();
    private static final int LOGICAL_PORT_BASE = 1_000_000;
    private static final AtomicInteger NEXT_LOGICAL_PORT =
            new AtomicInteger(LOGICAL_PORT_BASE);

    /** Lease for a registered endpoint; it releases only the identity bound at registration. */
    public static final class Registration implements AutoCloseable {
        private final String key;
        private final Endpoint endpoint;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Registration(String key, Endpoint endpoint) {
            this.key = key;
            this.endpoint = endpoint;
        }

        public int handlerThreadCount() {
            return endpoint.executor.createdThreadCount();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true)) {
                return;
            }
            if (ENDPOINTS.remove(key, endpoint)) {
                endpoint.executor.shutdownNow();
            }
        }
    }

    private InProcessRpcRegistry() {
    }

    private static String key(String host, int port) {
        return host + ":" + port;
    }

    public static int allocateLogicalPort() {
        int allocated = NEXT_LOGICAL_PORT.getAndIncrement();
        if (allocated < LOGICAL_PORT_BASE) {
            throw new IllegalStateException("in-process RPC logical endpoint space exhausted");
        }
        return allocated;
    }

    public static boolean isLogicalPort(int port) {
        return port >= LOGICAL_PORT_BASE;
    }

    public static Registration registerLogical(
            String host, int logicalPort, Function<RpcMessage, RpcMessage> handler,
            int maxHandlers) {
        if (!isLogicalPort(logicalPort)) {
            throw new IllegalArgumentException(
                    "in-process RPC logical port is outside its namespace");
        }
        return register(host, logicalPort, handler, maxHandlers);
    }

    /** Registers an in-process endpoint and returns the owner lease; the active key keeps the first registration. */
    public static Registration register(String host, int port, Function<RpcMessage, RpcMessage> handler) {
        return register(host, port, handler,
                RpcAdmissionConfig.fromSystemProperties().maxConnections());
    }

    static Registration registerForTesting(String host, int port,
                                           Function<RpcMessage, RpcMessage> handler,
                                           int maxHandlers) {
        return register(host, port, handler, maxHandlers);
    }

    private static Registration register(String host, int port,
                                         Function<RpcMessage, RpcMessage> handler,
                                         int maxHandlers) {
        if (host == null || handler == null) {
            throw new IllegalArgumentException("host and handler must not be null");
        }
        BoundedInProcessExecutor executor = BoundedInProcessExecutor.create(
                "rowweave-inproc-rpc-" + host + ":" + port + "-", maxHandlers);
        String endpointKey = key(host, port);
        Endpoint candidate = new Endpoint(handler, executor);
        Endpoint existing = ENDPOINTS.putIfAbsent(endpointKey, candidate);
        if (existing != null) {
            executor.shutdownNow();
            throw new IllegalStateException("in-process RPC endpoint already registered: " + endpointKey);
        }
        return new Registration(endpointKey, candidate);
    }

    /** Whether an endpoint is in process, which decides object handoff versus serialization for a stage command. */
    public static boolean isRegistered(String host, int port) {
        return ENDPOINTS.containsKey(key(host, port));
    }

    /** Looks up an in-process transport; null when unregistered, so the caller falls back to a socket. Closing the result is a no-op because the registry owns the pool. */
    public static RpcTransport lookup(String host, int port) {
        Endpoint e = ENDPOINTS.get(key(host, port));
        if (e == null) {
            return null;
        }
        return new InProcessRpcTransport(e.handler, e.executor);  // pool injected from the registry, so close is a no-op
    }
}
