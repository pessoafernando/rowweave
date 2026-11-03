package io.rowweave.cluster.rpc;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Connection reuse for the control plane, keyed per endpoint, using a client map with
 * putIfAbsent and eviction on failure so that no control message pays for a connect, call and
 * close cycle. {@link MasterServer} holds one cache towards its workers and {@link
 * WorkerServer} holds one towards the current leader, so assignment, reports and cancellations
 * share one physical connection and skip the three-way handshake each time.<p>Guards that make
 * reuse safe:<ul><li><b>The cache key is {@code host:port}</b>, not a workerId or a logical
 * leader name. A worker restart that picks a different port, or a leader failover that moves to
 * a different endpoint, changes the key, so the lookup misses and a fresh connection is built
 * while the stale entry is evicted. Leader resolution is never cached.</li><li><b>A call or
 * send that throws causes eviction, close, reconnect and one retry.</b> {@link RpcClient#call}
 * does not close its socket when it throws, so the cache has to close and remove it; a broken
 * connection is never reused a second time, and only a second failure is
 * propagated.</li><li><b>In-process endpoints bypass the cache.</b> Only socket endpoints are
 * cached: an in-process endpoint connects directly every time, which costs no handshake, and
 * its pool belongs to {@link InProcessRpcRegistry}, so a cached wrapper would raise
 * RejectedExecutionException after unregistration shut the pool down.</li></ul><p>Thread safety
 * comes from {@link ConcurrentHashMap} with {@code computeIfAbsent} deduplicating connection
 * creation atomically.
 */
public final class RpcTransportCache implements AutoCloseable {

    /**
     * Factory for connections, injectable in tests; the default builds a socket {@link
     * RpcClient}.
     */
    public interface Connector {
        RpcTransport connect(String host, int port);
    }

    /**
     * Whether an endpoint is cacheable, injectable in tests; the default is anything not in
     * process, that is a socket.
     */
    public interface Cacheable {
        boolean test(String host, int port);
    }

    private final Connector connector;
    private final Cacheable cacheable;
    private final Map<String, RpcTransport> cache = new ConcurrentHashMap<>();
    private final java.util.concurrent.atomic.AtomicBoolean shutdown = new java.util.concurrent.atomic.AtomicBoolean();
    private final java.util.concurrent.locks.ReentrantReadWriteLock lifecycle =
            new java.util.concurrent.locks.ReentrantReadWriteLock();

    /**
     * Production entry point: socket connections plus the in-process bypass.
     */
    public RpcTransportCache() {
        this(RpcTransports::connect, (h, p) -> !InProcessRpcRegistry.isRegistered(h, p));
    }

    public RpcTransportCache(Connector connector, Cacheable cacheable) {
        if (connector == null || cacheable == null) {
            throw new IllegalArgumentException("connector and cacheable must not be null");
        }
        this.connector = connector;
        this.cacheable = cacheable;
    }

    private static String key(String host, int port) {
        return host + ":" + port;
    }

    /**
     * Sends on a reused connection and waits for the response; a connection that throws is
     * evicted, reconnected and retried once, and a second failure is propagated.
     */
    public RpcMessage call(String host, int port, RpcMessage request) {
        return call(host, port, request, () -> true);
    }

    /**
     * Request fenced by an endpoint generation. After a first failure a reconnect is allowed
     * only while the caller still confirms that generation, and it is checked once more after
     * the new connection is built, which closes the window where a worker departs between the
     * check and the connection being inserted.
     */
    public RpcMessage call(String host, int port, RpcMessage request,
                           java.util.function.BooleanSupplier retryAllowed) {
        ensureOpen();
        lifecycle.readLock().lock();
        try {
            ensureOpen();
            if (!cacheable.test(host, port)) {
                return bypassCall(host, port, request);
            }
            String k = key(host, port);
            RpcTransport t = cache.computeIfAbsent(k, kk -> connectIfOpen(host, port));
            try {
                return t.call(request);
            } catch (RuntimeException first) {
                evict(k, t);
                ensureOpen();
                if (!retryAllowed.getAsBoolean()) {
                    throw first;
                }
                RpcTransport t2 = cache.computeIfAbsent(k, kk -> connectIfOpen(host, port));
                if (!retryAllowed.getAsBoolean()) {
                    evict(k, t2);
                    throw first;
                }
                return t2.call(request);  // a second failure is propagated rather than swallowed
            }
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    /**
     * Sends one-way on a reused connection with no response awaited; a connection that throws
     * is evicted, reconnected and resent once.
     */
    public void send(String host, int port, RpcMessage request) {
        ensureOpen();
        lifecycle.readLock().lock();
        try {
            ensureOpen();
            if (!cacheable.test(host, port)) {
                bypassSend(host, port, request);
                return;
            }
            String k = key(host, port);
            RpcTransport t = cache.computeIfAbsent(k, kk -> connectIfOpen(host, port));
            try {
                t.send(request);
            } catch (RuntimeException first) {
                evict(k, t);
                ensureOpen();
                RpcTransport t2 = cache.computeIfAbsent(k, kk -> connectIfOpen(host, port));
                t2.send(request);
            }
        } finally {
            lifecycle.readLock().unlock();
        }
    }

    private RpcTransport connectIfOpen(String host, int port) {
        ensureOpen();
        return connector.connect(host, port);
    }

    private void ensureOpen() {
        if (shutdown.get()) {
            throw new IllegalStateException("RPC transport cache is shut down");
        }
    }

    /**
     * Evict the connection to one endpoint, on a lost worker or an explicit invalidation: close
     * and remove it, so the next call rebuilds it.
     */
    public void invalidate(String host, int port) {
        RpcTransport t = cache.remove(key(host, port));
        closeQuietly(t);
    }

    /**
     * Removes the entry only while it is still the same instance, so a concurrently rebuilt
     * connection is not dropped.
     */
    private void evict(String k, RpcTransport expected) {
        if (cache.remove(k, expected)) {
            closeQuietly(expected);
        }
    }

    /**
     * In-process bypass: a one-time connection that is closed immediately afterwards, and
     * closing in process is a free no-op.
     */
    private RpcMessage bypassCall(String host, int port, RpcMessage request) {
        RpcTransport t = connector.connect(host, port);
        try {
            return t.call(request);
        } finally {
            closeQuietly(t);
        }
    }

    private void bypassSend(String host, int port, RpcMessage request) {
        RpcTransport t = connector.connect(host, port);
        try {
            t.send(request);
        } finally {
            closeQuietly(t);
        }
    }

    private static void closeQuietly(RpcTransport t) {
        if (t != null) {
            try {
                t.close();
            } catch (RuntimeException ignored) {
                // best-effort
            }
        }
    }

    /**
     * Closes every cached connection, on the shutdown or release path of a server.
     */
    @Override
    public void close() {
        for (RpcTransport t : cache.values()) {
            closeQuietly(t);
        }
        cache.clear();
    }

    /**
     * Bars the door permanently, for a lane pool with an explicit lifecycle, as distinct from
     * {@link #close()} which still allows reconnection.
     */
    public void shutdown() {
        shutdown.set(true);  // bar the door first, then close the existing sockets so a blocked read fails quickly
        close();
        lifecycle.writeLock().lock();
        try {
            close();  // sweep once more after every in-flight call has left the read lock, closing the close-versus-connect race
        } finally {
            lifecycle.writeLock().unlock();
        }
    }
}
