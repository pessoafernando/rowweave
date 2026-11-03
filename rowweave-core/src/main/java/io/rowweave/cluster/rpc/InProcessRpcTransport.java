package io.rowweave.cluster.rpc;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * {@link #call} delivers to the handler pool of the target server and waits on a Future, which
 * keeps the thread decoupling of one thread per connection and never runs the handler on the
 * caller's thread.<p>Three reasons a same-thread call would deadlock: {@code RpcClient.call} is
 * synchronized, so a long blocking submit would lock the whole transport; {@code
 * MasterServer.awaitJob} polls in a loop, so a same-thread call would hand the caller's thread
 * away permanently; and fail-fast depends on separation, because over a socket the submit
 * thread and the report thread are different, and inlining them into one stack breaks exception
 * and timeout propagation. Delivering to the target pool instead means submit occupies one
 * master pool thread while a stage report or port report is handled by another, the same shape
 * as a socket, so there is no deadlock.<p>No socket and no serialization: the {@link
 * RpcMessage} object reference is passed directly, which is sound inside one JVM, so plan graph
 * bytes and sink references need no codec.
 */
public final class InProcessRpcTransport implements RpcTransport {

    private static final long DEFAULT_RESPONSE_TIMEOUT_MS = 30_000L;

    private final Function<RpcMessage, RpcMessage> handler;
    private final BoundedInProcessExecutor executor;
    private final boolean ownsPool;
    private final long responseTimeoutMs;
    private final RpcSecurityMetadata securityMetadata;
    private final boolean requireSecure;

    /** Bounded direct-handoff pool built here, and shut down when this transport closes. */
    public InProcessRpcTransport(Function<RpcMessage, RpcMessage> handler) {
        this(handler, defaultExecutor(), true, DEFAULT_RESPONSE_TIMEOUT_MS, null, false);
    }

    /** Handler pool of the target server, injected; only a provably bounded direct-handoff pool with AbortPolicy is accepted. */
    public InProcessRpcTransport(Function<RpcMessage, RpcMessage> handler, ExecutorService pool) {
        this(handler, BoundedInProcessExecutor.fromInjected(pool), false,
                DEFAULT_RESPONSE_TIMEOUT_MS, null, false);
    }

    InProcessRpcTransport(Function<RpcMessage, RpcMessage> handler,
                          BoundedInProcessExecutor executor) {
        this(handler, executor, false, DEFAULT_RESPONSE_TIMEOUT_MS, null, false);
    }

    public static InProcessRpcTransport secure(
            Function<RpcMessage, RpcMessage> handler, RpcSecurityMetadata securityMetadata) {
        if (securityMetadata == null) {
            throw new IllegalArgumentException("secure in-process identity is required");
        }
        return new InProcessRpcTransport(handler, defaultExecutor(), true,
                DEFAULT_RESPONSE_TIMEOUT_MS, securityMetadata, true);
    }

    static InProcessRpcTransport forTesting(Function<RpcMessage, RpcMessage> handler,
                                            int maxHandlers, long responseTimeoutMs) {
        return new InProcessRpcTransport(handler,
                BoundedInProcessExecutor.create("rowweave-inproc-rpc-test-", maxHandlers),
                true, responseTimeoutMs, null, false);
    }

    private InProcessRpcTransport(Function<RpcMessage, RpcMessage> handler,
                                  BoundedInProcessExecutor executor,
                                  boolean owns, long responseTimeoutMs,
                                  RpcSecurityMetadata securityMetadata, boolean requireSecure) {
        if (handler == null || executor == null) {
            throw new IllegalArgumentException("handler and executor must not be null");
        }
        if (responseTimeoutMs < 0L) {
            throw new IllegalArgumentException("responseTimeoutMs must be >= 0");
        }
        this.handler = handler;
        this.executor = executor;
        this.ownsPool = owns;
        this.responseTimeoutMs = responseTimeoutMs;
        this.securityMetadata = securityMetadata;
        this.requireSecure = requireSecure;
    }

    /** Applies the read timeout semantics of the socket path to the wrapper of the shared handler pool once a factory hit is in process. */
    InProcessRpcTransport withResponseTimeout(long timeoutMs) {
        return new InProcessRpcTransport(handler, executor, false, timeoutMs,
                securityMetadata, requireSecure);
    }

    private static BoundedInProcessExecutor defaultExecutor() {
        return BoundedInProcessExecutor.create("rowweave-inproc-rpc-",
                RpcAdmissionConfig.fromSystemProperties().maxConnections());
    }

    @Override
    public RpcMessage call(RpcMessage request) {
        RpcMessage outbound = RpcSecurityFence.outbound(
                request, securityMetadata, requireSecure);
        Future<RpcMessage> f = executor.submit(() -> {
            RpcMessage inbound = RpcSecurityFence.inbound(
                    outbound, securityMetadata, requireSecure);
            RpcMessage response = handler.apply(inbound);
            return response == null ? null : RpcSecurityFence.outbound(
                    response, securityMetadata, requireSecure);
        });
        try {
            RpcMessage response = responseTimeoutMs == 0L
                    ? f.get()
                    : f.get(responseTimeoutMs, TimeUnit.MILLISECONDS);
            return response == null ? null : RpcSecurityFence.inbound(
                    response, securityMetadata, requireSecure);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("in-process RPC call interrupted", e);
        } catch (TimeoutException timeout) {
            // Matches SO_TIMEOUT on a socket: only the waiter is released, work already inside the server handler is not interrupted.
            throw new RuntimeException(
                    "in-process RPC call timed out after " + responseTimeoutMs + "ms", timeout);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            throw new RuntimeException("in-process RPC handler failed", cause);
        }
    }

    @Override
    public void send(RpcMessage request) {
        RpcMessage outbound = RpcSecurityFence.outbound(
                request, securityMetadata, requireSecure);
        executor.submit(() -> handler.apply(RpcSecurityFence.inbound(
                outbound, securityMetadata, requireSecure))); // fire-and-forget
    }

    @Override
    public void close() {
        if (ownsPool) {
            executor.shutdownNow();
        }
    }
}
