package io.rowweave.cluster.rpc;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** Bounded long-lived connections to one endpoint: serial inside a lane, genuinely concurrent across lanes. */
public final class RpcTransportLanePool implements AutoCloseable {
    private final RpcTransportCache[] lanes;
    private final AtomicInteger next = new AtomicInteger();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RpcTransportLanePool(int parallelism) {
        this(parallelism, RpcTransports::connect, (h, p) -> !InProcessRpcRegistry.isRegistered(h, p));
    }

    public RpcTransportLanePool(int parallelism, RpcTransportCache.Connector connector,
                                RpcTransportCache.Cacheable cacheable) {
        if (parallelism < 1) {
            throw new IllegalArgumentException("lane parallelism must be >= 1");
        }
        lanes = new RpcTransportCache[parallelism];
        for (int i = 0; i < parallelism; i++) {
            lanes[i] = new RpcTransportCache(connector, cacheable);
        }
    }

    public RpcMessage call(String host, int port, RpcMessage request) {
        return call(host, port, request, () -> true);
    }

    public RpcMessage call(String host, int port, RpcMessage request,
                           java.util.function.BooleanSupplier retryAllowed) {
        if (closed.get()) {
            throw new IllegalStateException("RPC lane pool is closed");
        }
        int index = Math.floorMod(next.getAndIncrement(), lanes.length);
        return lanes[index].call(host, port, request, retryAllowed);
    }

    public void invalidate(String host, int port) {
        if (closed.get()) {
            return;
        }
        for (RpcTransportCache lane : lanes) {
            lane.invalidate(host, port);
        }
    }

    public int parallelism() {
        return lanes.length;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        for (RpcTransportCache lane : lanes) {
            lane.shutdown();
        }
    }
}
