package io.rowweave.cluster.rpc;

import java.util.concurrent.atomic.AtomicLong;

/** Non-blocking in-flight wire frame budget for one RpcServer. */
final class RpcFrameBudget {
    private final long limit;
    private final AtomicLong used = new AtomicLong();

    RpcFrameBudget(long limit) { this.limit = limit; }

    Reservation reserve(int bytes) throws java.io.IOException {
        while (true) {
            long current = used.get();
            long next = current + bytes;
            if (next < current || next > limit) {
                throw new java.io.IOException("RPC in-flight frame budget exceeded");
            }
            if (used.compareAndSet(current, next)) return new Reservation(this, bytes);
        }
    }

    long used() { return used.get(); }

    private void release(int bytes) { used.addAndGet(-bytes); }

    static final class Reservation implements AutoCloseable {
        private RpcFrameBudget owner;
        private final int bytes;
        Reservation(RpcFrameBudget owner, int bytes) { this.owner = owner; this.bytes = bytes; }
        @Override public void close() {
            RpcFrameBudget current = owner;
            if (current != null) { owner = null; current.release(bytes); }
        }
    }
}
