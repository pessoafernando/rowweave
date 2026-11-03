package io.rowweave.sched.net;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Handshake admission for the data plane of a single worker.<p>Inbound and outbound share one
 * overall configuration, but each keeps a partition of slots and bytes that the other cannot
 * borrow. Without that split, two workers could each hold an outbound permit and wait on the
 * other's inbound permit. Waiting happens inside the single absolute deadline passed in by the
 * caller.
 */
public final class DataPlaneHandshakeAdmission implements AutoCloseable {

    /** Process-wide TLS heap-used plus direct-buffer cold-initialization charge, measured once for the attested build and not duplicated per direction or connection. */
    public static final long PROCESS_COLD_RESERVE_BYTES = 64L * 1024L * 1024L;
    /**
     * Charge for one active full handshake in a certified build: heap used plus direct buffers,
     * grown by 20% headroom over the measured peak.
     */
    public static final long HANDSHAKE_CHARGE_BYTES = 192L * 1024L * 1024L;
    /** The matrix is certified for at most two inbound and two outbound handshakes per process. */
    public static final int CERTIFIED_MAX_CONCURRENT_HANDSHAKES = 4;

    public enum Direction {
        INBOUND,
        OUTBOUND
    }

    private final int totalCapacity;
    private final long configuredResidentBytes;
    private final DirectionalGate inbound;
    private final DirectionalGate outbound;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Object lifecycle = new Object();

    private DataPlaneHandshakeAdmission(int maxConcurrentHandshakes,
                                        long handshakeResidentBytes) {
        if (maxConcurrentHandshakes < 2
                || maxConcurrentHandshakes > CERTIFIED_MAX_CONCURRENT_HANDSHAKES) {
            throw new IllegalArgumentException(
                    "maxConcurrentHandshakes must be in [2,"
                            + CERTIFIED_MAX_CONCURRENT_HANDSHAKES
                            + "] for the certified MTLS build");
        }
        long requiredBytes = requiredResidentBytes(maxConcurrentHandshakes);
        if (handshakeResidentBytes < requiredBytes) {
            throw new IllegalArgumentException(
                    "handshakeResidentBytes is smaller than process cold reserve"
                            + " plus concurrent handshake charge");
        }
        this.totalCapacity = maxConcurrentHandshakes;
        this.configuredResidentBytes = handshakeResidentBytes;
        int inboundCapacity = (maxConcurrentHandshakes + 1) / 2;
        int outboundCapacity = maxConcurrentHandshakes / 2;
        this.inbound = new DirectionalGate(this, Direction.INBOUND, inboundCapacity);
        this.outbound = new DirectionalGate(this, Direction.OUTBOUND, outboundCapacity);
    }

    static DataPlaneHandshakeAdmission forTesting(int maxConcurrentHandshakes,
                                                  long handshakeResidentBytes) {
        return new DataPlaneHandshakeAdmission(
                maxConcurrentHandshakes, handshakeResidentBytes);
    }

    public static DataPlaneHandshakeAdmission create(int maxConcurrentHandshakes,
                                                     long handshakeResidentBytes) {
        return new DataPlaneHandshakeAdmission(
                maxConcurrentHandshakes, handshakeResidentBytes);
    }

    public static long requiredResidentBytes(int maxConcurrentHandshakes) {
        if (maxConcurrentHandshakes < 0) {
            throw new IllegalArgumentException(
                    "maxConcurrentHandshakes must not be negative");
        }
        return Math.addExact(PROCESS_COLD_RESERVE_BYTES,
                Math.multiplyExact((long) maxConcurrentHandshakes,
                        HANDSHAKE_CHARGE_BYTES));
    }

    public Lease acquireInbound(long absoluteDeadlineNanos)
            throws InterruptedException, TimeoutException {
        return inbound.acquire(absoluteDeadlineNanos);
    }

    public Lease acquireOutbound(long absoluteDeadlineNanos)
            throws InterruptedException, TimeoutException {
        return outbound.acquire(absoluteDeadlineNanos);
    }

    public int inboundCapacity() {
        return inbound.capacity;
    }

    public int outboundCapacity() {
        return outbound.capacity;
    }

    public Snapshot snapshot() {
        int activeInbound = inbound.active.get();
        int activeOutbound = outbound.active.get();
        return new Snapshot(activeInbound, activeOutbound,
                Math.multiplyExact((long) activeInbound + activeOutbound,
                        HANDSHAKE_CHARGE_BYTES),
                Math.max(inbound.peak.get(), outbound.peak.get()),
                configuredResidentBytes);
    }

    /**
     * The handler factory consumes, exactly once, an open lease of the right direction owned by
     * this admission.
     */
    public HandlerLease claimForHandler(Lease lease, Direction direction) {
        if (lease == null || direction == null) {
            throw new IllegalArgumentException("handshake lease and direction are required");
        }
        synchronized (lifecycle) {
            requireOpen();
            if (lease.admission != this) {
                throw new IllegalArgumentException(
                        "handshake lease belongs to another admission");
            }
            if (lease.direction != direction) {
                throw new IllegalArgumentException("handshake lease direction mismatch");
            }
            if (lease.closed.get()) {
                throw new RejectedExecutionException("handshake lease is closed");
            }
            if (!lease.claimed.compareAndSet(false, true)) {
                throw new RejectedExecutionException(
                        "handshake lease was already claimed by a handler");
            }
            return new HandlerLease(lease);
        }
    }

    @Override
    public void close() {
        synchronized (lifecycle) {
            if (!closed.compareAndSet(false, true)) return;
            inbound.wakeWaiters();
            outbound.wakeWaiters();
        }
        Throwable failure = null;
        try {
            inbound.close();
        } catch (Throwable inboundFailure) {
            failure = inboundFailure;
        }
        try {
            outbound.close();
        } catch (Throwable outboundFailure) {
            if (failure == null) failure = outboundFailure;
            else failure.addSuppressed(outboundFailure);
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1L);
        synchronized (lifecycle) {
            while (inbound.active.get() != 0 || outbound.active.get() != 0) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) break;
                try {
                    TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    failure = append(failure, new IllegalStateException(
                            "interrupted draining data-plane handshake leases", interrupted));
                    break;
                }
            }
            if (inbound.active.get() != 0 || outbound.active.get() != 0) {
                failure = append(failure, new IllegalStateException(
                        "data-plane handshake admission closed with active leases: inbound="
                                + inbound.active.get() + ", outbound="
                                + outbound.active.get()));
            }
        }
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        if (failure != null) throw new IllegalStateException(failure);
    }

    private void requireOpen() {
        if (closed.get()) {
            throw new RejectedExecutionException("data-plane handshake admission is closed");
        }
    }

    private static Throwable append(Throwable current, Throwable next) {
        if (current == null) return next;
        current.addSuppressed(next);
        return current;
    }

    int totalCapacity() {
        return totalCapacity;
    }

    long configuredResidentBytes() {
        return configuredResidentBytes;
    }

    int inboundWaitersForTesting() {
        return inbound.waiters.get();
    }

    private static final class DirectionalGate {
        private final DataPlaneHandshakeAdmission admission;
        private final Direction direction;
        private final int capacity;
        private final Semaphore permits;
        private final AtomicInteger waiters = new AtomicInteger();
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger peak = new AtomicInteger();
        private final ThreadPoolExecutor delegatedTasks;

        private DirectionalGate(DataPlaneHandshakeAdmission admission,
                                Direction direction, int capacity) {
            this.admission = admission;
            this.direction = direction;
            this.capacity = capacity;
            this.permits = new Semaphore(capacity, true);
            // SslTasksRunner posts its event-loop resume before returning to the
            // pool. A successor from the same admitted handshake may arrive
            // while all preceding runners are still returning. Keep one bounded
            // handoff entry per directional slot, not a zero-capacity rendezvous.
            // Thread count, handshake slots and byte charge remain unchanged;
            // overflow still rejects instead of running TLS on an event loop.
            this.delegatedTasks = new ThreadPoolExecutor(
                    capacity, capacity, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(capacity), runnable -> {
                        Thread thread = new Thread(runnable,
                                "rowweave-shuffle-tls-" + direction.name().toLowerCase());
                        thread.setDaemon(true);
                        return thread;
                    }, new ThreadPoolExecutor.AbortPolicy());
        }

        private Lease acquire(long absoluteDeadlineNanos)
                throws InterruptedException, TimeoutException {
            synchronized (admission.lifecycle) {
                admission.requireOpen();
                int waiting = waiters.incrementAndGet();
                if (waiting > capacity) {
                    waiters.decrementAndGet();
                    throw new RejectedExecutionException(
                            direction + " handshake waiter queue is full");
                }
            }
            boolean acquired = false;
            try {
                long remaining = absoluteDeadlineNanos - System.nanoTime();
                acquired = remaining > 0L
                        && permits.tryAcquire(remaining, TimeUnit.NANOSECONDS);
                if (!acquired) {
                    throw new TimeoutException(direction + " handshake admission deadline");
                }
            } finally {
                waiters.decrementAndGet();
            }
            synchronized (admission.lifecycle) {
                if (admission.closed.get()) {
                    permits.release();
                    throw new RejectedExecutionException(
                            "data-plane handshake admission is closed");
                }
                int now = active.incrementAndGet();
                updatePeak(now);
                return new Lease(admission, this, direction);
            }
        }

        private void updatePeak(int candidate) {
            int current = peak.get();
            while (candidate > current && !peak.compareAndSet(current, candidate)) {
                current = peak.get();
            }
        }

        private void release() {
            int remaining = active.decrementAndGet();
            if (remaining < 0) {
                active.incrementAndGet();
                throw new IllegalStateException("handshake admission lease underflow");
            }
            permits.release();
            admission.lifecycle.notifyAll();
        }

        private void close() {
            delegatedTasks.shutdownNow();
            try {
                if (!delegatedTasks.awaitTermination(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            direction + " delegated-task executor did not terminate");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "interrupted closing " + direction + " delegated-task executor",
                        interrupted);
            }
        }

        private void wakeWaiters() {
            permits.release(capacity);
        }
    }

    public static final class Lease implements AutoCloseable {
        private final DataPlaneHandshakeAdmission admission;
        private final DirectionalGate owner;
        private final Direction direction;
        private final AtomicBoolean closed = new AtomicBoolean();
        private final AtomicBoolean claimed = new AtomicBoolean();

        private Lease(DataPlaneHandshakeAdmission admission,
                      DirectionalGate owner, Direction direction) {
            this.admission = admission;
            this.owner = owner;
            this.direction = direction;
        }

        public Direction direction() {
            return direction;
        }

        @Override
        public void close() {
            synchronized (admission.lifecycle) {
                // Claiming hands ownership to the HandlerLease, so the caller's finally block must not
                // release it first.
                if (claimed.get()) return;
                releaseTerminal();
            }
        }

        private void releaseFromHandler() {
            synchronized (admission.lifecycle) {
                if (!claimed.get()) {
                    throw new IllegalStateException(
                            "handshake lease ownership was not transferred to a handler");
                }
                releaseTerminal();
            }
        }

        private void releaseTerminal() {
            if (closed.compareAndSet(false, true)) owner.release();
        }
    }

    /**
     * The only terminal capability after a claim; the original lease is a side-effect-free
     * caller handle from then on.
     */
    public static final class HandlerLease implements AutoCloseable {
        private final Lease lease;

        private HandlerLease(Lease lease) {
            this.lease = lease;
        }

        public Executor delegatedTaskExecutor() {
            return lease.owner.delegatedTasks;
        }

        @Override
        public void close() {
            lease.releaseFromHandler();
        }
    }

    public static final class Snapshot {
        private final int activeInbound;
        private final int activeOutbound;
        private final long residentBytes;
        private final int peakPerDirection;
        private final long configuredResidentBytes;

        private Snapshot(int activeInbound, int activeOutbound,
                         long residentBytes, int peakPerDirection,
                         long configuredResidentBytes) {
            this.activeInbound = activeInbound;
            this.activeOutbound = activeOutbound;
            this.residentBytes = residentBytes;
            this.peakPerDirection = peakPerDirection;
            this.configuredResidentBytes = configuredResidentBytes;
        }

        public int activeInbound() {
            return activeInbound;
        }

        public int activeOutbound() {
            return activeOutbound;
        }

        public int activeTotal() {
            return activeInbound + activeOutbound;
        }

        public long residentBytes() {
            return residentBytes;
        }

        /** The process's single heap-used plus direct-buffer cold-initialization charge, not the process RSS. */
        public long processColdReservedBytes() {
            return PROCESS_COLD_RESERVE_BYTES;
        }

        /** The cold-initialization charge plus the charge for handshakes currently active, not the process RSS. */
        public long totalReservedBytes() {
            return Math.addExact(PROCESS_COLD_RESERVE_BYTES, residentBytes);
        }

        /**
         * Exclusive budget for the configured TLS envelope of heap used plus direct buffers,
         * not the resident size of the whole process.
         */
        public long configuredResidentBytes() {
            return configuredResidentBytes;
        }

        public int peakPerDirection() {
            return peakPerDirection;
        }
    }
}
