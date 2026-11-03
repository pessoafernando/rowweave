package io.rowweave.deployment;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Independent aggregate budget for compressed images, canonical indexes and active entries. */
public final class DeploymentMemoryBudget {
    public static final long DEFAULT_BYTES = 256L * 1024L * 1024L;

    private final long capacityBytes;
    private final AtomicLong usedBytes = new AtomicLong();

    private DeploymentMemoryBudget(long capacityBytes) {
        if (capacityBytes < 0L) throw new IllegalArgumentException("deployment budget must be non-negative");
        this.capacityBytes = capacityBytes;
    }

    public static DeploymentMemoryBudget forTesting(long capacityBytes) {
        return new DeploymentMemoryBudget(capacityBytes);
    }

    public static DeploymentMemoryBudget withCapacity(long capacityBytes) {
        return new DeploymentMemoryBudget(capacityBytes);
    }

    public static DeploymentMemoryBudget withDefaultCapacity() {
        return new DeploymentMemoryBudget(DEFAULT_BYTES);
    }

    public long capacityBytes() { return capacityBytes; }
    public long usedBytes() { return usedBytes.get(); }

    Lease reserve(long bytes) {
        if (bytes < 0L) throw new IllegalArgumentException("deployment reservation must be non-negative");
        for (;;) {
            long current = usedBytes.get();
            if (bytes > capacityBytes - current) {
                throw failure("deployment memory budget exceeded: requested=" + bytes
                        + " used=" + current + " capacity=" + capacityBytes);
            }
            if (usedBytes.compareAndSet(current, current + bytes)) return new Lease(this, bytes);
        }
    }

    private void release(long bytes) {
        long after = usedBytes.addAndGet(-bytes);
        if (after < 0L) {
            usedBytes.addAndGet(bytes);
            throw new IllegalStateException("deployment memory budget underflow");
        }
    }

    static DeploymentValidationException failure(String message) {
        return new DeploymentValidationException("DEPLOYMENT_MEMORY_BUDGET_EXCEEDED", message);
    }

    static final class Lease implements AutoCloseable {
        private final DeploymentMemoryBudget owner;
        private final long bytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        Lease(DeploymentMemoryBudget owner, long bytes) {
            this.owner = owner;
            this.bytes = bytes;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) owner.release(bytes);
        }
    }
}
