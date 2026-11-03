package io.rowweave.cluster.rpc;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Aggregate resident token owned by one request. The generation inside the owner token stops an
 * older finally block from releasing a budget already handed off to an asynchronous run or
 * assignment.
 */
public final class ControlResidentLease {
    private static final AtomicLong IDS = new AtomicLong();

    private final long id = IDS.incrementAndGet();
    private final ControlResidentBudget budget;
    private final long maximumBytes;
    private final AtomicReference<OwnerState> state = new AtomicReference<>(
            new OwnerState(ControlResidentOwner.DECODE_OWNED, 0L));
    private long reservedBytes;

    ControlResidentLease(ControlResidentBudget budget, long maximumBytes) {
        if (maximumBytes <= 0L) {
            throw new IllegalArgumentException("resident lease maximum must be positive");
        }
        this.budget = budget;
        this.maximumBytes = maximumBytes;
    }

    public OwnerToken ownerToken() {
        OwnerState current = state.get();
        return new OwnerToken(id, current.generation, current.owner);
    }

    public synchronized void reserve(OwnerToken expected, long bytes) {
        requireCurrent(expected);
        if (expected.owner == ControlResidentOwner.RELEASED) {
            throw new IllegalStateException("resident lease is released");
        }
        if (bytes < 0L) {
            throw new IllegalArgumentException("resident reservation must not be negative");
        }
        if (bytes > maximumBytes - reservedBytes) {
            throw new ProtocolNegotiationException("RESIDENT_LIMIT_EXCEEDED",
                    "control resident bytes exceed negotiated maximum: reserved="
                            + reservedBytes + ", requested=" + bytes
                            + ", max=" + maximumBytes);
        }
        budget.reserve(bytes);
        reservedBytes += bytes;
    }

    public synchronized OwnerToken transfer(OwnerToken expected, ControlResidentOwner target) {
        if (target == null) throw new IllegalArgumentException("target owner must not be null");
        OwnerState current = requireCurrent(expected);
        if (!allows(current.owner, target)) {
            throw new IllegalStateException("illegal resident owner transition: "
                    + current.owner + " -> " + target);
        }
        OwnerState next = new OwnerState(target, current.generation + 1L);
        if (!state.compareAndSet(current, next)) {
            throw new IllegalStateException("resident owner changed concurrently");
        }
        return new OwnerToken(id, next.generation, next.owner);
    }

    public synchronized boolean release(OwnerToken expected) {
        OwnerState current = state.get();
        if (!matches(expected, current) || current.owner == ControlResidentOwner.RELEASED) {
            return false;
        }
        OwnerState released = new OwnerState(ControlResidentOwner.RELEASED,
                current.generation + 1L);
        if (!state.compareAndSet(current, released)) return false;
        long bytes = reservedBytes;
        reservedBytes = 0L;
        budget.release(bytes);
        return true;
    }

    public synchronized long reservedBytes() {
        return reservedBytes;
    }

    private OwnerState requireCurrent(OwnerToken expected) {
        OwnerState current = state.get();
        if (!matches(expected, current)) {
            throw new IllegalStateException("stale or foreign resident owner token");
        }
        return current;
    }

    private boolean matches(OwnerToken token, OwnerState current) {
        return token != null && token.leaseId == id
                && token.generation == current.generation && token.owner == current.owner;
    }

    private static boolean allows(ControlResidentOwner from, ControlResidentOwner to) {
        if (from == ControlResidentOwner.DECODE_OWNED) {
            return to == ControlResidentOwner.HANDLER_OWNED;
        }
        if (from == ControlResidentOwner.HANDLER_OWNED) {
            return to == ControlResidentOwner.RUN_OWNED
                    || to == ControlResidentOwner.ASSIGNMENT_OWNED;
        }
        return false;
    }

    private static final class OwnerState {
        private final ControlResidentOwner owner;
        private final long generation;

        private OwnerState(ControlResidentOwner owner, long generation) {
            this.owner = owner;
            this.generation = generation;
        }
    }

    public static final class OwnerToken {
        private final long leaseId;
        private final long generation;
        private final ControlResidentOwner owner;

        private OwnerToken(long leaseId, long generation, ControlResidentOwner owner) {
            this.leaseId = leaseId;
            this.generation = generation;
            this.owner = owner;
        }

        public long leaseId() { return leaseId; }
        public long generation() { return generation; }
        public ControlResidentOwner owner() { return owner; }
    }
}
