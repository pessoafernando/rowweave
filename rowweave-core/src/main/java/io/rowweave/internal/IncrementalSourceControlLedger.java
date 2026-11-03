package io.rowweave.internal;

import io.rowweave.incremental.IncrementalSourceActionBudget;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Fixed role-local admission for incremental source control work.
 *
 * <p>The normal lane covers bounded provider metadata work. The large lane
 * covers source-action RPCs/materialization responses. Both byte ceilings are
 * independent of, and subordinate to, the process-wide control-resident
 * budget.</p>
 */
public final class IncrementalSourceControlLedger {
    public static final int MAX_NORMAL_ACTIONS = 64;
    public static final int MAX_LARGE_ACTIONS = 8;
    public static final long MAX_NORMAL_RESIDENT_BYTES =
            64L * 1024L * 1024L;
    public static final long MAX_LARGE_RESIDENT_BYTES =
            192L * 1024L * 1024L;

    private final Lane normal = new Lane(
            "NORMAL", MAX_NORMAL_ACTIONS, MAX_NORMAL_RESIDENT_BYTES,
            IncrementalSourceActionBudget.MAX_NORMAL_RESIDENT_BYTES);
    private final Lane large = new Lane(
            "LARGE", MAX_LARGE_ACTIONS, MAX_LARGE_RESIDENT_BYTES,
            IncrementalSourceActionBudget.MAX_LARGE_RESIDENT_BYTES);

    private IncrementalSourceControlLedger() {
    }

    /** One ledger per production role; production deploys one role per JVM. */
    public static IncrementalSourceControlLedger forRole() {
        return new IncrementalSourceControlLedger();
    }

    static IncrementalSourceControlLedger forTesting() {
        return new IncrementalSourceControlLedger();
    }

    public Lease acquireNormal(long residentBytes, long deadlineNanos) {
        return normal.acquire(residentBytes, deadlineNanos);
    }

    public Lease acquireLarge(long residentBytes, long deadlineNanos) {
        return large.acquire(residentBytes, deadlineNanos);
    }

    int normalActionsInUse() { return normal.actionsInUse.get(); }
    int largeActionsInUse() { return large.actionsInUse.get(); }
    long normalResidentBytesInUse() { return normal.residentBytes.get(); }
    long largeResidentBytesInUse() { return large.residentBytes.get(); }

    private static final class Lane {
        private final String name;
        private final Semaphore permits;
        private final long maximumResidentBytes;
        private final long maximumPerActionBytes;
        private final AtomicInteger actionsInUse = new AtomicInteger();
        private final AtomicLong residentBytes = new AtomicLong();

        private Lane(String name, int maximumActions,
                     long maximumResidentBytes, long maximumPerActionBytes) {
            this.name = name;
            this.permits = new Semaphore(maximumActions, true);
            this.maximumResidentBytes = maximumResidentBytes;
            this.maximumPerActionBytes = maximumPerActionBytes;
        }

        private Lease acquire(long bytes, long deadlineNanos) {
            if (bytes <= 0L || bytes > maximumPerActionBytes) {
                throw new IllegalArgumentException(
                        "incremental source " + name.toLowerCase()
                                + " action resident bytes are invalid");
            }
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0L) throw exhausted();
            try {
                if (!permits.tryAcquire(remaining, TimeUnit.NANOSECONDS)) {
                    throw exhausted();
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "INCREMENTAL_SOURCE_" + name
                                + "_LANE_INTERRUPTED", interrupted);
            }
            boolean reserved = false;
            try {
                reserve(bytes);
                actionsInUse.incrementAndGet();
                reserved = true;
                return new Lease(this, bytes);
            } finally {
                if (!reserved) permits.release();
            }
        }

        private void reserve(long bytes) {
            long current = residentBytes.get();
            while (true) {
                if (current > maximumResidentBytes - bytes) {
                    throw exhausted();
                }
                if (residentBytes.compareAndSet(current, current + bytes)) {
                    return;
                }
                current = residentBytes.get();
            }
        }

        private void release(long bytes) {
            long remaining = residentBytes.addAndGet(-bytes);
            int actions = actionsInUse.decrementAndGet();
            if (remaining < 0L || actions < 0) {
                throw new IllegalStateException(
                        "incremental source lane accounting underflow");
            }
            permits.release();
        }

        private IllegalStateException exhausted() {
            return new IllegalStateException(
                    "INCREMENTAL_SOURCE_" + name + "_LANE_EXHAUSTED");
        }
    }

    public static final class Lease implements AutoCloseable {
        private final Lane lane;
        private final long residentBytes;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Lease(Lane lane, long residentBytes) {
            this.lane = lane;
            this.residentBytes = residentBytes;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                lane.release(residentBytes);
            }
        }
    }
}
