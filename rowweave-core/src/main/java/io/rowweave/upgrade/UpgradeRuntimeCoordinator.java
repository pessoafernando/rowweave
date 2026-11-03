package io.rowweave.upgrade;

/**
 * Linearizes new runtime admission, active-work accounting, and upgrade
 * transitions. Existing work retains a lease until its authoritative terminal
 * reconciliation has completed.
 */
public final class UpgradeRuntimeCoordinator {
    public interface Lease extends AutoCloseable {
        @Override
        void close();
    }

    private enum WorkKind { RUN, INTERVAL }

    private final UpgradeAuthorityStore authority;
    private final UpgradeActiveWorkCensus durableWork;
    private long activeRuns;
    private long activeIntervals;

    public UpgradeRuntimeCoordinator(UpgradeAuthorityStore authority) {
        this(authority, UpgradeActiveWorkCensus.EMPTY);
    }

    public UpgradeRuntimeCoordinator(
            UpgradeAuthorityStore authority,
            UpgradeActiveWorkCensus durableWork) {
        if (authority == null || durableWork == null) {
            throw new IllegalArgumentException(
                    "authority and durable-work census are required");
        }
        this.authority = authority;
        this.durableWork = durableWork;
    }

    public synchronized Lease beginRun() {
        requireAllowed(UpgradeWriteAdmission.Action.NEW_SUBMISSION);
        activeRuns = increment(activeRuns, "active Run");
        return new RuntimeLease(WorkKind.RUN);
    }

    public synchronized Lease beginInterval() {
        requireAllowed(UpgradeWriteAdmission.Action.NEW_CLAIM);
        activeIntervals = increment(activeIntervals, "active interval");
        return new RuntimeLease(WorkKind.INTERVAL);
    }

    public synchronized void requireNewClaimAllowed() {
        requireAllowed(UpgradeWriteAdmission.Action.NEW_CLAIM);
    }

    public synchronized void requireNewAssignmentAllowed() {
        requireAllowed(UpgradeWriteAdmission.Action.NEW_ASSIGNMENT);
    }

    public synchronized void requireReconciliationAllowed() {
        requireAllowed(UpgradeWriteAdmission.Action.RECONCILIATION);
    }

    public synchronized UpgradeTransition apply(
            UpgradeCommand command, long authorityTimeMillis) {
        if (command == null) {
            throw new IllegalArgumentException("command is required");
        }
        UpgradeActiveWorkSnapshot durable = command.kind()
                == UpgradeCommand.Kind.QUIESCE
                ? durableWork.snapshot()
                : new UpgradeActiveWorkSnapshot(0L, 0L);
        long observedRuns = Math.addExact(
                activeRuns, durable.activeRunCount());
        long observedIntervals = Math.addExact(
                activeIntervals, durable.activeIntervalCount());
        return authority.apply(command, observedRuns, observedIntervals,
                authorityTimeMillis);
    }

    public synchronized long activeRunCount() {
        return activeRuns;
    }

    public synchronized long activeIntervalCount() {
        return activeIntervals;
    }

    private void requireAllowed(UpgradeWriteAdmission.Action action) {
        UpgradeWriteAdmission.requireAllowed(authority.read(), action);
    }

    private static long increment(long value, String name) {
        if (value == Long.MAX_VALUE) {
            throw new IllegalStateException(name + " count overflow");
        }
        return value + 1L;
    }

    private synchronized void release(WorkKind kind) {
        if (kind == WorkKind.RUN) {
            if (activeRuns <= 0L) {
                throw new IllegalStateException("active Run count underflow");
            }
            activeRuns--;
        } else {
            if (activeIntervals <= 0L) {
                throw new IllegalStateException("active interval count underflow");
            }
            activeIntervals--;
        }
    }

    private final class RuntimeLease implements Lease {
        private final WorkKind kind;
        private boolean closed;

        private RuntimeLease(WorkKind kind) {
            this.kind = kind;
        }

        @Override
        public void close() {
            synchronized (UpgradeRuntimeCoordinator.this) {
                if (closed) return;
                release(kind);
                closed = true;
            }
        }
    }
}
