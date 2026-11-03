package io.rowweave.cluster.rpc;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Process-wide, non-blocking, fail-closed budget for the control plane. */
public final class ControlResidentBudget {
    private final long limitBytes;
    private final AtomicLong usedBytes = new AtomicLong();
    private final AtomicBoolean admissionClosed = new AtomicBoolean();
    private volatile String invariantError;

    private ControlResidentBudget(long limitBytes) {
        if (limitBytes <= 0L) throw new IllegalArgumentException("resident budget must be positive");
        this.limitBytes = limitBytes;
    }

    /** The only production budget in this JVM: master, worker, client and every server share it. */
    public static ControlResidentBudget processWide() {
        return ProcessWideHolder.INSTANCE;
    }

    public static ControlResidentBudget forTesting(long limitBytes) {
        return new ControlResidentBudget(limitBytes);
    }

    public ControlResidentLease openLease() {
        return openLease(Long.MAX_VALUE);
    }

    ControlResidentLease openLease(long maximumLeaseBytes) {
        if (admissionClosed.get()) {
            throw new IllegalStateException("control resident admission is permanently closed: "
                    + invariantError);
        }
        return new ControlResidentLease(this, maximumLeaseBytes);
    }

    public Snapshot snapshot() {
        return new Snapshot(limitBytes, usedBytes.get(), admissionClosed.get(), invariantError);
    }

    void reserve(long bytes) {
        if (bytes < 0L) throw new IllegalArgumentException("resident reservation must not be negative");
        if (bytes == 0L) return;
        while (true) {
            if (admissionClosed.get()) {
                throw new IllegalStateException("control resident admission is permanently closed: "
                        + invariantError);
            }
            long current = usedBytes.get();
            if (bytes > limitBytes - current) {
                throw new ControlResidentBudgetExceededException(bytes, current, limitBytes);
            }
            if (usedBytes.compareAndSet(current, current + bytes)) return;
        }
    }

    /** The release path must never mask the original failure; once corrupted it closes admission for good. */
    void release(long bytes) {
        if (bytes <= 0L) {
            if (bytes < 0L) closeForInvariant("negative release: " + bytes);
            return;
        }
        while (true) {
            long current = usedBytes.get();
            if (bytes > current) {
                closeForInvariant("resident budget underflow: release=" + bytes + ", used=" + current);
                return;
            }
            if (usedBytes.compareAndSet(current, current - bytes)) return;
        }
    }

    private void closeForInvariant(String error) {
        invariantError = error;
        admissionClosed.set(true);
    }

    private static final class ProcessWideHolder {
        private static final ControlResidentBudget INSTANCE = new ControlResidentBudget(
                ControlResidentConfig.fromSystemProperties().residentBytes());
    }

    public static final class Snapshot {
        private final long limitBytes;
        private final long usedBytes;
        private final boolean admissionClosed;
        private final String invariantError;

        private Snapshot(long limitBytes, long usedBytes, boolean admissionClosed,
                         String invariantError) {
            this.limitBytes = limitBytes;
            this.usedBytes = usedBytes;
            this.admissionClosed = admissionClosed;
            this.invariantError = invariantError;
        }

        public long limitBytes() { return limitBytes; }
        public long usedBytes() { return usedBytes; }
        public boolean admissionClosed() { return admissionClosed; }
        public String invariantError() { return invariantError; }
    }
}
