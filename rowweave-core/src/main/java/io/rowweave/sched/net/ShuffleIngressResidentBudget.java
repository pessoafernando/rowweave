package io.rowweave.sched.net;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Budget for the decoded-but-not-drained payload and Frame ownership of one WorkerDataServer. */
final class ShuffleIngressResidentBudget {

    static final long CONNECTION_SCRATCH_BYTES = 64L * 1024L;

    /**
     * One conservative charge covering the Frame, its UUID, the Reservation, the byte[] header
     * and the ArrayDeque reference. Accounting only for payload or wire bytes would let very
     * small frames create a large number of heap objects for a tiny recorded total.
     */
    private static final long FRAME_HEAP_OVERHEAD_BYTES = 128L;

    private final long limitBytes;
    private final AtomicLong usedBytes = new AtomicLong();
    private final AtomicLong peakBytes = new AtomicLong();
    private final AtomicBoolean admissionClosed = new AtomicBoolean();

    ShuffleIngressResidentBudget(long limitBytes) {
        if (limitBytes <= 0L) {
            throw new IllegalArgumentException("shuffle ingress resident budget must be positive");
        }
        this.limitBytes = limitBytes;
    }

    static ShuffleIngressResidentBudget forTesting(long limitBytes) {
        return new ShuffleIngressResidentBudget(limitBytes);
    }

    static long minimumSingleFrameBytes() {
        return accountedBytes(FrameCodec.HEADER_BYTES + (long) FrameCodec.MAX_FRAME_BYTES);
    }

    static long accountedFrameBytes(long wireBytes) {
        return accountedBytes(wireBytes);
    }

    private static long accountedBytes(long wireBytes) {
        if (wireBytes < 0L || wireBytes > Long.MAX_VALUE - FRAME_HEAP_OVERHEAD_BYTES) {
            throw new IllegalArgumentException("invalid shuffle ingress wire reservation: "
                    + wireBytes);
        }
        return wireBytes + FRAME_HEAP_OVERHEAD_BYTES;
    }

    Reservation reserveBeforePayloadCopy(long wireBytes) {
        return reserve(accountedBytes(wireBytes));
    }

    Reservation reserveConnectionScratch() {
        return reserve(CONNECTION_SCRATCH_BYTES);
    }

    private Reservation reserve(long bytes) {
        if (admissionClosed.get()) {
            throw new IllegalStateException("shuffle ingress resident budget is closed");
        }
        while (true) {
            long current = usedBytes.get();
            if (bytes > limitBytes - current) {
                throw new IllegalStateException("shuffle ingress resident budget exhausted: requested="
                        + bytes + " used=" + current + " limit=" + limitBytes);
            }
            long next = current + bytes;
            if (usedBytes.compareAndSet(current, next)) {
                updatePeak(next);
                return new Reservation(this, bytes);
            }
        }
    }

    Snapshot snapshot() {
        return new Snapshot(limitBytes, usedBytes.get(), peakBytes.get(), admissionClosed.get());
    }

    private void release(long bytes) {
        while (true) {
            long current = usedBytes.get();
            if (bytes > current) {
                admissionClosed.set(true);
                return;
            }
            if (usedBytes.compareAndSet(current, current - bytes)) return;
        }
    }

    private void updatePeak(long candidate) {
        long current = peakBytes.get();
        while (candidate > current && !peakBytes.compareAndSet(current, candidate)) {
            current = peakBytes.get();
        }
    }

    static final class Reservation {
        private final ShuffleIngressResidentBudget owner;
        private final long bytes;
        private final AtomicBoolean released = new AtomicBoolean();

        private Reservation(ShuffleIngressResidentBudget owner, long bytes) {
            this.owner = owner;
            this.bytes = bytes;
        }

        void release() {
            if (released.compareAndSet(false, true)) owner.release(bytes);
        }
    }

    static final class Snapshot {
        private final long limitBytes;
        private final long usedBytes;
        private final long peakBytes;
        private final boolean admissionClosed;

        private Snapshot(long limitBytes, long usedBytes, long peakBytes,
                         boolean admissionClosed) {
            this.limitBytes = limitBytes;
            this.usedBytes = usedBytes;
            this.peakBytes = peakBytes;
            this.admissionClosed = admissionClosed;
        }

        long limitBytes() { return limitBytes; }
        long usedBytes() { return usedBytes; }
        long peakBytes() { return peakBytes; }
        boolean admissionClosed() { return admissionClosed; }
    }
}
