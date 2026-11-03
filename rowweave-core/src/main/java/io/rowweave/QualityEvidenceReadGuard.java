package io.rowweave;

import java.io.IOException;
import java.nio.channels.InterruptibleChannel;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

/**
 * caller-owned finite deadline/cancellation scope for prepared evidence reads.
 * The caller's existing deadline owner may terminate this scope asynchronously; no timer or
 * executor is created here.
 */
public final class QualityEvidenceReadGuard {
    private static final long MAX_FUTURE_NANOS = TimeUnit.HOURS.toNanos(24L);

    public enum Termination {
        CANCELLED,
        DEADLINE_EXCEEDED
    }

    private final long deadlineNanos;
    private final BooleanSupplier cancellationProbe;
    private final AtomicReference<Termination> termination = new AtomicReference<>();
    private final AtomicReference<InterruptibleChannel> channel = new AtomicReference<>();

    private QualityEvidenceReadGuard(
            long deadlineNanos, BooleanSupplier cancellationProbe) {
        this.deadlineNanos = deadlineNanos;
        this.cancellationProbe = Objects.requireNonNull(
                cancellationProbe, "cancellationProbe");
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining > MAX_FUTURE_NANOS) {
            throw new IllegalArgumentException(
                    "quality evidence deadline must be finite and within 24 hours");
        }
    }

    /** Absolute deadline from the same monotonic {@link System#nanoTime()} clock. */
    public static QualityEvidenceReadGuard until(
            long deadlineNanos, BooleanSupplier cancellationProbe) {
        return new QualityEvidenceReadGuard(deadlineNanos, cancellationProbe);
    }

    /** Called by the owner of the existing cancellation scope. */
    public void cancel() {
        terminate(Termination.CANCELLED);
    }

    /** Called by the owner of the existing deadline alarm. */
    public void deadlineExceeded() {
        terminate(Termination.DEADLINE_EXCEEDED);
    }

    void register(InterruptibleChannel openedChannel) {
        Objects.requireNonNull(openedChannel, "openedChannel");
        check();
        if (!channel.compareAndSet(null, openedChannel)) {
            throw new IllegalStateException(
                    "quality evidence guard already has an open channel");
        }
        try {
            check();
        } catch (RuntimeException terminated) {
            channel.compareAndSet(openedChannel, null);
            try {
                openedChannel.close();
            } catch (IOException ignored) {
                // Preserve the stable termination raised by check().
            }
            throw terminated;
        }
    }

    void unregister(InterruptibleChannel openedChannel) {
        if (openedChannel != null) channel.compareAndSet(openedChannel, null);
    }

    void check() {
        if (cancellationProbe.getAsBoolean()) {
            terminate(Termination.CANCELLED);
        }
        Termination current = termination.get();
        if (current == Termination.CANCELLED) throw new Terminated(current);
        if (deadlineNanos - System.nanoTime() <= 0L) {
            terminate(Termination.DEADLINE_EXCEEDED);
        }
        current = termination.get();
        if (current != null) throw new Terminated(current);
    }

    private void terminate(Termination requested) {
        while (true) {
            Termination current = termination.get();
            if (current == Termination.CANCELLED
                    || (current == Termination.DEADLINE_EXCEEDED
                    && requested == Termination.DEADLINE_EXCEEDED)) {
                break;
            }
            if (termination.compareAndSet(current, requested)) break;
        }
        InterruptibleChannel opened = channel.get();
        if (opened != null) {
            try {
                opened.close();
            } catch (IOException ignored) {
                // Stable cancellation/deadline classification is more authoritative than close.
            }
        }
    }

    public static final class Terminated extends IllegalStateException {
        private final Termination termination;

        private Terminated(Termination termination) {
            super(termination == Termination.CANCELLED
                    ? "QUALITY_EVIDENCE_READ_CANCELLED"
                    : "QUALITY_EVIDENCE_READ_DEADLINE_EXCEEDED");
            this.termination = termination;
        }

        public Termination termination() {
            return termination;
        }
    }
}
