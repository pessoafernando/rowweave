package io.rowweave.stream.cluster;

import java.util.function.Supplier;

/** Swaps the coordinator-fenced run factory whenever one process regains leadership. */
public final class LeadershipBoundStreamProductRunFactory
        implements StreamProductRunFactory {
    private final Supplier<? extends StreamProductRunFactory> factory;
    private volatile StreamProductRunFactory delegate;
    private boolean startupReservation = true;

    public LeadershipBoundStreamProductRunFactory(
            StreamProductRunFactory initial,
            Supplier<? extends StreamProductRunFactory> factory) {
        if (initial == null || factory == null) {
            throw new IllegalArgumentException(
                    "initial and replacement Stream run factories are required");
        }
        this.delegate = initial;
        this.factory = factory;
    }

    /** Uses the startup term once and reserves a new term after any later regain. */
    public synchronized void activate() {
        if (startupReservation) {
            startupReservation = false;
            return;
        }
        StreamProductRunFactory replacement = factory.get();
        if (replacement == null) {
            throw new IllegalStateException(
                    "replacement Stream run factory is unavailable");
        }
        delegate = replacement;
    }

    @Override
    public StreamProductRun create(StreamProductJobSnapshot snapshot) {
        return delegate.create(snapshot);
    }

    @Override
    public StreamProductRun restart(StreamProductJobSnapshot snapshot,
            Throwable failure) {
        return delegate.restart(snapshot, failure);
    }

    @Override
    public StreamProductDecisionConvergence convergeDurableDecision(
            StreamProductJobSnapshot snapshot) {
        return delegate.convergeDurableDecision(snapshot);
    }

    @Override
    public void retire(StreamProductJobSnapshot snapshot) {
        delegate.retire(snapshot);
    }
}
