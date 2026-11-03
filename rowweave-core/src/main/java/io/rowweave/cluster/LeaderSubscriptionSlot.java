package io.rowweave.cluster;

/** Inert construction-time slot that owns at most one post-init leader subscription. */
final class LeaderSubscriptionSlot implements AutoCloseable {
    private RegistryMasterLocator.LeaderChangeSubscription subscription;
    private boolean closed;

    void install(RegistryMasterLocator.LeaderChangeSubscription candidate) {
        if (candidate == null) {
            throw new IllegalArgumentException("leader subscription is required");
        }
        boolean reject;
        synchronized (this) {
            reject = closed || subscription != null;
            if (!reject) {
                subscription = candidate;
                return;
            }
        }
        IllegalStateException duplicate = closed ? null
                : new IllegalStateException("leader subscription already installed");
        try {
            candidate.close();
        } catch (Throwable cleanupFailure) {
            if (duplicate == null) rethrow(cleanupFailure);
            duplicate.addSuppressed(cleanupFailure);
        }
        if (duplicate != null) throw duplicate;
    }

    @Override
    public void close() {
        RegistryMasterLocator.LeaderChangeSubscription owned;
        synchronized (this) {
            if (closed) return;
            closed = true;
            owned = subscription;
            subscription = null;
        }
        if (owned != null) owned.close();
    }

    private static void rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
        throw new RuntimeException(failure);
    }
}
