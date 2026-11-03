package io.rowweave.cluster.registry;

import java.util.List;

/**
 * Service registry abstraction: the seam for dynamic worker arrival and departure plus Master election.
 * The engine core depends only on this interface, with {@link InProcessRegistry} as the default (pure memory, no external dependency, narrow dependency boundary).
 * A deployment may plug in {@code ZkServiceRegistry} (Curator EPHEMERAL lease plus watch) without changing the interface; core itself ships only InProcess and leaves the seam open.
 *
 * <p>It carries two things: worker registration and discovery (register, renew, deregister, liveWorkers - a Master-owned implementation
 * writes on the Master's behalf, while a session-lease implementation is held by the Worker itself, so that two masters share one worker
 * view and the truth about a death is not tied to the leader); and the Master election primitive {@link #tryClaimExclusive}.
 */
public interface ServiceRegistry {

    /**
     * Whether a worker resource record must be held by the Worker's own registry session.
 *
     * <p>The default false preserves the existing contract for InProcess and third-party implementations: the Master writes the shared registry after receiving REGISTER_WORKER.
     * A ZK EPHEMERAL node must return true, otherwise the node would wrongly belong to the Master's session and would not disappear when the Worker crashes.
     */
    default boolean workerRegistrationOwnedByWorker() {
        return false;
    }

    /** Registers a worker's resource information (idempotent: the same {@code workerId} overwrites, which is a re-register or re-report). Triggers an ADDED watch. */
    void register(RegistryWorker worker);

    /**
     * Atomic exclusive claim (the Master election primitive): registers and returns true only when {@code worker.workerId()} is currently unclaimed (triggering ADDED);
     * when it is already held, nothing is overwritten and false is returned. Used to CAS the leader role (InProcess = putIfAbsent; ZK = an EPHEMERAL create fails precisely because it is held).
     * The opposite of {@link #register}'s idempotent overwrite: this is winner-takes-all.
     */
    boolean tryClaimExclusive(RegistryWorker worker);

    /** Periodic resource report that doubles as a renewal (keep-alive); an id that was never registered behaves as register. Triggers an UPDATED watch. */
    void report(RegistryWorker worker);

    /** Renew (refresh the lease for keep-alive); a no-op for an unregistered id. Lease and expiry logic belong to the implementation. */
    void renew(String workerId);

    /** Deregisters a worker (exit or lease expiry reclaim); a no-op when absent. Triggers a REMOVED watch. */
    void deregister(String workerId);

    /** Snapshot of currently active workers, including election role records. */
    List<RegistryWorker> liveWorkers();

    /**
     * Point lookup of the current resource record by logical id; returns null when absent.
     * The default scans the snapshot to keep third-party implementations source-compatible; a built-in registry must override this with a native O(1) or single-node read.
     */
    default RegistryWorker worker(String workerId) {
        for (RegistryWorker worker : liveWorkers()) {
            if (worker.workerId().equals(workerId)) {
                return worker;
            }
        }
        return null;
    }

    /** Registers an arrival and departure listener (register -> ADDED, report -> UPDATED, deregister -> REMOVED). Several may be registered. */
    void watch(WatchListener listener);

    /** Event kinds for arrival, departure and resource change. */
    enum EventType { ADDED, UPDATED, REMOVED }

    /** One such event: its kind plus the worker involved. */
    final class WatchEvent {
        private final EventType type;
        private final RegistryWorker worker;

        public WatchEvent(EventType type, RegistryWorker worker) {
            this.type = type;
            this.worker = worker;
        }

        public EventType type() { return type; }
        public RegistryWorker worker() { return worker; }
    }

    /** Callback for arrival and departure events. */
    interface WatchListener {
        void onEvent(WatchEvent event);
    }
}
