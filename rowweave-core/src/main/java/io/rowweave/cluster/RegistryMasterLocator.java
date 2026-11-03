package io.rowweave.cluster;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Resolves the master leader from the registry. A leader registers itself under the role id
 * {@link MasterServer#MASTER_LEADER_ROLE_ID} in a {@link ServiceRegistry} (see {@link
 * MasterElection}), and every call to {@link #locateLeader()} reads the record under that fixed
 * id, so once a leader fails over, with the old leader releasing the role and a standby
 * claiming the same id, the next call sees the new address. Reading one record by id also means
 * churn among unrelated workers never yields a partial snapshot. With no such record the lookup
 * fails fast with {@link IllegalStateException}.<p>Change is noticed rather than polled: {@link
 * #addLeaderChangeListener} lets a worker or client subscribe to the leader record being added,
 * updated or removed, so a failover is acted on at once instead of waiting for the next
 * heartbeat or submit cycle. {@link #locateLeader()} remains the authority, since a watch is
 * only a notification, and even a missed event costs correctness nothing because the next
 * resolution sees the truth. The subscription on the registry is created for the first listener
 * only, so a locator nobody listens to costs nothing.
 */
public final class RegistryMasterLocator implements MasterLocator {

    /** The endpoint and the term in its exclusive role record must come from one registry snapshot. */
    static final class LeaderLease {
        final Endpoint endpoint;
        final long termToken;

        LeaderLease(Endpoint endpoint, long termToken) {
            this.endpoint = endpoint;
            this.termToken = termToken;
        }
    }

    private final ServiceRegistry registry;
    private final io.rowweave.cluster.rpc.RpcSecurityMetadata expectedSecurityMetadata;
    private static final long SUBSCRIPTION_CLOSE_TIMEOUT_MILLIS = 5_000L;

    private final CopyOnWriteArrayList<ListenerRegistration> leaderChangeListeners =
            new CopyOnWriteArrayList<>();
    private volatile boolean watching;

    /** Independently removable listener ownership; the underlying registry remains borrowed. */
    public interface LeaderChangeSubscription extends AutoCloseable {
        @Override
        void close();
    }

    private final class ListenerRegistration implements LeaderChangeSubscription {
        private final Runnable listener;
        private final Object lifecycle = new Object();
        private final ThreadLocal<Boolean> invoking = new ThreadLocal<>();
        private boolean active = true;
        private int inFlight;

        private ListenerRegistration(Runnable listener) {
            this.listener = listener;
        }

        private void fire() {
            synchronized (lifecycle) {
                if (!active) return;
                inFlight++;
            }
            invoking.set(Boolean.TRUE);
            try {
                listener.run();
            } finally {
                invoking.remove();
                synchronized (lifecycle) {
                    inFlight--;
                    lifecycle.notifyAll();
                }
            }
        }

        @Override
        public void close() {
            synchronized (lifecycle) {
                if (!active) return;
                active = false;
            }
            leaderChangeListeners.remove(this);
            if (Boolean.TRUE.equals(invoking.get())) return;

            boolean interrupted = false;
            long deadline = System.nanoTime()
                    + java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(
                    SUBSCRIPTION_CLOSE_TIMEOUT_MILLIS);
            synchronized (lifecycle) {
                while (inFlight > 0 && System.nanoTime() < deadline) {
                    try {
                        long remainMillis = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                Math.max(1L, deadline - System.nanoTime()));
                        lifecycle.wait(remainMillis);
                    } catch (InterruptedException interruption) {
                        interrupted = true;
                        break;
                    }
                }
                if (interrupted) Thread.currentThread().interrupt();
                if (inFlight > 0) {
                    throw new IllegalStateException(
                            "leader listener callback did not stop within "
                                    + SUBSCRIPTION_CLOSE_TIMEOUT_MILLIS + "ms");
                }
            }
        }
    }

    public RegistryMasterLocator(ServiceRegistry registry) {
        this(registry, null);
    }

    public RegistryMasterLocator(
            ServiceRegistry registry,
            io.rowweave.cluster.rpc.RpcSecurityMetadata expectedSecurityMetadata) {
        this.registry = registry;
        this.expectedSecurityMetadata = expectedSecurityMetadata;
    }

    /** Lets a worker in this package hold its own lease on the session used to locate the leader; not part of the public API. */
    ServiceRegistry workerLeaseRegistry() {
        return registry.workerRegistrationOwnedByWorker() ? registry : null;
    }

    @Override
    public Endpoint locateLeader() {
        return locateLeaderLease().endpoint;
    }

    LeaderLease locateLeaderLease() {
        RegistryWorker leader = registry.worker(MasterServer.MASTER_LEADER_ROLE_ID);
        if (leader != null) {
            if (expectedSecurityMetadata != null
                    && !expectedSecurityMetadata.equals(leader.securityMetadata())) {
                throw new SecurityException("master leader deployment/authz identity mismatch");
            }
            return new LeaderLease(new Endpoint(leader.host(), leader.port()), MasterElection.termToken(leader));
        }
        throw new IllegalStateException(
                "no active master leader registered (role id=" + MasterServer.MASTER_LEADER_ROLE_ID + ")");
    }

    /**
     * Registers a leader change callback. It fires when the role record for the leader, whose
     * id is {@link MasterServer#MASTER_LEADER_ROLE_ID}, is added, updated or removed, which is
     * how a worker or client evicts the connection to the old leader and reconnects to the new
     * one. Callbacks run on the registry's watch dispatch thread, synchronously in process and
     * on the Curator thread under ZooKeeper, so they must be short, non-blocking, and swallow
     * their own exceptions. The first call subscribes lazily under a double-checked guard.
     */
    public void addLeaderChangeListener(Runnable onLeaderChange) {
        subscribeLeaderChanges(onLeaderChange);
    }

    /**
     * Adds a listener with explicit ownership. Closing the returned handle is
     * linearized before subsequent callbacks and does not close the registry.
     */
    public synchronized LeaderChangeSubscription subscribeLeaderChanges(
            Runnable onLeaderChange) {
        if (onLeaderChange == null) {
            throw new IllegalArgumentException("leader change listener is required");
        }
        ListenerRegistration registration = new ListenerRegistration(onLeaderChange);
        leaderChangeListeners.add(registration);
        if (!watching) {
            watching = true;
            try {
                registry.watch(event -> {
                    if (MasterServer.MASTER_LEADER_ROLE_ID.equals(
                            event.worker().workerId())) {
                        fireLeaderChange();
                    }
                });
            } catch (RuntimeException | Error watchFailure) {
                watching = false;
                leaderChangeListeners.remove(registration);
                synchronized (registration.lifecycle) {
                    registration.active = false;
                }
                throw watchFailure;
            }
        }
        return registration;
    }

    private void fireLeaderChange() {
        for (ListenerRegistration registration : leaderChangeListeners) {
            try {
                registration.fire();
            } catch (RuntimeException ignored) {
                // One listener throwing must neither affect the others nor poison the registry watch dispatch thread.
            }
        }
    }

    /** Package-private lifecycle diagnostic used by construction/close tests. */
    int leaderChangeListenerCount() {
        return leaderChangeListeners.size();
    }
}
