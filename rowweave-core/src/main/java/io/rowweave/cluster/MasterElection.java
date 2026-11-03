package io.rowweave.cluster;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;

/**
 * Master leader/standby election: several Master instances race to claim the well-known role id
 * {@link MasterServer#MASTER_LEADER_ROLE_ID} into the registry with **a CAS exclusive claim**
 * ({@link ServiceRegistry#tryClaimExclusive}), so exactly one wins the leadership and the rest stay
 * standby. A standby watches for the leader role's REMOVED event (leader close or crash → registry
 * deregister, or a ZK session expiry deleting the node) and re-claims to take over.
 *
 * <p>Fail-fast with a seamless handover: the leader disappears, a standby wins the claim, and the
 * client-side {@code MasterLocator} points at the new leader on its next locate. **No job state is
 * migrated**, which keeps the fail-fast contract intact: jobs in flight when leadership changes fail, and
 * the caller resubmits them idempotently.
 *
 * <p>This class publishes gain/loss and a term token per lease; the HA production path must go through
 * {@link MasterServer#bindLeadership(MasterElection)} to bind leadership to request admission, in-flight
 * failure propagation and Worker assignment fencing.
 */
public final class MasterElection implements AutoCloseable {

    private static final org.slf4j.Logger LOG =
            org.slf4j.LoggerFactory.getLogger(MasterElection.class);

    /** Well-known id of the leader role in the registry, reusing the constant from MasterServer, since scheduling must exclude records of that role. */
    public static final String MASTER_LEADER_ID = MasterServer.MASTER_LEADER_ROLE_ID;

    private final ServiceRegistry registry;
    private final Endpoint self;
    private final io.rowweave.cluster.rpc.RpcSecurityMetadata securityMetadata;
    private final java.util.concurrent.CopyOnWriteArrayList<java.util.function.Consumer<Boolean>> listeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    private volatile boolean leader;
    private volatile long termToken;
    private volatile boolean closed;

    public MasterElection(ServiceRegistry registry, Endpoint self) {
        this(registry, self, null);
    }

    public MasterElection(ServiceRegistry registry, Endpoint self,
                          io.rowweave.cluster.rpc.RpcSecurityMetadata securityMetadata) {
        this.registry = registry;
        this.self = self;
        this.securityMetadata = securityMetadata;
    }

    /** Joins the election: watches for the leader role being REMOVED so it can take over, then tries to claim it once right away. */
    public void start() {
        registry.watch(event -> {
            if (event.type() == ServiceRegistry.EventType.REMOVED
                    && MASTER_LEADER_ID.equals(event.worker().workerId())) {
                onLeaderRoleRemoved();
            }
        });
        tryElect();
    }

    /** Observes gain and loss; callbacks run outside the election lock. */
    public void addLeadershipListener(java.util.function.Consumer<Boolean> listener) {
        listeners.add(java.util.Objects.requireNonNull(listener, "listener"));
    }

    /** Our own role can be deleted when a session expires: step down first, then be re-elected under a new term. */
    private void onLeaderRoleRemoved() {
        boolean lost;
        synchronized (this) {
            if (closed) {
                return;
            }
            lost = leader;
            if (lost) {
                leader = false;
                termToken = 0L;
            }
        }
        if (lost) {
            notifyLeadership(false);
        }
        try {
            tryElect();
        } catch (RuntimeException reelectionFailure) {
            // When a session expires the connection may be briefly unusable; stay a follower and let the same watch event notify workers and standbys.
            LOG.warn("master reelection failed after leadership loss endpoint={}", self, reelectionFailure);
        }
    }

    /** Claims the leader role with compare-and-set; every attempt uses a fresh 64-bit term nonce. */
    private void tryElect() {
        boolean gained = false;
        synchronized (this) {
            if (closed || leader) {
                return;
            }
            long candidate = nextTermToken();
            RegistryWorker role = new RegistryWorker(MASTER_LEADER_ID, self.host(), self.port(),
                    high(candidate), low(candidate), null, securityMetadata);
            if (registry.tryClaimExclusive(role)) {
                termToken = candidate;
                leader = true;
                gained = true;
            } else if (securityMetadata != null) {
                RegistryWorker active = registry.worker(MASTER_LEADER_ID);
                if (active != null && !securityMetadata.equals(active.securityMetadata())) {
                    throw new SecurityException(
                            "active master leader deployment/authz identity mismatch");
                }
            }
        }
        if (gained) {
            notifyLeadership(true);
        }
    }

    private static long nextTermToken() {
        long token;
        do {
            token = java.util.concurrent.ThreadLocalRandom.current().nextLong();
        } while (token == 0L);
        return token;
    }

    static int high(long token) { return (int) (token >>> 32); }
    static int low(long token) { return (int) token; }
    static long termToken(RegistryWorker role) {
        return ((long) role.slots() << 32) | (role.dataPort() & 0xffffffffL);
    }

    private void notifyLeadership(boolean active) {
        for (java.util.function.Consumer<Boolean> listener : listeners) {
            try {
                listener.accept(active);
            } catch (RuntimeException ignored) {
                // One control plane observer failing must not block the registry watch or the remaining observers.
            }
        }
    }

    public boolean isLeader() {
        return leader;
    }

    public Endpoint self() {
        return self;
    }

    /** The current leader term, or zero for anyone who is not the leader. */
    public long termToken() {
        return termToken;
    }

    /** Leaves the election: a leader releases the role, which deregisters it so that REMOVED lets a standby take over; idempotent. */
    @Override
    public void close() {
        boolean lost;
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            lost = leader;
            leader = false;
            termToken = 0L;
        }
        if (lost) {
            notifyLeadership(false);
            registry.deregister(MASTER_LEADER_ID);
        }
    }
}
