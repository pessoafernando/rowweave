package io.rowweave.cluster;

import io.rowweave.cluster.registry.RegistryWorker;
import io.rowweave.cluster.registry.ServiceRegistry;
import io.rowweave.cluster.rpc.RpcSecurityMetadata;

import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;

/** Exclusive, session-backed election for a caller-owned registry role ID. */
public final class RegistryRoleElection implements AutoCloseable {
    private final ServiceRegistry registry;
    private final String roleId;
    private final Endpoint self;
    private final RpcSecurityMetadata securityMetadata;
    private final CopyOnWriteArrayList<Consumer<Boolean>> listeners =
            new CopyOnWriteArrayList<>();
    private volatile boolean leader;
    private volatile long termToken;
    private volatile boolean closed;

    public RegistryRoleElection(ServiceRegistry registry, String roleId,
            Endpoint self, RpcSecurityMetadata securityMetadata) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.roleId = roleId(roleId);
        this.self = Objects.requireNonNull(self, "self");
        this.securityMetadata = securityMetadata;
    }

    public void start() {
        registry.watch(event -> {
            if (event.type() == ServiceRegistry.EventType.REMOVED
                    && roleId.equals(event.worker().workerId())) {
                roleRemoved();
            }
        });
        tryElect();
    }

    public void addLeadershipListener(Consumer<Boolean> listener) {
        listeners.add(Objects.requireNonNull(listener, "listener"));
    }

    private void roleRemoved() {
        boolean lost;
        synchronized (this) {
            if (closed) return;
            lost = leader;
            leader = false;
            termToken = 0L;
        }
        if (lost) notifyListeners(false);
        try {
            tryElect();
        } catch (RuntimeException unavailable) {
            // A lost registry session leaves this candidate a follower. A
            // later registry watch/session recovery may trigger another try.
        }
    }

    private void tryElect() {
        boolean gained = false;
        synchronized (this) {
            if (closed || leader) return;
            long candidate = nextTerm();
            RegistryWorker role = new RegistryWorker(roleId, self.host(),
                    self.port(), high(candidate), low(candidate), null,
                    securityMetadata);
            if (registry.tryClaimExclusive(role)) {
                leader = true;
                termToken = candidate;
                gained = true;
            } else if (securityMetadata != null) {
                RegistryWorker active = registry.worker(roleId);
                if (active != null && !securityMetadata.equals(
                        active.securityMetadata())) {
                    throw new SecurityException(
                            "active registry role security identity mismatch");
                }
            }
        }
        if (gained) notifyListeners(true);
    }

    public boolean isLeader() { return leader; }
    public long termToken() { return termToken; }
    public Endpoint self() { return self; }
    public String roleId() { return roleId; }

    @Override
    public void close() {
        boolean lost;
        synchronized (this) {
            if (closed) return;
            closed = true;
            lost = leader;
            leader = false;
            termToken = 0L;
        }
        if (lost) {
            notifyListeners(false);
            registry.deregister(roleId);
        }
    }

    private void notifyListeners(boolean active) {
        for (Consumer<Boolean> listener : listeners) {
            try {
                listener.accept(active);
            } catch (RuntimeException ignored) {
                // One observer cannot poison registry watch convergence.
            }
        }
    }

    static long termToken(RegistryWorker role) {
        return ((long) role.slots() << 32)
                | (role.dataPort() & 0xffffffffL);
    }

    private static int high(long value) { return (int) (value >>> 32); }
    private static int low(long value) { return (int) value; }

    private static long nextTerm() {
        long result;
        do {
            result = ThreadLocalRandom.current().nextLong();
        } while (result == 0L);
        return result;
    }

    static String roleId(String value) {
        if (value == null || value.trim().isEmpty() || value.length() > 256) {
            throw new IllegalArgumentException("registry role ID is invalid");
        }
        return value;
    }
}
