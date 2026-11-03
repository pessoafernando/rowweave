package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlResidentLease;
import io.rowweave.cluster.rpc.ControlResidentOwner;
import io.rowweave.run.RunAttemptFence;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * source-side assignment registry. A cohort root is charged to the
 * process control resident budget before its bitmap/root objects are published and
 * remains attempt-owned after individual assignment handles disappear.
 */
public final class OutboundShuffleCohortRegistry implements AutoCloseable {
    static final long ROOT_RESIDENT_BYTES = 512L;
    static final long ORDINAL_RESIDENT_BYTES = 64L;

    private final ControlResidentBudget residentBudget;
    private final Map<CohortKey, Root> roots = new HashMap<>();
    private boolean closed;

    public OutboundShuffleCohortRegistry(ControlResidentBudget residentBudget) {
        if (residentBudget == null) {
            throw new IllegalArgumentException("control resident budget is required");
        }
        this.residentBudget = residentBudget;
    }

    static long residentCharge(int expectedOrdinalCount) {
        if (expectedOrdinalCount < 1
                || expectedOrdinalCount > ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET) {
            throw new IllegalArgumentException("invalid expected ordinal count");
        }
        return Math.addExact(ROOT_RESIDENT_BYTES,
                Math.multiplyExact(ORDINAL_RESIDENT_BYTES, expectedOrdinalCount));
    }

    /** Atomically registers the distinct authorized routes carried by one assignment. */
    public synchronized PreparedHandle prepare(
            String assignmentId, RunAttemptFence fence,
            List<ShuffleEndpoint> endpoints) {
        requireOpen();
        if (assignmentId == null || assignmentId.isEmpty() || fence == null
                || endpoints == null) {
            throw new IllegalArgumentException(
                    "assignment id, attempt fence and endpoints are required");
        }
        List<Registration> added = new ArrayList<>();
        Set<RouteKey> commandRoutes = new HashSet<>();
        try {
            for (ShuffleEndpoint endpoint : endpoints) {
                Descriptor descriptor = Descriptor.from(fence, endpoint);
                RouteKey route = new RouteKey(descriptor.cohortId,
                        descriptor.producerOrdinal);
                if (!commandRoutes.add(route)) {
                    continue; // aliases in one immutable command are one route
                }
                Root root = find(descriptor.cohortId);
                boolean created = false;
                if (root == null) {
                    root = newRoot(descriptor);
                    roots.put(root.key, root);
                    created = true;
                } else {
                    root.requireCompatible(descriptor);
                }
                if (root.registered.get(descriptor.producerOrdinal)) {
                    throw new IllegalStateException(
                            "outbound shuffle ordinal is already registered");
                }
                root.registered.set(descriptor.producerOrdinal);
                added.add(new Registration(root, descriptor.producerOrdinal, created));
            }
            return new PreparedHandle();
        } catch (RuntimeException | Error failure) {
            rollback(added);
            throw failure;
        }
    }

    /** Claims a previously assignment-registered ordinal exactly once. */
    public synchronized StreamHandle open(ShuffleEndpoint endpoint) {
        requireOpen();
        if (endpoint == null || endpoint.cohortId() == null) {
            throw new IllegalArgumentException("authorized shuffle endpoint is required");
        }
        byte[] cohortId = endpoint.cohortId();
        Root root = find(cohortId);
        if (root == null) {
            throw new IllegalStateException(
                    "outbound shuffle cohort was not prepared by assignment");
        }
        root.requireCompatible(Descriptor.from(root.fence, endpoint));
        int ordinal = endpoint.producerOrdinal();
        if (!root.registered.get(ordinal)) {
            throw new IllegalStateException(
                    "outbound shuffle ordinal was not prepared by assignment");
        }
        if (root.opened.get(ordinal) || root.terminal.get(ordinal)) {
            throw new IllegalStateException(
                    "authorized shuffle route is already opened");
        }
        root.opened.set(ordinal);
        return new StreamHandle(root, ordinal);
    }

    public synchronized List<byte[]> cancelAttempt(RunAttemptFence fence) {
        if (fence == null) return java.util.Collections.emptyList();
        List<Root> cancelled = new ArrayList<>();
        for (Root root : roots.values()) {
            if (root.fence.equals(fence)) cancelled.add(root);
        }
        List<byte[]> cohortIds = new ArrayList<>(cancelled.size());
        for (Root root : cancelled) cohortIds.add(root.key.value.clone());
        for (Root root : cancelled) retire(root);
        return java.util.Collections.unmodifiableList(cohortIds);
    }

    synchronized int cohortCount() {
        return roots.size();
    }

    synchronized boolean isClosed() {
        return closed;
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        List<Root> snapshot = new ArrayList<>(roots.values());
        for (Root root : snapshot) retire(root);
    }

    private Root newRoot(Descriptor descriptor) {
        ControlResidentLease lease = residentBudget.openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            lease.reserve(owner, residentCharge(descriptor.expectedOrdinalCount));
            owner = lease.transfer(owner, ControlResidentOwner.HANDLER_OWNED);
            owner = lease.transfer(owner, ControlResidentOwner.ASSIGNMENT_OWNED);
            return new Root(descriptor, lease, owner);
        } catch (RuntimeException | Error failure) {
            lease.release(owner);
            throw failure;
        }
    }

    private Root find(byte[] cohortId) {
        for (Map.Entry<CohortKey, Root> entry : roots.entrySet()) {
            if (Arrays.equals(entry.getKey().value, cohortId)) return entry.getValue();
        }
        return null;
    }

    private void rollback(List<Registration> added) {
        for (int i = added.size() - 1; i >= 0; i--) {
            Registration registration = added.get(i);
            registration.root.registered.clear(registration.ordinal);
            if (registration.created
                    && registration.root.registered.isEmpty()
                    && registration.root.opened.isEmpty()) {
                retire(registration.root);
            }
        }
    }

    private void completeIfPossible(Root root) {
        if (!root.retired
                && root.terminal.cardinality() == root.expectedOrdinalCount
                && root.outstandingWireBytes == 0L) {
            retire(root);
        }
    }

    private void retire(Root root) {
        if (root.retired) return;
        root.retired = true;
        roots.remove(root.key, root);
        root.lease.release(root.owner);
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("outbound shuffle cohort registry closed");
    }

    /** Assignment completion does not own or retire the process-level cohort root. */
    public static final class PreparedHandle implements AutoCloseable {
        private boolean closed;

        @Override
        public synchronized void close() {
            closed = true;
        }

        public synchronized boolean isClosed() {
            return closed;
        }
    }

    public final class StreamHandle implements AutoCloseable {
        private final Root root;
        private final int ordinal;
        private boolean terminal;

        private StreamHandle(Root root, int ordinal) {
            this.root = root;
            this.ordinal = ordinal;
        }

        void requireActive() {
            synchronized (OutboundShuffleCohortRegistry.this) {
                if (root.retired || terminal) {
                    throw new IllegalStateException(
                            "outbound shuffle stream is terminal");
                }
            }
        }

        public void recordSent(long wireBytes) {
            if (wireBytes <= 0L) {
                throw new IllegalArgumentException("record wire bytes must be positive");
            }
            synchronized (OutboundShuffleCohortRegistry.this) {
                if (root.retired || terminal) {
                    throw new IllegalStateException("outbound shuffle stream is terminal");
                }
                root.outstandingWireBytes = Math.addExact(
                        root.outstandingWireBytes, wireBytes);
            }
        }

        /** May run after EOS because the consumer can release the final row later. */
        public void recordReturned(long wireBytes) {
            if (wireBytes <= 0L) {
                throw new IllegalArgumentException("returned wire bytes must be positive");
            }
            synchronized (OutboundShuffleCohortRegistry.this) {
                if (root.retired) return; // attempt cancellation owns late callback isolation
                if (wireBytes > root.outstandingWireBytes) {
                    throw new IllegalStateException(
                            "returned shuffle credit exceeds cohort ownership");
                }
                root.outstandingWireBytes -= wireBytes;
                completeIfPossible(root);
            }
        }

        @Override
        public void close() {
            synchronized (OutboundShuffleCohortRegistry.this) {
                if (terminal || root.retired) return;
                terminal = true;
                root.terminal.set(ordinal);
                completeIfPossible(root);
            }
        }
    }

    private static final class Root {
        final CohortKey key;
        final RunAttemptFence fence;
        final UUID targetId;
        final String host;
        final int port;
        final WorkerDataIdentity destinationIdentity;
        final int expectedOrdinalCount;
        final BitSet registered;
        final BitSet opened;
        final BitSet terminal;
        final ControlResidentLease lease;
        final ControlResidentLease.OwnerToken owner;
        long outstandingWireBytes;
        boolean retired;

        Root(Descriptor descriptor, ControlResidentLease lease,
             ControlResidentLease.OwnerToken owner) {
            this.key = new CohortKey(descriptor.cohortId);
            this.fence = descriptor.fence;
            this.targetId = descriptor.targetId;
            this.host = descriptor.host;
            this.port = descriptor.port;
            this.destinationIdentity = descriptor.destinationIdentity;
            this.expectedOrdinalCount = descriptor.expectedOrdinalCount;
            this.registered = new BitSet(ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET + 1);
            this.opened = new BitSet(ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET + 1);
            this.terminal = new BitSet(ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET + 1);
            this.lease = lease;
            this.owner = owner;
        }

        void requireCompatible(Descriptor descriptor) {
            if (retired || !fence.equals(descriptor.fence)
                    || !targetId.equals(descriptor.targetId)
                    || !host.equals(descriptor.host) || port != descriptor.port
                    || !destinationIdentity.equals(descriptor.destinationIdentity)
                    || expectedOrdinalCount != descriptor.expectedOrdinalCount) {
                throw new IllegalStateException(
                        "outbound shuffle cohort descriptor conflict");
            }
        }
    }

    private static final class Descriptor {
        final byte[] cohortId;
        final RunAttemptFence fence;
        final UUID targetId;
        final String host;
        final int port;
        final WorkerDataIdentity destinationIdentity;
        final int producerOrdinal;
        final int expectedOrdinalCount;

        private Descriptor(byte[] cohortId, RunAttemptFence fence, UUID targetId,
                           String host, int port,
                           WorkerDataIdentity destinationIdentity,
                           int producerOrdinal, int expectedOrdinalCount) {
            this.cohortId = cohortId;
            this.fence = fence;
            this.targetId = targetId;
            this.host = host;
            this.port = port;
            this.destinationIdentity = destinationIdentity;
            this.producerOrdinal = producerOrdinal;
            this.expectedOrdinalCount = expectedOrdinalCount;
        }

        static Descriptor from(RunAttemptFence fence, ShuffleEndpoint endpoint) {
            if (fence == null || endpoint == null
                    || endpoint.producerGrant() == null
                    || endpoint.destinationIdentity() == null
                    || endpoint.cohortId() == null) {
                throw new IllegalArgumentException(
                        "attempt fence and authorized endpoint are required");
            }
            ShuffleProducerGrant.requireOrdinal(endpoint.producerOrdinal());
            residentCharge(endpoint.expectedOrdinalCount());
            return new Descriptor(endpoint.cohortId(), fence, endpoint.targetId(),
                    endpoint.host(), endpoint.port(), endpoint.destinationIdentity(),
                    endpoint.producerOrdinal(), endpoint.expectedOrdinalCount());
        }
    }

    private static final class Registration {
        final Root root;
        final int ordinal;
        final boolean created;

        Registration(Root root, int ordinal, boolean created) {
            this.root = root;
            this.ordinal = ordinal;
            this.created = created;
        }
    }

    private static final class CohortKey {
        final byte[] value;

        CohortKey(byte[] value) {
            this.value = value.clone();
        }

        @Override
        public boolean equals(Object other) {
            return this == other || other instanceof CohortKey
                    && Arrays.equals(value, ((CohortKey) other).value);
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(value);
        }
    }

    private static final class RouteKey {
        final byte[] cohortId;
        final int ordinal;

        RouteKey(byte[] cohortId, int ordinal) {
            this.cohortId = cohortId.clone();
            this.ordinal = ordinal;
        }

        @Override
        public boolean equals(Object other) {
            return this == other || other instanceof RouteKey
                    && ordinal == ((RouteKey) other).ordinal
                    && Arrays.equals(cohortId, ((RouteKey) other).cohortId);
        }

        @Override
        public int hashCode() {
            return 31 * Arrays.hashCode(cohortId) + ordinal;
        }
    }
}
