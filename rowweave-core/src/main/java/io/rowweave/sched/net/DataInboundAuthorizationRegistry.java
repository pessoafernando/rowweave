package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlResidentLease;
import io.rowweave.cluster.rpc.ControlResidentOwner;
import io.rowweave.run.RunAttemptFence;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Attempt-local authorization registry installed synchronously before ASSIGN is acknowledged.
 * It never stores raw grants and never creates or waits on a business inbound target.
 */
public final class DataInboundAuthorizationRegistry implements AutoCloseable {

    public static final class AuthorizationRejectedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        AuthorizationRejectedException() {
            super("shuffle stream authorization rejected");
        }
    }

    private enum StreamState { UNUSED, OPENED, EOS, FAILED }

    private static final class Expected {
        final ShuffleAuthorizationRecord record;
        final OwnerPool ownerPool;
        StreamState state = StreamState.UNUSED;
        long connectionGeneration;
        WorkerDataIdentity peer;

        Expected(ShuffleAuthorizationRecord record, OwnerPool ownerPool) {
            this.record = record;
            this.ownerPool = ownerPool;
        }
    }

    private static final class OwnerPool {
        final WorkerDataIdentity owner;
        final int stableIndex;
        long connectionGeneration;
        boolean initialCreditGranted;

        OwnerPool(WorkerDataIdentity owner, int stableIndex) {
            this.owner = owner;
            this.stableIndex = stableIndex;
        }
    }

    private static final class Target {
        final Aggregate aggregate;
        final UUID targetId;
        final Map<Integer, Expected> expected;
        final Map<WorkerDataIdentity, OwnerPool> ownerPools;
        final int expectedPoolCount;
        boolean closing;

        Target(Aggregate aggregate, ShuffleInboundAuthorization authorization) {
            this.aggregate = aggregate;
            this.targetId = authorization.targetId();
            Map<WorkerDataIdentity, OwnerPool> pools = new LinkedHashMap<>();
            Map<Integer, Expected> values = new LinkedHashMap<>();
            for (ShuffleAuthorizationRecord record : authorization.records()) {
                OwnerPool pool = pools.get(record.expectedSourceIdentity());
                if (pool == null) {
                    pool = new OwnerPool(record.expectedSourceIdentity(), pools.size());
                    pools.put(record.expectedSourceIdentity(), pool);
                }
                values.put(record.producerOrdinal(), new Expected(record, pool));
            }
            if (pools.size() != authorization.expectedPoolCount()) {
                throw new IllegalArgumentException("authorization pool closure mismatch");
            }
            this.expected = Collections.unmodifiableMap(values);
            this.ownerPools = Collections.unmodifiableMap(pools);
            this.expectedPoolCount = authorization.expectedPoolCount();
        }
    }

    private static final class Aggregate {
        final String assignmentKey;
        final RunAttemptFence fence;
        final ControlResidentLease lease;
        final ControlResidentLease.OwnerToken leaseOwner;
        final AtomicInteger references = new AtomicInteger(1); // prepared slot root
        final AtomicBoolean preparedClosed = new AtomicBoolean();
        List<Target> targets;

        Aggregate(String assignmentKey, RunAttemptFence fence,
                  ControlResidentLease lease,
                  ControlResidentLease.OwnerToken leaseOwner) {
            this.assignmentKey = assignmentKey;
            this.fence = fence;
            this.lease = lease;
            this.leaseOwner = leaseOwner;
        }

        boolean retain() {
            for (;;) {
                int current = references.get();
                if (current == 0) return false;
                if (references.compareAndSet(current, Math.addExact(current, 1))) return true;
            }
        }

        void release() {
            int remaining = references.decrementAndGet();
            if (remaining < 0) {
                throw new IllegalStateException("authorization aggregate reference underflow");
            }
            if (remaining == 0) lease.release(leaseOwner);
        }
    }

    public final class PreparedHandle implements AutoCloseable {
        private final Aggregate aggregate;
        private final AtomicBoolean released = new AtomicBoolean();
        private PreparedHandle(Aggregate aggregate) { this.aggregate = aggregate; }
        @Override public void close() {
            if (released.compareAndSet(false, true)) unprepare(aggregate);
        }
    }

    public final class AuthorizedStream implements AutoCloseable {
        private final Target target;
        private final Expected expected;
        private final OwnerPool ownerPool;
        private final AtomicBoolean released = new AtomicBoolean();

        private AuthorizedStream(Target target, Expected expected) {
            this.target = target;
            this.expected = expected;
            this.ownerPool = expected.ownerPool;
        }

        public boolean matchesOwner(long connectionGeneration, WorkerDataIdentity peer) {
            synchronized (target) {
                return expected.connectionGeneration == connectionGeneration
                        && expected.peer.equals(peer)
                        && expected.state == StreamState.OPENED;
            }
        }

        public boolean markEos() {
            synchronized (target) {
                if (expected.state == StreamState.EOS) return false;
                if (expected.state != StreamState.OPENED) return false;
                expected.state = StreamState.EOS;
            }
            releaseReference();
            return true;
        }

        /** Grants one deterministic share exactly once for the bound source-owner pool. */
        public int claimInitialCredit(long windowBytes) {
            if (windowBytes < 0L) {
                throw new IllegalArgumentException("shuffle window must be non-negative");
            }
            synchronized (target) {
                if (expected.state != StreamState.OPENED
                        || ownerPool.initialCreditGranted) {
                    return 0;
                }
                long total = Math.max(windowBytes, target.expectedPoolCount);
                long extra = total - target.expectedPoolCount;
                long share = 1L + extra / target.expectedPoolCount;
                if (ownerPool.stableIndex < extra % target.expectedPoolCount) share++;
                if (share > Integer.MAX_VALUE) {
                    throw new IllegalArgumentException(
                            "initial shuffle credit exceeds wire limit");
                }
                ownerPool.initialCreditGranted = true;
                return (int) share;
            }
        }

        public void fail() {
            synchronized (target) {
                if (expected.state == StreamState.OPENED) {
                    expected.state = StreamState.FAILED;
                } else {
                    return;
                }
            }
            releaseReference();
        }

        private void releaseReference() {
            if (released.compareAndSet(false, true)) {
                openStreams.decrementAndGet();
                target.aggregate.release();
            }
        }

        @Override public void close() { fail(); }
    }

    private final ControlResidentBudget residentBudget;
    private final Map<UUID, Target> targets = new ConcurrentHashMap<>();
    private final Map<String, Aggregate> assignments = new HashMap<>();
    private final Object lifecycle = new Object();
    private final AtomicInteger openStreams = new AtomicInteger();
    private boolean closed;

    public DataInboundAuthorizationRegistry() {
        this(ControlResidentBudget.processWide());
    }

    public DataInboundAuthorizationRegistry(ControlResidentBudget residentBudget) {
        if (residentBudget == null) {
            throw new IllegalArgumentException("control resident budget is required");
        }
        this.residentBudget = residentBudget;
    }

    public PreparedHandle prepare(
            String assignmentKey, RunAttemptFence fence,
            List<ShuffleInboundAuthorization> authorizations) {
        if (assignmentKey == null || assignmentKey.isEmpty() || fence == null
                || authorizations == null || authorizations.isEmpty()
                || authorizations.size() > 2) {
            throw new IllegalArgumentException("invalid inbound authorization preparation");
        }
        Set<UUID> targetIds = new HashSet<>();
        long charge = 512L;
        for (ShuffleInboundAuthorization authorization : authorizations) {
            if (authorization == null || !targetIds.add(authorization.targetId())) {
                throw new IllegalArgumentException("duplicate inbound authorization target");
            }
            charge = Math.addExact(charge, 512L);
            for (ShuffleAuthorizationRecord record : authorization.records()) {
                charge = Math.addExact(charge, recordCharge(record));
            }
        }

        ControlResidentLease lease = residentBudget.openLease();
        ControlResidentLease.OwnerToken owner = lease.ownerToken();
        try {
            lease.reserve(owner, charge);
            owner = lease.transfer(owner, ControlResidentOwner.HANDLER_OWNED);
            owner = lease.transfer(owner, ControlResidentOwner.ASSIGNMENT_OWNED);
            Aggregate aggregate = new Aggregate(assignmentKey, fence, lease, owner);
            List<Target> preparedTargets = new ArrayList<>(authorizations.size());
            for (ShuffleInboundAuthorization authorization : authorizations) {
                preparedTargets.add(new Target(aggregate, authorization));
            }
            aggregate.targets = Collections.unmodifiableList(preparedTargets);
            synchronized (lifecycle) {
                if (closed) throw new IllegalStateException("authorization registry is closed");
                if (assignments.containsKey(assignmentKey)) {
                    throw new IllegalStateException("assignment authorization is already prepared");
                }
                for (Target target : preparedTargets) {
                    if (targets.containsKey(target.targetId)) {
                        throw new IllegalStateException("shuffle authorization target conflict");
                    }
                }
                assignments.put(assignmentKey, aggregate);
                for (Target target : preparedTargets) targets.put(target.targetId, target);
            }
            return new PreparedHandle(aggregate);
        } catch (RuntimeException | Error failure) {
            lease.release(owner);
            throw failure;
        }
    }

    public AuthorizedStream open(
            UUID targetId, int producerOrdinal, ShuffleProducerGrant rawGrant,
            long connectionGeneration, WorkerDataIdentity exactPeer) {
        if (targetId == null || rawGrant == null || connectionGeneration <= 0L
                || exactPeer == null) {
            throw rejected();
        }
        Target target = targets.get(targetId);
        if (target == null) throw rejected();
        synchronized (target) {
            if (target.closing) throw rejected();
            Expected expected = target.expected.get(producerOrdinal);
            if (expected == null
                    || !expected.record.expectedSourceIdentity().equals(exactPeer)) {
                throw rejected();
            }
            byte[] actual = rawGrant.authorizationDigest(
                    target.aggregate.fence, targetId, producerOrdinal);
            boolean digestMatches = MessageDigest.isEqual(
                    expected.record.authorizationDigest(), actual);
            java.util.Arrays.fill(actual, (byte) 0);
            if (!digestMatches || expected.state != StreamState.UNUSED) {
                throw rejected();
            }
            OwnerPool ownerPool = expected.ownerPool;
            long ownerGeneration = ownerPool.connectionGeneration;
            if (ownerGeneration != 0L && ownerGeneration != connectionGeneration) {
                throw rejected();
            }
            if (!target.aggregate.retain()) throw rejected();
            if (ownerGeneration == 0L) ownerPool.connectionGeneration = connectionGeneration;
            expected.connectionGeneration = connectionGeneration;
            expected.peer = exactPeer;
            expected.state = StreamState.OPENED;
            openStreams.incrementAndGet();
            return new AuthorizedStream(target, expected);
        }
    }

    public int preparedTargetCount() { return targets.size(); }
    public int openStreamCount() { return openStreams.get(); }

    private void unprepare(Aggregate aggregate) {
        if (!aggregate.preparedClosed.compareAndSet(false, true)) return;
        synchronized (lifecycle) {
            assignments.remove(aggregate.assignmentKey, aggregate);
            for (Target target : aggregate.targets) {
                targets.remove(target.targetId, target);
                synchronized (target) { target.closing = true; }
            }
        }
        aggregate.release();
    }

    @Override
    public void close() {
        List<Aggregate> aggregates;
        synchronized (lifecycle) {
            if (closed) return;
            closed = true;
            aggregates = new ArrayList<>(assignments.values());
        }
        for (Aggregate aggregate : aggregates) unprepare(aggregate);
    }

    private static long recordCharge(ShuffleAuthorizationRecord record) {
        if (record == null) throw new IllegalArgumentException("authorization record is required");
        WorkerDataIdentity identity = record.expectedSourceIdentity();
        long bytes = 256L + ShuffleAuthorizationRecord.DIGEST_BYTES
                + identity.certificateFingerprint().length;
        bytes = Math.addExact(bytes, utf8Bytes(identity.workerId()));
        bytes = Math.addExact(bytes, utf8Bytes(identity.incarnationId()));
        bytes = Math.addExact(bytes, utf8Bytes(identity.deploymentSetId()));
        return Math.addExact(bytes, utf8Bytes(identity.authzSetId()));
    }

    private static int utf8Bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    private static AuthorizationRejectedException rejected() {
        return new AuthorizationRejectedException();
    }
}
