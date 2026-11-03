package io.rowweave.mem;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Process-local managed-memory ledger. Fixed legacy leases and elastic operator owners share one hard total.
 * All pooled root/child accounting is committed under {@link #lock}; callbacks always run after that lock is released.
 */
public final class ManagedMemory {

    private final long totalBytes;
    private final Object lock = new Object();
    private long guaranteedBytes;
    private long committedBytes;
    private long reservedBytes;
    private long peakReservedBytes;
    private int activeOwnerCount;
    private int activeLeafCount;
    private final Map<MemoryManager, OwnerRecord> outstanding = new IdentityHashMap<>();

    private ManagedMemory(long totalBytes) {
        if (totalBytes <= 0) {
            throw new IllegalArgumentException("totalBytes must be > 0");
        }
        this.totalBytes = totalBytes;
    }

    public static ManagedMemory ofBytes(long totalBytes) {
        return new ManagedMemory(totalBytes);
    }

    /** Resolve a managed sub-envelope from the JVM maximum heap. */
    public static ManagedMemory ofFraction(double fraction) {
        if (fraction <= 0 || fraction > 1) {
            throw new IllegalArgumentException("fraction must be in (0, 1]");
        }
        return new ManagedMemory((long) (Runtime.getRuntime().maxMemory() * fraction));
    }

    public long totalBytes() {
        return totalBytes;
    }

    /** Uncommitted bytes at this instant. Reading does not create a reservation or priority. */
    public long availableBytes() {
        synchronized (lock) {
            return totalBytes - committedBytes;
        }
    }

    public ManagedMemorySnapshot snapshot() {
        synchronized (lock) {
            return new ManagedMemorySnapshot(totalBytes, guaranteedBytes, committedBytes,
                    reservedBytes, peakReservedBytes, activeOwnerCount, activeLeafCount);
        }
    }

    /** Fixed exclusive legacy lease: its whole cap is guaranteed and cannot be borrowed. */
    public MemoryManager acquire(long bytes) {
        if (bytes <= 0) {
            throw new IllegalArgumentException("bytes must be > 0");
        }
        return acquireOwners(OwnerSpec.elastic(bytes, bytes))[0];
    }

    /** Atomically register a complete owner set, or leave the ledger unchanged on failure. */
    public MemoryManager[] acquireOwners(OwnerSpec... specs) {
        if (specs == null) {
            throw new IllegalArgumentException("owner specs must not be null");
        }
        OwnerSpec[] copy = specs.clone();
        long additionalGuarantee = 0L;
        for (int i = 0; i < copy.length; i++) {
            OwnerSpec spec = copy[i];
            if (spec == null) {
                throw new IllegalArgumentException("owner spec " + i + " must not be null");
            }
            spec.validateAgainst(totalBytes);
            if (spec.guaranteedBytes > Long.MAX_VALUE - additionalGuarantee) {
                throw capacityFailure("owner guarantee sum overflow");
            }
            additionalGuarantee += spec.guaranteedBytes;
        }

        synchronized (lock) {
            if (additionalGuarantee > totalBytes - committedBytes) {
                throw capacityFailure("cannot guarantee " + additionalGuarantee
                        + " bytes: committed=" + committedBytes + " total=" + totalBytes);
            }
            if (copy.length > Integer.MAX_VALUE - activeOwnerCount
                    || copy.length > Integer.MAX_VALUE - activeLeafCount) {
                throw capacityFailure("active owner count overflow");
            }
            MemoryManager[] managers = new MemoryManager[copy.length];
            for (int i = 0; i < copy.length; i++) {
                OwnerSpec spec = copy[i];
                managers[i] = MemoryManager.pooledRoot(
                        spec.capBytes, spec.guaranteedBytes, spec.childArity, this);
            }
            for (MemoryManager manager : managers) {
                outstanding.put(manager, new OwnerRecord(manager));
            }
            guaranteedBytes += additionalGuarantee;
            committedBytes += additionalGuarantee;
            activeOwnerCount += managers.length;
            activeLeafCount += managers.length;
            return managers;
        }
    }

    /** Unknown/repeated release remains a compatibility no-op. Dirty leases finalize after cleanup drains. */
    public void release(MemoryManager manager) {
        requestClose(manager, true, null);
    }

    /** Package-local lifecycle hook used by ExecutionMemory cancellation and owner-exit paths. */
    void requestClose(MemoryManager manager, boolean ownerExited, Runnable finalizer) {
        Runnable run;
        synchronized (lock) {
            OwnerRecord record = recordFor(manager);
            if (record == null) return;
            if (manager != record.root) {
                throw new IllegalArgumentException("only a pooled root owner can be closed");
            }
            MemoryManager root = manager.root;
            if (root.leaseState == MemoryManager.LeaseState.OPEN) {
                root.leaseState = MemoryManager.LeaseState.CLOSING;
            }
            record.closeRequested = true;
            record.ownerExited |= ownerExited;
            if (finalizer != null) {
                if (record.finalizer != null && record.finalizer != finalizer) {
                    throw new IllegalStateException("pooled owner already has a terminal finalizer");
                }
                record.finalizer = finalizer;
            }
            run = finalizeIfReady(record);
        }
        runFinalizer(run);
    }

    boolean tryReserve(MemoryManager manager, long bytes) {
        return reserveAvailableUpTo(manager, bytes, bytes) == bytes;
    }

    long reserveAvailableUpTo(MemoryManager manager, long minimumBytes, long preferredBytes) {
        synchronized (lock) {
            OwnerRecord record = requireRecord(manager);
            MemoryManager root = record.root;
            requireOpen(root, "reserve");
            rejectDirectPartitionedRoot(manager);
            long available = availableBytesLocked(manager, record);
            if (available < minimumBytes) return 0;
            long bytes = Math.min(preferredBytes, available);

            long before = committedOf(manager);
            long nextReserved = manager.reserved + bytes;
            long after = Math.max(manager.guaranteed, nextReserved);
            long addedCommitment = after - before;

            manager.reserved = nextReserved;
            manager.peakReserved = Math.max(manager.peakReserved, nextReserved);
            if (manager != root) {
                root.reserved += bytes;
                root.peakReserved = Math.max(root.peakReserved, root.reserved);
            }
            committedBytes += addedCommitment;
            reservedBytes += bytes;
            peakReservedBytes = Math.max(peakReservedBytes, reservedBytes);
            return bytes;
        }
    }

    void releaseBytes(MemoryManager manager, long bytes, boolean exact) {
        Runnable run;
        synchronized (lock) {
            OwnerRecord record = requireRecord(manager);
            MemoryManager root = record.root;
            requireReleasable(root);
            rejectDirectPartitionedRoot(manager);
            long actual;
            if (exact) {
                if (bytes > manager.reserved || (manager != root && bytes > root.reserved)) {
                    throw new IllegalStateException("release exceeds reservation: bytes=" + bytes
                            + " reserved=" + manager.reserved + " rootReserved=" + root.reserved);
                }
                actual = bytes;
            } else {
                actual = Math.min(bytes, manager.reserved);
            }
            long before = committedOf(manager);
            manager.reserved -= actual;
            if (manager != root) root.reserved -= actual;
            long after = committedOf(manager);
            committedBytes -= before - after;
            reservedBytes -= actual;
            run = finalizeIfReady(record);
        }
        runFinalizer(run);
    }

    long reservedBytes(MemoryManager manager) {
        synchronized (lock) {
            requireKnownOwner(manager);
            return manager.reserved;
        }
    }

    long availableBytes(MemoryManager manager) {
        synchronized (lock) {
            OwnerRecord record = recordFor(manager);
            if (record == null || record.root.leaseState != MemoryManager.LeaseState.OPEN) return 0L;
            if (manager == record.root && manager.partitioned) return 0L;
            return availableBytesLocked(manager, record);
        }
    }

    private long availableBytesLocked(MemoryManager manager, OwnerRecord record) {
        long ownerRoom = manager.budget - manager.reserved;
        long rootRoom = manager == record.root
                ? ownerRoom : record.root.budget - record.root.reserved;
        long guaranteeRoom = Math.max(0L, manager.guaranteed - manager.reserved);
        long sharedRoom = totalBytes - committedBytes;
        long ledgerRoom = guaranteeRoom > Long.MAX_VALUE - sharedRoom
                ? Long.MAX_VALUE : guaranteeRoom + sharedRoom;
        return Math.min(ownerRoom, Math.min(rootRoom, ledgerRoom));
    }

    long peakReservedBytes(MemoryManager manager) {
        synchronized (lock) {
            requireKnownOwner(manager);
            return manager.peakReserved;
        }
    }

    MemoryManager[] partitionEvenly(MemoryManager manager, int parts) {
        synchronized (lock) {
            OwnerRecord record = requireRecord(manager);
            MemoryManager root = record.root;
            requireOpen(root, "partition");
            if (manager != root) {
                throw new IllegalStateException("only root MemoryManager can be partitioned");
            }
            if (parts <= 0) {
                throw new IllegalArgumentException("parts must be > 0");
            }
            if (parts != root.expectedChildArity) {
                throw new IllegalStateException("pooled owner admitted for childArity="
                        + root.expectedChildArity + " but partition requested " + parts);
            }
            if (root.partitioned || root.reserved != 0 || !root.spillables.isEmpty()) {
                throw new IllegalStateException("MemoryManager handoff requires unused root: partitioned="
                        + root.partitioned + " reserved=" + root.reserved
                        + " spillables=" + root.spillables.size());
            }
            if (root.budget < parts || root.guaranteed < parts) {
                throw capacityFailure("owner cap/guarantee is smaller than child arity");
            }
            if (parts - 1 > Integer.MAX_VALUE - activeLeafCount) {
                throw capacityFailure("active leaf count overflow");
            }
            MemoryManager[] children = new MemoryManager[parts];
            long capBase = root.budget / parts;
            long capRemainder = root.budget % parts;
            long guaranteeBase = root.guaranteed / parts;
            long guaranteeRemainder = root.guaranteed % parts;
            for (int i = 0; i < parts; i++) {
                children[i] = MemoryManager.pooledChild(
                        capBase + (i < capRemainder ? 1 : 0),
                        guaranteeBase + (i < guaranteeRemainder ? 1 : 0), root);
                record.members.add(children[i]);
            }
            root.partitioned = true;
            record.leafCount = parts;
            activeLeafCount += parts - 1;
            return children;
        }
    }

    void registerSpillable(MemoryManager manager, MemorySpillable spillable) {
        synchronized (lock) {
            OwnerRecord record = requireRecord(manager);
            requireOpen(record.root, "register spillable");
            manager.spillables.add(spillable);
        }
    }

    void unregisterSpillable(MemoryManager manager, MemorySpillable spillable) {
        Runnable run;
        synchronized (lock) {
            OwnerRecord record = recordFor(manager);
            if (record == null) return;
            manager.spillables.remove(spillable);
            run = finalizeIfReady(record);
        }
        runFinalizer(run);
    }

    private Runnable finalizeIfReady(OwnerRecord record) {
        MemoryManager root = record.root;
        if (!record.closeRequested || !record.ownerExited
                || root.reserved != 0 || !allSpillablesEmpty(record)) {
            return null;
        }
        outstanding.remove(root);
        committedBytes -= root.guaranteed;
        guaranteedBytes -= root.guaranteed;
        activeOwnerCount--;
        activeLeafCount -= record.leafCount;
        root.leaseState = MemoryManager.LeaseState.CLOSED;
        Runnable finalizer = record.finalizer;
        record.finalizer = null;
        return finalizer;
    }

    private static long committedOf(MemoryManager manager) {
        return Math.max(manager.guaranteed, manager.reserved);
    }

    private static boolean allSpillablesEmpty(OwnerRecord record) {
        for (MemoryManager member : record.members) {
            if (!member.spillables.isEmpty()) return false;
        }
        return true;
    }

    private OwnerRecord recordFor(MemoryManager manager) {
        if (manager == null || manager.pool != this) return null;
        return outstanding.get(manager.root);
    }

    private OwnerRecord requireRecord(MemoryManager manager) {
        OwnerRecord record = recordFor(manager);
        if (record == null) {
            throw new IllegalStateException("pooled memory owner is closed or not registered");
        }
        return record;
    }

    private void requireKnownOwner(MemoryManager manager) {
        if (manager == null || manager.pool != this) {
            throw new IllegalStateException("memory owner does not belong to this pool");
        }
    }

    private static void requireOpen(MemoryManager root, String action) {
        if (root.leaseState != MemoryManager.LeaseState.OPEN) {
            throw new IllegalStateException("cannot " + action + " pooled owner in " + root.leaseState);
        }
    }

    private static void requireReleasable(MemoryManager root) {
        if (root.leaseState == MemoryManager.LeaseState.CLOSED) {
            throw new IllegalStateException("cannot release closed pooled owner");
        }
    }

    private static void rejectDirectPartitionedRoot(MemoryManager manager) {
        if (manager == manager.root && manager.partitioned) {
            throw new IllegalStateException("partitioned root must be accounted through a child MemoryManager");
        }
    }

    private static void runFinalizer(Runnable finalizer) {
        if (finalizer != null) finalizer.run();
    }

    private static MemoryBudgetExceededException capacityFailure(String message) {
        return new MemoryBudgetExceededException(message);
    }

    /** Immutable owner admission descriptor derived from a node-local resource contract. */
    public static final class OwnerSpec {
        private final long guaranteedBytes;
        private final long capBytes;
        private final int childArity;

        private OwnerSpec(long guaranteedBytes, long capBytes, int childArity) {
            this.guaranteedBytes = guaranteedBytes;
            this.capBytes = capBytes;
            this.childArity = childArity;
        }

        public static OwnerSpec elastic(long guaranteedBytes, long capBytes) {
            return new OwnerSpec(guaranteedBytes, capBytes, 1);
        }

        public static OwnerSpec partitioned(long guaranteedBytes, long capBytes, int childArity) {
            return new OwnerSpec(guaranteedBytes, capBytes, childArity);
        }

        public long guaranteedBytes() { return guaranteedBytes; }
        public long capBytes() { return capBytes; }
        public int childArity() { return childArity; }

        private void validateAgainst(long totalBytes) {
            if (guaranteedBytes <= 0 || capBytes <= 0 || childArity <= 0) {
                throw new IllegalArgumentException("owner guarantee, cap and childArity must be positive");
            }
            if (guaranteedBytes > capBytes) {
                throw new IllegalArgumentException("owner guarantee must not exceed cap");
            }
            if (capBytes > totalBytes) {
                throw capacityFailure("owner cap " + capBytes + " exceeds pool total " + totalBytes);
            }
            if (guaranteedBytes < childArity || capBytes < childArity) {
                throw capacityFailure("owner guarantee/cap cannot cover childArity=" + childArity);
            }
        }
    }

    private static final class OwnerRecord {
        private final MemoryManager root;
        private final List<MemoryManager> members = new ArrayList<>();
        private int leafCount = 1;
        private boolean closeRequested;
        private boolean ownerExited;
        private Runnable finalizer;

        private OwnerRecord(MemoryManager root) {
            this.root = root;
            members.add(root);
        }
    }
}
