package io.rowweave.mem;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Memory quota arbitration. Operators reserve and release byte budgets here; when a reservation cannot be met, the manager asks
 * the registered MemorySpillable holders to cooperate by spilling, and if that still leaves too little it throws {@link MemoryBudgetExceededException}
 * -- an explicit failure rather than a silent OOM.
 *
 * <p>Thread safety: reserve/tryReserve/release update the quota counters atomically under an internal lock, so concurrent
 * partition threads reserving, returning and triggering spills neither race nor lose updates nor oversubscribe. That is the shared precondition for one global managed-memory pool plus bounded multi-threaded parallelism.
 */
public final class MemoryManager {

    enum LeaseState { OPEN, CLOSING, CLOSED }

    final long budget;
    final long guaranteed;
    final int expectedChildArity;
    final ManagedMemory pool;
    private final Ledger ledger;
    final MemoryManager root;
    long reserved; // guarded by ledger.lock for standalone, or ManagedMemory process lock for pooled
    long peakReserved; // same guard as reserved
    boolean partitioned; // root only; same guard as reserved
    LeaseState leaseState; // pooled root only; guarded by ManagedMemory process lock
    final List<MemorySpillable> spillables = new CopyOnWriteArrayList<>();
    private final java.util.concurrent.atomic.AtomicLong spillCount =
            new java.util.concurrent.atomic.AtomicLong(); // how often reserve triggered a spill (read-only metric)
    private final java.util.concurrent.atomic.AtomicBoolean spillTelemetryInvalid =
            new java.util.concurrent.atomic.AtomicBoolean();
    private final java.util.concurrent.locks.ReentrantLock externalSortPhaseLock =
            new java.util.concurrent.locks.ReentrantLock(true);
    private final java.util.concurrent.ConcurrentHashMap<Object, Long> replayScratchEscrows =
            new java.util.concurrent.ConcurrentHashMap<>();

    public MemoryManager(long budget) {
        if (budget <= 0) {
            throw new IllegalArgumentException("budget must be > 0");
        }
        this.budget = budget;
        this.guaranteed = budget;
        this.expectedChildArity = 1;
        this.pool = null;
        this.ledger = new Ledger();
        this.root = this;
        this.leaseState = LeaseState.OPEN;
    }

    private MemoryManager(long budget, long guaranteed, int expectedChildArity,
                          ManagedMemory pool, MemoryManager root) {
        this.budget = budget;
        this.guaranteed = guaranteed;
        this.expectedChildArity = expectedChildArity;
        this.pool = pool;
        this.root = root == null ? this : root;
        this.ledger = root == null ? new Ledger() : root.ledger;
        this.leaseState = root == null ? LeaseState.OPEN : null;
    }

    static MemoryManager pooledRoot(long budget, long guaranteed, int expectedChildArity,
                                    ManagedMemory pool) {
        return new MemoryManager(budget, guaranteed, expectedChildArity, pool, null);
    }

    static MemoryManager pooledChild(long budget, long guaranteed, MemoryManager root) {
        return new MemoryManager(budget, guaranteed, 1, root.pool, root);
    }

    public void registerSpillable(MemorySpillable spillable) {
        if (pool == null) {
            spillables.add(spillable);
        } else {
            pool.registerSpillable(this, spillable);
        }
    }

    /** Unregister symmetrically once a resource owner closes, so a closed object cannot keep receiving cross-spill callbacks. */
    public void unregisterSpillable(MemorySpillable spillable) {
        if (pool == null) {
            spillables.remove(spillable);
        } else {
            pool.unregisterSpillable(this, spillable);
        }
    }

    /** Non-blocking reservation: returns true and takes the space when enough is free, false otherwise (no spill triggered, no state changed). */
    public boolean tryReserve(long bytes) {
        if (bytes <= 0) {
            return true;
        }
        if (pool != null) {
            return pool.tryReserve(this, bytes);
        }
        synchronized (ledger.lock) {
            rejectDirectRootAccountingAfterPartition();
            if (bytes <= budget - reserved && (this == root || bytes <= root.budget - root.reserved)) {
                reserved += bytes;
                if (reserved > peakReserved) {
                    peakReserved = reserved; // raises the high-water mark on the success path
                }
                if (this != root) {
                    root.reserved += bytes;
                    if (root.reserved > root.peakReserved) {
                        root.peakReserved = root.reserved;
                    }
                }
                return true;
            }
            return false;
        }
    }

    /**
     * Atomically reserves as much of the current owner/ledger capacity as possible, up to {@code preferredBytes}.
     * Returns zero without changing the ledger when {@code minimumBytes} cannot be satisfied.
     */
    long reserveAvailableUpTo(long minimumBytes, long preferredBytes) {
        if (minimumBytes < 0 || preferredBytes < minimumBytes) {
            throw new IllegalArgumentException("invalid bounded reservation: minimum="
                    + minimumBytes + " preferred=" + preferredBytes);
        }
        if (preferredBytes == 0) {
            return 0;
        }
        if (pool != null) {
            return pool.reserveAvailableUpTo(this, minimumBytes, preferredBytes);
        }
        synchronized (ledger.lock) {
            rejectDirectRootAccountingAfterPartition();
            long ownerRoom = budget - reserved;
            long rootRoom = this == root ? ownerRoom : root.budget - root.reserved;
            long available = Math.min(ownerRoom, rootRoom);
            if (available < minimumBytes) {
                return 0;
            }
            long actual = Math.min(preferredBytes, available);
            reserved += actual;
            peakReserved = Math.max(peakReserved, reserved);
            if (this != root) {
                root.reserved += actual;
                root.peakReserved = Math.max(root.peakReserved, root.reserved);
            }
            return actual;
        }
    }

    /** Partitioned binary sides retain one replay scratch across sort EOF for their grouped downstream phase. */
    boolean requiresGroupedReplayScratch() {
        return this != root && root.expectedChildArity > 1;
    }

    void publishReplayScratch(Object owner, long bytes) {
        if (owner == null || bytes < 0) {
            throw new IllegalArgumentException("invalid replay scratch publication");
        }
        if (bytes == 0) replayScratchEscrows.remove(owner);
        else replayScratchEscrows.put(owner, bytes);
    }

    /** Read-only amount already reserved by a live upstream buffer and reusable by one sequential replay action. */
    public long reusableReplayScratchBytes() {
        long max = 0L;
        for (Long bytes : replayScratchEscrows.values()) {
            if (bytes != null) max = Math.max(max, bytes);
        }
        return max;
    }

    /**
     * Reserve bytes: when short, ask spillables to yield one at a time until enough is free; if they have all yielded and it is still short, throw. Returns true on success.
 *
     * <p>spill is invoked outside the lock, because spillable.spill calls back into {@link #release} and needs that lock itself, so calling it while holding the lock would deadlock.
     * The atomic tryReserve is retried after each spill.
     */
    public boolean reserve(long bytes) {
        if (bytes <= 0) {
            return true;
        }
        if (tryReserve(bytes)) {
            return true;
        }
        for (MemorySpillable s : spillables) {
            s.spill();
            incrementSpillCount(); // counts one per yielding spillable (read-only metric; yield semantics unchanged)
            if (tryReserve(bytes)) {
                return true;
            }
        }
        throw new MemoryBudgetExceededException(
                "cannot reserve " + bytes + " bytes: reserved=" + reservedBytes() + " budget=" + budget
                        + " (spilled all holders, still insufficient)");
    }

    public void release(long bytes) {
        if (bytes <= 0) {
            return;
        }
        if (pool != null) {
            pool.releaseBytes(this, bytes, false);
            return;
        }
        synchronized (ledger.lock) {
            rejectDirectRootAccountingAfterPartition();
            long actual = Math.min(bytes, reserved);
            reserved -= actual;
            if (this != root) {
                root.reserved -= actual;
            }
        }
    }

    /** Exact release: an underflow is validated before child and root are updated, so there is no half commit and no ghost release across the two sides. */
    public void releaseExact(long bytes) {
        if (bytes <= 0) {
            return;
        }
        if (pool != null) {
            pool.releaseBytes(this, bytes, true);
            return;
        }
        synchronized (ledger.lock) {
            rejectDirectRootAccountingAfterPartition();
            if (bytes > reserved || (this != root && bytes > root.reserved)) {
                throw new IllegalStateException("release exceeds reservation: bytes=" + bytes
                        + " reserved=" + reserved + " rootReserved=" + root.reserved);
            }
            reserved -= bytes;
            if (this != root) {
                root.reserved -= bytes;
            }
        }
    }

    /**
     * Statically splits the unused part of the root budget into child managers. Every child shares the
     * root's ledger lock, so each reserve or release updates both levels inside one critical section.
     */
    public MemoryManager[] partitionEvenly(int parts) {
        if (parts <= 0) {
            throw new IllegalArgumentException("parts must be > 0");
        }
        if (pool != null) {
            return pool.partitionEvenly(this, parts);
        }
        synchronized (ledger.lock) {
            if (this != root) {
                throw new IllegalStateException("only root MemoryManager can be partitioned");
            }
            if (partitioned || reserved != 0 || !spillables.isEmpty()) {
                throw new IllegalStateException("MemoryManager handoff requires unused root: partitioned="
                        + partitioned + " reserved=" + reserved + " spillables=" + spillables.size());
            }
            if (budget < parts) {
                throw new IllegalArgumentException("budget " + budget + " is smaller than parts " + parts);
            }
            MemoryManager[] children = new MemoryManager[parts];
            long base = budget / parts;
            long remainder = budget % parts;
            for (int i = 0; i < parts; i++) {
                long childBudget = base + (i < remainder ? 1 : 0);
                children[i] = new MemoryManager(childBudget, childBudget, 1, null, this);
            }
            partitioned = true;
            return children;
        }
    }

    public long reservedBytes() {
        if (pool != null) {
            return pool.reservedBytes(this);
        }
        synchronized (ledger.lock) {
            return reserved;
        }
    }

    public long availableBytes() {
        if (pool != null) {
            return pool.availableBytes(this);
        }
        synchronized (ledger.lock) {
            return budget - reserved;
        }
    }

    public long budget() {
        return budget;
    }

    /** Minimum commitment protected from borrowing by other pooled owners. */
    public long guaranteedBytes() {
        return guaranteed;
    }

    /** External-sort merge passes on one operator MemoryManager are serialized; each MemoryManager instance has its own fair lock. */
    java.util.concurrent.locks.ReentrantLock externalSortPhaseLock() {
        return externalSortPhaseLock;
    }

    /** All-time high reservation watermark (monotonic; release does not lower it). The no-OOM invariant is that peak stays at or below budget throughout. */
    public long peakReservedBytes() {
        if (pool != null) {
            return pool.peakReservedBytes(this);
        }
        synchronized (ledger.lock) {
            return peakReserved;
        }
    }

    /** How often reserve caused a spillable to yield (read-only metric; a signal of skew and memory pressure). */
    public long spillCount() {
        return spillCount.get();
    }

    boolean spillTelemetryComplete() {
        return !spillTelemetryInvalid.get();
    }

    /**
     * Lets an extension that genuinely spills on its own, outside {@link #reserve(long)} arbitration, record one observation.
     * A built-in buffer should go through reserve arbitration instead, so the same spill is not counted twice.
     */
    public void recordSelfSpill() {
        incrementSpillCount();
    }

    private void incrementSpillCount() {
        incrementSpillCount(spillCount, spillTelemetryInvalid);
        if (this != root) {
            incrementSpillCount(root.spillCount, root.spillTelemetryInvalid);
        }
    }

    private static void incrementSpillCount(
            java.util.concurrent.atomic.AtomicLong count,
            java.util.concurrent.atomic.AtomicBoolean invalid) {
        while (true) {
            long current = count.get();
            if (current == Long.MAX_VALUE) {
                invalid.set(true);
                return;
            }
            if (count.compareAndSet(current, current + 1L)) return;
        }
    }

    private void rejectDirectRootAccountingAfterPartition() {
        if (this == root && partitioned) {
            throw new IllegalStateException("partitioned root must be accounted through a child MemoryManager");
        }
    }

    private static final class Ledger {
        private final Object lock = new Object();
    }
}
