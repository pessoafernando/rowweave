package io.rowweave.mem;

import io.rowweave.mem.seg.HeapPool;
import io.rowweave.mem.seg.MemorySegmentPool;
import io.rowweave.mem.spill.SpillJobTelemetrySnapshot;
import io.rowweave.mem.spill.SpillJobWorkspace;
import io.rowweave.mem.spill.SpillStore;
import io.rowweave.mem.spill.SpillWorkspaces;

import java.io.File;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Bounded execution-memory context. pre-registers the complete owner set while actual reservations remain
 * demand activated. Each owner has an isolated spill callback set but borrows unused bytes through one process ledger.
 */
public final class ExecutionMemory implements AutoCloseable {

    private enum State { OPEN, CLOSING, CLOSED }

    private final ManagedMemory pool;
    private final boolean workspaceNeeded;
    private final SpillJobWorkspace spillWorkspace;
    private final List<MemoryManager> unclaimedOwners = new ArrayList<>();
    private final Map<MemoryManager, Integer> ownerChildArities = new IdentityHashMap<>();
    private final List<MemoryManager> opMemories = new ArrayList<>();
    private final Map<MemoryManager, Runnable> ownerFinalizers = new IdentityHashMap<>();
    private final Map<MemoryManager, Boolean> finalizedOwners = new IdentityHashMap<>();
    private final java.util.concurrent.atomic.AtomicLong acquiredAcc = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong peakAcc = new java.util.concurrent.atomic.AtomicLong();
    private final java.util.concurrent.atomic.AtomicLong spillAcc = new java.util.concurrent.atomic.AtomicLong();
    private boolean telemetryInvalid;
    private int pendingOwners;
    private State state = State.OPEN;
    private boolean workspaceClosed;
    private ExecutionMemorySnapshot frozenTelemetry;

    private ExecutionMemory(ManagedMemory pool, boolean workspaceNeeded,
                            SpillJobWorkspace spillWorkspace, MemoryManager[] owners,
                            MemoryOwnerRequirement[] requirements) {
        this.pool = pool;
        this.workspaceNeeded = workspaceNeeded;
        this.spillWorkspace = spillWorkspace;
        if (owners.length != requirements.length) {
            throw new IllegalArgumentException("owner and requirement counts must match");
        }
        for (int i = 0; i < owners.length; i++) {
            MemoryManager owner = owners[i];
            unclaimedOwners.add(owner);
            ownerChildArities.put(owner, requirements[i].childArity());
            ownerFinalizers.put(owner, () -> ownerFinalized(owner));
        }
        this.pendingOwners = owners.length;
    }

    /** Compatibility entry: positive count is now the exact number of ordinary owners. */
    public static ExecutionMemory forJob(int ownerCount) {
        return forJob(GlobalMemoryPool.instance(), ownerCount);
    }

    /** Compatibility entry: positive count is exact and also requests a spill workspace. */
    public static ExecutionMemory forJob(ManagedMemory pool, int ownerCount) {
        return forJob(pool, ownerCount, SpillWorkspaces::openJob);
    }

    /** Test seam retained for existing callers; positive count is exact. */
    static ExecutionMemory forJob(ManagedMemory pool, int ownerCount,
                                  Supplier<SpillJobWorkspace> workspaceSupplier) {
        if (ownerCount < 0) {
            throw new IllegalArgumentException("ownerCount must be >= 0, got " + ownerCount);
        }
        MemoryOwnerRequirement[] requirements = new MemoryOwnerRequirement[ownerCount];
        for (int i = 0; i < ownerCount; i++) requirements[i] = MemoryOwnerRequirement.ordinary();
        return forJob(pool, ownerCount > 0, workspaceSupplier, requirements);
    }

    /** Explicit production entry separating true buffer owners from disk-only workspace demand. */
    public static ExecutionMemory forJob(boolean workspaceNeeded,
                                         MemoryOwnerRequirement... requirements) {
        return forJob(GlobalMemoryPool.instance(), workspaceNeeded, requirements);
    }

    /** Explicit injectable-pool entry. */
    public static ExecutionMemory forJob(ManagedMemory pool, boolean workspaceNeeded,
                                         MemoryOwnerRequirement... requirements) {
        return forJob(pool, workspaceNeeded, SpillWorkspaces::openJob, requirements);
    }

    /** Package-local seam proving admission occurs before workspace creation. */
    static ExecutionMemory forJob(ManagedMemory pool, boolean workspaceNeeded,
                                  Supplier<SpillJobWorkspace> workspaceSupplier,
                                  MemoryOwnerRequirement... requirements) {
        if (pool == null) {
            throw new IllegalArgumentException("managed memory pool must not be null");
        }
        if (workspaceSupplier == null) {
            throw new IllegalArgumentException("spill workspace supplier must not be null");
        }
        if (requirements == null) {
            throw new IllegalArgumentException("owner requirements must not be null");
        }
        MemoryOwnerRequirement[] copy = requirements.clone();
        long cap = Math.max(1L, Math.min(WorkerConfig.operatorMaxMemory(), pool.totalBytes()));
        long guarantee = Math.max(1L, Math.min(WorkerConfig.operatorMinMemory(), cap));
        ManagedMemory.OwnerSpec[] specs = new ManagedMemory.OwnerSpec[copy.length];
        for (int i = 0; i < copy.length; i++) {
            MemoryOwnerRequirement requirement = copy[i];
            if (requirement == null) {
                throw new IllegalArgumentException("owner requirement " + i + " must not be null");
            }
            specs[i] = requirement.childArity() == 1
                    ? ManagedMemory.OwnerSpec.elastic(guarantee, cap)
                    : ManagedMemory.OwnerSpec.partitioned(guarantee, cap, requirement.childArity());
        }

        MemoryManager[] owners = pool.acquireOwners(specs);
        SpillJobWorkspace workspace = null;
        Throwable failure = null;
        try {
            if (workspaceNeeded) {
                workspace = workspaceSupplier.get();
                if (workspace == null) {
                    throw new IllegalStateException("spill workspace supplier returned null");
                }
            }
            return new ExecutionMemory(pool, workspaceNeeded, workspace, owners, copy);
        } catch (RuntimeException | Error setupFailure) {
            failure = setupFailure;
            throw setupFailure;
        } finally {
            if (failure != null) {
                for (MemoryManager owner : owners) {
                    try {
                        pool.release(owner);
                    } catch (RuntimeException | Error cleanupFailure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
                if (workspace != null) {
                    try {
                        workspace.close();
                    } catch (RuntimeException | Error cleanupFailure) {
                        failure.addSuppressed(cleanupFailure);
                    }
                }
            }
        }
    }

    /** Claims one pre-admitted owner token. Actual reservation remains zero until the buffer asks for bytes. */
    public MemoryManager newOperatorMemory() {
        return claimOperatorMemory(MemoryOwnerRequirement.ordinary());
    }

    /** Claims a token compatible with the node-local owner contract, independent of activation order. */
    public MemoryManager newOperatorMemoryFor(MemoryOwnerRequirement requirement) {
        if (requirement == null) {
            throw new IllegalArgumentException("owner requirement must not be null");
        }
        return claimOperatorMemory(requirement);
    }

    private MemoryManager claimOperatorMemory(MemoryOwnerRequirement requirement) {
        MemoryManager owner;
        synchronized (this) {
            requireOpen("claim operator memory");
            if (unclaimedOwners.isEmpty()) {
                throw new IllegalStateException("execution requested more memory owners than were admitted");
            }
            int match = -1;
            for (int i = 0; i < unclaimedOwners.size(); i++) {
                MemoryManager candidate = unclaimedOwners.get(i);
                if (ownerChildArities.get(candidate) == requirement.childArity()) {
                    match = i;
                    break;
                }
            }
            if (match < 0) {
                throw new IllegalStateException("execution requested memory owner childArity="
                        + requirement.childArity() + " but no compatible owner was admitted");
            }
            owner = unclaimedOwners.remove(match);
            opMemories.add(owner);
        }
        AcquireListener listener = diagListener;
        if (listener != null) {
            int level = owner.budget() >= WorkerConfig.operatorMaxMemory() ? 1 : 2;
            try {
                listener.onAcquire(level, owner.budget(), 0L, 0);
            } catch (Throwable ignoredDiagnosticFailure) {
                // Diagnostics cannot change a successful claim.
            }
        }
        return owner;
    }

    /** Signals that the operator body and its resources have exited; terminal cleanup may run synchronously. */
    public void releaseOperatorMemory(MemoryManager owner) {
        Runnable finalizer;
        synchronized (this) {
            if (!opMemories.contains(owner)) return;
            finalizer = ownerFinalizers.get(owner);
        }
        pool.requestClose(owner, true, finalizer);
        synchronized (this) {
            if (opMemories.contains(owner)) {
                throw new IllegalStateException("operator memory owner exited with live reservations or spillables");
            }
        }
    }

    public MemorySegmentPool segmentPool() {
        synchronized (this) {
            requireOpen("create segment pool");
        }
        return new HeapPool();
    }

    public SpillStore newSpillStore() {
        synchronized (this) {
            requireOpen("create spill store");
            if (!workspaceNeeded || spillWorkspace == null) {
                throw new IllegalStateException("ExecutionMemory has no spill workspace");
            }
            return spillWorkspace.newSpillStore();
        }
    }

    public synchronized File spillRootDir() {
        return spillWorkspace == null ? null : spillWorkspace.directory().toFile();
    }

    public synchronized long acquiredBytes() {
        long sum = acquiredAcc.get();
        for (MemoryManager owner : opMemories) sum += owner.budget();
        return sum;
    }

    public synchronized long peakReservedBytes() {
        long sum = peakAcc.get();
        for (MemoryManager owner : opMemories) sum += owner.peakReservedBytes();
        return sum;
    }

    public synchronized long spillCount() {
        long sum = spillAcc.get();
        for (MemoryManager owner : opMemories) sum += owner.spillCount();
        return sum;
    }

    /** OPEN/CLOSING calls return a current view; only CLOSED returns the one frozen terminal object. */
    public synchronized ExecutionMemorySnapshot telemetrySnapshot() {
        if (frozenTelemetry != null) return frozenTelemetry;
        return buildSnapshot();
    }

    public synchronized void assertWithinBudget() {
        long peak = peakReservedBytes();
        long acquired = acquiredBytes();
        if (peak > acquired) {
            throw new MemoryBudgetExceededException(
                    "no-OOM tripwire: peak reserved " + peak + " > acquired " + acquired
                            + " (MemoryManager ceiling regressed)");
        }
    }

    /**
     * Requests closure. Claimed owners are not declared exited by a cancellation thread; they finalize later through
     * releaseOperatorMemory. A dirty first close is explicit and retains both commitment and workspace.
     */
    @Override
    public void close() {
        List<MemoryManager> unclaimed;
        List<MemoryManager> active;
        synchronized (this) {
            if (state == State.CLOSED) return;
            state = State.CLOSING;
            unclaimed = new ArrayList<>(unclaimedOwners);
            active = new ArrayList<>(opMemories);
        }

        Throwable failure = null;
        for (MemoryManager owner : unclaimed) {
            try {
                pool.requestClose(owner, true, ownerFinalizers.get(owner));
            } catch (RuntimeException | Error closeFailure) {
                failure = addFailure(failure, closeFailure);
            }
        }
        for (MemoryManager owner : active) {
            try {
                pool.requestClose(owner, false, ownerFinalizers.get(owner));
            } catch (RuntimeException | Error closeFailure) {
                failure = addFailure(failure, closeFailure);
            }
        }

        synchronized (this) {
            if (pendingOwners == 0 && state != State.CLOSED) {
                try {
                    finishTerminal();
                } catch (RuntimeException | Error closeFailure) {
                    failure = addFailure(failure, closeFailure);
                }
            }
            if (pendingOwners != 0) {
                failure = addFailure(failure, new IllegalStateException(
                        "execution memory close deferred: " + pendingOwners + " owner(s) still active"));
            }
        }
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
    }

    private synchronized void ownerFinalized(MemoryManager owner) {
        if (finalizedOwners.put(owner, Boolean.TRUE) != null) return;
        unclaimedOwners.remove(owner);
        ownerChildArities.remove(owner);
        boolean claimed = opMemories.remove(owner);
        if (claimed) {
            addTelemetry(acquiredAcc, owner.budget());
            addTelemetry(peakAcc, owner.peakReservedBytes());
            addTelemetry(spillAcc, owner.spillCount());
            if (!owner.spillTelemetryComplete()) telemetryInvalid = true;
        }
        pendingOwners--;
        if (pendingOwners < 0) {
            telemetryInvalid = true;
            throw new IllegalStateException("execution owner finalization underflow");
        }
        if (pendingOwners == 0 && state == State.CLOSING) finishTerminal();
    }

    /** Called only under this monitor and never under the ManagedMemory ledger lock. */
    private void finishTerminal() {
        if (state == State.CLOSED) return;
        Throwable failure = null;
        if (!workspaceClosed && spillWorkspace != null) {
            workspaceClosed = true;
            try {
                spillWorkspace.close();
            } catch (RuntimeException | Error cleanupFailure) {
                failure = cleanupFailure;
                telemetryInvalid = true;
            }
        }
        state = State.CLOSED;
        frozenTelemetry = buildSnapshot();
        if (failure instanceof RuntimeException) throw (RuntimeException) failure;
        if (failure instanceof Error) throw (Error) failure;
    }

    private ExecutionMemorySnapshot buildSnapshot() {
        long acquired = sumTelemetry(acquiredAcc, 0);
        long peak = sumTelemetry(peakAcc, 1);
        long pressureSpills = sumTelemetry(spillAcc, 2);
        SpillJobTelemetrySnapshot spill = spillWorkspace == null
                ? null : spillWorkspace.telemetrySnapshot();
        boolean complete = !telemetryInvalid && (spill == null || spill.complete());
        return complete
                ? new ExecutionMemorySnapshot(true, peak, acquired, pressureSpills,
                spill == null ? 0 : spill.runCount(),
                spill == null ? 0 : spill.logicalBytesWritten(),
                spill == null ? 0 : spill.peakReservedBytes())
                : new ExecutionMemorySnapshot(false, 0, 0, 0, 0, 0, 0);
    }

    private void requireOpen(String action) {
        if (state != State.OPEN) {
            throw new IllegalStateException("cannot " + action + " after execution memory close started");
        }
    }

    private void addTelemetry(java.util.concurrent.atomic.AtomicLong accumulator, long value) {
        long current = accumulator.get();
        if (value < 0L || Long.MAX_VALUE - current < value) {
            telemetryInvalid = true;
            return;
        }
        accumulator.set(current + value);
    }

    private long sumTelemetry(java.util.concurrent.atomic.AtomicLong released, int metric) {
        long sum = released.get();
        for (MemoryManager owner : opMemories) {
            long value = metric == 0 ? owner.budget()
                    : metric == 1 ? owner.peakReservedBytes() : owner.spillCount();
            if (!owner.spillTelemetryComplete() || value < 0L || Long.MAX_VALUE - sum < value) {
                telemetryInvalid = true;
                return 0L;
            }
            sum += value;
        }
        return sum;
    }

    private static Throwable addFailure(Throwable primary, Throwable added) {
        if (primary == null) return added;
        primary.addSuppressed(added);
        return primary;
    }

    public interface AcquireListener {
        void onAcquire(int level, long grantedBytes, long waitedNanos, int parkRounds);
    }

    private static volatile AcquireListener diagListener;

    public static void setAcquireListenerForTest(AcquireListener listener) {
        diagListener = listener;
    }
}
