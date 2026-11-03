package io.rowweave.run;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * Bounded, leader-owned maintenance kernel for terminal Runs.
 *
 * <p>Each call reads one lifecycle page and handles at most
 * {@link RunRetentionPolicy#maxDeletesPerPass()} Runs, and for each Run that has taken ownership it
 * performs one backend-bounded purge step.</p>
 */
public final class TerminalRunJanitor implements AutoCloseable {
    private static final int MAX_DIAGNOSTIC_SAMPLES = 16;

    private final RunLifecycleCatalog catalog;
    private final RunRetentionAuthority authority;
    private final RunRetentionPolicy policy;
    private final LongSupplier clock;
    private final Consumer<Snapshot> observer;
    private final ScheduledExecutorService worker;
    private final Object lifecycle = new Object();
    private final Object passLock = new Object();

    private String cursor;
    private volatile long generation;
    private volatile long runningGeneration;
    private volatile boolean active;
    private volatile boolean closed;
    private ScheduledFuture<?> scheduled;
    private volatile Snapshot snapshot;

    public TerminalRunJanitor(RunLifecycleCatalog catalog,
                              RunRetentionAuthority authority,
                              RunRetentionPolicy policy,
                              LongSupplier clock) {
        this(catalog, authority, policy, clock, ignored -> { });
    }

    public TerminalRunJanitor(RunLifecycleCatalog catalog,
                              RunRetentionAuthority authority,
                              RunRetentionPolicy policy,
                              LongSupplier clock,
                              Consumer<Snapshot> observer) {
        if (catalog == null) throw new IllegalArgumentException("catalog is required");
        if (authority == null) throw new IllegalArgumentException("authority is required");
        if (policy == null) throw new IllegalArgumentException("policy is required");
        if (clock == null) throw new IllegalArgumentException("clock is required");
        if (observer == null) throw new IllegalArgumentException("observer is required");
        this.catalog = catalog;
        this.authority = authority;
        this.policy = policy;
        this.clock = clock;
        this.observer = observer;
        this.snapshot = Snapshot.empty(policy);
        this.worker = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rowweave-terminal-run-janitor");
            thread.setDaemon(true);
            return thread;
        });
    }

    public PassResult runOnePass(RetentionOwner owner,
                                 BooleanSupplier stillLeader) {
        if (owner == null) throw new IllegalArgumentException("owner is required");
        if (stillLeader == null) {
            throw new IllegalArgumentException("stillLeader is required");
        }
        if (closed) throw new IllegalStateException("janitor is closed");

        synchronized (passLock) {
            if (closed) throw new IllegalStateException("janitor is closed");
            PassResult result = runOnePassSerial(owner, stillLeader);
            Snapshot next = snapshot.after(result);
            snapshot = next;
            notifyObserver(next);
            return result;
        }
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    private void notifyObserver(Snapshot current) {
        try {
            observer.accept(current);
        } catch (RuntimeException ignored) {
            // the monitor is advisory and must never change a retention decision or the background loop.
        }
    }

    private PassResult runOnePassSerial(RetentionOwner owner,
                                        BooleanSupplier stillLeader) {
        long nowMillis = clock.getAsLong();
        if (nowMillis < 0L) throw new IllegalStateException("clock returned negative time");
        long cutoffMillis = nowMillis >= policy.terminalRetentionMs()
                ? nowMillis - policy.terminalRetentionMs() : 0L;
        String pageCursor = cursor;
        RunLifecyclePage page = catalog.scanLifecycle(
                pageCursor, policy.scanPageSize());
        MutableResult result = new MutableResult(page.runs().size());
        String lastProcessedDigest = null;

        for (RunLifecycleRef ref : page.runs()) {
            if (result.claimAttempts >= policy.maxDeletesPerPass()) {
                break;
            }
            if (closed || !stillLeader.getAsBoolean()) {
                result.leadershipLost = true;
                break;
            }
            result.observe(ref, nowMillis);
            result.claimAttempts++;
            lastProcessedDigest = RunLifecycleCatalogKernel.digestHex(ref.runId());
            try {
                long observedEpoch = ref.state() == RunLifecycleState.DELETING
                        ? ref.retentionToken().ownershipEpoch() : 0L;
                RunRetentionClaim claim = authority.claim(
                        ref.runId(), ref.revision(), cutoffMillis, owner, observedEpoch);
                if (claim == null) {
                    result.corrupt(ref.runId(), "NULL_CLAIM");
                    continue;
                }
                switch (claim.code()) {
                    case NOT_DUE:
                        result.notDueRuns++;
                        continue;
                    case LOST_RACE:
                        result.lostRaceRuns++;
                        continue;
                    case STALE_RUN:
                        result.staleRuns++;
                        continue;
                    case ACQUIRED:
                    case REPLAYED:
                        result.claimedRuns++;
                        break;
                    default:
                        result.corrupt(ref.runId(), "UNKNOWN_CLAIM");
                        continue;
                }
                if (closed || !stillLeader.getAsBoolean()) {
                    result.leadershipLost = true;
                    break;
                }
                result.purgeSteps++;
                classifyPurge(ref.runId(), authority.purge(
                        claim.token(), policy.maxDeletesPerPass()), result);
            } catch (RunStateStoreUnavailableException unavailable) {
                result.unavailable(ref.runId(), "AUTHORITY_UNAVAILABLE");
            } catch (IllegalArgumentException | IllegalStateException corrupt) {
                result.corrupt(ref.runId(), "CORRUPT_STATE");
            } catch (RuntimeException unavailable) {
                result.unavailable(ref.runId(), "AUTHORITY_FAILURE");
            }
        }

        boolean consumedPage = result.claimAttempts == page.runs().size();
        if (consumedPage) {
            cursor = page.nextCursor();
        } else if (lastProcessedDigest != null) {
            RunLifecycleCatalogKernel.Cursor position =
                    RunLifecycleCatalogKernel.decodeCursor(pageCursor);
            cursor = RunLifecycleCatalogKernel.encodeCursor(
                    position.cycle(), position.shard(), lastProcessedDigest);
        }
        result.cycleComplete = consumedPage && page.cycleComplete();
        return result.freeze();
    }

    /** Starts one leadership generation: the first pass is submitted at once, later ones follow the policy interval in sequence. */
    public long begin(RetentionOwner owner, BooleanSupplier stillLeader) {
        if (owner == null) throw new IllegalArgumentException("owner is required");
        if (stillLeader == null) {
            throw new IllegalArgumentException("stillLeader is required");
        }
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("janitor is closed");
            long next = generation + 1L;
            if (next <= 0L) throw new IllegalStateException("janitor generation exhausted");
            generation = next;
            active = true;
            if (scheduled != null) scheduled.cancel(true);
            scheduled = worker.schedule(
                    () -> runScheduled(next, owner, stillLeader), 0L,
                    TimeUnit.MILLISECONDS);
            return next;
        }
    }

    /** Local fast fence on leadership loss, which does not wait for backend I/O it cannot bound. */
    public void loseLeadership() {
        synchronized (lifecycle) {
            if (closed) return;
            active = false;
            advanceGeneration();
            if (scheduled != null) scheduled.cancel(true);
            lifecycle.notifyAll();
        }
    }

    public boolean active() {
        return active && !closed;
    }

    /** For tests and bounded shutdown: waits until the passes of one generation still in flight have really exited. */
    public boolean awaitStopped(long expectedGeneration, long timeout,
                                TimeUnit unit) throws InterruptedException {
        if (expectedGeneration <= 0L) {
            throw new IllegalArgumentException("expectedGeneration must be > 0");
        }
        if (timeout < 0L) throw new IllegalArgumentException("timeout must be >= 0");
        if (unit == null) throw new IllegalArgumentException("unit is required");
        long remaining = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remaining;
        synchronized (lifecycle) {
            while (runningGeneration == expectedGeneration) {
                if (remaining <= 0L) return false;
                TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
                remaining = deadline - System.nanoTime();
            }
            return true;
        }
    }

    private void runScheduled(long expectedGeneration, RetentionOwner owner,
                              BooleanSupplier externalLeadership) {
        synchronized (lifecycle) {
            if (!termCurrent(expectedGeneration, externalLeadership)) return;
            runningGeneration = expectedGeneration;
        }
        try {
            runOnePass(owner,
                    () -> termCurrent(expectedGeneration, externalLeadership));
        } catch (RunStateStoreUnavailableException unavailable) {
            recordBackgroundFailure("catalog:AUTHORITY_UNAVAILABLE", false);
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            recordBackgroundFailure("catalog:CORRUPT_STATE", true);
        } catch (RuntimeException unavailable) {
            recordBackgroundFailure("catalog:AUTHORITY_FAILURE", false);
        } finally {
            synchronized (lifecycle) {
                if (runningGeneration == expectedGeneration) {
                    runningGeneration = 0L;
                }
                if (termCurrent(expectedGeneration, externalLeadership)) {
                    scheduled = worker.schedule(
                            () -> runScheduled(expectedGeneration, owner,
                                    externalLeadership),
                            policy.maintenanceIntervalMs(), TimeUnit.MILLISECONDS);
                }
                lifecycle.notifyAll();
            }
        }
    }

    private void recordBackgroundFailure(String diagnostic,
                                         boolean skipCorruptShard) {
        synchronized (passLock) {
            if (skipCorruptShard) advanceCursorPastCurrentShard();
            Snapshot next = snapshot.afterFailure(diagnostic);
            snapshot = next;
            notifyObserver(next);
        }
    }

    private void advanceCursorPastCurrentShard() {
        RunLifecycleCatalogKernel.Cursor position =
                RunLifecycleCatalogKernel.decodeCursor(cursor);
        boolean completesCycle =
                position.shard() == RunLifecycleCatalogKernel.SHARD_COUNT - 1;
        long nextCycle = completesCycle ? position.cycle() + 1L : position.cycle();
        if (completesCycle && nextCycle < 0L) {
            throw new IllegalStateException("lifecycle cursor overflow");
        }
        cursor = RunLifecycleCatalogKernel.encodeCursor(
                nextCycle, completesCycle ? 0 : position.shard() + 1, null);
    }

    private boolean termCurrent(long expectedGeneration,
                                BooleanSupplier externalLeadership) {
        return !closed && active && generation == expectedGeneration
                && externalLeadership.getAsBoolean();
    }

    private void advanceGeneration() {
        long next = generation + 1L;
        if (next <= 0L) {
            closed = true;
            active = false;
            return;
        }
        generation = next;
    }

    private static void classifyPurge(String runId, RunPurgeProgress progress,
                                      MutableResult result) {
        if (progress == null) {
            result.corrupt(runId, "NULL_PURGE_PROGRESS");
            return;
        }
        switch (progress.code()) {
            case PURGED:
                result.purgedRuns++;
                break;
            case IN_PROGRESS:
                result.inProgressRuns++;
                result.deletedRecords += progress.deletedRecords();
                break;
            case STALE_OWNER:
                result.lostRaceRuns++;
                break;
            case CORRUPT:
                result.corrupt(runId, "PURGE_CORRUPT");
                break;
            default:
                result.corrupt(runId, "UNKNOWN_PURGE_PROGRESS");
        }
    }

    @Override
    public void close() {
        synchronized (lifecycle) {
            if (closed) return;
            closed = true;
            active = false;
            advanceGeneration();
            if (scheduled != null) scheduled.cancel(true);
            lifecycle.notifyAll();
        }
        worker.shutdownNow();
    }

    /** Immutable observation of one bounded maintenance pass. */
    public static final class PassResult {
        private final int scannedRefs;
        private final int claimAttempts;
        private final int claimedRuns;
        private final int purgeSteps;
        private final int purgedRuns;
        private final int inProgressRuns;
        private final int notDueRuns;
        private final int lostRaceRuns;
        private final int staleRuns;
        private final int corruptRuns;
        private final int unavailableRuns;
        private final int deletedRecords;
        private final long oldestTerminalAgeMillis;
        private final boolean leadershipLost;
        private final boolean cycleComplete;
        private final List<String> diagnosticSamples;

        private PassResult(MutableResult source) {
            this.scannedRefs = source.scannedRefs;
            this.claimAttempts = source.claimAttempts;
            this.claimedRuns = source.claimedRuns;
            this.purgeSteps = source.purgeSteps;
            this.purgedRuns = source.purgedRuns;
            this.inProgressRuns = source.inProgressRuns;
            this.notDueRuns = source.notDueRuns;
            this.lostRaceRuns = source.lostRaceRuns;
            this.staleRuns = source.staleRuns;
            this.corruptRuns = source.corruptRuns;
            this.unavailableRuns = source.unavailableRuns;
            this.deletedRecords = source.deletedRecords;
            this.oldestTerminalAgeMillis = source.oldestTerminalAgeMillis;
            this.leadershipLost = source.leadershipLost;
            this.cycleComplete = source.cycleComplete;
            this.diagnosticSamples = Collections.unmodifiableList(
                    new ArrayList<>(source.diagnosticSamples));
        }

        public int scannedRefs() { return scannedRefs; }
        public int claimAttempts() { return claimAttempts; }
        public int claimedRuns() { return claimedRuns; }
        public int purgeSteps() { return purgeSteps; }
        public int purgedRuns() { return purgedRuns; }
        public int inProgressRuns() { return inProgressRuns; }
        public int notDueRuns() { return notDueRuns; }
        public int lostRaceRuns() { return lostRaceRuns; }
        public int staleRuns() { return staleRuns; }
        public int corruptRuns() { return corruptRuns; }
        public int unavailableRuns() { return unavailableRuns; }
        public int deletedRecords() { return deletedRecords; }
        public long oldestTerminalAgeMillis() { return oldestTerminalAgeMillis; }
        public boolean leadershipLost() { return leadershipLost; }
        public boolean cycleComplete() { return cycleComplete; }
        public List<String> diagnosticSamples() { return diagnosticSamples; }
    }

    /** Bounded observation of the janitor; the backlog is the last page observed and does not claim to be a global exact count. */
    public static final class Snapshot {
        private final int backlog;
        private final int lifecycleCapacityPerShard;
        private final int scanPageCapacity;
        private final long oldestTerminalAgeMillis;
        private final long claimedRuns;
        private final long purgedRuns;
        private final long deletedRecords;
        private final long errorRuns;
        private final boolean cycleComplete;
        private final List<String> diagnosticSamples;

        private Snapshot(int backlog, int lifecycleCapacityPerShard,
                         int scanPageCapacity, long oldestTerminalAgeMillis,
                         long claimedRuns, long purgedRuns, long deletedRecords,
                         long errorRuns, boolean cycleComplete,
                         List<String> diagnosticSamples) {
            this.backlog = backlog;
            this.lifecycleCapacityPerShard = lifecycleCapacityPerShard;
            this.scanPageCapacity = scanPageCapacity;
            this.oldestTerminalAgeMillis = oldestTerminalAgeMillis;
            this.claimedRuns = claimedRuns;
            this.purgedRuns = purgedRuns;
            this.deletedRecords = deletedRecords;
            this.errorRuns = errorRuns;
            this.cycleComplete = cycleComplete;
            this.diagnosticSamples = Collections.unmodifiableList(
                    new ArrayList<>(diagnosticSamples));
        }

        private static Snapshot empty(RunRetentionPolicy policy) {
            return new Snapshot(0, policy.maxEntriesPerShard(),
                    policy.scanPageSize(), 0L, 0L, 0L, 0L, 0L,
                    false, Collections.emptyList());
        }

        private Snapshot after(PassResult pass) {
            long passErrors = (long) pass.corruptRuns() + pass.unavailableRuns();
            return new Snapshot(pass.scannedRefs(), lifecycleCapacityPerShard,
                    scanPageCapacity, pass.oldestTerminalAgeMillis(),
                    saturatedAdd(claimedRuns, pass.claimedRuns()),
                    saturatedAdd(purgedRuns, pass.purgedRuns()),
                    saturatedAdd(deletedRecords, pass.deletedRecords()),
                    saturatedAdd(errorRuns, passErrors), pass.cycleComplete(),
                    pass.diagnosticSamples());
        }

        private Snapshot afterFailure(String diagnostic) {
            return new Snapshot(0, lifecycleCapacityPerShard, scanPageCapacity,
                    0L, claimedRuns, purgedRuns, deletedRecords,
                    saturatedAdd(errorRuns, 1L), false,
                    Collections.singletonList(diagnostic));
        }

        public int backlog() { return backlog; }
        public int lifecycleCapacityPerShard() { return lifecycleCapacityPerShard; }
        public int scanPageCapacity() { return scanPageCapacity; }
        public long oldestTerminalAgeMillis() { return oldestTerminalAgeMillis; }
        public long claimedRuns() { return claimedRuns; }
        public long purgedRuns() { return purgedRuns; }
        public long deletedRecords() { return deletedRecords; }
        public long errorRuns() { return errorRuns; }
        public boolean cycleComplete() { return cycleComplete; }
        public List<String> diagnosticSamples() { return diagnosticSamples; }

        private static long saturatedAdd(long left, long right) {
            if (right < 0L) throw new IllegalArgumentException("counter delta must be >= 0");
            return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
        }
    }

    private static final class MutableResult {
        private final int scannedRefs;
        private int claimAttempts;
        private int claimedRuns;
        private int purgeSteps;
        private int purgedRuns;
        private int inProgressRuns;
        private int notDueRuns;
        private int lostRaceRuns;
        private int staleRuns;
        private int corruptRuns;
        private int unavailableRuns;
        private int deletedRecords;
        private long oldestTerminalAgeMillis;
        private boolean leadershipLost;
        private boolean cycleComplete;
        private final List<String> diagnosticSamples = new ArrayList<>();

        private MutableResult(int scannedRefs) {
            this.scannedRefs = scannedRefs;
        }

        private void observe(RunLifecycleRef ref, long nowMillis) {
            long age = nowMillis >= ref.terminalAtMillis()
                    ? nowMillis - ref.terminalAtMillis() : 0L;
            oldestTerminalAgeMillis = Math.max(oldestTerminalAgeMillis, age);
        }

        private void corrupt(String runId, String code) {
            corruptRuns++;
            sample(runId, code);
        }

        private void unavailable(String runId, String code) {
            unavailableRuns++;
            sample(runId, code);
        }

        private void sample(String runId, String code) {
            if (diagnosticSamples.size() < MAX_DIAGNOSTIC_SAMPLES) {
                diagnosticSamples.add(runId + ':' + code);
            }
        }

        private PassResult freeze() {
            return new PassResult(this);
        }
    }
}
