package io.rowweave.artifact;

import java.io.UncheckedIOException;
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

/** A leader-owned, barrier-gated, bounded Artifact janitor. */
public final class ArtifactJanitor implements AutoCloseable {
    private static final int MAX_DIAGNOSTIC_SAMPLES = 16;

    private final ArtifactLifecycleRepository repository;
    private final ArtifactReferenceAuthority references;
    private final ArtifactRetentionPolicy policy;
    private final LongSupplier clock;
    private final Consumer<Snapshot> observer;
    private final ScheduledExecutorService worker;
    private final Object lifecycle = new Object();
    private final Object passLock = new Object();

    private String liveCursor;
    private String recoveryCursor;
    private int inventoryUsageShard;
    private volatile long generation;
    private volatile long runningGeneration;
    private volatile boolean active;
    private volatile boolean closed;
    private ScheduledFuture<?> scheduled;
    private volatile Snapshot snapshot;

    public ArtifactJanitor(ArtifactLifecycleRepository repository,
                           ArtifactReferenceAuthority references,
                           ArtifactRetentionPolicy policy,
                           LongSupplier clock) {
        this(repository, references, policy, clock, ignored -> { });
    }

    public ArtifactJanitor(ArtifactLifecycleRepository repository,
                           ArtifactReferenceAuthority references,
                           ArtifactRetentionPolicy policy,
                           LongSupplier clock,
                           Consumer<Snapshot> observer) {
        if (repository == null) {
            throw new IllegalArgumentException("repository is required");
        }
        if (references == null) {
            throw new IllegalArgumentException("references is required");
        }
        if (policy == null) throw new IllegalArgumentException("policy is required");
        if (clock == null) throw new IllegalArgumentException("clock is required");
        if (observer == null) throw new IllegalArgumentException("observer is required");
        this.repository = repository;
        this.references = references;
        this.policy = policy;
        this.clock = clock;
        this.observer = observer;
        this.snapshot = Snapshot.empty(policy);
        this.worker = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rowweave-artifact-janitor");
            thread.setDaemon(true);
            return thread;
        });
    }

    public PassResult runOnePass(ArtifactGcOwner owner,
                                 BooleanSupplier stillLeader) {
        if (owner == null) throw new IllegalArgumentException("owner is required");
        if (stillLeader == null) {
            throw new IllegalArgumentException("stillLeader is required");
        }
        if (closed) throw new IllegalStateException("janitor is closed");
        synchronized (passLock) {
            if (closed) throw new IllegalStateException("janitor is closed");
            PassResult result = runOnePassSerial(owner, stillLeader);
            ArtifactInventoryUsage usage = sampleInventoryUsage();
            Snapshot next = snapshot.after(result, usage);
            snapshot = next;
            notifyObserver(next);
            return result;
        }
    }

    private ArtifactInventoryUsage sampleInventoryUsage() {
        try {
            int shardCount = repository.inventoryUsageShardCount();
            if (shardCount < 1 || shardCount > 256) return null;
            int shard = inventoryUsageShard % shardCount;
            inventoryUsageShard = shard + 1 == shardCount ? 0 : shard + 1;
            return repository.inventoryUsage(shard);
        } catch (RuntimeException advisoryFailure) {
            // Usage is advisory. Failure cannot affect a destructive decision.
            return null;
        }
    }

    private PassResult runOnePassSerial(
            ArtifactGcOwner owner, BooleanSupplier stillLeader) {
        long nowMillis = clock.getAsLong();
        if (nowMillis < 0L) {
            throw new IllegalStateException("clock returned negative time");
        }
        MutableResult result = new MutableResult();
        if (!advanceBarriers(result)) return result.freeze();

        int remaining = policy.maxDeletesPerPass();
        ArtifactRecoveryPage recovering;
        try {
            recovering = repository.scanRecovering(
                    recoveryCursor, policy.scanPageSize());
        } catch (ArtifactLifecycleUnavailableException | UncheckedIOException failure) {
            result.unavailable("recovery", "SCAN_UNAVAILABLE");
            return result.freeze();
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            result.corrupt("recovery", "SCAN_CORRUPT");
            recoveryCursor = nextShardCursor(recoveryCursor);
            return result.freeze();
        }
        result.scannedCandidates += recovering.candidates().size();
        int recovered = 0;
        String lastRecovery = null;
        for (ArtifactRecoveryCandidate candidate : recovering.candidates()) {
            if (remaining == 0) break;
            if (closed || !stillLeader.getAsBoolean()) {
                result.leadershipLost = true;
                break;
            }
            result.observe(candidate.candidate(), nowMillis);
            recovered++;
            remaining--;
            lastRecovery = candidate.candidate().artifactId();
            ArtifactGcToken token = recoveryToken(candidate, owner, result);
            if (token != null) collect(candidate.candidate(), token, result);
        }
        recoveryCursor = advanceCursor(recoveryCursor,
                recovering.nextCursor(), recovering.candidates().size(),
                recovered, lastRecovery);
        boolean recoveryConsumed = recovered == recovering.candidates().size();
        if (!recoveryConsumed || result.leadershipLost || remaining == 0) {
            result.cycleComplete = recoveryConsumed
                    && recovering.cycleComplete();
            return result.freeze();
        }

        long cutoffMillis = nowMillis >= policy.orphanGraceMs()
                ? nowMillis - policy.orphanGraceMs() : 0L;
        ArtifactCandidatePage live;
        try {
            live = repository.scanCandidates(
                    liveCursor, policy.scanPageSize(), cutoffMillis);
        } catch (ArtifactLifecycleUnavailableException | UncheckedIOException failure) {
            result.unavailable("live", "SCAN_UNAVAILABLE");
            return result.freeze();
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            result.corrupt("live", "SCAN_CORRUPT");
            liveCursor = nextShardCursor(liveCursor);
            return result.freeze();
        }
        result.scannedCandidates += live.candidates().size();
        int processed = 0;
        String lastLive = null;
        for (ArtifactCandidate candidate : live.candidates()) {
            if (remaining == 0) break;
            if (closed || !stillLeader.getAsBoolean()) {
                result.leadershipLost = true;
                break;
            }
            result.observe(candidate, nowMillis);
            processed++;
            remaining--;
            lastLive = candidate.artifactId();
            ArtifactGcToken token = ArtifactGcToken.create(
                    candidate.artifactId(), owner, 1L,
                    candidate.inventoryGeneration());
            collect(candidate, token, result);
        }
        liveCursor = advanceCursor(liveCursor, live.nextCursor(),
                live.candidates().size(), processed, lastLive);
        result.cycleComplete = recoveryConsumed
                && recovering.cycleComplete()
                && processed == live.candidates().size()
                && live.cycleComplete();
        return result.freeze();
    }

    private boolean advanceBarriers(MutableResult result) {
        try {
            ArtifactBackfillStatus reference = references.backfillStatus();
            if (reference == null) {
                throw new IllegalStateException("null reference barrier");
            }
            if (reference.code() == ArtifactBackfillStatus.Code.BACKFILLING) {
                reference = references.advanceBackfill(policy.scanPageSize());
            }
            ArtifactBackfillStatus inventory = repository.inventoryStatus();
            if (inventory == null) {
                throw new IllegalStateException("null inventory barrier");
            }
            if (inventory.code() == ArtifactBackfillStatus.Code.BACKFILLING) {
                inventory = repository.advanceInventoryBackfill(
                        policy.scanPageSize());
            }
            result.referenceStatus = requireStatus(reference, "reference");
            result.inventoryStatus = requireStatus(inventory, "inventory");
            if (result.referenceStatus == ArtifactBackfillStatus.Code.CORRUPT
                    || result.inventoryStatus == ArtifactBackfillStatus.Code.CORRUPT) {
                result.corrupt("barrier", "BARRIER_CORRUPT");
                return false;
            }
            if (result.referenceStatus != ArtifactBackfillStatus.Code.READY
                    || result.inventoryStatus != ArtifactBackfillStatus.Code.READY) {
                result.notReadyPasses++;
                return false;
            }
            return true;
        } catch (ArtifactLifecycleUnavailableException | UncheckedIOException failure) {
            result.unavailable("barrier", "BARRIER_UNAVAILABLE");
            return false;
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            result.corrupt("barrier", "BARRIER_CORRUPT");
            return false;
        }
    }

    private static ArtifactBackfillStatus.Code requireStatus(
            ArtifactBackfillStatus status, String authority) {
        if (status == null || status.code() == null) {
            throw new IllegalStateException(authority + " barrier returned no state");
        }
        return status.code();
    }

    private static ArtifactGcToken recoveryToken(
            ArtifactRecoveryCandidate candidate, ArtifactGcOwner owner,
            MutableResult result) {
        ArtifactGcToken observed = candidate.observedToken();
        if (observed.owner().equals(owner)) return observed;
        if (observed.ownershipEpoch() == Long.MAX_VALUE) {
            result.staleOwners++;
            return null;
        }
        return ArtifactGcToken.create(candidate.candidate().artifactId(), owner,
                observed.ownershipEpoch() + 1L,
                candidate.candidate().inventoryGeneration());
    }

    private void collect(ArtifactCandidate candidate, ArtifactGcToken token,
                         MutableResult result) {
        result.collectAttempts++;
        try {
            ArtifactCollectResult collected = repository.collect(
                    candidate, token, references);
            if (collected == null || collected.code() == null) {
                result.corrupt(candidate.artifactId(), "NULL_COLLECT_RESULT");
                return;
            }
            switch (collected.code()) {
                case DELETED:
                    result.deletedArtifacts++;
                    break;
                case ALREADY_DELETED:
                    result.alreadyDeletedArtifacts++;
                    break;
                case RETAINED_REFERENCED:
                    result.retainedReferenced++;
                    break;
                case STALE_CANDIDATE:
                    result.staleCandidates++;
                    break;
                case STALE_OWNER:
                    result.staleOwners++;
                    break;
                default:
                    result.corrupt(candidate.artifactId(),
                            "UNKNOWN_COLLECT_RESULT");
            }
        } catch (ArtifactLifecycleUnavailableException | UncheckedIOException failure) {
            result.unavailable(candidate.artifactId(), "AUTHORITY_FAILURE");
        } catch (IllegalArgumentException | IllegalStateException corruption) {
            result.corrupt(candidate.artifactId(), "CORRUPT_STATE");
        } catch (RuntimeException failure) {
            result.unavailable(candidate.artifactId(), "AUTHORITY_FAILURE");
        }
    }

    private static String advanceCursor(
            String current, String completedCursor, int pageSize,
            int processed, String lastArtifactId) {
        if (processed == pageSize) return completedCursor;
        if (lastArtifactId == null) return current;
        ArtifactLifecycleCursor position = current == null
                ? ArtifactLifecycleCursor.start(
                ArtifactLifecycleCursor.Kind.INVENTORY)
                : ArtifactLifecycleCursor.decode(
                ArtifactLifecycleCursor.Kind.INVENTORY, current);
        return ArtifactLifecycleCursor.at(
                ArtifactLifecycleCursor.Kind.INVENTORY,
                position.cycle(), position.shard(), lastArtifactId).encode();
    }

    private static String nextShardCursor(String current) {
        ArtifactLifecycleCursor position = current == null
                ? ArtifactLifecycleCursor.start(
                ArtifactLifecycleCursor.Kind.INVENTORY)
                : ArtifactLifecycleCursor.decode(
                ArtifactLifecycleCursor.Kind.INVENTORY, current);
        return position.nextShard().encode();
    }

    public Snapshot snapshot() { return snapshot; }

    public long begin(ArtifactGcOwner owner, BooleanSupplier stillLeader) {
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
                    () -> runScheduled(next, owner, stillLeader),
                    0L, TimeUnit.MILLISECONDS);
            return next;
        }
    }

    public void loseLeadership() {
        synchronized (lifecycle) {
            if (closed) return;
            active = false;
            advanceGeneration();
            if (scheduled != null) scheduled.cancel(true);
            lifecycle.notifyAll();
        }
    }

    public boolean active() { return active && !closed; }

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

    private void runScheduled(long expectedGeneration, ArtifactGcOwner owner,
                              BooleanSupplier externalLeadership) {
        synchronized (lifecycle) {
            if (!termCurrent(expectedGeneration, externalLeadership)) return;
            runningGeneration = expectedGeneration;
        }
        try {
            runOnePass(owner,
                    () -> termCurrent(expectedGeneration, externalLeadership));
        } catch (RuntimeException failure) {
            synchronized (passLock) {
                Snapshot next = snapshot.afterBackgroundFailure();
                snapshot = next;
                notifyObserver(next);
            }
        } finally {
            synchronized (lifecycle) {
                if (runningGeneration == expectedGeneration) {
                    runningGeneration = 0L;
                }
                if (termCurrent(expectedGeneration, externalLeadership)) {
                    scheduled = worker.schedule(
                            () -> runScheduled(expectedGeneration, owner,
                                    externalLeadership),
                            policy.maintenanceIntervalMs(),
                            TimeUnit.MILLISECONDS);
                }
                lifecycle.notifyAll();
            }
        }
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

    private void notifyObserver(Snapshot current) {
        try {
            observer.accept(current);
        } catch (RuntimeException ignored) {
            // Monitoring is advisory and cannot alter a destructive decision.
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

    public static final class PassResult {
        private final int scannedCandidates;
        private final int collectAttempts;
        private final int deletedArtifacts;
        private final int alreadyDeletedArtifacts;
        private final int retainedReferenced;
        private final int staleCandidates;
        private final int staleOwners;
        private final int corruptArtifacts;
        private final int unavailableArtifacts;
        private final int notReadyPasses;
        private final long oldestCandidateAgeMillis;
        private final boolean leadershipLost;
        private final boolean cycleComplete;
        private final ArtifactBackfillStatus.Code referenceStatus;
        private final ArtifactBackfillStatus.Code inventoryStatus;
        private final List<String> diagnosticSamples;

        private PassResult(MutableResult source) {
            scannedCandidates = source.scannedCandidates;
            collectAttempts = source.collectAttempts;
            deletedArtifacts = source.deletedArtifacts;
            alreadyDeletedArtifacts = source.alreadyDeletedArtifacts;
            retainedReferenced = source.retainedReferenced;
            staleCandidates = source.staleCandidates;
            staleOwners = source.staleOwners;
            corruptArtifacts = source.corruptArtifacts;
            unavailableArtifacts = source.unavailableArtifacts;
            notReadyPasses = source.notReadyPasses;
            oldestCandidateAgeMillis = source.oldestCandidateAgeMillis;
            leadershipLost = source.leadershipLost;
            cycleComplete = source.cycleComplete;
            referenceStatus = source.referenceStatus;
            inventoryStatus = source.inventoryStatus;
            diagnosticSamples = Collections.unmodifiableList(
                    new ArrayList<>(source.diagnosticSamples));
        }

        public int scannedCandidates() { return scannedCandidates; }
        public int collectAttempts() { return collectAttempts; }
        public int deletedArtifacts() { return deletedArtifacts; }
        public int alreadyDeletedArtifacts() { return alreadyDeletedArtifacts; }
        public int retainedReferenced() { return retainedReferenced; }
        public int staleCandidates() { return staleCandidates; }
        public int staleOwners() { return staleOwners; }
        public int corruptArtifacts() { return corruptArtifacts; }
        public int unavailableArtifacts() { return unavailableArtifacts; }
        public int notReadyPasses() { return notReadyPasses; }
        public long oldestCandidateAgeMillis() { return oldestCandidateAgeMillis; }
        public boolean leadershipLost() { return leadershipLost; }
        public boolean cycleComplete() { return cycleComplete; }
        public ArtifactBackfillStatus.Code referenceStatus() { return referenceStatus; }
        public ArtifactBackfillStatus.Code inventoryStatus() { return inventoryStatus; }
        public List<String> diagnosticSamples() { return diagnosticSamples; }
    }

    public static final class Snapshot {
        private final int backlog;
        private final int scanPageCapacity;
        private final long oldestCandidateAgeMillis;
        private final long deletedArtifacts;
        private final long retainedReferenced;
        private final long staleOwners;
        private final long errorArtifacts;
        private final long notReadyPasses;
        private final boolean cycleComplete;
        private final ArtifactBackfillStatus.Code referenceStatus;
        private final ArtifactBackfillStatus.Code inventoryStatus;
        private final ArtifactInventoryUsage inventoryUsage;
        private final List<String> diagnosticSamples;

        private Snapshot(int backlog, int scanPageCapacity,
                         long oldestCandidateAgeMillis,
                         long deletedArtifacts, long retainedReferenced,
                         long staleOwners, long errorArtifacts,
                         long notReadyPasses, boolean cycleComplete,
                         ArtifactBackfillStatus.Code referenceStatus,
                         ArtifactBackfillStatus.Code inventoryStatus,
                         ArtifactInventoryUsage inventoryUsage,
                         List<String> diagnosticSamples) {
            this.backlog = backlog;
            this.scanPageCapacity = scanPageCapacity;
            this.oldestCandidateAgeMillis = oldestCandidateAgeMillis;
            this.deletedArtifacts = deletedArtifacts;
            this.retainedReferenced = retainedReferenced;
            this.staleOwners = staleOwners;
            this.errorArtifacts = errorArtifacts;
            this.notReadyPasses = notReadyPasses;
            this.cycleComplete = cycleComplete;
            this.referenceStatus = referenceStatus;
            this.inventoryStatus = inventoryStatus;
            this.inventoryUsage = inventoryUsage;
            this.diagnosticSamples = Collections.unmodifiableList(
                    new ArrayList<>(diagnosticSamples));
        }

        private static Snapshot empty(ArtifactRetentionPolicy policy) {
            return new Snapshot(0, policy.scanPageSize(), 0L,
                    0L, 0L, 0L, 0L, 0L, false,
                    ArtifactBackfillStatus.Code.BACKFILLING,
                    ArtifactBackfillStatus.Code.BACKFILLING,
                    ArtifactInventoryUsage.unknown(
                            policy.maxEntriesPerShard(),
                            policy.maxBytesPerShard()),
                    Collections.emptyList());
        }

        private Snapshot after(PassResult pass, ArtifactInventoryUsage usage) {
            return new Snapshot(pass.scannedCandidates(), scanPageCapacity,
                    pass.oldestCandidateAgeMillis(),
                    saturatedAdd(deletedArtifacts, pass.deletedArtifacts()),
                    saturatedAdd(retainedReferenced,
                            pass.retainedReferenced()),
                    saturatedAdd(staleOwners, pass.staleOwners()),
                    saturatedAdd(errorArtifacts,
                            (long) pass.corruptArtifacts()
                                    + pass.unavailableArtifacts()),
                    saturatedAdd(notReadyPasses, pass.notReadyPasses()),
                    pass.cycleComplete(), pass.referenceStatus(),
                    pass.inventoryStatus(),
                    usage == null ? inventoryUsage : usage,
                    pass.diagnosticSamples());
        }

        private Snapshot afterBackgroundFailure() {
            return new Snapshot(0, scanPageCapacity, 0L,
                    deletedArtifacts, retainedReferenced, staleOwners,
                    saturatedAdd(errorArtifacts, 1L), notReadyPasses,
                    false, referenceStatus, inventoryStatus,
                    inventoryUsage,
                    Collections.singletonList("background:UNEXPECTED_FAILURE"));
        }

        public int backlog() { return backlog; }
        public int scanPageCapacity() { return scanPageCapacity; }
        public long oldestCandidateAgeMillis() { return oldestCandidateAgeMillis; }
        public long deletedArtifacts() { return deletedArtifacts; }
        public long retainedReferenced() { return retainedReferenced; }
        public long staleOwners() { return staleOwners; }
        public long errorArtifacts() { return errorArtifacts; }
        public long notReadyPasses() { return notReadyPasses; }
        public boolean cycleComplete() { return cycleComplete; }
        public ArtifactBackfillStatus.Code referenceStatus() { return referenceStatus; }
        public ArtifactBackfillStatus.Code inventoryStatus() { return inventoryStatus; }
        public ArtifactInventoryUsage.Scope inventoryScope() {
            return inventoryUsage.scope();
        }
        public int inventoryObservedShard() { return inventoryUsage.shard(); }
        public int inventoryEntries() { return inventoryUsage.entries(); }
        public long inventoryBytes() { return inventoryUsage.bytes(); }
        public int inventoryEntryCapacity() {
            return inventoryUsage.entryCapacity();
        }
        public long inventoryByteCapacity() {
            return inventoryUsage.byteCapacity();
        }
        public List<String> diagnosticSamples() { return diagnosticSamples; }

        private static long saturatedAdd(long left, long right) {
            if (right < 0L) {
                throw new IllegalArgumentException("counter delta must be >= 0");
            }
            return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
        }
    }

    private static final class MutableResult {
        private int scannedCandidates;
        private int collectAttempts;
        private int deletedArtifacts;
        private int alreadyDeletedArtifacts;
        private int retainedReferenced;
        private int staleCandidates;
        private int staleOwners;
        private int corruptArtifacts;
        private int unavailableArtifacts;
        private int notReadyPasses;
        private long oldestCandidateAgeMillis;
        private boolean leadershipLost;
        private boolean cycleComplete;
        private ArtifactBackfillStatus.Code referenceStatus =
                ArtifactBackfillStatus.Code.BACKFILLING;
        private ArtifactBackfillStatus.Code inventoryStatus =
                ArtifactBackfillStatus.Code.BACKFILLING;
        private final List<String> diagnosticSamples = new ArrayList<>();

        private void observe(ArtifactCandidate candidate, long nowMillis) {
            long age = nowMillis >= candidate.createdAtMillis()
                    ? nowMillis - candidate.createdAtMillis() : 0L;
            oldestCandidateAgeMillis = Math.max(oldestCandidateAgeMillis, age);
        }

        private void corrupt(String artifactId, String code) {
            corruptArtifacts++;
            sample(artifactId, code);
        }

        private void unavailable(String artifactId, String code) {
            unavailableArtifacts++;
            sample(artifactId, code);
        }

        private void sample(String artifactId, String code) {
            if (diagnosticSamples.size() < MAX_DIAGNOSTIC_SAMPLES) {
                diagnosticSamples.add(artifactId + ':' + code);
            }
        }

        private PassResult freeze() { return new PassResult(this); }
    }
}
