package io.rowweave.cluster;

import io.rowweave.artifact.ArtifactRepository;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.artifact.PlanArtifactValidator;
import io.rowweave.artifact.PlanArtifactValidationException;
import io.rowweave.run.AttemptSnapshot;
import io.rowweave.run.DurableStageAssignmentAuthority;
import io.rowweave.run.RecoverableRunCatalog;
import io.rowweave.run.RecoverableRunPage;
import io.rowweave.run.RecoverableRunRef;
import io.rowweave.run.RecoveryClaim;
import io.rowweave.run.RecoveryOwner;
import io.rowweave.run.RecoveryToken;
import io.rowweave.run.RunRecoveryCatalogKernel;
import io.rowweave.run.RunRecoveryOwnership;
import io.rowweave.run.RunSnapshot;
import io.rowweave.run.RunStateStore;
import io.rowweave.run.RunStateStoreUnavailableException;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RetryReadyEvaluation;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.WholeAttemptRetryAuthority;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;

/** Bounded inventory of inherited runs, taken once per leadership term. */
final class LeaderRecoveryInventory implements AutoCloseable {
    private static final long MAINTENANCE_SAFETY_WAKE_MILLIS = 30_000L;
    enum State {
        IDLE,
        SCANNING,
        EMPTY_READY,
        BLOCKED_PENDING_RECOVERY,
        BLOCKED_CORRUPT,
        BLOCKED_UNAVAILABLE,
        BLOCKED_CAPABILITY,
        LEADERSHIP_LOST,
        CLOSED
    }

    static final class Config {
        final int pageSize;
        final long initialBackoffMillis;
        final long maxBackoffMillis;
        final int sampleLimit;
        final long maintenanceContinuationMillis;

        Config(int pageSize, long initialBackoffMillis,
               long maxBackoffMillis, int sampleLimit) {
            this(pageSize, initialBackoffMillis, maxBackoffMillis,
                    sampleLimit, 1L);
        }

        Config(int pageSize, long initialBackoffMillis,
               long maxBackoffMillis, int sampleLimit,
               long maintenanceContinuationMillis) {
            if (pageSize < 1 || pageSize > RunRecoveryCatalogKernel.MAX_PAGE_SIZE) {
                throw new IllegalArgumentException("recovery inventory pageSize out of bounds");
            }
            if (initialBackoffMillis < 1L || maxBackoffMillis < initialBackoffMillis) {
                throw new IllegalArgumentException("recovery inventory backoff out of bounds");
            }
            if (sampleLimit < 1 || sampleLimit > 64) {
                throw new IllegalArgumentException("recovery inventory sampleLimit out of bounds");
            }
            if (maintenanceContinuationMillis < 1L) {
                throw new IllegalArgumentException(
                        "maintenance continuation must be positive");
            }
            this.pageSize = pageSize;
            this.initialBackoffMillis = initialBackoffMillis;
            this.maxBackoffMillis = maxBackoffMillis;
            this.sampleLimit = sampleLimit;
            this.maintenanceContinuationMillis =
                    maintenanceContinuationMillis;
        }
    }

    static final class Snapshot {
        private final long generation;
        private final State state;
        private final int activeRuns;
        private final List<String> samples;
        private final String failure;
        private final String maintenanceCursor;
        private final int waitingRetries;
        private final long nextRetryDelayMillis;

        Snapshot(long generation, State state, int activeRuns,
                 List<String> samples, String failure) {
            this(generation, state, activeRuns, samples, failure, null);
        }

        Snapshot(long generation, State state, int activeRuns,
                 List<String> samples, String failure,
                 String maintenanceCursor) {
            this.generation = generation;
            this.state = state;
            this.activeRuns = activeRuns;
            this.samples = Collections.unmodifiableList(new ArrayList<>(samples));
            this.failure = failure;
            this.maintenanceCursor = maintenanceCursor;
            this.waitingRetries = 0;
            this.nextRetryDelayMillis = Long.MAX_VALUE;
        }

        Snapshot(long generation, State state, int activeRuns,
                 List<String> samples, String failure,
                 String maintenanceCursor, int waitingRetries,
                 long nextRetryDelayMillis) {
            this.generation = generation;
            this.state = state;
            this.activeRuns = activeRuns;
            this.samples = Collections.unmodifiableList(new ArrayList<>(samples));
            this.failure = failure;
            this.maintenanceCursor = maintenanceCursor;
            this.waitingRetries = waitingRetries;
            this.nextRetryDelayMillis = nextRetryDelayMillis;
        }

        long generation() { return generation; }
        State state() { return state; }
        int activeRuns() { return activeRuns; }
        List<String> samples() { return samples; }
        String failure() { return failure; }
        String maintenanceCursor() { return maintenanceCursor; }
        int waitingRetries() { return waitingRetries; }
        long nextRetryDelayMillis() { return nextRetryDelayMillis; }
    }

    private final RunStateStore stateStore;
    private final RecoverableRunCatalog catalog;
    private final RunRecoveryOwnership ownership;
    private final ArtifactRepository artifacts;
    private final DurableStageAssignmentAuthority assignments;
    private final Config config;
    private final InheritedRunRecovery.Handler recoveryHandler;
    private final WholeAttemptRetryAuthority retryAuthority;
    private final Predicate<String> locallyExecuting;
    private final RetryRecoveryMetrics metrics;
    private final String deploymentSetId;
    private final boolean requireDeploymentFeature;
    private final ExecutorService worker;
    private final java.util.concurrent.atomic.AtomicReference<Thread> workerThread =
            new java.util.concurrent.atomic.AtomicReference<>();
    private final Object lifecycle = new Object();

    private volatile long generation;
    private volatile Snapshot snapshot = new Snapshot(
            0L, State.IDLE, 0, Collections.emptyList(), null);
    private volatile boolean closed;
    private Future<?> currentTask;
    private long signalVersion;
    private long nextRetryDelayMillis = Long.MAX_VALUE;

    LeaderRecoveryInventory(RunStateStore stateStore,
                            RecoverableRunCatalog catalog,
                            RunRecoveryOwnership ownership,
                            ArtifactRepository artifacts,
                            Config config) {
        this(stateStore, catalog, ownership, artifacts, config, null);
    }

    LeaderRecoveryInventory(RunStateStore stateStore,
                            RecoverableRunCatalog catalog,
                            RunRecoveryOwnership ownership,
                            ArtifactRepository artifacts,
                            Config config,
                            InheritedRunRecovery.Handler recoveryHandler) {
        this(stateStore, catalog, ownership, artifacts, null, config, recoveryHandler,
                null, false);
    }

    LeaderRecoveryInventory(RunStateStore stateStore,
                            RecoverableRunCatalog catalog,
                            RunRecoveryOwnership ownership,
                            ArtifactRepository artifacts,
                            DurableStageAssignmentAuthority assignments,
                            Config config,
                            InheritedRunRecovery.Handler recoveryHandler) {
        this(stateStore, catalog, ownership, artifacts, assignments, config, recoveryHandler,
                null, false, runId -> false);
    }

    LeaderRecoveryInventory(RunStateStore stateStore,
                            RecoverableRunCatalog catalog,
                            RunRecoveryOwnership ownership,
                            ArtifactRepository artifacts,
                            DurableStageAssignmentAuthority assignments,
                            Config config,
                            InheritedRunRecovery.Handler recoveryHandler,
                            String deploymentSetId,
                            boolean requireDeploymentFeature) {
        this(stateStore, catalog, ownership, artifacts, assignments, config,
                recoveryHandler, deploymentSetId, requireDeploymentFeature,
                runId -> false);
    }

    LeaderRecoveryInventory(RunStateStore stateStore,
                            RecoverableRunCatalog catalog,
                            RunRecoveryOwnership ownership,
                            ArtifactRepository artifacts,
                            DurableStageAssignmentAuthority assignments,
                            Config config,
                            InheritedRunRecovery.Handler recoveryHandler,
                            String deploymentSetId,
                            boolean requireDeploymentFeature,
                            Predicate<String> locallyExecuting) {
        this(stateStore, catalog, ownership, artifacts, assignments, config,
                recoveryHandler, deploymentSetId, requireDeploymentFeature,
                locallyExecuting, null);
    }

    LeaderRecoveryInventory(RunStateStore stateStore,
                            RecoverableRunCatalog catalog,
                            RunRecoveryOwnership ownership,
                            ArtifactRepository artifacts,
                            DurableStageAssignmentAuthority assignments,
                            Config config,
                            InheritedRunRecovery.Handler recoveryHandler,
                            String deploymentSetId,
                            boolean requireDeploymentFeature,
                            Predicate<String> locallyExecuting,
                            RetryRecoveryMetrics metrics) {
        this.stateStore = Objects.requireNonNull(stateStore, "stateStore");
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.ownership = Objects.requireNonNull(ownership, "ownership");
        this.artifacts = Objects.requireNonNull(artifacts, "artifacts");
        this.assignments = assignments;
        this.config = Objects.requireNonNull(config, "config");
        this.recoveryHandler = recoveryHandler;
        this.retryAuthority = stateStore instanceof WholeAttemptRetryAuthority
                ? (WholeAttemptRetryAuthority) stateStore : null;
        this.locallyExecuting = Objects.requireNonNull(
                locallyExecuting, "locallyExecuting");
        this.deploymentSetId = deploymentSetId;
        this.requireDeploymentFeature = requireDeploymentFeature;
        this.metrics = metrics;
        this.worker = Executors.newSingleThreadExecutor(runnable -> {
            Thread thread = new Thread(runnable, "rowweave-recovery-inventory");
            thread.setDaemon(true);
            workerThread.set(thread);
            return thread;
        });
    }

    long begin(RecoveryOwner owner, BooleanSupplier stillLeader) {
        Objects.requireNonNull(owner, "owner");
        Objects.requireNonNull(stillLeader, "stillLeader");
        synchronized (lifecycle) {
            if (closed) throw new IllegalStateException("recovery inventory is closed");
            long next = generation + 1L;
            if (next <= 0L) throw new IllegalStateException("recovery inventory generation exhausted");
            generation = next;
            if (currentTask != null) currentTask.cancel(true);
            publishLocked(new Snapshot(next, State.SCANNING,
                    0, Collections.emptyList(), null));
            currentTask = worker.submit(() -> run(next, owner, stillLeader));
            return next;
        }
    }

    void loseLeadership() {
        synchronized (lifecycle) {
            if (closed) return;
            generation++;
            if (currentTask != null) currentTask.cancel(true);
            publishLocked(new Snapshot(generation, State.LEADERSHIP_LOST,
                    0, Collections.emptyList(), null));
        }
    }

    void signal() {
        synchronized (lifecycle) {
            if (closed) return;
            signalVersion++;
            lifecycle.notifyAll();
        }
    }

    Snapshot snapshot() {
        return snapshot;
    }

    boolean ready(long expectedGeneration) {
        Snapshot current = snapshot;
        return current.generation == expectedGeneration && current.state == State.EMPTY_READY;
    }

    boolean awaitSettled(long expectedGeneration, long timeout, TimeUnit unit)
            throws InterruptedException {
        if (timeout < 0L) throw new IllegalArgumentException("timeout must be >= 0");
        long remaining = unit.toNanos(timeout);
        long deadline = System.nanoTime() + remaining;
        synchronized (lifecycle) {
            while (true) {
                Snapshot current = snapshot;
                if (current.generation != expectedGeneration
                        || (current.state != State.SCANNING
                        && current.state != State.BLOCKED_UNAVAILABLE)) {
                    return current.generation == expectedGeneration;
                }
                if (remaining <= 0L) return false;
                TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
                remaining = deadline - System.nanoTime();
            }
        }
    }

    private void run(long expectedGeneration, RecoveryOwner owner,
                     BooleanSupplier stillLeader) {
        long backoff = config.initialBackoffMillis;
        while (termCurrent(expectedGeneration, stillLeader)) {
            try {
                Snapshot result = scanCycle(expectedGeneration, owner, stillLeader);
                publish(expectedGeneration, result);
                if (result.state == State.EMPTY_READY) {
                    runMaintenance(expectedGeneration, owner, stillLeader);
                }
                return;
            } catch (LeadershipLost lost) {
                return;
            } catch (RetryCycle retry) {
                publish(expectedGeneration, new Snapshot(expectedGeneration,
                        State.BLOCKED_UNAVAILABLE, retry.activeRuns,
                        retry.samples, retry.getMessage()));
            } catch (RunStateStoreUnavailableException unavailable) {
                publish(expectedGeneration, new Snapshot(expectedGeneration,
                        State.BLOCKED_UNAVAILABLE, 0,
                        Collections.emptyList(), summarize(unavailable)));
            } catch (IllegalArgumentException | IllegalStateException corruption) {
                publish(expectedGeneration, new Snapshot(expectedGeneration,
                        State.BLOCKED_CORRUPT, 0,
                        Collections.emptyList(), summarize(corruption)));
                return;
            } catch (RuntimeException unavailable) {
                publish(expectedGeneration, new Snapshot(expectedGeneration,
                        State.BLOCKED_UNAVAILABLE, 0,
                        Collections.emptyList(), summarize(unavailable)));
            }
            if (!termCurrent(expectedGeneration, stillLeader)) return;
            try {
                Thread.sleep(backoff);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
            backoff = Math.min(config.maxBackoffMillis,
                    backoff > config.maxBackoffMillis / 2L
                            ? config.maxBackoffMillis : backoff * 2L);
            publish(expectedGeneration, new Snapshot(expectedGeneration,
                    State.SCANNING, 0, Collections.emptyList(), null));
        }
    }

    private Snapshot scanCycle(long expectedGeneration, RecoveryOwner owner,
                               BooleanSupplier stillLeader) {
        nextRetryDelayMillis = Long.MAX_VALUE;
        String cursor = null;
        int activeRuns = 0;
        int waitingRetries = 0;
        boolean corrupt = false;
        boolean capabilityBlocked = false;
        List<String> samples = new ArrayList<>(config.sampleLimit);
        do {
            requireTerm(expectedGeneration, stillLeader);
            RecoverableRunPage page = catalog.scan(cursor, config.pageSize);
            requireTerm(expectedGeneration, stillLeader);
            for (RecoverableRunRef ref : page.runs()) {
                Inspection result = inspect(ref, owner,
                        expectedGeneration, stillLeader);
                if (result.kind == InspectionKind.IGNORE) continue;
                if (result.kind == InspectionKind.SAFE_WAIT) {
                    waitingRetries++;
                    nextRetryDelayMillis = Math.min(
                            nextRetryDelayMillis, result.delayMillis);
                    if (samples.size() < config.sampleLimit) {
                        samples.add(ref.runId() + ":RETRY_WAIT");
                    }
                    continue;
                }
                if (result.kind == InspectionKind.RETRY) {
                    throw new RetryCycle(result.detail, activeRuns, samples);
                }
                activeRuns++;
                if (samples.size() < config.sampleLimit) {
                    samples.add(ref.runId() + ':' + result.detail);
                }
                if (result.kind == InspectionKind.CORRUPT) corrupt = true;
                if (result.kind == InspectionKind.CAPABILITY) capabilityBlocked = true;
            }
            cursor = page.nextCursor();
            if (page.cycleComplete()) break;
        } while (true);
        if (corrupt) {
            return new Snapshot(expectedGeneration, State.BLOCKED_CORRUPT,
                    activeRuns, samples, "one or more recovery candidates are invalid",
                    null, waitingRetries, nextRetryDelayMillis);
        }
        if (capabilityBlocked) {
            return new Snapshot(expectedGeneration, State.BLOCKED_CAPABILITY,
                    activeRuns, samples,
                    "one or more recovery candidates require another deployment set",
                    null, waitingRetries, nextRetryDelayMillis);
        }
        if (activeRuns > 0) {
            return new Snapshot(expectedGeneration, State.BLOCKED_PENDING_RECOVERY,
                    activeRuns, samples, null, null,
                    waitingRetries, nextRetryDelayMillis);
        }
        return new Snapshot(expectedGeneration, State.EMPTY_READY,
                0, samples, null, null,
                waitingRetries, nextRetryDelayMillis);
    }

    private Inspection inspect(RecoverableRunRef ref, RecoveryOwner owner,
                               long expectedGeneration, BooleanSupplier stillLeader) {
        requireTerm(expectedGeneration, stillLeader);
        RunSnapshot snapshot = stateStore.get(ref.runId());
        if (snapshot == null || snapshot.status().isTerminal()) return Inspection.ignore();
        if (locallyExecuting.test(ref.runId())) return Inspection.ignore();
        if (snapshot.revision() != ref.revision() || snapshot.status() != ref.status()) {
            return Inspection.retry("STALE_CATALOG_REF");
        }
        if (snapshot.status() == io.rowweave.run.RunStatus.READY
                && snapshot.attemptCount() > 0
                && snapshot.currentAttempt() != null
                && snapshot.currentAttempt().status().isTerminal()) {
            if (retryAuthority == null) {
                return Inspection.capability("RETRY_AUTHORITY_UNAVAILABLE");
            }
            RetryReadyEvaluation evaluation = retryAuthority.evaluateRetryReady(
                    snapshot.runId());
            if (evaluation.code() == RetryReadyEvaluation.Code.NOT_DUE) {
                long delay = Math.max(1L, evaluation.notBeforeMillis()
                        - evaluation.observedAtMillis());
                if (snapshot.deadlineEpochMillis() > 0L) {
                    delay = Math.min(delay, Math.max(1L,
                            snapshot.deadlineEpochMillis()
                                    - evaluation.observedAtMillis()));
                }
                return Inspection.safeWait(delay);
            }
            if (evaluation.code() == RetryReadyEvaluation.Code.UNAVAILABLE
                    || evaluation.code()
                    == RetryReadyEvaluation.Code.STALE_RECOVERY_OWNER) {
                return Inspection.retry("RETRY_AUTHORITY_UNAVAILABLE");
            }
            if (evaluation.code() == RetryReadyEvaluation.Code.NOT_FOUND) {
                return Inspection.retry("RETRY_AUTHORITY_MISSING");
            }
            if (evaluation.code() == RetryReadyEvaluation.Code.RETIRING) {
                return Inspection.ignore();
            }
            if (evaluation.code() == RetryReadyEvaluation.Code.NOT_RETRY_WAIT) {
                return Inspection.corrupt("INVALID_RETRY_READY_STATE");
            }
        }
        RecoveryToken observed = ownership.ownership(ref.runId());
        long observedEpoch = observed == null ? 0L : observed.ownershipEpoch();
        requireTerm(expectedGeneration, stillLeader);
        RecoveryClaim claim = ownership.claim(ref.runId(), snapshot.revision(),
                observedEpoch, owner);
        if (claim.code() == RecoveryClaim.Code.STALE_RUN) {
            RunSnapshot latest = stateStore.get(ref.runId());
            return latest == null || latest.status().isTerminal()
                    ? Inspection.ignore() : Inspection.retry("STALE_RUN");
        }
        if (claim.code() == RecoveryClaim.Code.LOST_RACE || claim.token() == null) {
            return Inspection.retry("LOST_OWNERSHIP");
        }
        requireTerm(expectedGeneration, stillLeader);
        RunSnapshot latest = stateStore.get(ref.runId());
        if (latest == null || latest.status().isTerminal()) return Inspection.ignore();
        if (!ownership.current(claim.token())) {
            return Inspection.retry("LOST_OWNERSHIP");
        }
        io.rowweave.cluster.rpc.ControlResidentLease residentLease =
                io.rowweave.cluster.rpc.ControlResidentBudget.processWide().openLease();
        io.rowweave.cluster.rpc.ControlResidentLease.OwnerToken inventoryOwner =
                residentLease.ownerToken();
        try {
            PlanArtifact artifact;
            try {
                byte[] encoded = artifacts.get(latest.artifactId(),
                        bytes -> residentLease.reserve(residentLease.ownerToken(), bytes));
                if (encoded == null) {
                    return Inspection.corrupt("MISSING_OR_INVALID_ARTIFACT");
                }
                artifact = PlanArtifactCodec.decode(encoded,
                        bytes -> residentLease.reserve(residentLease.ownerToken(), bytes));
                if (deploymentSetId != null) {
                    PlanArtifactDeploymentBinding.requireMatches(
                            artifact, deploymentSetId, requireDeploymentFeature);
                }
                if (artifact.payloadFormat()
                        == PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1) {
                    PlanArtifactValidator.validateForPublicV1Execution(artifact);
                } else {
                    PlanArtifactValidator.validateForCurrentRuntime(artifact);
                }
                if (!latest.artifactId().equals(artifact.artifactId())
                        || !latest.definitionId().equals(artifact.definitionId())) {
                    return Inspection.corrupt("MISSING_OR_INVALID_ARTIFACT");
                }
            } catch (PlanArtifactValidationException invalidArtifact) {
                if ("DEPLOYMENT_SET_MISMATCH".equals(invalidArtifact.code())
                        || "DEPLOYMENT_FEATURE_REQUIRED".equals(invalidArtifact.code())) {
                    return Inspection.capability(invalidArtifact.code());
                }
                return Inspection.corrupt("MISSING_OR_INVALID_ARTIFACT");
            } catch (io.rowweave.cluster.rpc.ControlResidentBudgetExceededException exhausted) {
                return Inspection.retry(exhausted.code());
            } catch (RuntimeException invalidArtifact) {
                return Inspection.corrupt("MISSING_OR_INVALID_ARTIFACT");
            }
            InheritedRunRecovery.Eligibility eligibility =
                    InheritedRunRecovery.eligibility(latest);
            String assignmentEvidence = assignmentEvidence(latest);
            if (eligibility == InheritedRunRecovery.Eligibility.RECONCILE_IN_FLIGHT
                    && assignmentEvidence == null
                    && (recoveryHandler == null
                    || latest.deadlineEpochMillis() == 0L)) {
                return Inspection.corrupt("MISSING_ASSIGNMENT_MANIFEST");
            }
            if (recoveryHandler != null
                    && eligibility != InheritedRunRecovery.Eligibility.PENDING) {
                inventoryOwner = residentLease.transfer(inventoryOwner,
                        io.rowweave.cluster.rpc.ControlResidentOwner.HANDLER_OWNED);
                InheritedRunRecovery.Result handled = Objects.requireNonNull(
                        recoveryHandler.recover(new InheritedRunRecovery.Candidate(
                                latest, claim.token(), artifact,
                                residentLease, inventoryOwner)),
                        "recovery handler result");
                requireTerm(expectedGeneration, stillLeader);
                RunSnapshot after = stateStore.get(ref.runId());
                if (!ownership.current(claim.token())) {
                    return Inspection.retry("LOST_OWNERSHIP");
                }
                if (after == null || after.status().isTerminal()) {
                    return Inspection.ignore();
                }
                if (handled.code() == InheritedRunRecovery.ResultCode.RETRY) {
                    return Inspection.retry(handled.detail());
                }
                if (handled.code() == InheritedRunRecovery.ResultCode.RESOLVED) {
                    return Inspection.valid("RECOVERY_NOT_TERMINAL");
                }
                return Inspection.valid(handled.detail());
            }
            AttemptSnapshot attempt = latest.currentAttempt();
            if (assignmentEvidence != null) {
                return Inspection.valid(assignmentEvidence);
            }
            return Inspection.valid(attempt == null
                    ? "READY_NO_ATTEMPT" : "ATTEMPT_" + attempt.status().name());
        } finally {
            residentLease.release(inventoryOwner);
        }
    }

    private String assignmentEvidence(RunSnapshot run) {
        AttemptSnapshot attempt = run.currentAttempt();
        if (assignments == null || attempt == null) return null;
        RunAttemptFence fence = new RunAttemptFence(
                run.runId(), attempt.attemptId(), attempt.fenceToken());
        StageAssignmentSnapshot evidence = assignments.assignmentSnapshot(fence);
        if (evidence == null) return null;
        if (!evidence.manifest().fence().equals(fence)
                || !evidence.manifest().artifactId().equals(run.artifactId())) {
            throw new IllegalStateException("assignment evidence identity mismatch");
        }
        return evidence.allNone()
                ? "ASSIGNMENTS_NONE_FENCED"
                : "ASSIGNMENTS_ACCEPTED_" + evidence.acceptedCount();
    }

    private boolean termCurrent(long expectedGeneration, BooleanSupplier stillLeader) {
        return !closed && generation == expectedGeneration && stillLeader.getAsBoolean();
    }

    private void runMaintenance(long expectedGeneration, RecoveryOwner owner,
                                BooleanSupplier stillLeader) {
        long handledSignal = 0L;
        long delay = maintenanceDelay();
        long backoff = config.initialBackoffMillis;
        String cursor = null;
        MaintenanceAggregate aggregate = null;
        while (termCurrent(expectedGeneration, stillLeader)) {
            handledSignal = awaitMaintenanceWake(
                    expectedGeneration, stillLeader, handledSignal, delay);
            if (!termCurrent(expectedGeneration, stillLeader)) return;
            try {
                if (cursor == null) aggregate = new MaintenanceAggregate();
                RecoverableRunPage page = scanMaintenancePage(
                        cursor, expectedGeneration, owner, stillLeader,
                        aggregate);
                cursor = page.cycleComplete() ? null : page.nextCursor();
                Snapshot result = aggregate.snapshot(
                        expectedGeneration, cursor, page.cycleComplete(), snapshot);
                publish(expectedGeneration, result);
                if (page.cycleComplete()) {
                    nextRetryDelayMillis = aggregate.nextRetryDelayMillis;
                    aggregate = null;
                    delay = result.state == State.EMPTY_READY
                            ? maintenanceDelay() : config.initialBackoffMillis;
                } else {
                    delay = config.maintenanceContinuationMillis;
                }
                backoff = config.initialBackoffMillis;
            } catch (LeadershipLost lost) {
                return;
            } catch (RetryCycle | RunStateStoreUnavailableException unavailable) {
                publish(expectedGeneration, new Snapshot(
                        expectedGeneration, State.BLOCKED_UNAVAILABLE,
                        0, Collections.emptyList(), summarize(unavailable)));
                delay = backoff;
                backoff = Math.min(config.maxBackoffMillis,
                        backoff > config.maxBackoffMillis / 2L
                                ? config.maxBackoffMillis : backoff * 2L);
            } catch (IllegalArgumentException | IllegalStateException corruption) {
                publish(expectedGeneration, new Snapshot(
                        expectedGeneration, State.BLOCKED_CORRUPT,
                        0, Collections.emptyList(), summarize(corruption)));
                return;
            } catch (RuntimeException unavailable) {
                publish(expectedGeneration, new Snapshot(
                        expectedGeneration, State.BLOCKED_UNAVAILABLE,
                        0, Collections.emptyList(), summarize(unavailable)));
                delay = backoff;
            }
        }
    }

    private RecoverableRunPage scanMaintenancePage(
            String cursor, long expectedGeneration, RecoveryOwner owner,
            BooleanSupplier stillLeader, MaintenanceAggregate aggregate) {
        requireTerm(expectedGeneration, stillLeader);
        RecoverableRunPage page = catalog.scan(cursor, config.pageSize);
        requireTerm(expectedGeneration, stillLeader);
        for (RecoverableRunRef ref : page.runs()) {
            Inspection result = inspect(
                    ref, owner, expectedGeneration, stillLeader);
            if (result.kind == InspectionKind.IGNORE) continue;
            if (result.kind == InspectionKind.SAFE_WAIT) {
                aggregate.waitingRetries++;
                aggregate.nextRetryDelayMillis = Math.min(
                        aggregate.nextRetryDelayMillis, result.delayMillis);
                if (aggregate.samples.size() < config.sampleLimit) {
                    aggregate.samples.add(ref.runId() + ":RETRY_WAIT");
                }
                continue;
            }
            if (result.kind == InspectionKind.RETRY) {
                throw new RetryCycle(
                        result.detail, aggregate.activeRuns, aggregate.samples);
            }
            aggregate.activeRuns++;
            if (aggregate.samples.size() < config.sampleLimit) {
                aggregate.samples.add(ref.runId() + ':' + result.detail);
            }
            if (result.kind == InspectionKind.CORRUPT) {
                aggregate.corrupt = true;
            }
            if (result.kind == InspectionKind.CAPABILITY) {
                aggregate.capabilityBlocked = true;
            }
        }
        return page;
    }

    private static final class MaintenanceAggregate {
        int activeRuns;
        int waitingRetries;
        boolean corrupt;
        boolean capabilityBlocked;
        long nextRetryDelayMillis = Long.MAX_VALUE;
        final List<String> samples = new ArrayList<>();

        Snapshot snapshot(long generation, String cursor,
                          boolean cycleComplete, Snapshot previous) {
            if (corrupt) {
                return new Snapshot(generation, State.BLOCKED_CORRUPT,
                        activeRuns, samples,
                        "one or more recovery candidates are invalid", cursor,
                        waitingRetries, nextRetryDelayMillis);
            }
            if (capabilityBlocked) {
                return new Snapshot(generation, State.BLOCKED_CAPABILITY,
                        activeRuns, samples,
                        "one or more recovery candidates require another deployment set",
                        cursor, waitingRetries, nextRetryDelayMillis);
            }
            if (activeRuns > 0) {
                return new Snapshot(generation,
                        State.BLOCKED_PENDING_RECOVERY,
                        activeRuns, samples, null, cursor,
                        waitingRetries, nextRetryDelayMillis);
            }
            if (!cycleComplete && previous.state != State.EMPTY_READY) {
                return new Snapshot(generation, previous.state,
                        previous.activeRuns, previous.samples,
                        previous.failure, cursor, waitingRetries,
                        nextRetryDelayMillis);
            }
            return new Snapshot(generation, State.EMPTY_READY,
                    0, samples, null, cursor,
                    waitingRetries, nextRetryDelayMillis);
        }
    }

    private long maintenanceDelay() {
        return Math.min(MAINTENANCE_SAFETY_WAKE_MILLIS,
                nextRetryDelayMillis);
    }

    private long awaitMaintenanceWake(
            long expectedGeneration, BooleanSupplier stillLeader,
            long handledSignal, long delayMillis) {
        long boundedDelay = Math.max(1L, delayMillis);
        long deadline = System.nanoTime()
                + TimeUnit.MILLISECONDS.toNanos(boundedDelay);
        synchronized (lifecycle) {
            while (termCurrent(expectedGeneration, stillLeader)
                    && signalVersion == handledSignal) {
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0L) break;
                try {
                    TimeUnit.NANOSECONDS.timedWait(lifecycle, remaining);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return signalVersion;
                }
            }
            return signalVersion;
        }
    }

    private void requireTerm(long expectedGeneration, BooleanSupplier stillLeader) {
        if (!termCurrent(expectedGeneration, stillLeader)) throw new LeadershipLost();
    }

    private void publish(long expectedGeneration, Snapshot next) {
        synchronized (lifecycle) {
            if (!closed && generation == expectedGeneration) publishLocked(next);
        }
    }

    private void publishLocked(Snapshot next) {
        snapshot = next;
        if (metrics != null) metrics.observe(next);
        lifecycle.notifyAll();
    }

    private static String summarize(Throwable failure) {
        String message = failure.getMessage();
        return failure.getClass().getSimpleName()
                + (message == null ? "" : ": " + message);
    }

    @Override
    public void close() {
        synchronized (lifecycle) {
            if (closed) return;
            closed = true;
            generation++;
            if (currentTask != null) currentTask.cancel(true);
            publishLocked(new Snapshot(generation, State.CLOSED,
                    0, Collections.emptyList(), null));
        }
        worker.shutdownNow();
        long closeDeadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(5L);
        try {
            long remaining = closeDeadline - System.nanoTime();
            if (remaining <= 0L
                    || !worker.awaitTermination(remaining, TimeUnit.NANOSECONDS)) {
                throw new IllegalStateException("recovery inventory worker did not terminate");
            }
            // ThreadPoolExecutor signals TERMINATED from inside its worker runnable;
            // the backing Thread may still be alive for a few instructions. close()
            // promises an actual join, so wait for the Thread object as well.
            Thread thread = workerThread.get();
            if (thread != null && thread != Thread.currentThread() && thread.isAlive()) {
                remaining = closeDeadline - System.nanoTime();
                if (remaining > 0L) {
                    long joinMillis = TimeUnit.NANOSECONDS
                            .toMillis(remaining - 1L) + 1L;
                    thread.join(joinMillis);
                }
                if (thread.isAlive()) {
                    throw new IllegalStateException(
                            "recovery inventory thread did not terminate");
                }
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted closing recovery inventory", interrupted);
        }
    }

    private enum InspectionKind {
        IGNORE, SAFE_WAIT, VALID, CAPABILITY, CORRUPT, RETRY
    }

    private static final class Inspection {
        final InspectionKind kind;
        final String detail;
        final long delayMillis;

        private Inspection(InspectionKind kind, String detail, long delayMillis) {
            this.kind = kind;
            this.detail = detail;
            this.delayMillis = delayMillis;
        }

        static Inspection ignore() {
            return new Inspection(InspectionKind.IGNORE, "", 0L);
        }
        static Inspection safeWait(long delayMillis) {
            return new Inspection(
                    InspectionKind.SAFE_WAIT, "RETRY_NOT_DUE", delayMillis);
        }
        static Inspection valid(String detail) {
            return new Inspection(InspectionKind.VALID, detail, 0L);
        }
        static Inspection capability(String detail) {
            return new Inspection(InspectionKind.CAPABILITY, detail, 0L);
        }
        static Inspection corrupt(String detail) {
            return new Inspection(InspectionKind.CORRUPT, detail, 0L);
        }
        static Inspection retry(String detail) {
            return new Inspection(InspectionKind.RETRY, detail, 0L);
        }
    }

    private static final class RetryCycle extends RuntimeException {
        private static final long serialVersionUID = 1L;
        final int activeRuns;
        final List<String> samples;

        RetryCycle(String message, int activeRuns, List<String> samples) {
            super(message);
            this.activeRuns = activeRuns;
            this.samples = new ArrayList<>(samples);
        }
    }

    private static final class LeadershipLost extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
}
