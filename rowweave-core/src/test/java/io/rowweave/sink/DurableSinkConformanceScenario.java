package io.rowweave.sink;

import io.rowweave.commit.BatchSinkOutcome;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/** backend-neutral durable Sink failure scenarios. */
public enum DurableSinkConformanceScenario {
    PROVIDER_RESOLUTION(Fault.NONE, outcomes(BatchSinkOutcome.State.COMMITTED), 1, false, false),
    DUPLICATE_SUBMIT(Fault.DUPLICATE, outcomes(BatchSinkOutcome.State.COMMITTED), 1, false, false),
    PREPARE_ACK_LOSS(Fault.PREPARE_ACK_LOSS, outcomes(BatchSinkOutcome.State.COMMITTED), 1, true, false),
    PUBLISH_ACK_LOSS(Fault.PUBLISH_ACK_LOSS, outcomes(BatchSinkOutcome.State.COMMITTED), 1, true, false),
    FINALIZE_ACK_LOSS(Fault.FINALIZE_ACK_LOSS, outcomes(BatchSinkOutcome.State.COMMITTED), 1, true, false),
    WORKER_CRASH_BEFORE_LOCATOR(Fault.WORKER_CRASH_BEFORE_LOCATOR,
            outcomes(BatchSinkOutcome.State.ABORT_DECIDED, BatchSinkOutcome.State.ABORTED), 1, true, true),
    WORKER_CRASH_AFTER_LOCATOR(Fault.WORKER_CRASH_AFTER_LOCATOR,
            outcomes(BatchSinkOutcome.State.COMMITTED), 1, true, false),
    MASTER_REPLACEMENT_BEFORE_DECISION(Fault.MASTER_REPLACEMENT_BEFORE_DECISION,
            outcomes(BatchSinkOutcome.State.ABORT_DECIDED, BatchSinkOutcome.State.ABORTED), 1, true, false),
    MASTER_REPLACEMENT_AFTER_DECISION(Fault.MASTER_REPLACEMENT_AFTER_DECISION,
            outcomes(BatchSinkOutcome.State.COMMIT_DECIDED, BatchSinkOutcome.State.COMMITTED), 1, true, false),
    LATE_ATTEMPT(Fault.LATE_ATTEMPT, outcomes(BatchSinkOutcome.State.COMMITTED), 1, true, false),
    ABORT_REPLAY(Fault.ABORT_REPLAY,
            outcomes(BatchSinkOutcome.State.ABORT_DECIDED, BatchSinkOutcome.State.ABORTED), 1, true, false),
    FINALIZE_REPLAY(Fault.FINALIZE_REPLAY, outcomes(BatchSinkOutcome.State.COMMITTED), 1, true, false),
    ORPHAN_CLEANUP(Fault.ORPHAN_CLEANUP,
            outcomes(BatchSinkOutcome.State.ABORT_DECIDED, BatchSinkOutcome.State.ABORTED), 1, true, false),
    MULTI_SINK_TWO(Fault.NONE, outcomes(BatchSinkOutcome.State.COMMITTED), 2, false, false),
    MULTI_SINK_FIVE(Fault.NONE, outcomes(BatchSinkOutcome.State.COMMITTED), 5, false, false),
    UNKNOWN_OUTCOME_BEFORE_DECISION(Fault.UNKNOWN_OUTCOME_BEFORE_DECISION,
            outcomes(BatchSinkOutcome.State.PREPARING, BatchSinkOutcome.State.ABORT_DECIDED,
                    BatchSinkOutcome.State.ABORTED), 1, true, false),
    UNKNOWN_OUTCOME_AFTER_DECISION(Fault.UNKNOWN_OUTCOME_AFTER_DECISION,
            outcomes(BatchSinkOutcome.State.COMMIT_DECIDED, BatchSinkOutcome.State.COMMITTED), 1, true, false),
    BLOCKING_OPEN(Fault.BLOCKING_OPEN,
            outcomes(BatchSinkOutcome.State.ABORT_DECIDED, BatchSinkOutcome.State.ABORTED), 1, true, true),
    BLOCKING_WRITE(Fault.BLOCKING_WRITE,
            outcomes(BatchSinkOutcome.State.ABORT_DECIDED, BatchSinkOutcome.State.ABORTED), 1, true, true),
    BLOCKING_PREPARE(Fault.BLOCKING_PREPARE,
            outcomes(BatchSinkOutcome.State.ABORT_DECIDED, BatchSinkOutcome.State.ABORTED), 1, true, true),
    BLOCKING_PUBLISH(Fault.BLOCKING_PUBLISH,
            outcomes(BatchSinkOutcome.State.COMMIT_DECIDED, BatchSinkOutcome.State.COMMITTED), 1, true, true),
    BLOCKING_ABORT(Fault.BLOCKING_ABORT,
            outcomes(BatchSinkOutcome.State.ABORT_DECIDED, BatchSinkOutcome.State.ABORTED), 1, true, true),
    BLOCKING_FINALIZE(Fault.BLOCKING_FINALIZE,
            outcomes(BatchSinkOutcome.State.COMMITTED), 1, true, true),
    BLOCKING_QUERY(Fault.BLOCKING_QUERY,
            outcomes(BatchSinkOutcome.State.PREPARING, BatchSinkOutcome.State.COMMIT_DECIDED,
                    BatchSinkOutcome.State.COMMITTED), 1, true, true),
    CROSS_PROVIDER_COMPOSITION(Fault.CROSS_PROVIDER_COMPOSITION,
            outcomes(BatchSinkOutcome.State.NOT_FOUND), 2, false, false),
    CROSS_DOMAIN_COMPOSITION(Fault.CROSS_DOMAIN_COMPOSITION,
            outcomes(BatchSinkOutcome.State.NOT_FOUND), 2, false, false),
    MIXED_DISCARD_PROVIDER_COMPOSITION(Fault.MIXED_DISCARD_PROVIDER_COMPOSITION,
            outcomes(BatchSinkOutcome.State.NOT_FOUND), 2, false, false),
    DUPLICATE_MEMBER_COMPOSITION(Fault.DUPLICATE_MEMBER_COMPOSITION,
            outcomes(BatchSinkOutcome.State.NOT_FOUND), 2, false, false),
    REPEATED_LOCAL_BLOCKING(Fault.REPEATED_LOCAL_BLOCKING,
            outcomes(BatchSinkOutcome.State.PREPARING, BatchSinkOutcome.State.COMMIT_DECIDED,
                    BatchSinkOutcome.State.ABORT_DECIDED,
                    BatchSinkOutcome.State.ABORTED), 1, false, false);

    public enum Fault {
        NONE,
        DUPLICATE,
        PREPARE_ACK_LOSS,
        PUBLISH_ACK_LOSS,
        FINALIZE_ACK_LOSS,
        WORKER_CRASH_BEFORE_LOCATOR,
        WORKER_CRASH_AFTER_LOCATOR,
        MASTER_REPLACEMENT_BEFORE_DECISION,
        MASTER_REPLACEMENT_AFTER_DECISION,
        LATE_ATTEMPT,
        ABORT_REPLAY,
        FINALIZE_REPLAY,
        ORPHAN_CLEANUP,
        UNKNOWN_OUTCOME_BEFORE_DECISION,
        UNKNOWN_OUTCOME_AFTER_DECISION,
        BLOCKING_OPEN,
        BLOCKING_WRITE,
        BLOCKING_PREPARE,
        BLOCKING_PUBLISH,
        BLOCKING_ABORT,
        BLOCKING_FINALIZE,
        BLOCKING_QUERY,
        CROSS_PROVIDER_COMPOSITION,
        CROSS_DOMAIN_COMPOSITION,
        MIXED_DISCARD_PROVIDER_COMPOSITION,
        DUPLICATE_MEMBER_COMPOSITION,
        REPEATED_LOCAL_BLOCKING
    }

    private final Fault fault;
    private final Set<BatchSinkOutcome.State> allowedOutcomes;
    private final int sinkCount;
    private final boolean requiresFreshRebuild;
    private final boolean requiresProcessIsolation;

    DurableSinkConformanceScenario(
            Fault fault, Set<BatchSinkOutcome.State> allowedOutcomes,
            int sinkCount, boolean requiresFreshRebuild,
            boolean requiresProcessIsolation) {
        this.fault = fault;
        this.allowedOutcomes = allowedOutcomes;
        this.sinkCount = sinkCount;
        this.requiresFreshRebuild = requiresFreshRebuild;
        this.requiresProcessIsolation = requiresProcessIsolation;
    }

    public Fault fault() { return fault; }
    public Set<BatchSinkOutcome.State> allowedOutcomes() { return allowedOutcomes; }
    public int sinkCount() { return sinkCount; }
    public boolean requiresFreshRebuild() { return requiresFreshRebuild; }
    public boolean requiresProcessIsolation() { return requiresProcessIsolation; }

    public boolean rejectsBeforeSideEffect() {
        return fault == Fault.CROSS_PROVIDER_COMPOSITION
                || fault == Fault.CROSS_DOMAIN_COMPOSITION
                || fault == Fault.MIXED_DISCARD_PROVIDER_COMPOSITION
                || fault == Fault.DUPLICATE_MEMBER_COMPOSITION;
    }

    public boolean requiresLocatorClosure() {
        switch (this) {
            case PREPARE_ACK_LOSS:
            case PUBLISH_ACK_LOSS:
            case FINALIZE_ACK_LOSS:
            case WORKER_CRASH_AFTER_LOCATOR:
            case MASTER_REPLACEMENT_AFTER_DECISION:
            case LATE_ATTEMPT:
            case FINALIZE_REPLAY:
            case MULTI_SINK_TWO:
            case MULTI_SINK_FIVE:
            case UNKNOWN_OUTCOME_AFTER_DECISION:
            case BLOCKING_PUBLISH:
            case BLOCKING_FINALIZE:
            case BLOCKING_QUERY:
                return true;
            default:
                return false;
        }
    }

    private static Set<BatchSinkOutcome.State> outcomes(
            BatchSinkOutcome.State first, BatchSinkOutcome.State... remaining) {
        EnumSet<BatchSinkOutcome.State> states = EnumSet.of(first, remaining);
        return Collections.unmodifiableSet(states);
    }
}
