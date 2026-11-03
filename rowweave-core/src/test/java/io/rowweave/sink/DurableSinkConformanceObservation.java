package io.rowweave.sink;

import io.rowweave.commit.BatchSinkOutcome;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/** Bounded public-path evidence consumed by the shared conformance assertions. */
public final class DurableSinkConformanceObservation {
    public static final int MAX_DETAIL_BYTES = 256;

    public enum Event {
        PROVIDER_RESOLVED,
        TRUST_ADMITTED,
        WORKER_READMITTED,
        OPENED,
        PREPARED,
        TERMINAL_RECORDED,
        LOCATOR_RECORDED,
        LOCATOR_CLOSURE_PROVEN,
        COMMIT_DECIDED,
        ABORT_DECIDED,
        FRESH_HANDLE_REBUILT,
        PUBLISHED,
        ABORTED,
        FINALIZED,
        OUTCOME_QUERIED,
        DUPLICATE_CONTROL_REPLAYED,
        DUPLICATE_QUERY_REPLAYED,
        CONTROL_DEADLINE_RETURNED,
        REJECTED_BEFORE_SIDE_EFFECT,
        WORKER_FENCED,
        WORKER_PROCESS_EXITED,
        ORPHAN_DELETED,
        LIVE_ORPHAN_PRESERVED,
        INTENDED_ORPHAN_PRESERVED,
        VISIBLE_ORPHAN_PRESERVED,
        CONFLICT_ORPHAN_PRESERVED,
        LOCAL_SUPERVISOR_POISONED,
        PARTIAL_VISIBILITY,
        STALE_ACTION_APPLIED,
        PROTECTED_ORPHAN_DELETED,
        PROVIDER_OBJECT_INJECTED
    }

    private final DurableSinkConformanceScenario scenario;
    private final BatchSinkOutcome.State outcome;
    private final Set<Event> events;
    private final int publicationCount;
    private final int visibleMemberCount;
    private final int providerActionThreadCount;
    private final String detailCode;

    public DurableSinkConformanceObservation(
            DurableSinkConformanceScenario scenario,
            BatchSinkOutcome.State outcome, Set<Event> events,
            int publicationCount, int visibleMemberCount,
            int providerActionThreadCount, String detailCode) {
        if (scenario == null || outcome == null || events == null) {
            throw new IllegalArgumentException("scenario, outcome and events are required");
        }
        if (publicationCount < 0 || publicationCount > 1
                || visibleMemberCount < 0
                || visibleMemberCount > scenario.sinkCount()
                || providerActionThreadCount < 0 || providerActionThreadCount > 64) {
            throw new IllegalArgumentException("conformance observation count is out of bounds");
        }
        if (detailCode == null || detailCode.isEmpty()
                || detailCode.getBytes(StandardCharsets.UTF_8).length > MAX_DETAIL_BYTES) {
            throw new IllegalArgumentException("conformance detail code is invalid");
        }
        this.scenario = scenario;
        this.outcome = outcome;
        this.events = Collections.unmodifiableSet(events.isEmpty()
                ? EnumSet.noneOf(Event.class) : EnumSet.copyOf(events));
        this.publicationCount = publicationCount;
        this.visibleMemberCount = visibleMemberCount;
        this.providerActionThreadCount = providerActionThreadCount;
        this.detailCode = detailCode;
    }

    public DurableSinkConformanceScenario scenario() { return scenario; }
    public BatchSinkOutcome.State outcome() { return outcome; }
    public Set<Event> events() { return events; }
    public int publicationCount() { return publicationCount; }
    public int visibleMemberCount() { return visibleMemberCount; }
    public int providerActionThreadCount() { return providerActionThreadCount; }
    public String detailCode() { return detailCode; }
}
