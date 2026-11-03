package io.rowweave.sink;

import org.junit.jupiter.api.DynamicTest;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** assertions that must run unchanged for every admitted backend. */
public final class DurableBatchSinkProviderConformance {
    private static final Set<DurableSinkConformanceObservation.Event> FORBIDDEN =
            EnumSet.of(
                    DurableSinkConformanceObservation.Event.PARTIAL_VISIBILITY,
                    DurableSinkConformanceObservation.Event.STALE_ACTION_APPLIED,
                    DurableSinkConformanceObservation.Event.PROTECTED_ORPHAN_DELETED,
                    DurableSinkConformanceObservation.Event.PROVIDER_OBJECT_INJECTED);
    private static final Set<DurableSinkConformanceObservation.Event> SIDE_EFFECTS =
            EnumSet.of(
                    DurableSinkConformanceObservation.Event.OPENED,
                    DurableSinkConformanceObservation.Event.PREPARED,
                    DurableSinkConformanceObservation.Event.TERMINAL_RECORDED,
                    DurableSinkConformanceObservation.Event.LOCATOR_RECORDED,
                    DurableSinkConformanceObservation.Event.COMMIT_DECIDED,
                    DurableSinkConformanceObservation.Event.ABORT_DECIDED,
                    DurableSinkConformanceObservation.Event.PUBLISHED,
                    DurableSinkConformanceObservation.Event.ABORTED,
                    DurableSinkConformanceObservation.Event.FINALIZED);

    private DurableBatchSinkProviderConformance() { }

    public static Stream<DynamicTest> scenarios(
            DurableSinkConformanceBackendFactory factory) {
        if (factory == null) throw new IllegalArgumentException("backend factory is required");
        return Arrays.stream(DurableSinkConformanceScenario.values()).map(scenario ->
                DynamicTest.dynamicTest(factory.backendId() + '/' + scenario.name(), () -> {
                    try (DurableSinkConformanceBackend backend = factory.open()) {
                        assertObservation(scenario, backend.run(scenario));
                    }
                }));
    }

    public static void assertObservation(
            DurableSinkConformanceScenario scenario,
            DurableSinkConformanceObservation observation) {
        assertEquals(scenario, observation.scenario(), "scenario identity drift");
        assertTrue(observation.events().contains(
                DurableSinkConformanceObservation.Event.PROVIDER_RESOLVED),
                "provider resolution was not exercised: " + observation.detailCode());
        assertTrue(observation.events().contains(
                DurableSinkConformanceObservation.Event.TRUST_ADMITTED),
                "trust/deployment admission was not exercised");
        if (scenario.rejectsBeforeSideEffect()) {
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.REJECTED_BEFORE_SIDE_EFFECT),
                    "composition was not rejected by the static prefix");
            assertFalse(observation.events().contains(
                    DurableSinkConformanceObservation.Event.WORKER_READMITTED),
                    "rejected composition reached Worker admission");
            for (DurableSinkConformanceObservation.Event sideEffect : SIDE_EFFECTS) {
                assertFalse(observation.events().contains(sideEffect),
                        "rejected composition produced side effect: " + sideEffect);
            }
        } else {
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.WORKER_READMITTED),
                    "Worker re-admission was not exercised");
        }
        assertTrue(observation.events().contains(
                DurableSinkConformanceObservation.Event.OUTCOME_QUERIED),
                "public outcome query was not exercised");
        assertTrue(scenario.allowedOutcomes().contains(observation.outcome()),
                "outcome is outside scenario contract: " + observation.outcome());
        for (DurableSinkConformanceObservation.Event forbidden : FORBIDDEN) {
            assertFalse(observation.events().contains(forbidden),
                    "forbidden event: " + forbidden);
        }
        assertTrue(observation.publicationCount() <= 1, "duplicate publication");
        assertTrue(observation.visibleMemberCount() == 0
                        || observation.visibleMemberCount() == scenario.sinkCount(),
                "partial multi-sink visibility");
        if (scenario.requiresFreshRebuild()) {
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.FRESH_HANDLE_REBUILT),
                    "fresh provider handle rebuild was not exercised");
        }
        if (scenario.requiresLocatorClosure()) {
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.TERMINAL_RECORDED),
                    "terminal receipt was not recorded");
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.LOCATOR_RECORDED),
                    "prepared locator was not recorded");
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.LOCATOR_CLOSURE_PROVEN),
                    "locator closure was not proven");
        }
        if (scenario.requiresProcessIsolation()) {
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.CONTROL_DEADLINE_RETURNED),
                    "blocking action did not return a bounded safe response");
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.WORKER_FENCED),
                    "blocking Worker was not fenced");
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.WORKER_PROCESS_EXITED),
                    "blocking Worker process did not exit");
        }
        if (scenario == DurableSinkConformanceScenario.DUPLICATE_SUBMIT) {
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.DUPLICATE_CONTROL_REPLAYED),
                    "duplicate control replay was not exercised");
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.DUPLICATE_QUERY_REPLAYED),
                    "duplicate query replay was not exercised");
        }
        if (scenario == DurableSinkConformanceScenario.ORPHAN_CLEANUP) {
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.ORPHAN_DELETED),
                    "provably safe orphan was not deleted");
            for (DurableSinkConformanceObservation.Event protectedEvidence
                    : EnumSet.of(
                    DurableSinkConformanceObservation.Event.LIVE_ORPHAN_PRESERVED,
                    DurableSinkConformanceObservation.Event.INTENDED_ORPHAN_PRESERVED,
                    DurableSinkConformanceObservation.Event.VISIBLE_ORPHAN_PRESERVED,
                    DurableSinkConformanceObservation.Event.CONFLICT_ORPHAN_PRESERVED)) {
                assertTrue(observation.events().contains(protectedEvidence),
                        "protected orphan case was not preserved: " + protectedEvidence);
            }
        }
        if (scenario == DurableSinkConformanceScenario.REPEATED_LOCAL_BLOCKING) {
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.CONTROL_DEADLINE_RETURNED),
                    "local blocking action did not return at its deadline");
            assertTrue(observation.events().contains(
                    DurableSinkConformanceObservation.Event.LOCAL_SUPERVISOR_POISONED),
                    "process-wide local poison was not observed");
            assertTrue(observation.providerActionThreadCount() <= 64,
                    "process-wide provider action thread cap exceeded");
        }
    }
}
