package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** exhaustive pure reference model and unsafe-order witnesses. */
class IncrementalSafetyReferenceModelContractTest {

    @Test
    void exhaustiveAdjacentInterleavingsPreserveAllNineSafetyProperties() {
        IncrementalSafetyReferenceModel.Exploration report =
                IncrementalSafetyReferenceModel.exhaustive(12);

        assertTrue(report.reachableStateCount() >= 50,
                "exploration must cover more than a hand-written happy path");
        assertTrue(report.transitionCount() >= 500,
                "exploration must exercise rejected and applied adjacent interleavings");
        assertEquals(EnumSet.allOf(IncrementalSafetyReferenceModel.Action.class),
                report.observedActions());
        assertEquals(EnumSet.allOf(IncrementalSafetyReferenceModel.Property.class),
                report.checkedProperties());
        for (IncrementalSafetyReferenceModel.Property property
                : IncrementalSafetyReferenceModel.Property.values()) {
            assertTrue(report.violations(property).isEmpty(),
                    property + " violated by " + report.violations(property));
        }
    }

    @Test
    void exhaustiveModelActuallyExercisesRecoveryFencesAndFrozenTargets() {
        IncrementalSafetyReferenceModel.Exploration report =
                IncrementalSafetyReferenceModel.exhaustive(12);

        assertTrue(report.completedNormalIntervals() > 0);
        assertTrue(report.completedBackfills() > 0);
        assertTrue(report.completedAfterLeaderReplacement() > 0);
        assertTrue(report.completedAfterRetry() > 0);
        assertTrue(report.ackLossReplays() > 0);
        assertTrue(report.rejectedStaleFenceCommands() > 0);
        assertTrue(report.rejectedStaleLeaderCommands() > 0);
        assertTrue(report.rejectedStaleIncarnationCommands() > 0);
        assertTrue(report.rejectedRetargetCommands() > 0);
        assertTrue(report.rejectedIntentRollbackCommands() > 0);
        assertTrue(report.conflictStates() > 0);
        assertTrue(report.rejectedAfterConflictCommands() > 0);
        assertTrue(report.durableAbortClosures() > 0);
        assertTrue(report.rejectedMissingAbortClosureCommands() > 0);
        assertTrue(report.rejectedStaleCloseCommands() > 0);
        assertTrue(report.rejectedSameRunRetargetCommands() > 0);
        assertTrue(report.claimAckLossReplays() > 0);
        assertTrue(report.publishAckLossReplays() > 0);
        assertTrue(report.terminalAckLossReplays() > 0);
        assertTrue(report.retryClaimAckLossReplays() > 0);
        assertTrue(report.rejectedCorruptAckReplays() > 0);
        assertTrue(report.rejectedPrematureQueries() > 0);
    }

    @Test
    void completedBackfillThenNormalCommitThenOldReplayHasNoSideEffect() {
        IncrementalSafetyReferenceModel.BackfillReplayProfile profile =
                IncrementalSafetyReferenceModel.backfillThenNormalThenReplay();

        assertEquals(profile.committedBeforeBackfill(), profile.committedAfterBackfill());
        assertEquals(1, profile.publicationsAfterBackfill());
        assertTrue(profile.committedAfterNormal() > profile.committedAfterBackfill());
        assertTrue(profile.normalAndBackfillNamespacesAreDistinct());
        assertTrue(profile.oldReplayRejectedBeforeArtifact());
        assertEquals(profile.publicationsBeforeReplay(), profile.publicationsAfterReplay());
        assertEquals(profile.lastBackfillOrdinalBeforeReplay(),
                profile.lastBackfillOrdinalAfterReplay());
    }

    @Test
    void arbitraryHistoryAndCatalogGrowthRemainHardBounded() {
        IncrementalSafetyReferenceModel.HistoryProfile history =
                IncrementalSafetyReferenceModel.boundedHistory(100_000);
        assertTrue(history.maxActiveIntervals() <= 1);
        assertTrue(history.maxRetainedNormalReceipts() <= 1);
        assertTrue(history.maxRetainedBackfillReceipts() <= 1);
        assertTrue(history.transitionsApplied() >= 800_000);
        assertTrue(history.maximumAuthorityWords() <= history.hardAuthorityWordLimit());

        IncrementalSafetyReferenceModel.CatalogProfile catalog =
                IncrementalSafetyReferenceModel.catalogProfile(5_000);
        assertEquals(4_096, catalog.acceptedFeeds());
        assertEquals(904, catalog.rejectedFeeds());
        assertEquals(256, catalog.shardCount());
        assertTrue(catalog.maximumEntriesPerShard() <= 16);
        assertTrue(catalog.maximumShardFrameBytes() <= 64 * 1024);
        assertTrue(catalog.canonicalFramesMeasured());
        assertTrue(catalog.guardEntriesSurvivePurge());
        assertTrue(catalog.incarnationAdvancedAfterPurge());
    }

    @Test
    void everyForbiddenOrderingHasADeterministicCounterexample() {
        for (IncrementalSafetyReferenceModel.UnsafeOrder order
                : IncrementalSafetyReferenceModel.UnsafeOrder.values()) {
            IncrementalSafetyReferenceModel.Counterexample first =
                    IncrementalSafetyReferenceModel.counterexample(order);
            IncrementalSafetyReferenceModel.Counterexample replay =
                    IncrementalSafetyReferenceModel.counterexample(order);

            assertTrue(first.detected(), order.name());
            assertFalse(first.violatedRule().isEmpty(), order.name());
            assertTrue(first.trace().size() >= 2, order.name());
            assertTrue(first.replayedTransitions() >= 2, order.name());
            assertEquals(first, replay, "counterexample must be deterministic for " + order);
        }
    }
}
