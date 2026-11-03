package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamJobProgressAccumulatorTest {

    @Test
    void replacesSameAttemptCumulativeCountersAndAddsNewFencedAttempt() {
        StreamJobProgress firstAttempt = progress(0, 5L, 4L, 50L, 40L,
                2, 20L, 30L, 1, 2, 3L);
        StreamJobEvidence prior = evidence("run-1", "attempt-1", 7L, 1L,
                firstAttempt, firstAttempt);

        StreamJobProgress laterAttempt = progress(0, 8L, 7L, 80L, 70L,
                3, 25L, 35L, 2, 3, 5L);
        StreamJobProgress replaced = StreamJobProgressAccumulator.lifetime(
                prior, snapshot("run-1", "attempt-1", 7L, 1L),
                laterAttempt);
        assertEquals(8L, replaced.recordsRead());
        assertEquals(7L, replaced.recordsWritten());
        assertEquals(5L, replaced.backpressuredMillis());
        assertEquals(35L, replaced.managedStateBytes());
        StreamJobProgress replacementMaster =
                StreamJobProgressAccumulator.lifetime(prior,
                        new StreamRunProgressSnapshot(
                                "run-1", "attempt-1", 7L, 3L, 1L,
                                new StreamTaskProgressAggregate(
                                        StreamJobProgress.empty(), 1, 1)),
                        laterAttempt);
        assertEquals(8L, replacementMaster.recordsRead());
        assertEquals(7L, replacementMaster.recordsWritten());

        StreamJobProgress afterCheckpointFloor = progress(
                0, 9L, 8L, 90L, 80L,
                1, 10L, 10L, 1, 1, 6L);
        StreamJobProgress flooredAttempt =
                StreamJobProgressAccumulator.attempt(prior,
                        snapshot("run-1", "attempt-1", 7L, 1L),
                        afterCheckpointFloor);
        assertEquals(2, flooredAttempt.mailboxHighWaterEvents());
        assertEquals(20L, flooredAttempt.channelHighWaterBytes());
        assertEquals(30L, flooredAttempt.managedStateBytes());
        assertEquals(1, flooredAttempt.timerHighWaterCount());
        assertEquals(2, flooredAttempt.windowHighWaterCount());

        StreamJobProgress secondAttempt = progress(1, 3L, 2L, 30L, 20L,
                1, 10L, 15L, 1, 1, 2L);
        StreamJobEvidence frozen = evidence(
                "run-1", "attempt-1", 7L, 1L,
                laterAttempt, replaced);
        StreamJobProgress accumulated = StreamJobProgressAccumulator.lifetime(
                frozen, snapshot("run-1", "attempt-2", 8L, 2L),
                secondAttempt);
        assertEquals(11L, accumulated.recordsRead());
        assertEquals(9L, accumulated.recordsWritten());
        assertEquals(110L, accumulated.bytesRead());
        assertEquals(90L, accumulated.bytesWritten());
        assertEquals(7L, accumulated.backpressuredMillis());
        assertEquals(3, accumulated.mailboxHighWaterEvents());
        assertEquals(25L, accumulated.channelHighWaterBytes());
        assertEquals(35L, accumulated.managedStateBytes());
        assertEquals(2, accumulated.timerHighWaterCount());
        assertEquals(3, accumulated.windowHighWaterCount());
    }

    @Test
    void addsResetTaskCountersAfterCoordinatorReplacement() {
        StreamJobProgress priorAttempt = progress(
                0, 600_000L, 500_000L, 6_000_000L, 5_000_000L,
                7, 70L, 80L, 8, 9, 100L);
        StreamJobProgress priorLifetime = progress(
                0, 700_000L, 650_000L, 7_000_000L, 6_500_000L,
                10, 100L, 120L, 11, 12, 150L);
        StreamJobEvidence prior = evidence(
                "run-1", "attempt-1", 7L, 1L,
                priorAttempt, priorLifetime);
        StreamJobProgress resetCounters = progress(
                0, 9L, 8L, 90L, 80L,
                1, 10L, 15L, 1, 1, 2L);
        StreamRunProgressSnapshot replacement =
                new StreamRunProgressSnapshot(
                        "run-1", "attempt-1", 7L, 3L, 1L,
                        new StreamTaskProgressAggregate(
                                resetCounters, 3, 3));

        StreamJobProgress attempt = StreamJobProgressAccumulator.attempt(
                prior, replacement, resetCounters);
        StreamJobProgress lifetime = StreamJobProgressAccumulator.lifetime(
                prior, replacement, attempt);

        assertEquals(9L, attempt.recordsRead());
        assertEquals(7, attempt.mailboxHighWaterEvents());
        assertEquals(700_009L, lifetime.recordsRead());
        assertEquals(650_008L, lifetime.recordsWritten());
        assertEquals(7_000_090L, lifetime.bytesRead());
        assertEquals(6_500_080L, lifetime.bytesWritten());
        assertEquals(152L, lifetime.backpressuredMillis());
        assertEquals(120L, lifetime.managedStateBytes());
    }

    private static StreamJobEvidence evidence(String runId, String attemptId,
            long fence, long generation, StreamJobProgress attempt,
            StreamJobProgress lifetime) {
        return new StreamJobEvidence(StreamJobId.of("job-progress"),
                runId, attemptId, attempt.attempt(), fence, 2L, generation,
                null, 0L, 0L, 0L, attempt, lifetime, 1, 1, 1L);
    }

    private static StreamRunProgressSnapshot snapshot(String runId,
            String attemptId, long fence, long generation) {
        return new StreamRunProgressSnapshot(runId, attemptId, fence, 2L,
                generation, new StreamTaskProgressAggregate(
                StreamJobProgress.empty(), 1, 1));
    }

    private static StreamJobProgress progress(int attempt, long recordsRead,
            long recordsWritten, long bytesRead, long bytesWritten,
            int mailbox, long channel, long state, int timers, int windows,
            long backpressure) {
        return new StreamJobProgress(attempt, 0L, 10L,
                StreamCdcPhase.NOT_APPLICABLE, attempt,
                recordsRead, recordsWritten, bytesRead, bytesWritten,
                0L, 0L, mailbox, channel, state, timers, windows,
                backpressure);
    }
}
