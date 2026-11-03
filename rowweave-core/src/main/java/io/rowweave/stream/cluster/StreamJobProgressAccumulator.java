package io.rowweave.stream.cluster;

import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobProgress;

/** Deterministic attempt-to-lifetime progress accumulation. */
final class StreamJobProgressAccumulator {
    private StreamJobProgressAccumulator() { }

    static StreamJobProgress attempt(StreamJobEvidence priorEvidence,
            StreamRunProgressSnapshot observed,
            StreamJobProgress current) {
        if (!sameAttempt(priorEvidence, observed)) return current;
        StreamJobProgress prior = priorEvidence.attemptProgress();
        return new StreamJobProgress(current.attempt(),
                current.completedCheckpoints(), current.operatorWatermark(),
                current.cdcPhase(), current.restarts(),
                current.recordsRead(), current.recordsWritten(),
                current.bytesRead(), current.bytesWritten(),
                current.backlogEvents(), current.backlogBytes(),
                Math.max(prior.mailboxHighWaterEvents(),
                        current.mailboxHighWaterEvents()),
                Math.max(prior.channelHighWaterBytes(),
                        current.channelHighWaterBytes()),
                Math.max(prior.managedStateBytes(),
                        current.managedStateBytes()),
                Math.max(prior.timerHighWaterCount(),
                        current.timerHighWaterCount()),
                Math.max(prior.windowHighWaterCount(),
                        current.windowHighWaterCount()),
                current.backpressuredMillis());
    }

    static StreamJobProgress lifetime(StreamJobEvidence priorEvidence,
            StreamRunProgressSnapshot observed,
            StreamJobProgress attempt) {
        StreamJobProgress fallback = priorEvidence.available()
                ? priorEvidence.lifetimeProgress()
                : StreamJobProgress.empty();
        return lifetime(priorEvidence, fallback, observed, attempt);
    }

    static StreamJobProgress lifetime(StreamJobEvidence priorEvidence,
            StreamJobProgress fallback,
            StreamRunProgressSnapshot observed,
            StreamJobProgress attempt) {
        StreamJobProgress priorLifetime = priorEvidence.available()
                ? priorEvidence.lifetimeProgress() : fallback;
        StreamJobProgress base = sameCounterEpoch(
                priorEvidence, observed, attempt)
                ? subtractCumulative(priorLifetime,
                priorEvidence.attemptProgress()) : priorLifetime;
        return new StreamJobProgress(attempt.attempt(),
                attempt.completedCheckpoints(), attempt.operatorWatermark(),
                attempt.cdcPhase(), attempt.restarts(),
                Math.addExact(base.recordsRead(), attempt.recordsRead()),
                Math.addExact(base.recordsWritten(),
                        attempt.recordsWritten()),
                Math.addExact(base.bytesRead(), attempt.bytesRead()),
                Math.addExact(base.bytesWritten(), attempt.bytesWritten()),
                attempt.backlogEvents(), attempt.backlogBytes(),
                Math.max(priorLifetime.mailboxHighWaterEvents(),
                        attempt.mailboxHighWaterEvents()),
                Math.max(priorLifetime.channelHighWaterBytes(),
                        attempt.channelHighWaterBytes()),
                Math.max(priorLifetime.managedStateBytes(),
                        attempt.managedStateBytes()),
                Math.max(priorLifetime.timerHighWaterCount(),
                        attempt.timerHighWaterCount()),
                Math.max(priorLifetime.windowHighWaterCount(),
                        attempt.windowHighWaterCount()),
                Math.addExact(base.backpressuredMillis(),
                        attempt.backpressuredMillis()));
    }

    private static boolean sameAttempt(StreamJobEvidence priorEvidence,
            StreamRunProgressSnapshot observed) {
        return priorEvidence.available()
                && priorEvidence.runId().equals(observed.runId())
                && priorEvidence.attemptId().equals(observed.attemptId())
                && priorEvidence.fenceToken() == observed.fenceToken()
                && priorEvidence.assignmentGeneration()
                == observed.assignmentGeneration();
    }

    private static boolean sameCounterEpoch(
            StreamJobEvidence priorEvidence,
            StreamRunProgressSnapshot observed,
            StreamJobProgress current) {
        if (!sameAttempt(priorEvidence, observed)) return false;
        if (observed.coordinatorTerm()
                <= priorEvidence.coordinatorTerm()) return true;
        if (!observed.aggregate().complete()) return true;
        return !cumulativeCountersRegressed(
                current, priorEvidence.attemptProgress());
    }

    private static boolean cumulativeCountersRegressed(
            StreamJobProgress current, StreamJobProgress prior) {
        return current.recordsRead() < prior.recordsRead()
                || current.recordsWritten() < prior.recordsWritten()
                || current.bytesRead() < prior.bytesRead()
                || current.bytesWritten() < prior.bytesWritten()
                || current.backpressuredMillis()
                < prior.backpressuredMillis();
    }

    private static StreamJobProgress subtractCumulative(
            StreamJobProgress lifetime, StreamJobProgress attempt) {
        return new StreamJobProgress(lifetime.attempt(),
                lifetime.completedCheckpoints(),
                lifetime.operatorWatermark(), lifetime.cdcPhase(),
                lifetime.restarts(),
                subtractNonnegative(lifetime.recordsRead(),
                        attempt.recordsRead()),
                subtractNonnegative(lifetime.recordsWritten(),
                        attempt.recordsWritten()),
                subtractNonnegative(lifetime.bytesRead(),
                        attempt.bytesRead()),
                subtractNonnegative(lifetime.bytesWritten(),
                        attempt.bytesWritten()),
                0L, 0L, 0, 0L, 0L, 0, 0,
                subtractNonnegative(lifetime.backpressuredMillis(),
                        attempt.backpressuredMillis()));
    }

    private static long subtractNonnegative(long left, long right) {
        if (right > left) {
            throw new IllegalStateException(
                    "Stream lifetime progress regressed below its attempt");
        }
        return left - right;
    }
}
