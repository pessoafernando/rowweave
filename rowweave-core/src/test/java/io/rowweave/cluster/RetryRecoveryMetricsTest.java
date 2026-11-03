package io.rowweave.cluster;

import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.run.WholeAttemptRetryAuthorityKernel;
import io.rowweave.run.WholeAttemptRetryDecision;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RetryRecoveryMetricsTest {

    @Test
    void decisionCountersAreSelectiveAndMonitorFailureIsAdvisory() {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(4);
        RetryRecoveryMetrics metrics = new RetryRecoveryMetrics(monitor, 8);

        metrics.record(decision(WholeAttemptRetryDecision.Code.SCHEDULED));
        metrics.record(decision(WholeAttemptRetryDecision.Code.ALREADY_SCHEDULED));
        metrics.record(decision(WholeAttemptRetryDecision.Code.EXHAUSTED));
        metrics.record(decision(WholeAttemptRetryDecision.Code.NOT_RETRYABLE));

        assertEquals(1L, monitor.retryRecovery().scheduledRetries());
        assertEquals(1L, monitor.retryRecovery().exhaustedRetries());
        assertEquals(1L, monitor.retryRecovery().notRetryableClosures());

        RetryRecoveryMetrics advisory = new RetryRecoveryMetrics(
                new ThrowingMonitor(), 8);
        advisory.record(decision(WholeAttemptRetryDecision.Code.SCHEDULED));
    }

    private static WholeAttemptRetryDecision decision(
            WholeAttemptRetryDecision.Code code) {
        return WholeAttemptRetryAuthorityKernel.decision(
                code, null, null, 0L);
    }

    private static final class ThrowingMonitor
            implements io.rowweave.monitor.JobMonitorRegistry {
        @Override public void onJobSubmit(io.rowweave.monitor.JobView job) { }
        @Override public void onStageUpdate(
                String jobId, io.rowweave.monitor.StageView stage) { }
        @Override public void onJobComplete(
                String jobId, String state, String error) { }
        @Override public java.util.List<io.rowweave.monitor.JobView> jobs() {
            return java.util.Collections.emptyList();
        }
        @Override public io.rowweave.monitor.JobView job(String jobId) {
            return null;
        }
        @Override public java.util.List<io.rowweave.monitor.WorkerView> workers() {
            return java.util.Collections.emptyList();
        }
        @Override public void onRetryRecovery(
                io.rowweave.monitor.RetryRecoveryView view) {
            throw new IllegalStateException("monitor unavailable");
        }
    }
}
