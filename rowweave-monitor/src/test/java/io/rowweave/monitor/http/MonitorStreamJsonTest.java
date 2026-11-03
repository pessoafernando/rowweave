package io.rowweave.monitor.http;

import io.rowweave.monitor.JobView;
import io.rowweave.monitor.StreamMonitorView;
import io.rowweave.stream.api.StreamCdcPhase;
import io.rowweave.stream.api.StreamJobEvidence;
import io.rowweave.stream.api.StreamJobId;
import io.rowweave.stream.api.StreamJobProgress;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Stream job detail carries separate attempt and lifetime operational facts. */
class MonitorStreamJsonTest {

    @Test
    void streamJobDetailIncludesTypedAttemptAndLifetimeProgress() {
        StreamJobProgress attempt = new StreamJobProgress(
                3, 1L, 123_456L, StreamCdcPhase.LOG, 2,
                1_000L, 900L, 8_000L, 7_000L, 12L, 4_096L,
                20, 32_768L, 65_536L, 40, 8, 5_000L);
        StreamJobProgress lifetime = new StreamJobProgress(
                3, 5L, 123_456L, StreamCdcPhase.LOG, 2,
                11_000L, 10_900L, 88_000L, 77_000L, 12L, 4_096L,
                20, 32_768L, 65_536L, 40, 8, 15_000L);
        StreamJobEvidence evidence = new StreamJobEvidence(
                StreamJobId.of("stream-json"), "run-1", "attempt-3", 3,
                9L, 4L, 7L, checkpointId(), 5L, 123L, 4_096L,
                attempt, lifetime, 6, 6, 1_000L);
        JobView job = JobView.builder("stream-json", "Orders")
                .state("RUNNING")
                .stream(new StreamMonitorView(evidence))
                .build();

        String json = MonitorJson.jobDetail(job);
        assertTrue(json.contains("\"stream\":{\"available\":true"), json);
        assertTrue(json.contains("\"attemptOrdinal\":3"), json);
        assertTrue(json.contains("\"attempt\":{\"attempt\":3,"), json);
        assertTrue(json.contains("\"lifetime\":{\"attempt\":3,"), json);
        assertTrue(json.contains("\"completedCheckpoints\":1,"), json);
        assertTrue(json.contains("\"completedCheckpoints\":5,"), json);
        assertTrue(json.contains("\"operatorWatermark\":123456,"), json);
        assertTrue(json.contains("\"cdcPhase\":\"LOG\""), json);
        assertTrue(json.contains("\"backpressuredMillis\":15000"), json);
        assertFalse(json.contains("rowCount"), json);
    }

    @Test
    void batchJobDetailUsesNullStreamProjection() {
        String json = MonitorJson.jobDetail(
                JobView.builder("batch-json", "Batch").build());
        assertTrue(json.contains("\"stream\":null"), json);
    }

    private static String checkpointId() {
        StringBuilder value = new StringBuilder("stream-checkpoint:");
        for (int index = 0; index < 64; index++) value.append('a');
        return value.toString();
    }
}
