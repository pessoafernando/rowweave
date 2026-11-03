package io.rowweave.soak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamSoakManifestTest {
    private static final long WALL_STARTED = 1_000_000L;

    @Test
    void shortRunCannotPassReleaseGate(@TempDir Path root) throws Exception {
        Path samples = root.resolve("samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            log.append(sample(60_000L, WALL_STARTED + 60_000L));
        }
        StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                samples, successfulSummary(60_000L));
        assertFalse(manifest.releaseGatePassed());
        assertEquals("SHORT_QUALIFIED",
                manifest.fields().get("qualification"));
        assertEquals(StreamSoakManifest.ENVIRONMENT,
                manifest.fields().get("environment"));
    }

    @Test
    void fullDurationAndAllHardConditionsPass(@TempDir Path root)
            throws Exception {
        Path samples = root.resolve("samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            log.append(sample(StreamSoakManifest.RELEASE_MINIMUM_MILLIS,
                    WALL_STARTED
                            + StreamSoakManifest.RELEASE_MINIMUM_MILLIS));
        }
        StreamSoakManifest manifest = StreamSoakManifest.evaluate(samples,
                successfulSummary(StreamSoakManifest.RELEASE_MINIMUM_MILLIS));
        assertTrue(manifest.releaseGatePassed());
        assertEquals("STREAM_SOAK_PASSED",
                manifest.fields().get("qualification"));
    }

    @Test
    void corruptedSampleFailsClosed(@TempDir Path root) throws Exception {
        Path samples = root.resolve("samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            log.append(sample(60_000L, WALL_STARTED + 60_000L));
        }
        byte[] bytes = Files.readAllBytes(samples);
        bytes[0] ^= 1;
        Files.write(samples, bytes);
        assertThrows(IllegalStateException.class,
                () -> StreamSoakManifest.evaluate(samples,
                        successfulSummary(
                                StreamSoakManifest.RELEASE_MINIMUM_MILLIS)));
    }

    @Test
    void sparseSamplesCannotPassFullDurationGate(@TempDir Path root)
            throws Exception {
        Path samples = root.resolve("samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            log.append(sample(0L, WALL_STARTED));
        }
        Map<String, String> summary = successfulSummary(
                StreamSoakManifest.RELEASE_MINIMUM_MILLIS);
        summary.put("threshold_sample_gap_millis", "120000");
        StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                samples, summary);
        assertFalse(manifest.releaseGatePassed());
        assertFalse(Boolean.parseBoolean(
                manifest.fields().get("sample_cadence_complete")));
        assertEquals("FAILED", manifest.fields().get("qualification"));
    }

    @Test
    void hostSleepFailsWallClockCadenceEvenWhenMonotonicCadencePasses(
            @TempDir Path root) throws Exception {
        Path samples = root.resolve("samples.log");
        long duration = StreamSoakManifest.RELEASE_MINIMUM_MILLIS;
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            log.append(sample(0L, WALL_STARTED));
            log.append(sample(duration,
                    WALL_STARTED + duration + 247_000L));
        }
        Map<String, String> summary = successfulSummary(duration);
        summary.put("resource_samples", "2");
        summary.put("threshold_sample_gap_millis",
                Long.toString(duration));

        StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                samples, summary);

        assertTrue(Boolean.parseBoolean(
                manifest.fields().get("sample_cadence_complete")));
        assertFalse(Boolean.parseBoolean(
                manifest.fields().get("wall_sample_cadence_complete")));
        assertEquals(Long.toString(duration + 247_000L),
                manifest.fields().get("max_wall_sample_gap_millis"));
        assertFalse(manifest.releaseGatePassed());
        assertEquals("FAILED", manifest.fields().get("qualification"));
    }

    @Test
    void wallClockRegressionFailsClosed(@TempDir Path root)
            throws Exception {
        Path samples = root.resolve("samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            log.append(sample(0L, WALL_STARTED + 10_000L));
            log.append(sample(10_000L, WALL_STARTED + 9_999L));
        }
        Map<String, String> summary = successfulSummary(10_000L);
        summary.put("resource_samples", "2");

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> StreamSoakManifest.evaluate(samples, summary));
        assertEquals("STREAM_SOAK_SAMPLE_WALL_TIME_INVALID",
                failure.getMessage());
    }

    @Test
    void fullMonotonicDurationCannotReplaceShortWallDuration(
            @TempDir Path root) throws Exception {
        Path samples = root.resolve("samples.log");
        long duration = StreamSoakManifest.RELEASE_MINIMUM_MILLIS;
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            log.append(sample(duration,
                    WALL_STARTED + duration - 1L));
        }

        StreamSoakManifest manifest = StreamSoakManifest.evaluate(samples,
                successfulSummary(duration));

        assertFalse(Boolean.parseBoolean(
                manifest.fields().get("wall_duration_requirement_met")));
        assertFalse(Boolean.parseBoolean(
                manifest.fields().get("duration_requirement_met")));
        assertFalse(manifest.releaseGatePassed());
        assertEquals("FAILED", manifest.fields().get("qualification"));
    }

    @Test
    void verifiedCheckpointRegressionCannotBeHiddenBySuccessfulSummary(
            @TempDir Path root) throws Exception {
        Path samples = root.resolve("samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            log.append(sample(10_000L, WALL_STARTED + 10_000L,
                    1, 2L, 14L));
            log.append(sample(20_000L, WALL_STARTED + 20_000L,
                    2, 2L, 0L));
        }
        Map<String, String> summary = successfulSummary(20_000L);
        summary.put("resource_samples", "2");
        summary.put("checkpoints_monotonic", "true");

        StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                samples, summary);

        assertFalse(Boolean.parseBoolean(manifest.fields().get(
                "sample_checkpoints_monotonic")));
        assertFalse(manifest.releaseGatePassed());
        assertEquals("FAILED", manifest.fields().get("qualification"));
    }

    @Test
    void explicitFourHourPlanPassesOnlyAcceleratedGate(
            @TempDir Path root) throws Exception {
        StreamSoakPlan plan = acceleratedPlan(root);
        Path samples = acceleratedSamples(root);
        StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                samples, acceleratedSummary(), plan);
        assertFalse(manifest.releaseGatePassed());
        assertEquals("false", manifest.fields().get("natural_24h_gate_passed"));
        assertEquals("true", manifest.fields().get("accelerated_gate_passed"));
        assertEquals("STREAM_SOAK_ACCELERATED_PASSED",
                manifest.fields().get("qualification"));
        assertEquals(plan.sha256(), manifest.fields().get("plan_sha256"));
    }

    @Test
    void acceleratedGateRejectsCoverageShortfallAndChangedPlan(
            @TempDir Path root) throws Exception {
        StreamSoakPlan plan = acceleratedPlan(root);
        Path samples = acceleratedSamples(root);
        for (String[] shortfall : new String[][] {
                {"s3_jobs", "95"}, {"s3_expected_rows", "24320"},
                {"postgres_expected_rows", "4119648"},
                {"completed_checkpoints", "2879"},
                {"purge_completed", "23"},
                {"threshold_disk_bytes", "8589934593"},
                {"postgres_oracle_match", "false"}}) {
            Map<String, String> summary = acceleratedSummary();
            summary.put(shortfall[0], shortfall[1]);
            StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                    samples, summary, plan);
            assertEquals("false", manifest.fields().get(
                    "accelerated_gate_passed"), shortfall[0]);
            assertEquals("FAILED", manifest.fields().get("qualification"),
                    shortfall[0]);
        }
        Files.write(root.resolve("qualification-plan.properties"),
                new byte[] {'x'});
        assertThrows(IllegalStateException.class,
                () -> StreamSoakManifest.evaluate(
                        samples, acceleratedSummary(), plan));
    }

    @Test
    void acceleratedGateRejectsSparseButGapCompliantSamples(
            @TempDir Path root) throws Exception {
        StreamSoakPlan plan = acceleratedPlan(root);
        Path samples = root.resolve("sparse-samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            for (int index = 0; index <= 120; index++) {
                long elapsed = index * 120_000L;
                int attempt = index >= 72 ? 2 : index >= 42 ? 1 : 0;
                long term = index >= 96 ? 3L : index >= 18 ? 2L : 1L;
                log.append(sample(elapsed, WALL_STARTED + elapsed,
                        attempt, term, index * 24L));
            }
        }
        Map<String, String> summary = acceleratedSummary();
        summary.put("resource_samples", "121");
        StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                samples, summary, plan);
        assertEquals("false", manifest.fields().get(
                "accelerated_gate_passed"));
    }

    @Test
    void feasibleShortRunCannotPassAcceleratedGate(
            @TempDir Path root) throws Exception {
        StreamSoakConfig config = StreamSoakConfig.parse(new String[] {
                "--qualification-profile=ACCELERATED_FEASIBILITY",
                "--qualification-impact=BEHAVIORAL_ACCEPTANCE",
                "--candidate-manifest-sha256="
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--duration-seconds=1200",
                "--sample-interval-seconds=2",
                "--checkpoint-interval-seconds=4",
                "--s3-interval-seconds=150", "--s3-rows=256",
                "--mutation-burst=256", "--mutation-pause-millis=2000",
                "--sink-delay-millis=1"});
        StreamSoakPlan plan = StreamSoakPlan.freeze(config, root);
        Path samples = root.resolve("feasibility-samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            for (int index = 0; index <= 600; index++) {
                long elapsed = index * 2_000L;
                int attempt = index >= 360 ? 2
                        : index >= 210 ? 1 : 0;
                long term = index >= 480 ? 3L
                        : index >= 90 ? 2L : 1L;
                log.append(sample(elapsed, WALL_STARTED + elapsed,
                        attempt, term, index * 2L / 5L));
            }
        }
        Map<String, String> summary = acceleratedSummary();
        summary.put("duration_millis", "1200000");
        summary.put("resource_samples", "601");
        summary.put("completed_checkpoints", "240");
        summary.put("s3_jobs", "8");
        summary.put("s3_expected_rows", "2048");
        summary.put("s3_actual_rows", "2048");
        summary.put("postgres_expected_rows", "343305");
        summary.put("postgres_actual_rows", "343305");
        summary.put("purge_completed", "2");
        StreamSoakManifest manifest = StreamSoakManifest.evaluate(
                samples, summary, plan);
        assertEquals("SHORT_QUALIFIED",
                manifest.fields().get("qualification"));
        assertEquals("true", manifest.fields().get("feasibility_gate_passed"));
        assertEquals("false", manifest.fields().get("accelerated_gate_passed"));
        assertFalse(manifest.releaseGatePassed());
    }

    private static StreamSoakPlan acceleratedPlan(Path root) throws Exception {
        StreamSoakConfig config = StreamSoakConfig.parse(new String[] {
                "--qualification-profile=ACCELERATED_4H",
                "--qualification-impact=BEHAVIORAL_ACCEPTANCE",
                "--candidate-manifest-sha256="
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--duration-seconds=14400",
                "--sample-interval-seconds=2",
                "--checkpoint-interval-seconds=4",
                "--s3-interval-seconds=150", "--s3-rows=256",
                "--mutation-burst=256", "--mutation-pause-millis=2000",
                "--sink-delay-millis=1"});
        return StreamSoakPlan.freeze(config, root);
    }

    private static Path acceleratedSamples(Path root) throws Exception {
        Path samples = root.resolve("samples.log");
        try (StreamSoakEvidenceLog log = new StreamSoakEvidenceLog(samples)) {
            for (int index = 0; index <= 7_200; index++) {
                long elapsed = index * 2_000L;
                int attempt = index >= 4_320 ? 2
                        : index >= 2_520 ? 1 : 0;
                long term = index >= 5_760 ? 3L
                        : index >= 1_080 ? 2L : 1L;
                log.append(sample(elapsed, WALL_STARTED + elapsed,
                        attempt, term, index * 2L / 5L));
            }
        }
        return samples;
    }

    private static Map<String, String> acceleratedSummary() {
        Map<String, String> summary = successfulSummary(14_400_000L);
        summary.put("resource_samples", "7201");
        summary.put("completed_checkpoints", "2880");
        summary.put("s3_jobs", "96");
        summary.put("s3_expected_rows", "24576");
        summary.put("s3_actual_rows", "24576");
        summary.put("s3_expected_sha256",
                "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
                        + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        summary.put("s3_actual_sha256", summary.get("s3_expected_sha256"));
        summary.put("postgres_expected_rows", "4119649");
        summary.put("postgres_actual_rows", "4119649");
        summary.put("postgres_expected_sha256",
                "cccccccccccccccccccccccccccccccc"
                        + "cccccccccccccccccccccccccccccccc");
        summary.put("postgres_actual_sha256",
                summary.get("postgres_expected_sha256"));
        summary.put("purge_completed", "24");
        summary.put("threshold_sample_gap_millis", "120000");
        for (String key : new String[] {
                "threshold_process_rss_bytes", "threshold_fd",
                "threshold_disk_bytes", "threshold_checkpoint_millis",
                "threshold_recovery_millis", "threshold_backlog_events",
                "threshold_state_bytes"}) {
            summary.put(key, key.equals("threshold_process_rss_bytes")
                    ? "1610612736" : key.equals("threshold_fd")
                    ? "2048" : key.equals("threshold_disk_bytes")
                    ? "8589934592" : key.equals("threshold_state_bytes")
                    ? "536870912" : key.equals("threshold_backlog_events")
                    ? "1000000" : "60000");
        }
        return summary;
    }

    private static Map<String, String> sample(long elapsed,
            long observedEpochMillis) {
        return sample(elapsed, observedEpochMillis, 1, 1L, 1L);
    }

    private static Map<String, String> sample(long elapsed,
            long observedEpochMillis, int attempt, long coordinatorTerm,
            long checkpointSequence) {
        Map<String, String> sample = new LinkedHashMap<>();
        sample.put("kind", "progress-resource");
        sample.put("job_id", "job-1");
        sample.put("run_id", "run-1");
        sample.put("attempt", Integer.toString(attempt));
        sample.put("coordinator_term", Long.toString(coordinatorTerm));
        sample.put("checkpoint_sequence",
                Long.toString(checkpointSequence));
        sample.put("elapsed_millis", Long.toString(elapsed));
        sample.put("wall_started_epoch_millis",
                Long.toString(WALL_STARTED));
        sample.put("observed_epoch_millis",
                Long.toString(observedEpochMillis));
        return sample;
    }

    private static Map<String, String> successfulSummary(long duration) {
        Map<String, String> result = new LinkedHashMap<>();
        result.put("duration_millis", Long.toString(duration));
        result.put("evidence_complete", "true");
        result.put("resource_samples_complete", "true");
        result.put("same_run_preserved", "true");
        result.put("real_minio_used", "true");
        result.put("real_postgres_used", "true");
        result.put("signed_connectors_used", "true");
        result.put("secret_resolution_complete", "true");
        result.put("backlog_observed", "true");
        result.put("backpressure_observed", "true");
        result.put("checkpoints_monotonic", "true");
        result.put("attempts_monotonic", "true");
        result.put("coordinator_terms_monotonic", "true");
        result.put("s3_oracle_match", "true");
        result.put("postgres_oracle_match", "true");
        result.put("postgres_ack_monotonic", "true");
        result.put("leader_replacements", "2");
        result.put("worker_replacements", "2");
        result.put("master_processes", "2");
        result.put("worker_processes", "3");
        result.put("completed_checkpoints", "3");
        result.put("unexplained_failures", "0");
        result.put("threshold_violations", "0");
        result.put("resource_samples", "1");
        result.put("threshold_sample_gap_millis", Long.toString(duration));
        return result;
    }
}
