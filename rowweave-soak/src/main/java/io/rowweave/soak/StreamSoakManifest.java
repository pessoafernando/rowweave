package io.rowweave.soak;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/** Fail-closed STREAM SOAK qualification manifest derived from verified samples. */
public final class StreamSoakManifest {
    public static final long RELEASE_MINIMUM_MILLIS = 24L * 60L * 60L * 1000L;
    public static final String ENVIRONMENT = "PROCESS_ISOLATED_SINGLE_HOST";

    private final Map<String, String> fields;

    private StreamSoakManifest(Map<String, String> fields) {
        this.fields = Collections.unmodifiableMap(
                new LinkedHashMap<>(new TreeMap<>(fields)));
    }

    public static StreamSoakManifest evaluate(Path samples,
            Map<String, String> summary) throws IOException {
        return evaluate(samples, summary, null);
    }

    static StreamSoakManifest evaluate(Path samples,
            Map<String, String> summary, StreamSoakPlan plan)
            throws IOException {
        if (plan != null) plan.verifyUnchanged();
        List<Map<String, String>> verified =
                StreamSoakEvidenceLog.readVerified(samples);
        if (verified.isEmpty()) {
            throw new IllegalStateException("STREAM_SOAK_SAMPLES_EMPTY");
        }
        Map<String, String> fields = new LinkedHashMap<>(summary);
        fields.put("environment", ENVIRONMENT);
        fields.put("sample_count", Integer.toString(verified.size()));
        fields.put("sample_log_sha256",
                StreamSoakEvidenceLog.fileSha256(samples));
        long duration = number(fields, "duration_millis");
        long sampleGapThreshold = number(
                fields, "threshold_sample_gap_millis");
        long priorElapsed = 0L;
        long wallStarted = -1L;
        long priorObserved = -1L;
        String jobId = null;
        String runId = null;
        long priorAttempt = -1L;
        long priorCoordinatorTerm = -1L;
        long priorCheckpointSequence = -1L;
        boolean sameJobPreserved = true;
        boolean sameRunPreserved = true;
        boolean attemptsMonotonic = true;
        boolean coordinatorTermsMonotonic = true;
        boolean checkpointsMonotonic = true;
        long maximumSampleGap = 0L;
        long maximumWallSampleGap = 0L;
        StringBuilder observedFaultOrder = new StringBuilder();
        for (Map<String, String> sample : verified) {
            if (!"progress-resource".equals(required(sample, "kind"))) {
                throw new IllegalStateException(
                        "STREAM_SOAK_SAMPLE_KIND_INVALID");
            }
            long elapsed = number(sample, "elapsed_millis");
            if (elapsed < priorElapsed || elapsed > duration) {
                throw new IllegalStateException(
                        "STREAM_SOAK_SAMPLE_TIME_INVALID");
            }
            maximumSampleGap = Math.max(maximumSampleGap,
                    elapsed - priorElapsed);
            priorElapsed = elapsed;
            long sampleWallStarted = number(sample,
                    "wall_started_epoch_millis");
            long observed = number(sample, "observed_epoch_millis");
            if (wallStarted < 0L) {
                wallStarted = sampleWallStarted;
                priorObserved = wallStarted;
            }
            if (sampleWallStarted != wallStarted
                    || observed < priorObserved
                    || observed < wallStarted) {
                throw new IllegalStateException(
                        "STREAM_SOAK_SAMPLE_WALL_TIME_INVALID");
            }
            maximumWallSampleGap = Math.max(maximumWallSampleGap,
                    observed - priorObserved);
            priorObserved = observed;
            String sampleJobId = required(sample, "job_id");
            String sampleRunId = required(sample, "run_id");
            long attempt = number(sample, "attempt");
            long coordinatorTerm = number(sample, "coordinator_term");
            long checkpointSequence = number(sample,
                    "checkpoint_sequence");
            if (jobId == null) {
                jobId = sampleJobId;
                runId = sampleRunId;
            } else {
                sameJobPreserved &= jobId.equals(sampleJobId);
                sameRunPreserved &= runId.equals(sampleRunId);
            }
            attemptsMonotonic &= attempt >= 0L
                    && attempt >= priorAttempt;
            coordinatorTermsMonotonic &= coordinatorTerm > 0L
                    && coordinatorTerm >= priorCoordinatorTerm;
            checkpointsMonotonic &= checkpointSequence >= 0L
                    && checkpointSequence >= priorCheckpointSequence;
            if (priorAttempt >= 0L) {
                if (coordinatorTerm > priorCoordinatorTerm) {
                    if (coordinatorTerm != priorCoordinatorTerm + 1L
                            || attempt != priorAttempt) {
                        observedFaultOrder.append('?');
                    } else observedFaultOrder.append('L');
                }
                if (attempt > priorAttempt) {
                    if (attempt != priorAttempt + 1L
                            || coordinatorTerm != priorCoordinatorTerm) {
                        observedFaultOrder.append('?');
                    } else observedFaultOrder.append('W');
                }
            }
            priorAttempt = attempt;
            priorCoordinatorTerm = coordinatorTerm;
            priorCheckpointSequence = checkpointSequence;
        }
        maximumSampleGap = Math.max(maximumSampleGap,
                duration - priorElapsed);
        boolean sampleCadenceComplete =
                maximumSampleGap <= sampleGapThreshold;
        long wallDuration = priorObserved - wallStarted;
        boolean wallSampleCadenceComplete =
                maximumWallSampleGap <= sampleGapThreshold;
        boolean sampleCountComplete = number(fields, "resource_samples")
                == verified.size();
        fields.put("max_sample_gap_millis",
                Long.toString(maximumSampleGap));
        fields.put("max_wall_sample_gap_millis",
                Long.toString(maximumWallSampleGap));
        fields.put("sample_cadence_complete",
                Boolean.toString(sampleCadenceComplete));
        fields.put("wall_sample_cadence_complete",
                Boolean.toString(wallSampleCadenceComplete));
        fields.put("sample_count_complete",
                Boolean.toString(sampleCountComplete));
        fields.put("sample_same_job_preserved",
                Boolean.toString(sameJobPreserved));
        fields.put("sample_same_run_preserved",
                Boolean.toString(sameRunPreserved));
        fields.put("sample_attempts_monotonic",
                Boolean.toString(attemptsMonotonic));
        fields.put("sample_coordinator_terms_monotonic",
                Boolean.toString(coordinatorTermsMonotonic));
        fields.put("sample_checkpoints_monotonic",
                Boolean.toString(checkpointsMonotonic));
        fields.put("wall_duration_millis", Long.toString(wallDuration));
        if (plan != null) {
            fields.put("plan_sha256", plan.sha256());
            fields.put("qualification_profile", plan.fields().get("profile"));
            fields.put("candidate_manifest_sha256",
                    plan.fields().get("candidate_manifest_sha256"));
        }
        boolean monotonicFullDuration =
                duration >= RELEASE_MINIMUM_MILLIS;
        boolean wallFullDuration =
                wallDuration >= RELEASE_MINIMUM_MILLIS;
        boolean durationAuthoritiesAgree =
                monotonicFullDuration == wallFullDuration;
        boolean fullDuration = monotonicFullDuration && wallFullDuration;
        boolean complete = truth(fields, "evidence_complete")
                && truth(fields, "resource_samples_complete")
                && sampleCadenceComplete
                && wallSampleCadenceComplete
                && sampleCountComplete
                && sameJobPreserved
                && sameRunPreserved
                && attemptsMonotonic
                && coordinatorTermsMonotonic
                && checkpointsMonotonic
                && durationAuthoritiesAgree
                && truth(fields, "same_run_preserved")
                && truth(fields, "real_minio_used")
                && truth(fields, "real_postgres_used")
                && truth(fields, "signed_connectors_used")
                && truth(fields, "secret_resolution_complete")
                && truth(fields, "backlog_observed")
                && truth(fields, "backpressure_observed")
                && truth(fields, "checkpoints_monotonic")
                && truth(fields, "attempts_monotonic")
                && truth(fields, "coordinator_terms_monotonic")
                && truth(fields, "s3_oracle_match")
                && truth(fields, "postgres_oracle_match")
                && truth(fields, "postgres_ack_monotonic")
                && number(fields, "leader_replacements") >= 2L
                && number(fields, "worker_replacements") >= 2L
                && number(fields, "master_processes") >= 2L
                && number(fields, "worker_processes") >= 3L
                && number(fields, "completed_checkpoints") > 0L
                && number(fields, "unexplained_failures") == 0L
                && number(fields, "threshold_violations") == 0L;
        fields.put("duration_requirement_met",
                Boolean.toString(fullDuration));
        fields.put("monotonic_duration_requirement_met",
                Boolean.toString(monotonicFullDuration));
        fields.put("wall_duration_requirement_met",
                Boolean.toString(wallFullDuration));
        boolean naturalProfile = plan == null || "NATURAL_24H".equals(
                plan.fields().get("profile"));
        boolean naturalPassed = naturalProfile && fullDuration && complete;
        boolean acceleratedDuration = duration >= 14_400_000L
                && wallDuration >= 14_400_000L;
        boolean acceleratedPassed = plan != null
                && "ACCELERATED_4H".equals(plan.fields().get("profile"))
                && acceleratedDuration && complete
                && "LWWL".contentEquals(observedFaultOrder)
                && priorCheckpointSequence
                        == number(fields, "completed_checkpoints")
                && profiledWorkAndBounds(fields, plan);
        boolean feasibilityDuration = duration >= 1_200_000L
                && wallDuration >= 1_200_000L;
        boolean feasibilityPassed = plan != null
                && "ACCELERATED_FEASIBILITY".equals(
                        plan.fields().get("profile"))
                && feasibilityDuration && complete
                && "LWWL".contentEquals(observedFaultOrder)
                && priorCheckpointSequence
                        == number(fields, "completed_checkpoints")
                && profiledWorkAndBounds(fields, plan);
        fields.put("accelerated_duration_requirement_met",
                Boolean.toString(acceleratedDuration));
        fields.put("feasibility_duration_requirement_met",
                Boolean.toString(feasibilityDuration));
        fields.put("feasibility_gate_passed",
                Boolean.toString(feasibilityPassed));
        fields.put("accelerated_gate_passed",
                Boolean.toString(acceleratedPassed));
        fields.put("natural_24h_gate_passed",
                Boolean.toString(naturalPassed));
        fields.put("release_gate_passed",
                Boolean.toString(naturalPassed));
        fields.put("qualification", acceleratedPassed
                ? "STREAM_SOAK_ACCELERATED_PASSED"
                : naturalPassed ? "STREAM_SOAK_PASSED"
                : feasibilityPassed ? "SHORT_QUALIFIED"
                : plan == null && complete ? "SHORT_QUALIFIED"
                : "FAILED");
        return new StreamSoakManifest(fields);
    }

    private static boolean profiledWorkAndBounds(
            Map<String, String> fields, StreamSoakPlan plan) {
        Map<String, String> frozen = plan.fields();
        for (String threshold : new String[] {
                "threshold_process_rss_bytes", "threshold_fd",
                "threshold_disk_bytes", "threshold_checkpoint_millis",
                "threshold_recovery_millis", "threshold_backlog_events",
                "threshold_state_bytes"}) {
            if (!frozen.get(threshold).equals(fields.get(threshold))) {
                return false;
            }
        }
        if (number(fields, "threshold_sample_gap_millis") > 120_000L
                || number(fields, "s3_jobs") < number(frozen,
                        "s3_minimum_jobs")
                || number(fields, "s3_expected_rows") < number(frozen,
                        "s3_minimum_rows")
                || number(fields, "postgres_expected_rows") < number(frozen,
                        "postgres_minimum_rows")
                || number(fields, "completed_checkpoints") < number(frozen,
                        "checkpoint_minimum_completed")
                || number(fields, "sample_count") < number(frozen,
                        "sample_minimum_count")
                || number(fields, "purge_completed") < number(frozen,
                        "purge_minimum_completed")) {
            return false;
        }
        for (String source : new String[] {"s3", "postgres"}) {
            if (number(fields, source + "_expected_rows")
                    != number(fields, source + "_actual_rows")) {
                return false;
            }
            String expected = fields.get(source + "_expected_sha256");
            if (expected == null || !expected.matches("[0-9a-f]{64}")
                    || !expected.equals(fields.get(
                            source + "_actual_sha256"))) {
                return false;
            }
        }
        return true;
    }

    public boolean releaseGatePassed() {
        return Boolean.parseBoolean(fields.get("release_gate_passed"));
    }

    public Map<String, String> fields() { return fields; }

    public void write(Path path) throws IOException {
        StringBuilder result = new StringBuilder();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            result.append(field.getKey()).append('=')
                    .append(field.getValue()).append('\n');
        }
        Files.write(path, result.toString().getBytes(StandardCharsets.UTF_8),
                StandardOpenOption.CREATE,
                StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE);
    }

    private static boolean truth(Map<String, String> fields, String key) {
        return Boolean.parseBoolean(required(fields, key));
    }

    private static long number(Map<String, String> fields, String key) {
        try {
            return Long.parseLong(required(fields, key));
        } catch (NumberFormatException invalid) {
            throw new IllegalStateException("STREAM_SOAK_MANIFEST_INVALID_" + key,
                    invalid);
        }
    }

    private static String required(Map<String, String> fields, String key) {
        String value = fields.get(key);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("STREAM_SOAK_MANIFEST_MISSING_" + key);
        }
        return value;
    }
}
