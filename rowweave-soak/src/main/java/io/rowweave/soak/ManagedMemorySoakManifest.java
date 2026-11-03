package io.rowweave.soak;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/** Fail-closed terminal manifest derived from checksum-framed Batch evidence. */
final class ManagedMemorySoakManifest {
    static final String FILE_NAME = "managed-memory-manifest.properties";

    private final Map<String, String> fields;

    private ManagedMemorySoakManifest(Map<String, String> fields) {
        this.fields = Collections.unmodifiableMap(
                new LinkedHashMap<>(new TreeMap<>(fields)));
    }

    static ManagedMemorySoakManifest evaluate(
            ManagedMemorySoakConfig config, ManagedMemorySoakPlan plan,
            Path jobs, Path resources, Path activities,
            Map<String, String> summary)
            throws IOException {
        if (config == null || jobs == null || resources == null
                || activities == null || summary == null) {
            throw new IllegalArgumentException(
                    "config, evidence and summary are required");
        }
        if (plan != null) plan.verifyUnchanged();
        List<Map<String, String>> jobRecords =
                StreamSoakEvidenceLog.readVerified(jobs);
        List<Map<String, String>> resourceRecords =
                StreamSoakEvidenceLog.readVerified(resources);
        List<Map<String, String>> activityRecords =
                StreamSoakEvidenceLog.readVerified(activities);
        if (jobRecords.isEmpty() || resourceRecords.isEmpty()
                || activityRecords.isEmpty()) {
            throw invalid("evidence logs must not be empty");
        }

        long oracleFailures = 0L;
        long budgetViolations = 0L;
        long workspaceLeaks = 0L;
        long spillJobs = 0L;
        for (Map<String, String> record : jobRecords) {
            require(record, "kind", "batch-job");
            if (!"true".equals(record.get("oracle_match"))) oracleFailures++;
            if (!"true".equals(record.get("within_budget"))
                    || !"COMPLETE".equals(record.get("resource_coverage"))) {
                budgetViolations++;
            }
            if (number(record, "workspace_job_directories") != 0L) {
                workspaceLeaks++;
            }
            if (Math.addExact(number(record, "pressure_spill_count"),
                    number(record, "spill_run_count")) > 0L) {
                spillJobs++;
            }
            if (number(record, "rows_out") != config.keyCardinality + 1L
                    || number(record, "acquired_bytes") <= 0L
                    || number(record, "peak_reserved_bytes") <= 0L) {
                budgetViolations++;
            }
        }

        long resourceViolations = 0L;
        for (Map<String, String> record : resourceRecords) {
            require(record, "kind", "process-resource");
            if (!"true".equals(record.get("alive"))
                    || number(record, "rss_bytes")
                    > config.maximumProcessRssBytes
                    || number(record, "file_descriptors")
                    > config.maximumFileDescriptors
                    || number(record, "disk_bytes")
                    > config.maximumDiskBytes) {
                resourceViolations++;
            }
        }

        Set<String> activityRoles = new HashSet<>();
        long activeWorkers = 0L;
        long workerActivityViolations = 0L;
        long expectedWaves = number(summary, "completed_waves");
        for (Map<String, String> record : activityRecords) {
            require(record, "kind", "role-activity");
            String role = record.get("role");
            if (role == null || !activityRoles.add(role)
                    || !record.getOrDefault("role_log_sha256", "")
                    .matches("[0-9a-f]{64}")) {
                throw invalid("role activity identity is invalid");
            }
            if (role.startsWith("worker-")) {
                boolean active = number(record, "connections") > 0L
                        && number(record, "connections") == expectedWaves
                        && number(record, "concurrent_mux_groups")
                        == expectedWaves
                        && number(record, "logical_streams") > 0L
                        && number(record, "records") > 0L
                        && number(record, "wire_bytes") > 0L;
                if (active) activeWorkers++;
                else workerActivityViolations++;
            }
        }
        Set<String> expectedRoles = new HashSet<>();
        expectedRoles.add("master");
        for (int worker = 0; worker < config.workerCount; worker++) {
            expectedRoles.add("worker-" + worker);
        }
        if (!activityRoles.equals(expectedRoles)) {
            throw invalid("role activity set is incomplete");
        }

        Map<String, String> fields = new LinkedHashMap<>(summary);
        fields.put("environment", "PROCESS_ISOLATED_SINGLE_HOST");
        fields.put("real_batch_cluster", "true");
        fields.put("worker_processes", Integer.toString(config.workerCount));
        fields.put("concurrent_jobs", Integer.toString(config.concurrentJobs));
        fields.put("completed_jobs", Integer.toString(jobRecords.size()));
        fields.put("resource_samples", Integer.toString(resourceRecords.size()));
        fields.put("job_log_sha256", StreamSoakEvidenceLog.fileSha256(jobs));
        fields.put("resource_log_sha256",
                StreamSoakEvidenceLog.fileSha256(resources));
        fields.put("role_activity_log_sha256",
                StreamSoakEvidenceLog.fileSha256(activities));
        fields.put("role_activity_records",
                Integer.toString(activityRecords.size()));
        fields.put("active_worker_processes", Long.toString(activeWorkers));
        fields.put("worker_activity_violations",
                Long.toString(workerActivityViolations));
        fields.put("oracle_failures", Long.toString(oracleFailures));
        fields.put("budget_violations", Long.toString(budgetViolations));
        fields.put("resource_violations", Long.toString(resourceViolations));
        fields.put("workspace_leaks", Long.toString(workspaceLeaks));
        fields.put("spill_jobs", Long.toString(spillJobs));
        fields.put("qualification_profile", config.profile.name());
        if (plan != null) {
            fields.put("plan_sha256", plan.sha256());
            fields.put("candidate_manifest_sha256",
                    config.candidateManifestSha256);
        }

        long waves = number(fields, "completed_waves");
        long monotonicDuration = number(fields, "duration_millis");
        long wallDuration = number(fields, "wall_duration_millis");
        boolean countsComplete = waves > 0L
                && jobRecords.size() == Math.multiplyExact(
                waves, (long) config.concurrentJobs);
        boolean clean = oracleFailures == 0L && budgetViolations == 0L
                && resourceViolations == 0L && workspaceLeaks == 0L
                && workerActivityViolations == 0L
                && activeWorkers == config.workerCount && countsComplete;
        String qualification;
        if (config.profile == ManagedMemorySoakConfig.Profile.DEVELOPMENT) {
            qualification = clean ? "DEVELOPMENT_COMPLETED" : "FAILED";
        } else {
            long required = config.profile
                    == ManagedMemorySoakConfig.Profile.FEASIBILITY
                    ? ManagedMemorySoakConfig.FEASIBILITY_DURATION_MILLIS
                    : ManagedMemorySoakConfig.NATURAL_DURATION_MILLIS;
            long minimumWaves = divideCeiling(required,
                    config.waveIntervalMillis);
            boolean durationComplete = monotonicDuration >= required
                    && wallDuration >= required;
            boolean workloadComplete = waves >= minimumWaves
                    && spillJobs == jobRecords.size();
            if (!clean || !durationComplete || !workloadComplete) {
                qualification = "FAILED";
            } else {
                qualification = config.profile
                        == ManagedMemorySoakConfig.Profile.FEASIBILITY
                        ? "MANAGED_MEMORY_FEASIBILITY_PASSED"
                        : "MANAGED_MEMORY_NATURAL_24H_PASSED";
            }
        }
        fields.put("qualification", qualification);
        ManagedMemorySoakManifest manifest =
                new ManagedMemorySoakManifest(fields);
        manifest.write(config.output.resolve(FILE_NAME));
        return manifest;
    }

    Map<String, String> fields() { return fields; }

    private void write(Path path) throws IOException {
        StringBuilder text = new StringBuilder();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            if (!field.getKey().matches("[a-z][a-z0-9_.-]{0,63}")
                    || field.getValue() == null
                    || field.getValue().contains("\n")
                    || field.getValue().contains("\r")) {
                throw invalid("manifest field is malformed");
            }
            text.append(field.getKey()).append('=')
                    .append(field.getValue()).append('\n');
        }
        try {
            Files.write(path, text.toString().getBytes(StandardCharsets.US_ASCII),
                    StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        } catch (java.nio.file.FileAlreadyExistsException exists) {
            throw invalid("manifest already exists", exists);
        }
    }

    private static void require(Map<String, String> record,
                                String name, String expected) {
        if (!expected.equals(record.get(name))) {
            throw invalid("record field is invalid: " + name);
        }
    }

    private static long number(Map<String, String> fields, String name) {
        String value = fields.get(name);
        if (value == null || !value.matches("[0-9]+")) {
            throw invalid("numeric field is invalid: " + name);
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException malformed) {
            throw invalid("numeric field is invalid: " + name, malformed);
        }
    }

    private static long divideCeiling(long numerator, long denominator) {
        return numerator / denominator
                + (numerator % denominator == 0L ? 0L : 1L);
    }

    private static IllegalStateException invalid(String detail) {
        return new IllegalStateException(
                "MANAGED_MEMORY_SOAK_MANIFEST_INVALID: " + detail);
    }

    private static IllegalStateException invalid(
            String detail, Throwable cause) {
        return new IllegalStateException(
                "MANAGED_MEMORY_SOAK_MANIFEST_INVALID: " + detail, cause);
    }
}
