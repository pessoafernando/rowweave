package io.rowweave.soak;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Immutable canonical qualification plan created before any role process starts. */
final class ManagedMemorySoakPlan {
    static final String FILE_NAME = "managed-memory-qualification-plan.properties";

    private static final String[] REQUIRED = {
            "candidate_manifest_sha256", "concurrent_jobs",
            "duration_millis", "key_cardinality", "max_disk_bytes",
            "max_fd", "max_process_rss_bytes",
            "max_tail_rss_slope_bytes_per_hour",
            "max_tail_fd_slope_milli_per_hour",
            "max_tail_disk_slope_bytes_per_hour",
            "max_adjacent_quarter_rss_growth_bytes",
            "max_adjacent_quarter_fd_growth",
            "max_adjacent_quarter_disk_growth_bytes",
            "operator_maximum_bytes",
            "operator_minimum_bytes", "profile", "rows_per_job",
            "sample_interval_millis", "wave_interval_millis",
            "worker_count", "worker_memory_bytes", "worker_slots"
    };

    private final Map<String, String> fields;
    private final String sha256;
    private final Path path;

    private ManagedMemorySoakPlan(Map<String, String> fields, String sha256,
                                 Path path) {
        this.fields = Collections.unmodifiableMap(
                new LinkedHashMap<>(fields));
        this.sha256 = sha256;
        this.path = path;
    }

    static ManagedMemorySoakPlan create(ManagedMemorySoakConfig config)
            throws IOException {
        if (config == null || config.profile
                == ManagedMemorySoakConfig.Profile.DEVELOPMENT) {
            throw invalid("qualified plan requires an explicit profile");
        }
        Files.createDirectories(config.output);
        Path path = config.output.resolve(FILE_NAME);
        Map<String, String> fields = fields(config);
        byte[] bytes = canonical(fields).getBytes(StandardCharsets.US_ASCII);
        try {
            Files.write(path, bytes, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.WRITE);
        } catch (java.nio.file.FileAlreadyExistsException exists) {
            throw new IllegalStateException(
                    "MANAGED_MEMORY_SOAK_PLAN_EXISTS: " + path, exists);
        }
        return new ManagedMemorySoakPlan(fields,
                StreamSoakEvidenceLog.fileSha256(path), path);
    }

    static ManagedMemorySoakPlan load(Path root) throws IOException {
        if (root == null) throw invalid("root is required");
        Path path = root.resolve(FILE_NAME);
        Map<String, String> fields = parse(Files.readAllLines(
                path, StandardCharsets.US_ASCII));
        validate(fields);
        return new ManagedMemorySoakPlan(fields,
                StreamSoakEvidenceLog.fileSha256(path), path);
    }

    Map<String, String> fields() { return fields; }
    String sha256() { return sha256; }

    void verifyUnchanged() throws IOException {
        if (!sha256.equals(StreamSoakEvidenceLog.fileSha256(path))) {
            throw invalid("plan changed after the qualification started");
        }
        ManagedMemorySoakPlan current = load(path.getParent());
        if (!fields.equals(current.fields)) {
            throw invalid("plan fields changed after the qualification started");
        }
    }

    private static Map<String, String> fields(ManagedMemorySoakConfig config) {
        Map<String, String> values = new TreeMap<>();
        values.put("candidate_manifest_sha256",
                config.candidateManifestSha256);
        values.put("profile", config.profile.name());
        values.put("duration_millis", Long.toString(config.durationMillis));
        values.put("sample_interval_millis",
                Long.toString(config.sampleIntervalMillis));
        values.put("wave_interval_millis",
                Long.toString(config.waveIntervalMillis));
        values.put("rows_per_job", Integer.toString(config.rowsPerJob));
        values.put("key_cardinality",
                Integer.toString(config.keyCardinality));
        values.put("concurrent_jobs",
                Integer.toString(config.concurrentJobs));
        values.put("worker_count", Integer.toString(config.workerCount));
        values.put("worker_slots", Integer.toString(config.workerSlots));
        values.put("worker_memory_bytes",
                Long.toString(config.workerMemoryBytes));
        values.put("operator_minimum_bytes",
                Long.toString(config.operatorMinimumBytes));
        values.put("operator_maximum_bytes",
                Long.toString(config.operatorMaximumBytes));
        values.put("max_process_rss_bytes",
                Long.toString(config.maximumProcessRssBytes));
        values.put("max_fd",
                Long.toString(config.maximumFileDescriptors));
        values.put("max_disk_bytes",
                Long.toString(config.maximumDiskBytes));
        values.put("max_tail_rss_slope_bytes_per_hour", Long.toString(
                config.maximumTailRssSlopeBytesPerHour));
        values.put("max_tail_fd_slope_milli_per_hour", Long.toString(
                config.maximumTailFdSlopeMilliPerHour));
        values.put("max_tail_disk_slope_bytes_per_hour", Long.toString(
                config.maximumTailDiskSlopeBytesPerHour));
        values.put("max_adjacent_quarter_rss_growth_bytes", Long.toString(
                config.maximumAdjacentQuarterRssGrowthBytes));
        values.put("max_adjacent_quarter_fd_growth", Long.toString(
                config.maximumAdjacentQuarterFdGrowth));
        values.put("max_adjacent_quarter_disk_growth_bytes", Long.toString(
                config.maximumAdjacentQuarterDiskGrowthBytes));
        return values;
    }

    private static String canonical(Map<String, String> fields) {
        StringBuilder text = new StringBuilder();
        for (Map.Entry<String, String> entry
                : new TreeMap<>(fields).entrySet()) {
            text.append(entry.getKey()).append('=')
                    .append(entry.getValue()).append('\n');
        }
        return text.toString();
    }

    private static Map<String, String> parse(java.util.List<String> lines) {
        Map<String, String> fields = new TreeMap<>();
        for (String line : lines) {
            int separator = line.indexOf('=');
            if (separator < 1 || separator == line.length() - 1
                    || fields.put(line.substring(0, separator),
                    line.substring(separator + 1)) != null) {
                throw invalid("malformed or duplicate field");
            }
        }
        return fields;
    }

    private static void validate(Map<String, String> fields) {
        if (fields.size() != REQUIRED.length) {
            throw invalid("field set is incomplete");
        }
        for (String required : REQUIRED) {
            if (!fields.containsKey(required)) {
                throw invalid("missing field: " + required);
            }
        }
        if (!fields.get("candidate_manifest_sha256")
                .matches("[0-9a-f]{64}")) {
            throw invalid("candidate digest is malformed");
        }
        String profile = fields.get("profile");
        if (!"FEASIBILITY".equals(profile)
                && !"NATURAL_24H".equals(profile)) {
            throw invalid("profile is malformed");
        }
        for (Map.Entry<String, String> field : fields.entrySet()) {
            if (!"profile".equals(field.getKey())
                    && !"candidate_manifest_sha256".equals(field.getKey())
                    && !field.getValue().matches("[1-9][0-9]*")) {
                throw invalid("numeric field is malformed: "
                        + field.getKey());
            }
        }
    }

    private static IllegalStateException invalid(String detail) {
        return new IllegalStateException(
                "MANAGED_MEMORY_SOAK_PLAN_INVALID: " + detail);
    }
}
