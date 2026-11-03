package io.rowweave.soak;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

/** Canonical, create-once contract frozen before a profiled qualification starts. */
final class StreamSoakPlan {
    static final String FILE_NAME = "qualification-plan.properties";

    private final Map<String, String> fields;
    private final byte[] bytes;
    private final Path path;

    private StreamSoakPlan(Map<String, String> source, Path path) {
        this.path = path;
        fields = Collections.unmodifiableMap(new TreeMap<>(source));
        StringBuilder content = new StringBuilder();
        for (Map.Entry<String, String> field : fields.entrySet()) {
            content.append(field.getKey()).append('=')
                    .append(field.getValue()).append('\n');
        }
        bytes = content.toString().getBytes(StandardCharsets.UTF_8);
    }

    static StreamSoakPlan freeze(StreamSoakConfig config, Path output)
            throws IOException {
        if (config.profile == StreamSoakConfig.Profile.LEGACY) {
            throw new IllegalArgumentException(
                    "legacy qualification has no explicit frozen plan");
        }
        Map<String, String> values = new LinkedHashMap<>();
        values.put("format", "STREAM_SOAK_QUALIFICATION_PLAN_V1");
        values.put("profile", config.profile.name());
        values.put("impact", config.qualificationImpact);
        values.put("candidate_manifest_sha256",
                config.candidateManifestSha256);
        put(values, "duration_millis", config.durationMillis);
        put(values, "sample_interval_millis", config.sampleIntervalMillis);
        put(values, "checkpoint_interval_millis",
                config.checkpointIntervalMillis);
        put(values, "s3_interval_millis", config.s3IntervalMillis);
        put(values, "s3_rows_per_job", config.s3Rows);
        put(values, "mutation_burst", config.mutationBurst);
        put(values, "mutation_pause_millis", config.mutationPauseMillis);
        put(values, "sink_delay_millis", config.sinkDelayMillis);
        put(values, "threshold_process_rss_bytes",
                config.maximumProcessRssBytes);
        put(values, "threshold_fd", config.maximumFileDescriptors);
        put(values, "threshold_disk_bytes", config.maximumDiskBytes);
        put(values, "threshold_checkpoint_millis",
                config.maximumCheckpointMillis);
        put(values, "threshold_recovery_millis",
                config.maximumRecoveryMillis);
        put(values, "threshold_backlog_events",
                config.maximumBacklogEvents);
        put(values, "threshold_state_bytes", config.maximumStateBytes);
        values.put("fault_offsets_millis", faultOffsets(config.durationMillis));
        if (config.profile == StreamSoakConfig.Profile.ACCELERATED_4H
                || config.profile
                == StreamSoakConfig.Profile.ACCELERATED_FEASIBILITY) {
            put(values, "zookeeper_snap_count",
                    StreamSoakConfig.ACCELERATED_ZOOKEEPER_SNAP_COUNT);
            put(values, "zookeeper_prealloc_kib",
                    StreamSoakConfig.ACCELERATED_ZOOKEEPER_PREALLOC_KIB);
            boolean feasibility = config.profile
                    == StreamSoakConfig.Profile.ACCELERATED_FEASIBILITY;
            put(values, "postgres_minimum_rows",
                    feasibility ? 343_305L
                            : StreamSoakConfig.ACCELERATED_POSTGRES_MINIMUM_ROWS);
            put(values, "s3_minimum_jobs",
                    feasibility ? 8L
                            : StreamSoakConfig.ACCELERATED_S3_MINIMUM_JOBS);
            put(values, "s3_minimum_rows",
                    feasibility ? 2_048L
                            : StreamSoakConfig.ACCELERATED_S3_MINIMUM_JOBS * 256L);
            put(values, "checkpoint_minimum_completed",
                    feasibility ? 240L
                            : StreamSoakConfig.ACCELERATED_CHECKPOINT_MINIMUM);
            put(values, "sample_minimum_count",
                    feasibility ? StreamSoakConfig.FEASIBILITY_SAMPLE_MINIMUM
                            : StreamSoakConfig.ACCELERATED_SAMPLE_MINIMUM);
            put(values, "purge_minimum_completed",
                    feasibility ? 2L
                            : StreamSoakConfig.ACCELERATED_PURGE_MINIMUM);
            put(values, "purge_first_offset_millis",
                    StreamSoakConfig.ACCELERATED_PURGE_FIRST_OFFSET_MILLIS);
            put(values, "purge_interval_millis",
                    StreamSoakConfig.ACCELERATED_PURGE_INTERVAL_MILLIS);
        } else {
            put(values, "purge_interval_millis", 3_600_000L);
        }
        StreamSoakPlan plan = new StreamSoakPlan(values,
                output.resolve(FILE_NAME));
        Files.write(plan.path, plan.bytes,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        return plan;
    }

    Map<String, String> fields() { return fields; }

    void verifyUnchanged() {
        try {
            if (!Arrays.equals(bytes, Files.readAllBytes(path))) {
                throw new IllegalStateException(
                        "STREAM_SOAK_QUALIFICATION_PLAN_CHANGED");
            }
        } catch (IOException invalid) {
            throw new IllegalStateException(
                    "STREAM_SOAK_QUALIFICATION_PLAN_UNREADABLE", invalid);
        }
    }

    String sha256() {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(bytes);
            StringBuilder hex = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                hex.append(Character.forDigit((value >>> 4) & 0xf, 16));
                hex.append(Character.forDigit(value & 0xf, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }

    private static String faultOffsets(long duration) {
        return (duration * 15L / 100L) + ","
                + (duration * 35L / 100L) + ","
                + (duration * 60L / 100L) + ","
                + (duration * 80L / 100L);
    }

    private static void put(Map<String, String> values,
            String key, long value) {
        values.put(key, Long.toString(value));
    }
}
