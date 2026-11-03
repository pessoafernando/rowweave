package io.rowweave.soak;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/** Strict command-line contract for the explicit STREAM SOAK qualification run. */
final class StreamSoakConfig {
    enum Profile { LEGACY, ACCELERATED_FEASIBILITY,
        ACCELERATED_4H, NATURAL_24H }

    static final long ACCELERATED_DURATION_MILLIS = 14_400_000L;
    static final long FEASIBILITY_DURATION_MILLIS = 1_200_000L;
    static final long ACCELERATED_POSTGRES_MINIMUM_ROWS = 4_119_649L;
    static final int ACCELERATED_S3_MINIMUM_JOBS = 96;
    static final int ACCELERATED_CHECKPOINT_MINIMUM = 2_880;
    static final int ACCELERATED_SAMPLE_MINIMUM = 6_960;
    static final int FEASIBILITY_SAMPLE_MINIMUM = 500;
    static final int ACCELERATED_PURGE_MINIMUM = 24;
    static final long ACCELERATED_PURGE_FIRST_OFFSET_MILLIS = 300_000L;
    static final long ACCELERATED_PURGE_INTERVAL_MILLIS = 600_000L;
    static final int ACCELERATED_ZOOKEEPER_SNAP_COUNT = 10_240;
    static final int ACCELERATED_ZOOKEEPER_PREALLOC_KIB = 8_192;

    final Profile profile;
    final String qualificationImpact;
    final String candidateManifestSha256;
    final long durationMillis;
    final Path output;
    /** Resolved from --minio-binary, then ROWWEAVE_MINIO_BINARY, then PATH. */
    final Path minioBinary;
    /** Resolved from --postgres-bin, then ROWWEAVE_POSTGRES_BIN, then PATH. */
    final Path postgresBin;
    final Path s3Deployment;
    final Path postgresDeployment;
    final long sampleIntervalMillis;
    final long checkpointIntervalMillis;
    final long s3IntervalMillis;
    final int s3Rows;
    final int mutationBurst;
    final long mutationPauseMillis;
    final long sinkDelayMillis;
    final long maximumProcessRssBytes;
    final long maximumFileDescriptors;
    final long maximumDiskBytes;
    final long maximumCheckpointMillis;
    final long maximumRecoveryMillis;
    final long maximumBacklogEvents;
    final long maximumStateBytes;

    private StreamSoakConfig(Map<String, String> values) {
        profile = parseProfile(values.get("qualification-profile"));
        qualificationImpact = values.get("qualification-impact");
        candidateManifestSha256 = values.get("candidate-manifest-sha256");
        durationMillis = seconds(values, "duration-seconds", 86_400L,
                60L, 7L * 86_400L);
        output = path(values, "output", "rowweave-soak/target/stream-soak");
        minioBinary = toolPath(values, "minio-binary",
                "ROWWEAVE_MINIO_BINARY", "minio");
        postgresBin = toolPath(values, "postgres-bin",
                "ROWWEAVE_POSTGRES_BIN", null);
        s3Deployment = path(values, "s3-deployment",
                "rowweave-connector-s3/target/rowweave-connector-s3-0.1.0-deployment.jar");
        postgresDeployment = path(values, "postgres-deployment",
                "rowweave-connector-postgres-cdc/target/rowweave-connector-postgres-cdc-0.1.0-deployment.jar");
        sampleIntervalMillis = seconds(values, "sample-interval-seconds",
                10L, 1L, 300L);
        checkpointIntervalMillis = seconds(values,
                "checkpoint-interval-seconds", 30L, 2L, 600L);
        s3IntervalMillis = seconds(values, "s3-interval-seconds",
                900L, 10L, 3_600L);
        s3Rows = integer(values, "s3-rows", 256, 1, 100_000);
        mutationBurst = integer(values, "mutation-burst", 32, 1, 1_024);
        mutationPauseMillis = number(values, "mutation-pause-millis",
                2_000L, 100L, 600_000L);
        sinkDelayMillis = number(values, "sink-delay-millis",
                10L, 0L, 10_000L);
        requireDrainHeadroom();
        maximumProcessRssBytes = number(values, "max-process-rss-bytes",
                1_610_612_736L, 64L * 1024L * 1024L,
                64L * 1024L * 1024L * 1024L);
        maximumFileDescriptors = number(values, "max-fd", 2_048L,
                32L, 1_000_000L);
        maximumDiskBytes = number(values, "max-disk-bytes",
                8L * 1024L * 1024L * 1024L,
                64L * 1024L * 1024L,
                Long.MAX_VALUE);
        maximumCheckpointMillis = number(values, "max-checkpoint-millis",
                60_000L, 1_000L, 3_600_000L);
        maximumRecoveryMillis = number(values, "max-recovery-millis",
                60_000L, 1_000L, 3_600_000L);
        maximumBacklogEvents = number(values, "max-backlog-events",
                1_000_000L, 1L, Integer.MAX_VALUE);
        maximumStateBytes = number(values, "max-state-bytes",
                536_870_912L, 1L, Long.MAX_VALUE);
        requireExplicitProfileContract();
    }

    private static Profile parseProfile(String value) {
        if (value == null) return Profile.LEGACY;
        try { return Profile.valueOf(value); }
        catch (IllegalArgumentException invalid) {
            throw invalid("unknown qualification profile");
        }
    }

    private void requireExplicitProfileContract() {
        if (profile == Profile.LEGACY) {
            if (qualificationImpact != null || candidateManifestSha256 != null)
                throw invalid("explicit profile is required for qualification identity");
            return;
        }
        if (qualificationImpact == null
                || !qualificationImpact.matches("[A-Z0-9_]{1,64}")
                || candidateManifestSha256 == null
                || !candidateManifestSha256.matches("[0-9a-f]{64}")) {
            throw invalid("profile requires frozen impact and candidate digest");
        }
        if (maximumProcessRssBytes > 1_610_612_736L
                || maximumFileDescriptors > 2_048L
                || maximumDiskBytes > 8_589_934_592L
                || maximumCheckpointMillis > 60_000L
                || maximumRecoveryMillis > 60_000L
                || maximumBacklogEvents > 1_000_000L
                || maximumStateBytes > 536_870_912L) {
            throw invalid("profile cannot relax default resource bounds");
        }
        if (profile == Profile.NATURAL_24H) {
            if (durationMillis < StreamSoakManifest.RELEASE_MINIMUM_MILLIS)
                throw invalid("natural profile requires 24 hours");
            return;
        }
        long projectedPostgresRows = Math.multiplyExact(
                Math.multiplyExact((long) mutationBurst,
                        StreamSoakPostgres.CHANGES_PER_MUTATION),
                durationMillis) / mutationPauseMillis;
        long requiredDuration = profile == Profile.ACCELERATED_FEASIBILITY
                ? FEASIBILITY_DURATION_MILLIS
                : ACCELERATED_DURATION_MILLIS;
        long minimumPostgresRows = profile
                == Profile.ACCELERATED_FEASIBILITY ? 343_305L
                : ACCELERATED_POSTGRES_MINIMUM_ROWS;
        if (durationMillis != requiredDuration
                || sampleIntervalMillis > 2_000L
                || checkpointIntervalMillis > 4_000L
                || s3IntervalMillis > 150_000L
                || s3Rows < 256
                || projectedPostgresRows < minimumPostgresRows
                || sinkDelayMillis < 1L) {
            throw invalid("accelerated profile lacks frozen coverage");
        }
    }

    private void requireDrainHeadroom() {
        long sinkMillisPerBurst = Math.multiplyExact(
                Math.multiplyExact((long) mutationBurst,
                        StreamSoakPostgres.CHANGES_PER_MUTATION),
                sinkDelayMillis);
        long maximumSinkBudgetMillis = Math.multiplyExact(
                mutationPauseMillis, 75L) / 100L;
        if (sinkMillisPerBurst > maximumSinkBudgetMillis) {
            throw invalid("mutation load must reserve at least 25% "
                    + "deterministic sink drain headroom");
        }
    }

    static StreamSoakConfig parse(String[] arguments) {
        Map<String, String> values = new LinkedHashMap<>();
        if (arguments == null) throw invalid("arguments are required");
        for (String argument : arguments) {
            if (argument == null || !argument.startsWith("--")) {
                throw invalid("arguments must use --name=value");
            }
            int separator = argument.indexOf('=');
            if (separator < 3 || separator == argument.length() - 1) {
                throw invalid("arguments must use --name=value");
            }
            String name = argument.substring(2, separator);
            if (!known(name) || values.put(name,
                    argument.substring(separator + 1)) != null) {
                throw invalid("unknown or duplicate option: " + name);
            }
        }
        return new StreamSoakConfig(values);
    }

    private static boolean known(String name) {
        return name.equals("duration-seconds") || name.equals("output")
                || name.equals("qualification-profile")
                || name.equals("qualification-impact")
                || name.equals("candidate-manifest-sha256")
                || name.equals("minio-binary")
                || name.equals("postgres-bin")
                || name.equals("s3-deployment")
                || name.equals("postgres-deployment")
                || name.equals("sample-interval-seconds")
                || name.equals("checkpoint-interval-seconds")
                || name.equals("s3-interval-seconds")
                || name.equals("s3-rows")
                || name.equals("mutation-burst")
                || name.equals("mutation-pause-millis")
                || name.equals("sink-delay-millis")
                || name.equals("max-process-rss-bytes")
                || name.equals("max-fd") || name.equals("max-disk-bytes")
                || name.equals("max-checkpoint-millis")
                || name.equals("max-recovery-millis")
                || name.equals("max-backlog-events")
                || name.equals("max-state-bytes");
    }

    /**
     * Locates one external tool binary. An explicit {@code --key=value} wins; then
     * the named environment variable; then a PATH scan for {@code exeName}. Returns
     * null when nothing is found, which the start-up check reports as an invalid
     * binary path rather than silently guessing a host-specific location.
     */
    private static Path toolPath(Map<String, String> values, String key,
            String envName, String exeName) {
        String explicit = values.get(key);
        if (explicit != null && !explicit.isEmpty()) {
            return Paths.get(explicit).toAbsolutePath().normalize();
        }
        String fromEnv = envName == null ? null : System.getenv(envName);
        if (fromEnv != null && !fromEnv.isEmpty()) {
            return Paths.get(fromEnv).toAbsolutePath().normalize();
        }
        return exeName == null ? null : findOnPath(exeName, System.getenv("PATH"));
    }

    static Path findOnPath(String exeName, String pathEnv) {
        if (exeName == null || pathEnv == null || pathEnv.isEmpty()) {
            return null;
        }
        for (String dir : pathEnv.split(java.io.File.pathSeparator)) {
            if (dir.isEmpty()) {
                continue;
            }
            Path candidate = Paths.get(dir).resolve(exeName);
            if (Files.isExecutable(candidate)) {
                return candidate.toAbsolutePath().normalize();
            }
        }
        return null;
    }

    private static Path path(Map<String, String> values, String key,
            String fallback) {
        String value = values.containsKey(key) ? values.get(key) : fallback;
        return Paths.get(value).toAbsolutePath().normalize();
    }

    private static long seconds(Map<String, String> values, String key,
            long fallback, long minimum, long maximum) {
        return Math.multiplyExact(number(values, key, fallback,
                minimum, maximum), 1_000L);
    }

    private static int integer(Map<String, String> values, String key,
            int fallback, int minimum, int maximum) {
        return (int) number(values, key, fallback, minimum, maximum);
    }

    private static long number(Map<String, String> values, String key,
            long fallback, long minimum, long maximum) {
        long value = fallback;
        if (values.containsKey(key)) {
            try { value = Long.parseLong(values.get(key)); }
            catch (NumberFormatException malformed) {
                throw invalid("numeric option is invalid: " + key);
            }
        }
        if (value < minimum || value > maximum) {
            throw invalid("numeric option is outside its bound: " + key);
        }
        return value;
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("STREAM_SOAK_SOAK_CONFIG_INVALID: "
                + detail);
    }
}
