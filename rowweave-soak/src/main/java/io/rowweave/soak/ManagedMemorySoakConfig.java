package io.rowweave.soak;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.Map;

/** Strict, create-before-run contract for the Batch managed-memory qualification. */
final class ManagedMemorySoakConfig {
    enum Profile { DEVELOPMENT, FEASIBILITY, NATURAL_24H }

    static final long FEASIBILITY_DURATION_MILLIS = 1_200_000L;
    static final long NATURAL_DURATION_MILLIS = 86_400_000L;
    static final long DEFAULT_WORKER_MEMORY_BYTES = 16L * 1024L * 1024L;
    static final int QUALIFICATION_WORKER_SLOTS = 8;
    static final long DEFAULT_OPERATOR_MINIMUM_BYTES = 1L * 1024L * 1024L;
    static final long DEFAULT_OPERATOR_MAXIMUM_BYTES = 4L * 1024L * 1024L;
    static final long DEFAULT_MAXIMUM_PROCESS_RSS_BYTES = 1_073_741_824L;
    static final long DEFAULT_MAXIMUM_FILE_DESCRIPTORS = 1_024L;
    static final long DEFAULT_MAXIMUM_DISK_BYTES = 2_147_483_648L;
    static final long DEFAULT_MAXIMUM_TAIL_RSS_SLOPE_BYTES_PER_HOUR =
            4L * 1024L * 1024L;
    static final long DEFAULT_MAXIMUM_TAIL_FD_SLOPE_MILLI_PER_HOUR = 500L;
    static final long DEFAULT_MAXIMUM_TAIL_DISK_SLOPE_BYTES_PER_HOUR =
            4L * 1024L * 1024L;
    static final long DEFAULT_MAXIMUM_ADJACENT_QUARTER_RSS_GROWTH_BYTES =
            32L * 1024L * 1024L;
    static final long DEFAULT_MAXIMUM_ADJACENT_QUARTER_FD_GROWTH = 4L;
    static final long DEFAULT_MAXIMUM_ADJACENT_QUARTER_DISK_GROWTH_BYTES =
            32L * 1024L * 1024L;

    final Profile profile;
    final String candidateManifestSha256;
    final Path output;
    final long durationMillis;
    final long sampleIntervalMillis;
    final long waveIntervalMillis;
    final int rowsPerJob;
    final int keyCardinality;
    final int concurrentJobs;
    final int workerCount;
    final int workerSlots;
    final long workerMemoryBytes;
    final long operatorMinimumBytes;
    final long operatorMaximumBytes;
    final long maximumProcessRssBytes;
    final long maximumFileDescriptors;
    final long maximumDiskBytes;
    final long maximumTailRssSlopeBytesPerHour;
    final long maximumTailFdSlopeMilliPerHour;
    final long maximumTailDiskSlopeBytesPerHour;
    final long maximumAdjacentQuarterRssGrowthBytes;
    final long maximumAdjacentQuarterFdGrowth;
    final long maximumAdjacentQuarterDiskGrowthBytes;

    private ManagedMemorySoakConfig(Map<String, String> values) {
        profile = profile(values.get("qualification-profile"));
        candidateManifestSha256 = values.get("candidate-manifest-sha256");
        output = path(values, "output", "rowweave-soak/target/managed-memory-soak");
        durationMillis = seconds(values, "duration-seconds",
                profile == Profile.NATURAL_24H ? 86_400L
                        : profile == Profile.FEASIBILITY ? 1_200L : 60L,
                5L, 86_400L);
        sampleIntervalMillis = seconds(values, "sample-interval-seconds",
                profile == Profile.NATURAL_24H ? 10L
                        : profile == Profile.FEASIBILITY ? 2L : 1L,
                1L, 300L);
        waveIntervalMillis = seconds(values, "wave-interval-seconds",
                profile == Profile.NATURAL_24H ? 60L
                        : profile == Profile.FEASIBILITY ? 15L : 1L,
                1L, 600L);
        rowsPerJob = integer(values, "rows-per-job", 100_000,
                1, 10_000_000);
        keyCardinality = integer(values, "key-cardinality", 25_000,
                1, 2_000_000);
        concurrentJobs = integer(values, "concurrent-jobs", 2, 1, 16);
        workerCount = integer(values, "worker-count", 2, 1, 8);
        workerSlots = QUALIFICATION_WORKER_SLOTS;
        workerMemoryBytes = number(values, "worker-memory-bytes",
                DEFAULT_WORKER_MEMORY_BYTES, 4L * 1024L * 1024L,
                512L * 1024L * 1024L);
        operatorMinimumBytes = number(values, "operator-minimum-bytes",
                DEFAULT_OPERATOR_MINIMUM_BYTES, 1L, workerMemoryBytes);
        operatorMaximumBytes = number(values, "operator-maximum-bytes",
                DEFAULT_OPERATOR_MAXIMUM_BYTES, operatorMinimumBytes,
                workerMemoryBytes);
        maximumProcessRssBytes = number(values, "max-process-rss-bytes",
                DEFAULT_MAXIMUM_PROCESS_RSS_BYTES,
                64L * 1024L * 1024L, 16L * 1024L * 1024L * 1024L);
        maximumFileDescriptors = number(values, "max-fd",
                DEFAULT_MAXIMUM_FILE_DESCRIPTORS, 32L, 1_000_000L);
        maximumDiskBytes = number(values, "max-disk-bytes",
                DEFAULT_MAXIMUM_DISK_BYTES, 64L * 1024L * 1024L,
                Long.MAX_VALUE);
        maximumTailRssSlopeBytesPerHour =
                DEFAULT_MAXIMUM_TAIL_RSS_SLOPE_BYTES_PER_HOUR;
        maximumTailFdSlopeMilliPerHour =
                DEFAULT_MAXIMUM_TAIL_FD_SLOPE_MILLI_PER_HOUR;
        maximumTailDiskSlopeBytesPerHour =
                DEFAULT_MAXIMUM_TAIL_DISK_SLOPE_BYTES_PER_HOUR;
        maximumAdjacentQuarterRssGrowthBytes =
                DEFAULT_MAXIMUM_ADJACENT_QUARTER_RSS_GROWTH_BYTES;
        maximumAdjacentQuarterFdGrowth =
                DEFAULT_MAXIMUM_ADJACENT_QUARTER_FD_GROWTH;
        maximumAdjacentQuarterDiskGrowthBytes =
                DEFAULT_MAXIMUM_ADJACENT_QUARTER_DISK_GROWTH_BYTES;
        validate();
    }

    static ManagedMemorySoakConfig parse(String[] arguments) {
        if (arguments == null) throw invalid("arguments are required");
        Map<String, String> values = new LinkedHashMap<>();
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
        return new ManagedMemorySoakConfig(values);
    }

    private void validate() {
        if ((long) keyCardinality * 4L > rowsPerJob) {
            throw invalid("rows must cover each key at least four times");
        }
        if (operatorMinimumBytes > operatorMaximumBytes
                || operatorMaximumBytes > workerMemoryBytes) {
            throw invalid("operator memory must fit the worker pool");
        }
        if (profile == Profile.DEVELOPMENT) {
            if (candidateManifestSha256 != null) {
                throw invalid("development profile cannot claim candidate identity");
            }
            return;
        }
        if (candidateManifestSha256 == null
                || !candidateManifestSha256.matches("[0-9a-f]{64}")) {
            throw invalid("qualification profile requires candidate digest");
        }
        long expectedDuration = profile == Profile.FEASIBILITY
                ? FEASIBILITY_DURATION_MILLIS : NATURAL_DURATION_MILLIS;
        long maximumSample = profile == Profile.FEASIBILITY ? 2_000L : 10_000L;
        long maximumWave = profile == Profile.FEASIBILITY ? 15_000L : 60_000L;
        if (durationMillis != expectedDuration
                || sampleIntervalMillis > maximumSample
                || waveIntervalMillis > maximumWave
                || rowsPerJob < 100_000 || keyCardinality < 25_000
                || concurrentJobs < 2 || workerCount < 2) {
            throw invalid("profile lacks frozen Batch workload coverage");
        }
        if (workerMemoryBytes != DEFAULT_WORKER_MEMORY_BYTES
                || operatorMinimumBytes != DEFAULT_OPERATOR_MINIMUM_BYTES
                || operatorMaximumBytes != DEFAULT_OPERATOR_MAXIMUM_BYTES) {
            throw invalid("profile requires the certified memory pressure envelope");
        }
        if (maximumProcessRssBytes > DEFAULT_MAXIMUM_PROCESS_RSS_BYTES
                || maximumFileDescriptors > DEFAULT_MAXIMUM_FILE_DESCRIPTORS
                || maximumDiskBytes > DEFAULT_MAXIMUM_DISK_BYTES) {
            throw invalid("profile cannot relax resource thresholds");
        }
    }

    private static Profile profile(String value) {
        if (value == null) return Profile.DEVELOPMENT;
        try {
            Profile parsed = Profile.valueOf(value);
            if (parsed == Profile.DEVELOPMENT) {
                throw invalid("development profile is implicit");
            }
            return parsed;
        } catch (IllegalArgumentException malformed) {
            if (malformed.getMessage() != null
                    && malformed.getMessage().startsWith(
                    "MANAGED_MEMORY_SOAK_CONFIG_INVALID:")) throw malformed;
            throw invalid("unknown qualification profile");
        }
    }

    private static boolean known(String name) {
        return name.equals("qualification-profile")
                || name.equals("candidate-manifest-sha256")
                || name.equals("output") || name.equals("duration-seconds")
                || name.equals("sample-interval-seconds")
                || name.equals("wave-interval-seconds")
                || name.equals("rows-per-job")
                || name.equals("key-cardinality")
                || name.equals("concurrent-jobs")
                || name.equals("worker-count")
                || name.equals("worker-memory-bytes")
                || name.equals("operator-minimum-bytes")
                || name.equals("operator-maximum-bytes")
                || name.equals("max-process-rss-bytes")
                || name.equals("max-fd")
                || name.equals("max-disk-bytes");
    }

    private static Path path(Map<String, String> values, String key,
                             String fallback) {
        return Paths.get(values.containsKey(key) ? values.get(key) : fallback)
                .toAbsolutePath().normalize();
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
        long parsed = fallback;
        if (values.containsKey(key)) {
            String value = values.get(key);
            if (!value.matches("[1-9][0-9]*")) {
                throw invalid("numeric option is invalid: " + key);
            }
            try {
                parsed = Long.parseLong(value);
            } catch (NumberFormatException malformed) {
                throw invalid("numeric option is invalid: " + key);
            }
        }
        if (parsed < minimum || parsed > maximum) {
            throw invalid("numeric option is outside its bound: " + key);
        }
        return parsed;
    }

    private static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException(
                "MANAGED_MEMORY_SOAK_CONFIG_INVALID: " + detail);
    }
}
