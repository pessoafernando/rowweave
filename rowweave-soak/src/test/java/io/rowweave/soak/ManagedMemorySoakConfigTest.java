package io.rowweave.soak;

import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagedMemorySoakConfigTest {
    private static final String DIGEST =
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    @Test
    void naturalProfileFreezesRelevantBatchWorkAndResourceBounds() {
        ManagedMemorySoakConfig config = ManagedMemorySoakConfig.parse(new String[] {
                "--qualification-profile=NATURAL_24H",
                "--candidate-manifest-sha256=" + DIGEST,
                "--output=target/managed-memory-natural"
        });

        assertEquals(ManagedMemorySoakConfig.Profile.NATURAL_24H,
                config.profile);
        assertEquals(86_400_000L, config.durationMillis);
        assertEquals(2, config.concurrentJobs);
        assertEquals(2, config.workerCount);
        assertTrue(config.rowsPerJob >= 100_000);
        assertTrue(config.keyCardinality >= 25_000);
        assertTrue(config.rowsPerJob >= config.keyCardinality * 4L);
        assertEquals(16L * 1024L * 1024L, config.workerMemoryBytes);
        assertEquals(1L * 1024L * 1024L,
                config.operatorMinimumBytes);
        assertEquals(4L * 1024L * 1024L,
                config.operatorMaximumBytes);
        assertEquals(4L * 1024L * 1024L,
                config.maximumTailRssSlopeBytesPerHour);
        assertEquals(500L,
                config.maximumTailFdSlopeMilliPerHour);
        assertEquals(4L * 1024L * 1024L,
                config.maximumTailDiskSlopeBytesPerHour);
        assertEquals(32L * 1024L * 1024L,
                config.maximumAdjacentQuarterRssGrowthBytes);
        assertEquals(4L,
                config.maximumAdjacentQuarterFdGrowth);
        assertEquals(32L * 1024L * 1024L,
                config.maximumAdjacentQuarterDiskGrowthBytes);
        assertEquals(Paths.get("target/managed-memory-natural")
                .toAbsolutePath().normalize(), config.output);
    }

    @Test
    void explicitProfilesRejectShortenedTimeOrWeakenedWork() {
        assertInvalid("--qualification-profile=NATURAL_24H",
                "--candidate-manifest-sha256=" + DIGEST,
                "--duration-seconds=86399");
        assertInvalid("--qualification-profile=FEASIBILITY",
                "--candidate-manifest-sha256=" + DIGEST,
                "--duration-seconds=1199");
        assertInvalid("--qualification-profile=FEASIBILITY",
                "--candidate-manifest-sha256=" + DIGEST,
                "--rows-per-job=99999");
        assertInvalid("--qualification-profile=FEASIBILITY",
                "--candidate-manifest-sha256=" + DIGEST,
                "--key-cardinality=24999");
        assertInvalid("--qualification-profile=FEASIBILITY",
                "--candidate-manifest-sha256=" + DIGEST,
                "--concurrent-jobs=1");
        assertInvalid("--qualification-profile=FEASIBILITY",
                "--candidate-manifest-sha256=" + DIGEST,
                "--worker-count=1");
    }

    @Test
    void explicitProfilesRejectRelaxedThresholdsAndMissingIdentity() {
        assertInvalid("--qualification-profile=NATURAL_24H");
        assertInvalid("--qualification-profile=NATURAL_24H",
                "--candidate-manifest-sha256=" + DIGEST,
                "--max-process-rss-bytes=1073741825");
        assertInvalid("--qualification-profile=NATURAL_24H",
                "--candidate-manifest-sha256=" + DIGEST,
                "--max-fd=1025");
        assertInvalid("--qualification-profile=NATURAL_24H",
                "--candidate-manifest-sha256=" + DIGEST,
                "--max-disk-bytes=2147483649");
        assertInvalid("--qualification-profile=NATURAL_24H",
                "--candidate-manifest-sha256=" + DIGEST,
                "--sample-interval-seconds=11");
        assertInvalid("--qualification-profile=NATURAL_24H",
                "--candidate-manifest-sha256=" + DIGEST,
                "--wave-interval-seconds=61");
    }

    @Test
    void developmentProfileAllowsAShortUnqualifiedHarnessRun() {
        ManagedMemorySoakConfig config = ManagedMemorySoakConfig.parse(
                new String[] {"--duration-seconds=5",
                        "--rows-per-job=1000", "--key-cardinality=100"});

        assertEquals(ManagedMemorySoakConfig.Profile.DEVELOPMENT,
                config.profile);
        assertEquals(5_000L, config.durationMillis);
        assertEquals(1_000, config.rowsPerJob);
        assertEquals(100, config.keyCardinality);
    }

    private static void assertInvalid(String... arguments) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> ManagedMemorySoakConfig.parse(arguments));
        assertTrue(failure.getMessage().startsWith(
                "MANAGED_MEMORY_SOAK_CONFIG_INVALID:"),
                failure.getMessage());
    }
}
