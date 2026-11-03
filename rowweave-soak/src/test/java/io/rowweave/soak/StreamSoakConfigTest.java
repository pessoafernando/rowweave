package io.rowweave.soak;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamSoakConfigTest {
    @Test void defaultsToActualTwentyFourHourReleaseRun() {
        StreamSoakConfig config = StreamSoakConfig.parse(new String[0]);
        assertEquals(StreamSoakManifest.RELEASE_MINIMUM_MILLIS,
                config.durationMillis);
        assertEquals(2_048L, config.maximumFileDescriptors);
        assertEquals(10L, config.sinkDelayMillis);
    }

    @Test void externalToolBinaryResolvesExplicitThenPathScan(
            @TempDir Path root) throws Exception {
        Path explicit = root.resolve("bin/minio");
        Files.createDirectories(explicit.getParent());
        Files.createFile(explicit);
        explicit.toFile().setExecutable(true);

        StreamSoakConfig forced = StreamSoakConfig.parse(new String[] {
                "--minio-binary=" + explicit});
        assertEquals(explicit.toAbsolutePath().normalize(), forced.minioBinary);

        Path scanned = StreamSoakConfig.findOnPath("minio",
                explicit.getParent() + java.io.File.pathSeparator
                        + root.resolve("missing"));
        assertEquals(explicit.toAbsolutePath().normalize(), scanned);

        assertNull(StreamSoakConfig.findOnPath("minio",
                root.resolve("missing").toString()));
        assertNull(StreamSoakConfig.findOnPath("minio", null));
    }

    @Test void acceptsBoundedShortQualificationConfiguration() {
        StreamSoakConfig config = StreamSoakConfig.parse(new String[] {
                "--duration-seconds=120", "--sample-interval-seconds=2",
                "--s3-rows=17", "--sink-delay-millis=9"});
        assertEquals(120_000L, config.durationMillis);
        assertEquals(2_000L, config.sampleIntervalMillis);
        assertEquals(17, config.s3Rows);
        assertEquals(9L, config.sinkDelayMillis);
    }

    @Test void rejectsUnknownDuplicateAndTooShortRuns() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamSoakConfig.parse(new String[] {"--oops=1"}));
        assertThrows(IllegalArgumentException.class,
                () -> StreamSoakConfig.parse(new String[] {
                        "--duration-seconds=60", "--duration-seconds=61"}));
        assertThrows(IllegalArgumentException.class,
                () -> StreamSoakConfig.parse(new String[] {
                        "--duration-seconds=59"}));
    }

    @Test void rejectsMutationLoadsWithoutDeterministicDrainHeadroom() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamSoakConfig.parse(new String[] {
                        "--sink-delay-millis=20"}));
        assertThrows(IllegalArgumentException.class,
                () -> StreamSoakConfig.parse(new String[] {
                        "--mutation-burst=64",
                        "--mutation-pause-millis=2000",
                        "--sink-delay-millis=10"}));
    }

    @Test void oldFourHourDurationRemainsShortQualification() {
        StreamSoakConfig config = StreamSoakConfig.parse(new String[] {
                "--duration-seconds=14400"});
        assertEquals(StreamSoakConfig.Profile.LEGACY, config.profile);
    }

    @Test void explicitAcceleratedProfileFreezesWorkAndSchedule(
            @TempDir Path root) throws Exception {
        StreamSoakConfig config = StreamSoakConfig.parse(accelerated());
        assertEquals(StreamSoakConfig.Profile.ACCELERATED_4H,
                config.profile);
        StreamSoakPlan plan = StreamSoakPlan.freeze(config, root);
        assertEquals("ACCELERATED_4H", plan.fields().get("profile"));
        assertEquals("14400000", plan.fields().get("duration_millis"));
        assertEquals("4119649", plan.fields().get("postgres_minimum_rows"));
        assertEquals("96", plan.fields().get("s3_minimum_jobs"));
        assertEquals("24576", plan.fields().get("s3_minimum_rows"));
        assertEquals("2880", plan.fields().get("checkpoint_minimum_completed"));
        assertEquals("6960", plan.fields().get("sample_minimum_count"));
        assertEquals("24", plan.fields().get("purge_minimum_completed"));
        assertEquals("300000", plan.fields().get("purge_first_offset_millis"));
        assertEquals("600000", plan.fields().get("purge_interval_millis"));
        assertEquals("10240", plan.fields().get("zookeeper_snap_count"));
        assertEquals("8192", plan.fields().get("zookeeper_prealloc_kib"));
        assertEquals("2160000,5040000,8640000,11520000",
                plan.fields().get("fault_offsets_millis"));
        assertEquals(64, plan.sha256().length());
        assertTrue(Files.isRegularFile(root.resolve(
                "qualification-plan.properties")));
        assertThrows(java.nio.file.FileAlreadyExistsException.class,
                () -> StreamSoakPlan.freeze(config, root));
    }

    @Test void fasterSamplingPreservesOriginalAcceleratedCoverageFloor(
            @TempDir Path root) throws Exception {
        String[] options = accelerated();
        options[4] = "--sample-interval-seconds=1";

        StreamSoakPlan plan = StreamSoakPlan.freeze(
                StreamSoakConfig.parse(options), root);

        assertEquals("1000", plan.fields().get("sample_interval_millis"));
        assertEquals("6960", plan.fields().get("sample_minimum_count"));
    }

    @Test void acceleratedProfileRejectsMissingIdentityAndRelaxedBounds() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamSoakConfig.parse(new String[] {
                        "--qualification-profile=ACCELERATED_4H"}));
        for (String relaxed : new String[] {
                "--max-process-rss-bytes=1610612737",
                "--max-fd=2049", "--max-disk-bytes=8589934593",
                "--max-checkpoint-millis=60001",
                "--max-recovery-millis=60001",
                "--max-backlog-events=1000001",
                "--max-state-bytes=536870913"}) {
            String[] options = java.util.Arrays.copyOf(accelerated(),
                    accelerated().length + 1);
            options[options.length - 1] = relaxed;
            assertThrows(IllegalArgumentException.class,
                    () -> StreamSoakConfig.parse(options), relaxed);
        }
    }

    @Test void acceleratedProfileRejectsReducedCoverageAndHeadroom() {
        for (String replacement : new String[] {
                "--duration-seconds=14399",
                "--sample-interval-seconds=10",
                "--checkpoint-interval-seconds=5",
                "--s3-interval-seconds=900",
                "--s3-rows=128",
                "--mutation-burst=32",
                "--sink-delay-millis=2"}) {
            String[] options = accelerated();
            String key = replacement.substring(0,
                    replacement.indexOf('='));
            for (int i = 0; i < options.length; i++) {
                if (options[i].startsWith(key + "=")) options[i] = replacement;
            }
            assertThrows(IllegalArgumentException.class,
                    () -> StreamSoakConfig.parse(options), replacement);
        }
    }

    @Test void feasibilityProfileFreezesARealShortQualification(
            @TempDir Path root) throws Exception {
        String[] options = accelerated();
        options[0] = "--qualification-profile=ACCELERATED_FEASIBILITY";
        options[3] = "--duration-seconds=1200";
        StreamSoakConfig config = StreamSoakConfig.parse(options);
        assertEquals(StreamSoakConfig.Profile.ACCELERATED_FEASIBILITY,
                config.profile);
        StreamSoakPlan plan = StreamSoakPlan.freeze(config, root);
        assertEquals("2", plan.fields().get("purge_minimum_completed"));
        assertEquals("8", plan.fields().get("s3_minimum_jobs"));
        assertEquals("240", plan.fields().get(
                "checkpoint_minimum_completed"));
        assertEquals("343305", plan.fields().get(
                "postgres_minimum_rows"));
        options[3] = "--duration-seconds=600";
        assertThrows(IllegalArgumentException.class,
                () -> StreamSoakConfig.parse(options));
    }

    private static String[] accelerated() {
        return new String[] {
                "--qualification-profile=ACCELERATED_4H",
                "--qualification-impact=BEHAVIORAL_ACCEPTANCE",
                "--candidate-manifest-sha256="
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        + "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "--duration-seconds=14400",
                "--sample-interval-seconds=2",
                "--checkpoint-interval-seconds=4",
                "--s3-interval-seconds=150",
                "--s3-rows=256", "--mutation-burst=256",
                "--mutation-pause-millis=2000",
                "--sink-delay-millis=1"};
    }
}
