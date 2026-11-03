package io.rowweave.soak;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("multiprocess")
class ManagedMemorySoakMainTest {
    @Test
    void qualificationLoopKeepsRunningUntilDurationAndWaveFloorPass() {
        assertTrue(ManagedMemorySoakMain.requiresAnotherWave(
                1_200_001L, 79L, 1_200_000L, 80L));
        assertTrue(ManagedMemorySoakMain.requiresAnotherWave(
                1_199_999L, 80L, 1_200_000L, 80L));
        assertEquals(false, ManagedMemorySoakMain.requiresAnotherWave(
                1_200_000L, 80L, 1_200_000L, 80L));
    }

    @Test
    void developmentRunProducesAuditableJobsResourcesAndCleanup()
            throws Exception {
        Path root = Files.createTempDirectory("managed-memory-main-");

        ManagedMemorySoakManifest manifest = ManagedMemorySoakMain.run(
                new String[] {"--duration-seconds=5",
                        "--rows-per-job=10000",
                        "--key-cardinality=1000",
                        "--output=" + root});

        Map<String, String> fields = manifest.fields();
        assertEquals("DEVELOPMENT_COMPLETED",
                fields.get("qualification"));
        assertEquals("true", fields.get("real_batch_cluster"));
        assertEquals("2", fields.get("worker_processes"));
        assertEquals("2", fields.get("concurrent_jobs"));
        assertTrue(Long.parseLong(fields.get("completed_waves")) >= 5L,
                "the run must satisfy its duration-derived wave floor");
        assertEquals(Long.parseLong(fields.get("completed_waves")) * 2L,
                Long.parseLong(fields.get("completed_jobs")));
        assertEquals("0", fields.get("oracle_failures"));
        assertEquals("0", fields.get("budget_violations"));
        assertEquals("0", fields.get("resource_violations"));
        assertEquals("0", fields.get("workspace_leaks"));
        assertEquals("2", fields.get("active_worker_processes"));
        assertEquals("0", fields.get("worker_activity_violations"));
        assertTrue(Files.isRegularFile(root.resolve(
                ManagedMemorySoakManifest.FILE_NAME)));
        java.util.List<Map<String, String>> jobs =
                StreamSoakEvidenceLog.readVerified(
                        root.resolve("batch-jobs.log"));
        assertTrue(jobs.size() >= 2);
        assertEquals("101", jobs.get(0).get("source_seed"));
        assertTrue(jobs.get(0).get("oracle_sha256")
                .matches("[0-9a-f]{64}"));
        assertTrue(Long.parseLong(jobs.get(0).get(
                "submission_elapsed_millis"))
                <= Long.parseLong(jobs.get(0).get("elapsed_millis")));
        assertTrue(Long.parseLong(jobs.get(0).get(
                "submission_epoch_millis"))
                <= Long.parseLong(jobs.get(0).get(
                "observed_epoch_millis")));
        long latestSubmission = Math.max(Long.parseLong(jobs.get(0).get(
                        "submission_elapsed_millis")),
                Long.parseLong(jobs.get(1).get(
                        "submission_elapsed_millis")));
        long earliestCompletion = Math.min(Long.parseLong(jobs.get(0).get(
                        "elapsed_millis")),
                Long.parseLong(jobs.get(1).get("elapsed_millis")));
        assertTrue(latestSubmission <= earliestCompletion);
        assertTrue(StreamSoakEvidenceLog.readVerified(
                root.resolve("resource-samples.log")).size() >= 3);
        java.util.List<Map<String, String>> activities =
                StreamSoakEvidenceLog.readVerified(
                        root.resolve("role-activity.log"));
        assertEquals(3, activities.size());
        for (Map<String, String> activity : activities) {
            if (!activity.get("role").startsWith("worker-")) continue;
            assertEquals(fields.get("completed_waves"),
                    activity.get("connections"));
            assertEquals(fields.get("completed_waves"),
                    activity.get("concurrent_mux_groups"));
        }
    }

    @Test
    void rejectsNonEmptyOutputBeforeLaunchingProcesses() throws Exception {
        Path root = Files.createTempDirectory("managed-memory-stale-");
        Files.write(root.resolve("batch-jobs.log"),
                java.util.Collections.singletonList("stale"));

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> ManagedMemorySoakMain.run(new String[] {
                        "--duration-seconds=5",
                        "--rows-per-job=10000",
                        "--key-cardinality=1000",
                        "--output=" + root}));

        assertTrue(failure.getMessage().contains(
                "MANAGED_MEMORY_SOAK_OUTPUT_NOT_EMPTY"));
        try (java.util.stream.Stream<Path> entries = Files.list(root)) {
            assertEquals(1L, entries.count());
        }
    }
}
