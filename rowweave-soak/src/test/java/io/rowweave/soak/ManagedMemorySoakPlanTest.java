package io.rowweave.soak;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ManagedMemorySoakPlanTest {
    private static final String DIGEST =
            "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";

    @Test
    void createOncePlanBindsCandidateWorkloadAndResourceContract()
            throws Exception {
        Path root = Files.createTempDirectory("managed-memory-plan-");
        ManagedMemorySoakConfig config = ManagedMemorySoakConfig.parse(
                new String[] {
                        "--qualification-profile=FEASIBILITY",
                        "--candidate-manifest-sha256=" + DIGEST,
                        "--output=" + root
                });

        ManagedMemorySoakPlan plan = ManagedMemorySoakPlan.create(config);

        assertEquals("FEASIBILITY", plan.fields().get("profile"));
        assertEquals(DIGEST,
                plan.fields().get("candidate_manifest_sha256"));
        assertEquals("1200000", plan.fields().get("duration_millis"));
        assertEquals("2", plan.fields().get("concurrent_jobs"));
        assertEquals("2", plan.fields().get("worker_count"));
        assertEquals("8", plan.fields().get("worker_slots"));
        assertEquals("16777216",
                plan.fields().get("worker_memory_bytes"));
        assertEquals("1048576",
                plan.fields().get("operator_minimum_bytes"));
        assertEquals("4194304",
                plan.fields().get("operator_maximum_bytes"));
        assertEquals("4194304",
                plan.fields().get("max_tail_rss_slope_bytes_per_hour"));
        assertEquals("500",
                plan.fields().get("max_tail_fd_slope_milli_per_hour"));
        assertEquals("4194304",
                plan.fields().get("max_tail_disk_slope_bytes_per_hour"));
        assertEquals("33554432", plan.fields().get(
                "max_adjacent_quarter_rss_growth_bytes"));
        assertEquals("4", plan.fields().get(
                "max_adjacent_quarter_fd_growth"));
        assertEquals("33554432", plan.fields().get(
                "max_adjacent_quarter_disk_growth_bytes"));
        assertTrue(plan.sha256().matches("[0-9a-f]{64}"));
        assertEquals(plan.fields(), ManagedMemorySoakPlan.load(root).fields());
        assertEquals(plan.sha256(), ManagedMemorySoakPlan.load(root).sha256());

        String text = new String(Files.readAllBytes(root.resolve(
                ManagedMemorySoakPlan.FILE_NAME)), StandardCharsets.US_ASCII);
        assertTrue(text.startsWith("candidate_manifest_sha256="));
        assertThrows(IllegalStateException.class,
                () -> ManagedMemorySoakPlan.create(config));
    }

    @Test
    void loadRejectsTamperedOrIncompletePlan() throws Exception {
        Path root = Files.createTempDirectory("managed-memory-plan-bad-");
        Files.write(root.resolve(ManagedMemorySoakPlan.FILE_NAME),
                "profile=FEASIBILITY\n".getBytes(StandardCharsets.US_ASCII));

        IllegalStateException failure = assertThrows(
                IllegalStateException.class,
                () -> ManagedMemorySoakPlan.load(root));
        assertTrue(failure.getMessage().startsWith(
                "MANAGED_MEMORY_SOAK_PLAN_INVALID:"));
    }
}
