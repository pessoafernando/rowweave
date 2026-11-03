package io.rowweave.monitor;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * JobMonitorRegistry contract plus InMemoryJobMonitor, whose bounded history prevents OOM.
 * It captures job and stage lifecycles for the read-only monitoring snapshots. NOOP costs
 * nothing, and a full history evicts the oldest entry.
 */
class InMemoryJobMonitorTest {

    private static JobView.Builder job(String id, String name) {
        return JobView.builder(id, name);
    }

    private static StageView stage(int idx, String role, String state) {
        return StageView.builder(idx).role(role).state(state).build();
    }

    @Test
    void submitThenListedAsRunning() {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        m.onJobSubmit(job("j1", "myjob")
                .addStage(stage(0, "SOURCE", "RUNNING"))
                .addStage(stage(1, "SINK", "RUNNING"))
                .build());
        List<JobView> jobs = m.jobs();
        assertEquals(1, jobs.size());
        JobView j = m.job("j1");
        assertNotNull(j);
        assertEquals("myjob", j.name());
        assertEquals("RUNNING", j.state());
        assertEquals(2, j.stages().size());
    }

    @Test
    void stageUpdateReflectsInSnapshot() {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        m.onJobSubmit(job("j1", "j").addStage(stage(0, "SINK", "RUNNING")).build());
        m.onStageUpdate("j1", StageView.builder(0).role("SINK").state("FINISHED").outputRows(42).workerId("w0").build());
        JobView j = m.job("j1");
        StageView s = j.stages().get(0);
        assertEquals("FINISHED", s.state());
        assertEquals(42, s.outputRows());
        assertEquals("w0", s.workerId());
    }

    @Test
    void jobCompleteSetsStateAndEndTime() {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        m.onJobSubmit(job("j1", "j").build());
        m.onJobComplete("j1", "FINISHED", null);
        assertEquals("FINISHED", m.job("j1").state());
        assertTrue(m.job("j1").endTime() > 0, "a finished job records an endTime");

        m.onJobSubmit(job("j2", "j").build());
        m.onJobComplete("j2", "FAILED", "worker lost");
        assertEquals("FAILED", m.job("j2").state());
        assertEquals("worker lost", m.job("j2").error());
    }

    /** The no-OOM invariant: a bounded history evicts the oldest entry once full and never grows without limit. */
    @Test
    void boundedHistoryEvictsOldest() {
        InMemoryJobMonitor m = new InMemoryJobMonitor(3);
        for (int i = 0; i < 5; i++) {
            m.onJobSubmit(job("j" + i, "j").build());
            m.onJobComplete("j" + i, "FINISHED", null);
        }
        assertEquals(3, m.jobs().size(), "only the 3 most recent jobs are kept");
        assertNull(m.job("j0"), "the oldest job is evicted");
        assertNull(m.job("j1"), "the next-oldest job is evicted");
        assertNotNull(m.job("j4"), "the newest job is kept");
    }

    @Test
    void terminalTurnoverKeepsRunningJobsAndTheirUpdates() {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(2);
        monitor.onJobSubmit(job("continuous", "continuous").build());
        for (int i = 0; i < 5; i++) {
            String id = "short-" + i;
            monitor.onJobSubmit(job(id, "short").build());
            monitor.onJobComplete(id, "FINISHED", null);
        }
        monitor.onStageUpdate("continuous",
                stage(0, "SOURCE", "RUNNING"));
        assertNotNull(monitor.job("continuous"));
        assertEquals(1, monitor.job("continuous").stages().size());
        assertNotNull(monitor.job("short-4"));
        assertEquals(2, monitor.jobs().size());
    }

    @Test
    void runningObservationCapacityIsBoundedWithoutEvictingActiveJobs() {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(2);
        monitor.onJobSubmit(job("first", "first").build());
        monitor.onJobSubmit(job("second", "second").build());
        monitor.onJobSubmit(job("excess", "excess").build());
        assertNotNull(monitor.job("first"));
        assertNotNull(monitor.job("second"));
        assertNull(monitor.job("excess"));
        assertEquals(2, monitor.jobs().size());
    }

    @Test
    void workersSnapshot() {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        m.setWorkers(java.util.Arrays.asList(
                new WorkerView("w0", "127.0.0.1", 100, 4, true),
                new WorkerView("w1", "127.0.0.1", 101, 4, false)));
        assertEquals(2, m.workers().size());
        assertTrue(m.workers().stream().anyMatch(w -> w.workerId().equals("w0") && w.alive()));
    }

    @Test
    void noopIsZeroCostAndSafe() {
        JobMonitorRegistry noop = JobMonitorRegistry.NOOP;
        // every call is silent and records nothing
        noop.onJobSubmit(job("x", "x").build());
        noop.onStageUpdate("x", stage(0, "SINK", "RUNNING"));
        noop.onJobComplete("x", "FINISHED", null);
        assertTrue(noop.jobs().isEmpty(), "a NOOP records nothing");
        assertNull(noop.job("x"));
        assertTrue(noop.workers().isEmpty());
        noop.onRunRetention(new RunRetentionView(
                1, 4096, 128, 10L, 2L, 1L, 3L,
                false, java.util.Collections.singletonList("run:CORRUPT")));
        assertNull(noop.runRetention());
    }

    @Test
    void runRetentionSnapshotIsReplacedAtomically() {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(10);
        RunRetentionView first = new RunRetentionView(
                4, 4096, 128, 99L, 3L, 2L, 1L,
                false, java.util.Collections.singletonList("run:CORRUPT"));

        monitor.onRunRetention(first);

        RunRetentionView observed = monitor.runRetention();
        assertEquals(4, observed.backlog());
        assertEquals(4096, observed.lifecycleCapacityPerShard());
        assertEquals(99L, observed.oldestTerminalAgeMillis());
        assertEquals(3L, observed.claimedRuns());
        assertEquals(2L, observed.purgedRuns());
        assertEquals(1L, observed.errorRuns());
        assertEquals(java.util.Collections.singletonList("run:CORRUPT"),
                observed.diagnosticSamples());
    }

    @Test
    void artifactRetentionSnapshotIsReplacedAtomically() {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(2);
        ArtifactRetentionView view = new ArtifactRetentionView(
                3, 16, "SHARD", 17, 6, 4096L, 256, 1L << 30,
                99L, 7L, 5L, 2L, 1L, 4L,
                false, "READY", "BACKFILLING",
                java.util.Collections.singletonList("artifact:unavailable"));

        monitor.onArtifactRetention(view);

        assertSame(view, monitor.artifactRetention());
        assertEquals(3, monitor.artifactRetention().backlog());
        assertEquals("SHARD", monitor.artifactRetention().inventoryScope());
        assertEquals(17, monitor.artifactRetention().inventoryObservedShard());
        assertEquals(6, monitor.artifactRetention().inventoryEntries());
        assertEquals(4096L, monitor.artifactRetention().inventoryBytes());
        assertEquals(256, monitor.artifactRetention().inventoryEntryCapacity());
        assertEquals(1L << 30,
                monitor.artifactRetention().inventoryByteCapacity());
        assertEquals("READY", monitor.artifactRetention().referenceStatus());
        assertEquals("BACKFILLING", monitor.artifactRetention().inventoryStatus());
    }

    @Test
    void retryRecoverySnapshotIsBoundedAndReplacedAtomically() {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(2);
        RetryRecoveryView view = new RetryRecoveryView(
                "EMPTY_READY", 0, 3, "LE_5S",
                7L, 2L, 4L, "CONTINUING", 128,
                java.util.Collections.singletonList("run:RETRY_WAIT"));

        monitor.onRetryRecovery(view);

        assertSame(view, monitor.retryRecovery());
        assertEquals(3, monitor.retryRecovery().waitingRetries());
        assertEquals("LE_5S", monitor.retryRecovery().nextDueDelayBucket());
        assertEquals(7L, monitor.retryRecovery().scheduledRetries());
        assertEquals(2L, monitor.retryRecovery().exhaustedRetries());
        assertEquals(4L, monitor.retryRecovery().notRetryableClosures());
        assertEquals("CONTINUING", monitor.retryRecovery().maintenanceCursorHealth());
        assertEquals(128, monitor.retryRecovery().scanPageCapacity());
    }

    /** DAG JSON export: nodes from stages and edges from the inputStages dependencies. */
    @Test
    void dagJsonExport() {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        m.onJobSubmit(job("j1", "j")
                .addStage(StageView.builder(0).role("SOURCE").state("RUNNING").opChain("source,map").build())
                .addStage(StageView.builder(1).role("SINK").state("RUNNING").opChain("groupBy,sink").addInput(0).build())
                .dagJson("{\"nodes\":[{\"id\":0},{\"id\":1}],\"edges\":[{\"from\":0,\"to\":1}]}")
                .build());
        String dag = m.job("j1").dagJson();
        assertTrue(dag.contains("\"edges\""));
        assertTrue(dag.contains("\"from\":0"));
    }
}
