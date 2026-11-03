package io.rowweave.mem.spill;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SpillWorkspaceActiveUsageProfileTest {

    @TempDir
    Path temp;

    @Test
    void snapshotsActiveJobsRunsAndExactFrameBytesAcrossLifecycle() throws Exception {
        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                temp.toRealPath().resolve("profile"), 8L << 20, 4L << 20,
                0, 0, 64, 32, 0);
        SpillWorkspaceManager manager = SpillWorkspaceManager.openForTests(
                config, () -> 100L, identities(
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "11111111111111111111111111111111",
                        "22222222222222222222222222222222"), ignored -> Long.MAX_VALUE);

        assertUsage(manager.usageSnapshot(), 0, 0, 0);
        SpillJobWorkspace first = manager.openJob();
        SpillJobWorkspace second = manager.openJob();
        BoundedReplayRun firstRun = first.newSpillStore().createRun();
        firstRun.append(new byte[10]);
        firstRun.finishWriting();
        BoundedReplayRun secondRun = second.newSpillStore().createRun();
        secondRun.append(new byte[20]);
        secondRun.finishWriting();

        assertUsage(manager.usageSnapshot(), 2, 2, 38);
        firstRun.dispose();
        assertUsage(manager.usageSnapshot(), 2, 1, 24);
        first.close();
        assertUsage(manager.usageSnapshot(), 1, 1, 24);
        second.close();
        assertUsage(manager.usageSnapshot(), 0, 0, 0);
        manager.close();
    }

    private static void assertUsage(SpillWorkspaceUsageSnapshot snapshot,
                                    int jobs, int runs, long bytes) {
        assertEquals(jobs, snapshot.activeJobs());
        assertEquals(runs, snapshot.activeRuns());
        assertEquals(bytes, snapshot.reservedBytes());
    }

    private static Supplier<String> identities(String... values) {
        Queue<String> queue = new ArrayDeque<>(Arrays.asList(values));
        return () -> queue.remove();
    }
}
