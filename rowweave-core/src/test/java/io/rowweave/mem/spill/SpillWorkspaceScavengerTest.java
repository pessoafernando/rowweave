package io.rowweave.mem.spill;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpillWorkspaceScavengerTest {

    private static final long MIB = 1024L * 1024;
    private static final String INSTANCE_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String INSTANCE_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    private static final String JOB_A = "11111111111111111111111111111111";

    @TempDir
    Path temp;

    @Test
    void initializationReclaimsDeadCanonicalInstanceAfterGrace() {
        Path base = base("dead");
        SpillWorkspaceManager crashed = open(base, 100, 0, 64, 32, INSTANCE_A);
        Path orphan = crashed.instanceDirectory();
        crashed.abandonForTests();

        SpillWorkspaceManager reaper = open(base, 200, 50, 64, 32, INSTANCE_B);

        assertFalse(Files.exists(orphan, LinkOption.NOFOLLOW_LINKS));
        assertTrue(reaper.lastScavengeReport().deletedEntries() >= 4);
        reaper.close();
    }

    @Test
    void liveExclusiveLockAndGraceBothPreventDeletion() {
        Path liveBase = base("live");
        SpillWorkspaceManager live = open(liveBase, 100, 0, 64, 32, INSTANCE_A);
        SpillWorkspaceManager observer = open(liveBase, 200, 0, 64, 32, INSTANCE_B);
        assertTrue(Files.exists(live.instanceDirectory()));
        assertTrue(observer.lastScavengeReport().liveInstances() >= 1);
        observer.close();
        live.close();

        Path youngBase = base("young");
        SpillWorkspaceManager young = open(youngBase, 100, 0, 64, 32, INSTANCE_A);
        Path orphan = young.instanceDirectory();
        young.abandonForTests();
        SpillWorkspaceManager tooSoon = open(youngBase, 150, 100, 64, 32, INSTANCE_B);
        assertTrue(Files.exists(orphan));
        assertTrue(tooSoon.lastScavengeReport().tooYoungInstances() >= 1);
        tooSoon.close();
    }

    @Test
    void corruptMarkerIsQuarantinedWithoutEchoingOrDeletingIt() throws Exception {
        Path base = base("corrupt");
        SpillWorkspaceManager crashed = open(base, 100, 0, 64, 32, INSTANCE_A);
        Path orphan = crashed.instanceDirectory();
        crashed.abandonForTests();
        byte[] hostile = "secret=/private/customer".getBytes("US-ASCII");
        Files.write(orphan.resolve(SpillWorkspaceManager.OWNER_MARKER), hostile);

        SpillWorkspaceManager reaper = open(base, 200, 0, 64, 32, INSTANCE_B);

        assertTrue(Files.exists(orphan));
        assertTrue(Arrays.equals(hostile,
                Files.readAllBytes(orphan.resolve(SpillWorkspaceManager.OWNER_MARKER))));
        assertTrue(reaper.lastScavengeReport().suspiciousInstances() >= 1);
        assertTrue(reaper.lastScavengeReport().samples().stream()
                .noneMatch(sample -> sample.contains("customer") || sample.contains("secret")));
        reaper.close();
    }

    @Test
    void canonicalIdentityMismatchAndFutureTimestampAreBothPreserved() throws Exception {
        Path mismatchBase = base("mismatch");
        SpillWorkspaceManager mismatch = open(mismatchBase, 100, 0, 64, 32, INSTANCE_A);
        Path mismatchedOrphan = mismatch.instanceDirectory();
        mismatch.abandonForTests();
        Files.write(mismatchedOrphan.resolve(SpillWorkspaceManager.OWNER_MARKER),
                SpillWorkspaceMarkerCodec.encode(SpillWorkspaceMarker.instance(INSTANCE_B, 100)));

        SpillWorkspaceManager mismatchReaper = open(mismatchBase, 200, 0, 64, 32,
                "cccccccccccccccccccccccccccccccc");
        assertTrue(Files.exists(mismatchedOrphan));
        assertTrue(mismatchReaper.lastScavengeReport().suspiciousInstances() >= 1);
        mismatchReaper.close();

        Path futureBase = base("future");
        SpillWorkspaceManager future = open(futureBase, 300, 0, 64, 32, INSTANCE_A);
        Path futureOrphan = future.instanceDirectory();
        future.abandonForTests();

        SpillWorkspaceManager futureReaper = open(futureBase, 200, 0, 64, 32, INSTANCE_B);
        assertTrue(Files.exists(futureOrphan));
        assertTrue(futureReaper.lastScavengeReport().suspiciousInstances() >= 1);
        futureReaper.close();
    }

    @Test
    void symlinkInsideCanonicalTreePreservesInstanceAndTarget() throws Exception {
        Path base = base("symlink");
        SpillWorkspaceManager crashed = open(base, 100, 0, 64, 32, INSTANCE_A);
        Path orphan = crashed.instanceDirectory();
        crashed.abandonForTests();
        Path target = base("outside-target");
        Files.createDirectories(target);
        Path sentinel = target.resolve("sentinel");
        Files.write(sentinel, new byte[]{9});
        try {
            Files.createSymbolicLink(orphan.resolve("jobs").resolve("job-" + JOB_A), target);
        } catch (UnsupportedOperationException | IOException unsupported) {
            return;
        }

        SpillWorkspaceManager reaper = open(base, 200, 0, 64, 32, INSTANCE_B);

        assertTrue(Files.exists(orphan));
        assertTrue(Files.exists(sentinel));
        assertTrue(reaper.lastScavengeReport().suspiciousInstances() >= 1);
        reaper.close();
    }

    @Test
    void deleteBudgetIsHardAndLargeOrphanMakesProgressAcrossPasses() {
        Path base = base("bounded");
        SpillWorkspaceManager crashed = open(base, 100, 0, 64, 3,
                INSTANCE_A, JOB_A);
        SpillJobWorkspace job = crashed.openJob();
        DiskSpillStore store = job.newSpillStore();
        for (int i = 0; i < 7; i++) {
            BoundedReplayRun run = store.createRun();
            run.append(new byte[10 + i]);
            run.finishWriting();
        }
        Path orphan = crashed.instanceDirectory();
        crashed.abandonForTests();

        SpillWorkspaceManager reaper = open(base, 200, 0, 64, 3, INSTANCE_B);
        int passes = 1;
        SpillScavengeReport report = reaper.lastScavengeReport();
        assertTrue(report.inspectedEntries() <= 64);
        assertTrue(report.deletedEntries() <= 3);
        while (Files.exists(orphan, LinkOption.NOFOLLOW_LINKS) && passes < 20) {
            report = reaper.scavengeNowForTests();
            assertTrue(report.inspectedEntries() <= 64);
            assertTrue(report.deletedEntries() <= 3);
            passes++;
        }

        assertFalse(Files.exists(orphan, LinkOption.NOFOLLOW_LINKS));
        assertTrue(passes > 1, "small delete budget must require multiple bounded passes");
        reaper.close();
    }

    @Test
    void scanCursorDoesNotLetLargeOrphanPermanentlyStarveSibling() {
        Path base = base("fair");
        SpillWorkspaceManager first = open(base, 100, 0, 8, 3,
                INSTANCE_A, JOB_A);
        SpillJobWorkspace job = first.openJob();
        DiskSpillStore store = job.newSpillStore();
        for (int i = 0; i < 12; i++) {
            BoundedReplayRun run = store.createRun();
            run.append(new byte[1]);
            run.finishWriting();
        }
        first.abandonForTests();

        SpillWorkspaceManager second = open(base, 101, 0, 8, 3, INSTANCE_B);
        Path small = second.instanceDirectory();
        second.abandonForTests();

        SpillWorkspaceManager reaper = open(base, 200, 0, 8, 3,
                "cccccccccccccccccccccccccccccccc");
        for (int i = 0; i < 12 && Files.exists(small); i++) {
            reaper.scavengeNowForTests();
        }

        assertFalse(Files.exists(small), "cursor must revisit a sibling while large orphan remains");
        reaper.close();
    }

    @Test
    void openJobRunsMaintenanceOnlyAfterConfiguredInterval() {
        Path base = base("interval");
        SpillWorkspaceManager crashed = open(base, 100, 0, 64, 32, INSTANCE_A);
        Path orphan = crashed.instanceDirectory();
        crashed.abandonForTests();

        AtomicLong now = new AtomicLong(120);
        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                base, 8 * MIB, 4 * MIB, 0, 50, 64, 32, 20);
        SpillWorkspaceManager reaper = SpillWorkspaceManager.openForTests(
                config, now::get, identities(INSTANCE_B, JOB_A), ignored -> Long.MAX_VALUE);
        assertTrue(Files.exists(orphan), "initial pass is still inside grace");

        now.set(200);
        SpillJobWorkspace job = reaper.openJob();

        assertFalse(Files.exists(orphan), "job admission must trigger due maintenance");
        job.close();
        reaper.close();
    }

    private SpillWorkspaceManager open(Path base, long now, long grace,
                                       int scanMax, int deleteMax, String... ids) {
        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                base, 8 * MIB, 4 * MIB, 0, grace,
                scanMax, deleteMax, 0);
        return SpillWorkspaceManager.openForTests(
                config, () -> now, identities(ids), ignored -> Long.MAX_VALUE);
    }

    private Path base(String name) {
        try {
            return temp.toRealPath().resolve(name);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static Supplier<String> identities(String... values) {
        Queue<String> queue = new ArrayDeque<>(Arrays.asList(values));
        return () -> {
            String value = queue.poll();
            if (value == null) throw new AssertionError("identity supplier exhausted");
            return value;
        };
    }
}
