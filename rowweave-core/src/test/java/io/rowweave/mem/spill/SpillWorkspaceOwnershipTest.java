package io.rowweave.mem.spill;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Queue;
import java.util.Set;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpillWorkspaceOwnershipTest {

    private static final String INSTANCE_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String INSTANCE_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    private static final String JOB_A = "11111111111111111111111111111111";
    private static final String JOB_B = "22222222222222222222222222222222";

    @TempDir
    Path temp;

    @Test
    void publishesCanonicalBaseOwnerAndJobOnlyAfterHoldingLease() throws Exception {
        Path base = realTemp().resolve("spill");
        SpillWorkspaceConfig config = config(base);
        SpillWorkspaceManager manager = SpillWorkspaceManager.openForTests(
                config, () -> 123_456L, identities(INSTANCE_A, JOB_A));
        Path instance = manager.instanceDirectory();

        assertEquals(base, manager.baseDirectory());
        assertEquals(base.resolve("instances").resolve("instance-" + INSTANCE_A), instance);
        assertEquals(SpillWorkspaceMarker.base(), readMarker(base.resolve(".rowweave-spill-base-v1")));
        assertEquals(SpillWorkspaceMarker.instance(INSTANCE_A, 123_456L),
                readMarker(instance.resolve(".rowweave-spill-owner-v1")));
        assertTrue(Files.isRegularFile(instance.resolve("lease.lock"), LinkOption.NOFOLLOW_LINKS));
        assertOwnerOnlyWhenPosix(instance);

        try (FileChannel contender = FileChannel.open(instance.resolve("lease.lock"),
                StandardOpenOption.WRITE)) {
            assertThrows(OverlappingFileLockException.class, contender::tryLock,
                    "manager must hold the exclusive lease for its full lifetime");
        }

        SpillJobWorkspace job = manager.openJob();
        Path jobDirectory = job.directory();
        assertEquals(instance.resolve("jobs").resolve("job-" + JOB_A), jobDirectory);
        assertEquals(SpillWorkspaceMarker.job(JOB_A, INSTANCE_A),
                readMarker(jobDirectory.resolve(".rowweave-spill-job-v1")));

        job.close();
        assertFalse(Files.exists(jobDirectory, LinkOption.NOFOLLOW_LINKS));
        manager.close();
        assertFalse(Files.exists(instance, LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.isRegularFile(base.resolve(".rowweave-spill-base-v1"),
                LinkOption.NOFOLLOW_LINKS), "stable base marker remains for later processes");
    }

    @Test
    void twoManagersShareBaseButOwnDistinctLockedInstances() {
        Path base = realTemp().resolve("shared");
        SpillWorkspaceManager first = SpillWorkspaceManager.openForTests(
                config(base), () -> 10L, identities(INSTANCE_A));
        SpillWorkspaceManager second = SpillWorkspaceManager.openForTests(
                config(base), () -> 11L, identities(INSTANCE_B));
        try {
            assertNotEquals(first.instanceDirectory(), second.instanceDirectory());
            assertTrue(Files.exists(first.instanceDirectory()));
            assertTrue(Files.exists(second.instanceDirectory()));
            assertEquals(SpillWorkspaceMarker.instance(INSTANCE_A, 10L),
                    readMarker(first.instanceDirectory().resolve(".rowweave-spill-owner-v1")));
            assertEquals(SpillWorkspaceMarker.instance(INSTANCE_B, 11L),
                    readMarker(second.instanceDirectory().resolve(".rowweave-spill-owner-v1")));
        } finally {
            first.close();
            second.close();
        }
    }

    @Test
    void corruptBaseMarkerFailsClosedBeforeCreatingAnInstance() throws Exception {
        Path base = realTemp().resolve("corrupt");
        Files.createDirectories(base);
        Path marker = base.resolve(".rowweave-spill-base-v1");
        byte[] hostile = "not-an-rowweave-marker:/private/customer".getBytes("US-ASCII");
        Files.write(marker, hostile);

        SpillDiskException failure = assertThrows(SpillDiskException.class,
                () -> SpillWorkspaceManager.openForTests(
                        config(base), () -> 1L, identities(INSTANCE_A)));

        assertEquals(SpillFailureCode.SPILL_WORKSPACE_UNAVAILABLE, failure.code());
        assertTrue(!failure.getMessage().contains("customer"));
        assertTrue(Arrays.equals(hostile, Files.readAllBytes(marker)), "bad marker is preserved for diagnosis");
        assertFalse(Files.exists(base.resolve("instances")),
                "no instance side effect after base ownership cannot be proven");
    }

    @Test
    void symlinkInConfiguredBaseChainFailsClosedAndLeavesTargetUntouched() throws Exception {
        Path target = realTemp().resolve("target");
        Files.createDirectories(target);
        Path sentinel = target.resolve("sentinel");
        Files.write(sentinel, new byte[]{7});
        Path link = realTemp().resolve("linked-base");
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | IOException unsupported) {
            return;
        }

        SpillDiskException failure = assertThrows(SpillDiskException.class,
                () -> SpillWorkspaceManager.openForTests(
                        config(link.toAbsolutePath()), () -> 1L, identities(INSTANCE_A)));

        assertEquals(SpillFailureCode.SPILL_WORKSPACE_UNAVAILABLE, failure.code());
        assertTrue(Files.exists(sentinel));
        assertEquals(1, directoryEntryCount(target), "must not publish through the symlink target");
    }

    @Test
    void invalidInjectedIdentityCreatesNoCanonicalInstance() {
        Path base = realTemp().resolve("bad-id");

        assertThrows(SpillDiskException.class,
                () -> SpillWorkspaceManager.openForTests(
                        config(base), () -> 1L, identities("NOT-CANONICAL")));

        Path instances = base.resolve("instances");
        assertTrue(!Files.exists(instances) || directoryIsEmpty(instances));
    }

    @Test
    void closedManagerRejectsNewJobsWithoutRecreatingItsInstance() {
        Path base = realTemp().resolve("closed");
        SpillWorkspaceManager manager = SpillWorkspaceManager.openForTests(
                config(base), () -> 1L, identities(INSTANCE_A, JOB_A));
        Path instance = manager.instanceDirectory();
        manager.close();

        assertThrows(IllegalStateException.class, manager::openJob);
        assertFalse(Files.exists(instance, LinkOption.NOFOLLOW_LINKS));
    }

    private static SpillWorkspaceConfig config(Path base) {
        return SpillWorkspaceConfig.forTests(base,
                8L * 1024 * 1024, 4L * 1024 * 1024,
                0, 0, 64, 32, 0);
    }

    private Path realTemp() {
        try {
            return temp.toRealPath();
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

    private static SpillWorkspaceMarker readMarker(Path path) {
        try {
            return SpillWorkspaceMarkerCodec.decode(Files.readAllBytes(path));
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static boolean directoryIsEmpty(Path directory) {
        try (java.util.stream.Stream<Path> children = Files.list(directory)) {
            return !children.findFirst().isPresent();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static long directoryEntryCount(Path directory) {
        try (java.util.stream.Stream<Path> children = Files.list(directory)) {
            return children.count();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void assertOwnerOnlyWhenPosix(Path directory) throws IOException {
        try {
            Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(
                    directory, LinkOption.NOFOLLOW_LINKS);
            assertEquals(EnumSet.of(
                    PosixFilePermission.OWNER_READ,
                    PosixFilePermission.OWNER_WRITE,
                    PosixFilePermission.OWNER_EXECUTE), permissions);
        } catch (UnsupportedOperationException ignored) {
            // Windows/non-POSIX: NOFOLLOW/containment remain mandatory; POSIX mode is not available.
        }
    }
}
