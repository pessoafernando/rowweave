package io.rowweave.run;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileRunPurgeCrashTest {
    @TempDir
    Path temp;

    @Test
    void crashAfterAtomicMoveResumesFromDeterministicTrash() {
        AtomicBoolean fail = new AtomicBoolean(true);
        FileRunStateStore store = store(new FileRunStateStore.PurgeObserver() {
            @Override
            public void afterMove() {
                if (fail.getAndSet(false)) throw new IllegalStateException("move ACK loss");
            }

            @Override
            public void afterLifecycleDelete() { }
        });
        RetentionToken token = claimed(store, "run");

        assertThrows(IllegalStateException.class, () -> store.purge(token, 1));
        assertNull(store.get("run"));
        assertNotNull(store.lifecycle("run"));
        assertTrue(Files.isDirectory(store.trashPathForTest("run")));
        assertThrows(RunHistoryRetiringException.class,
                () -> store.events("run", 0L, 10));
        RunDecision blockedReuse = store.apply(RunCommand.createRun(
                "reuse-too-early", "run", "definition", "artifact-new",
                1, 0L, 3L));
        assertEquals(RunDecision.Code.RETENTION_IN_PROGRESS, blockedReuse.code());
        assertNull(blockedReuse.snapshot());

        FileRunStateStore reopened = new FileRunStateStore(temp);
        assertEquals(RunPurgeProgress.Code.PURGED,
                reopened.purge(token, 1).code());
        assertNull(reopened.lifecycle("run"));
        assertFalse(Files.exists(reopened.trashPathForTest("run")));
    }

    @Test
    void crashAfterLifecycleDeleteReconcilesAsPurgedAndKeepsFixedLock() {
        AtomicBoolean fail = new AtomicBoolean(true);
        FileRunStateStore store = store(new FileRunStateStore.PurgeObserver() {
            @Override
            public void afterMove() { }

            @Override
            public void afterLifecycleDelete() {
                if (fail.getAndSet(false)) {
                    throw new IllegalStateException("lifecycle delete ACK loss");
                }
            }
        });
        RetentionToken token = claimed(store, "run");
        Path fixedLock = store.runStripeLockPathForTest("run");

        assertThrows(IllegalStateException.class, () -> store.purge(token, 1));
        assertNull(store.get("run"));
        assertNull(store.lifecycle("run"));
        assertTrue(Files.exists(fixedLock));

        FileRunStateStore reopened = new FileRunStateStore(temp);
        assertEquals(RunPurgeProgress.Code.PURGED,
                reopened.purge(token, 1).code());
        assertFalse(Files.exists(reopened.trashPathForTest("run")));
        assertTrue(Files.isRegularFile(fixedLock));
    }

    @Test
    void newIdentityCleansOldQuarantineAndOldTokenCannotDeleteTheReplacement() {
        AtomicBoolean fail = new AtomicBoolean(true);
        FileRunStateStore store = store(new FileRunStateStore.PurgeObserver() {
            @Override
            public void afterMove() { }

            @Override
            public void afterLifecycleDelete() {
                if (fail.getAndSet(false)) throw new IllegalStateException("leave trash");
            }
        });
        RetentionToken oldToken = claimed(store, "run");
        assertThrows(IllegalStateException.class, () -> store.purge(oldToken, 1));
        assertTrue(Files.isDirectory(store.trashPathForTest("run")));

        RunDecision replacement = store.apply(RunCommand.createRun(
                "create-replacement", "run", "definition", "artifact-new",
                1, 0L, 3L));

        assertEquals(RunDecision.Code.ACCEPTED, replacement.code());
        assertEquals("artifact-new", store.get("run").artifactId());
        assertFalse(Files.exists(store.trashPathForTest("run")));
        assertEquals(RunPurgeProgress.Code.STALE_OWNER,
                store.purge(oldToken, 1).code());
        assertEquals("artifact-new", store.get("run").artifactId());
    }

    @Test
    void crashDuringBestEffortTrashCleanupResumesFromRecognizedEmptyResidue()
            throws Exception {
        AtomicBoolean fail = new AtomicBoolean(true);
        FileRunStateStore store = store(new FileRunStateStore.PurgeObserver() {
            @Override
            public void afterMove() { }

            @Override
            public void afterLifecycleDelete() {
                if (fail.getAndSet(false)) throw new IllegalStateException("leave trash");
            }
        });
        RetentionToken token = claimed(store, "run");
        assertThrows(IllegalStateException.class, () -> store.purge(token, 1));
        Path trash = store.trashPathForTest("run");
        Files.delete(trash.resolve("journal.axr"));
        assertTrue(Files.isDirectory(trash));
        assertNull(store.lifecycle("run"));

        FileRunStateStore reopened = new FileRunStateStore(temp);
        assertEquals(RunPurgeProgress.Code.PURGED,
                reopened.purge(token, 1).code());
        assertFalse(Files.exists(trash));
        assertEquals(RunDecision.Code.ACCEPTED,
                reopened.apply(RunCommand.createRun(
                        "recreate", "run", "definition", "artifact-new",
                        1, 0L, 3L)).code());
    }

    @Test
    void realProcessHaltAtBothPublicationCutsResumesAfterJvmReopen()
            throws Exception {
        assertProcessCrashResumes("move", 92);
        assertProcessCrashResumes("lifecycle", 93);
    }

    @Test
    void unrecognizedQuarantineBlocksReuseAndPreservesEvidence() throws Exception {
        FileRunStateStore store = leavePublishedTrash("foreign");
        Path trash = store.trashPathForTest("run");
        Path evidence = trash.resolve("foreign.bin");
        Files.write(evidence, new byte[] { 7 }, StandardOpenOption.CREATE_NEW);

        assertThrows(IllegalStateException.class, () -> store.apply(
                RunCommand.createRun("recreate", "run", "definition",
                        "artifact-new", 1, 0L, 3L)));
        assertTrue(Files.exists(evidence));
        assertNull(store.get("run"));
    }

    @Test
    void quarantineSymlinkIsNeverFollowedOrDeleted() throws Exception {
        FileRunStateStore store = leavePublishedTrash("symlink");
        Path target = temp.resolve("outside-target");
        Files.write(target, new byte[] { 9 }, StandardOpenOption.CREATE_NEW);
        Path link = store.trashPathForTest("run").resolve("writer.lock");
        Files.createSymbolicLink(link, target);

        assertThrows(IllegalStateException.class, () -> store.apply(
                RunCommand.createRun("recreate", "run", "definition",
                        "artifact-new", 1, 0L, 3L)));
        assertTrue(Files.isSymbolicLink(link));
        assertEquals(9, Files.readAllBytes(target)[0]);
    }

    private FileRunStateStore store(FileRunStateStore.PurgeObserver observer) {
        return new FileRunStateStore(temp, () -> { },
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                () -> 100L, observer);
    }

    private FileRunStateStore leavePublishedTrash(String message) {
        AtomicBoolean fail = new AtomicBoolean(true);
        FileRunStateStore store = store(new FileRunStateStore.PurgeObserver() {
            @Override
            public void afterMove() { }

            @Override
            public void afterLifecycleDelete() {
                if (fail.getAndSet(false)) throw new IllegalStateException(message);
            }
        });
        RetentionToken token = claimed(store, "run");
        assertThrows(IllegalStateException.class, () -> store.purge(token, 1));
        assertNull(store.lifecycle("run"));
        return store;
    }

    private void assertProcessCrashResumes(String cut, int exitCode) throws Exception {
        Path base = temp.resolve("process-" + cut);
        FileRunStateStore prepared = new FileRunStateStore(base);
        RetentionToken token = claimed(prepared, "run");
        Process process = new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                FileRunPurgeProcessCrashProbe.class.getName(),
                base.toString(), cut)).redirectErrorStream(true).start();
        assertTrue(process.waitFor(20, TimeUnit.SECONDS));
        assertEquals(exitCode, process.exitValue());

        FileRunStateStore reopened = new FileRunStateStore(base);
        assertNull(reopened.get("run"));
        if ("move".equals(cut)) {
            assertNotNull(reopened.lifecycle("run"));
            assertThrows(RunHistoryRetiringException.class,
                    () -> reopened.events("run", 0L, 10));
        } else {
            assertNull(reopened.lifecycle("run"));
        }
        assertEquals(RunPurgeProgress.Code.PURGED,
                reopened.purge(token, 1).code());
        assertEquals(RunDecision.Code.ACCEPTED,
                reopened.apply(RunCommand.createRun(
                        "recreate", "run", "definition", "artifact-new",
                        1, 0L, 3L)).code());
    }

    private static RetentionToken claimed(FileRunStateStore store, String runId) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create", runId, "definition", "artifact", 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        RunDecision terminal = store.apply(RunCommand.cancelReadyRun(
                "cancel", runId, created.snapshot().revision(), 2L,
                "CANCELED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        RunLifecycleRef ref = store.lifecycle(runId);
        RunRetentionClaim claim = store.claim(
                runId, terminal.snapshot().revision(), ref.terminalAtMillis(),
                new RetentionOwner("master", 1L), 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }
}
