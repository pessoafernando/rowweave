package io.rowweave.mem;

import io.rowweave.mem.spill.BoundedReplayRun;
import io.rowweave.mem.spill.DiskSpillStore;
import io.rowweave.mem.spill.SpillDiskException;
import io.rowweave.mem.spill.SpillFailureCode;
import io.rowweave.mem.spill.SpillJobWorkspace;
import io.rowweave.mem.spill.SpillWorkspaceConfig;
import io.rowweave.mem.spill.SpillWorkspaceManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionMemoryWorkspaceTest {

    private static final String PREFIX = "rowweave.worker.spill.";

    @TempDir
    Path temp;

    @AfterEach
    void clearProperties() {
        System.clearProperty(PREFIX + "dir");
        System.clearProperty(PREFIX + "maxBytes");
        System.clearProperty(PREFIX + "jobMaxBytes");
        System.clearProperty(PREFIX + "minFreeBytes");
        System.clearProperty(PREFIX + "orphanGraceMs");
        System.clearProperty(PREFIX + "scanMaxEntries");
        System.clearProperty(PREFIX + "deleteMaxEntries");
        System.clearProperty(PREFIX + "maintenanceIntervalMs");
    }

    @Test
    void zeroBreakerDoesNotInitializeWorkspaceSupplierOrCreateBase() throws Exception {
        Path base = configure("zero-breaker");
        AtomicInteger calls = new AtomicInteger();
        Supplier<SpillJobWorkspace> workspace = () -> {
            calls.incrementAndGet();
            return SpillWorkspaceManager.open(SpillWorkspaceConfig.fromSystemProperties()).openJob();
        };

        ExecutionMemory memory = ExecutionMemory.forJob(
                ManagedMemory.ofBytes(1024), 0, workspace);
        memory.close();

        assertEquals(0, calls.get());
        assertFalse(Files.exists(base), "pass-through graph must not create spill workspace");
    }

    @Test
    void breakerUsesOwnedQuotaAccountedStoreAndCloseRemovesJobDirectory() throws Exception {
        configure("breaker");
        AtomicReference<SpillWorkspaceManager> managerRef = new AtomicReference<>();
        ExecutionMemory memory = ExecutionMemory.forJob(
                ManagedMemory.ofBytes(2L * 1024 * 1024), 1,
                workspaceSupplier(managerRef));
        Path jobDirectory = memory.spillRootDir().toPath();
        DiskSpillStore store = (DiskSpillStore) memory.newSpillStore();
        BoundedReplayRun run = store.createRun();

        run.append(new byte[10]);
        assertEquals(14L, managerRef.get().reservedBytes());
        assertTrue(jobDirectory.startsWith(managerRef.get().instanceDirectory().resolve("jobs")));

        memory.close();
        assertEquals(0L, managerRef.get().reservedBytes());
        assertFalse(Files.exists(jobDirectory));
        managerRef.get().close();
    }

    @Test
    void diskOnlyCloseAbortsUnfinishedWriterAndReleasesDiskCharge() throws Exception {
        configure("unfinished");
        AtomicReference<SpillWorkspaceManager> managerRef = new AtomicReference<>();
        ManagedMemory pool = ManagedMemory.ofBytes(2L * 1024 * 1024);
        ExecutionMemory memory = ExecutionMemory.forJob(
                pool, true, workspaceSupplier(managerRef));
        BoundedReplayRun run = ((DiskSpillStore) memory.newSpillStore()).createRun();
        run.append(new byte[100]);

        memory.close();

        assertEquals(pool.totalBytes(), pool.availableBytes());
        assertEquals(0L, managerRef.get().reservedBytes());
        managerRef.get().close();
    }

    @Test
    void cleanupFailureTurnsOtherwiseSuccessfulCloseIntoStructuredFailure() throws Exception {
        configure("cleanup-visible");
        AtomicReference<SpillWorkspaceManager> managerRef = new AtomicReference<>();
        ManagedMemory pool = ManagedMemory.ofBytes(2L * 1024 * 1024);
        ExecutionMemory memory = ExecutionMemory.forJob(
                pool, true, workspaceSupplier(managerRef));
        Files.write(memory.spillRootDir().toPath().resolve("foreign-entry"), new byte[]{1});

        SpillDiskException failure = assertThrows(SpillDiskException.class, memory::close);

        assertEquals(SpillFailureCode.SPILL_CLEANUP_FAILED, failure.code());
        assertEquals(pool.totalBytes(), pool.availableBytes(),
                "managed memory release must still complete when disk cleanup fails");
        assertTrue(!failure.getMessage().contains(memory.spillRootDir().getAbsolutePath()));
        closeExpectedFailure(managerRef.get());
    }

    @Test
    void bodyFailureRemainsPrimaryAndCleanupFailureIsSuppressed() throws Exception {
        configure("cleanup-suppressed");
        AtomicReference<SpillWorkspaceManager> managerRef = new AtomicReference<>();
        RuntimeException body = new RuntimeException("business-stage-failed");
        ExecutionMemory memory = ExecutionMemory.forJob(
                ManagedMemory.ofBytes(2L * 1024 * 1024), 1,
                workspaceSupplier(managerRef));

        RuntimeException actual = assertThrows(RuntimeException.class, () -> {
            try (ExecutionMemory ignored = memory) {
                Files.write(memory.spillRootDir().toPath().resolve("foreign-entry"), new byte[]{1});
                throw body;
            }
        });

        assertSame(body, actual);
        assertEquals(1, actual.getSuppressed().length);
        assertTrue(actual.getSuppressed()[0] instanceof SpillDiskException);
        assertEquals(SpillFailureCode.SPILL_CLEANUP_FAILED,
                ((SpillDiskException) actual.getSuppressed()[0]).code());
        closeExpectedFailure(managerRef.get());
    }

    private Path configure(String name) throws IOException {
        Path base = temp.toRealPath().resolve(name);
        System.setProperty(PREFIX + "dir", base.toString());
        System.setProperty(PREFIX + "maxBytes", Long.toString(2L * 1024 * 1024));
        System.setProperty(PREFIX + "jobMaxBytes", Long.toString(1024L * 1024));
        System.setProperty(PREFIX + "minFreeBytes", "0");
        System.setProperty(PREFIX + "orphanGraceMs", "60000");
        System.setProperty(PREFIX + "scanMaxEntries", "64");
        System.setProperty(PREFIX + "deleteMaxEntries", "32");
        System.setProperty(PREFIX + "maintenanceIntervalMs", "1000");
        return base;
    }

    private static Supplier<SpillJobWorkspace> workspaceSupplier(
            AtomicReference<SpillWorkspaceManager> managerRef) {
        return () -> {
            SpillWorkspaceManager manager = SpillWorkspaceManager.open(
                    SpillWorkspaceConfig.fromSystemProperties());
            assertTrue(managerRef.compareAndSet(null, manager), "one ExecutionMemory opens one job");
            SpillJobWorkspace job = manager.openJob();
            assertNotNull(job);
            return job;
        };
    }

    private static void closeExpectedFailure(SpillWorkspaceManager manager) {
        SpillDiskException failure = assertThrows(SpillDiskException.class, manager::close);
        assertEquals(SpillFailureCode.SPILL_CLEANUP_FAILED, failure.code());
    }
}
