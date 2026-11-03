package io.rowweave.mem.spill;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Closing a cancelled job must not invert the manager/store lock order. */
class SpillWorkspaceLockOrderTest {
    @Test
    void closeDuringRunOpenCannotDeadlockTheSharedWorkspaceManager(
            @TempDir Path temp) throws Exception {
        Path real = temp.toRealPath();
        verifyCloseDuringRunOpen(real.resolve("job-close"), false);
        verifyCloseDuringRunOpen(real.resolve("manager-close"), true);
    }

    private static void verifyCloseDuringRunOpen(Path base,
            boolean closeManager) throws Exception {
        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                base, 8L << 20,
                4L << 20, 0, 0, 64, 32, 0);
        java.util.concurrent.atomic.AtomicInteger ids =
                new java.util.concurrent.atomic.AtomicInteger();
        SpillWorkspaceManager manager = SpillWorkspaceManager.openForTests(
                config, () -> 100L,
                () -> ids.getAndIncrement() == 0
                        ? "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                        : "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                ignored -> Long.MAX_VALUE);
        SpillJobWorkspace job = manager.openJob();
        CountDownLatch opening = new CountDownLatch(1);
        CountDownLatch proceed = new CountDownLatch(1);
        DiskSpillStore store = job.newSpillStoreForTests(file -> {
            opening.countDown();
            try {
                if (!proceed.await(5L, TimeUnit.SECONDS)) {
                    throw new IOException("test did not release run open");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IOException(interrupted);
            }
            return new FileOutputStream(file);
        });
        AtomicReference<Throwable> openFailure = new AtomicReference<>();
        AtomicReference<Throwable> closeFailure = new AtomicReference<>();
        Thread opener = new Thread(() -> {
            try {
                store.createRun();
            } catch (Throwable failure) {
                openFailure.set(failure);
            }
        }, "spill-open-lock-order");
        Thread closer = new Thread(() -> {
            try {
                if (closeManager) manager.close();
                else job.close();
            } catch (Throwable failure) {
                closeFailure.set(failure);
            }
        }, "spill-close-lock-order");
        opener.setDaemon(true);
        closer.setDaemon(true);
        opener.start();
        assertTrue(opening.await(5L, TimeUnit.SECONDS));
        closer.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
        while (closer.getState() != Thread.State.BLOCKED
                && System.nanoTime() < deadline) {
            Thread.yield();
        }
        assertTrue(closer.getState() == Thread.State.BLOCKED);
        proceed.countDown();
        opener.join(5_000L);
        closer.join(5_000L);
        assertTrue(!opener.isAlive() && !closer.isAlive(),
                "run-open and cancellation close formed a lock cycle");
        assertNull(openFailure.get());
        assertNull(closeFailure.get());
        if (!closeManager) manager.close();
    }
}
