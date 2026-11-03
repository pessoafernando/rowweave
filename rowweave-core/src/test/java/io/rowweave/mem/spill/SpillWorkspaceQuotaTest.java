package io.rowweave.mem.spill;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SpillWorkspaceQuotaTest {

    private static final long MIB = 1024L * 1024;
    private static final String INSTANCE = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    private static final String JOB_A = "11111111111111111111111111111111";
    private static final String JOB_B = "22222222222222222222222222222222";

    @TempDir
    Path temp;

    @Test
    void chargesFramePrefixAndPayloadThenReleasesExactlyOnceOnDispose() {
        SpillWorkspaceManager manager = manager("exact", 2 * MIB, MIB, 0,
                Long.MAX_VALUE, INSTANCE, JOB_A);
        SpillJobWorkspace job = manager.openJob();
        DiskSpillStore store = job.newSpillStore();
        BoundedReplayRun run = store.createRun();

        run.append(new byte[10]);
        run.append(new byte[0]);
        assertEquals(18L, job.reservedBytes(), "two 4-byte prefixes + 10 payload bytes");
        assertEquals(18L, manager.reservedBytes());
        SpillJobTelemetrySnapshot active = job.telemetrySnapshot();
        assertEquals(1L, active.runCount());
        assertEquals(18L, active.logicalBytesWritten());
        assertEquals(18L, active.peakReservedBytes());
        assertTrue(active.complete());

        run.dispose();
        run.dispose();
        assertEquals(0L, job.reservedBytes());
        assertEquals(0L, manager.reservedBytes(), "idempotent dispose must not double-release");
        store.close();
        job.close();
        SpillJobTelemetrySnapshot closed = job.telemetrySnapshot();
        assertEquals(1L, closed.runCount(), "cumulative facts survive workspace close");
        assertEquals(18L, closed.logicalBytesWritten());
        assertEquals(18L, closed.peakReservedBytes());
        manager.close();
    }

    @Test
    void jobQuotaRejectsBeforeExtendingRunByOneMoreFrame() throws Exception {
        SpillWorkspaceManager manager = manager("job-limit", 2 * MIB, MIB, 0,
                Long.MAX_VALUE, INSTANCE, JOB_A);
        SpillJobWorkspace job = manager.openJob();
        DiskSpillStore store = job.newSpillStore();
        BoundedReplayRun run = store.createRun();
        run.append(new byte[(int) MIB - 4]);
        Path file = onlyRunFile(job.directory());
        long sizeBefore = Files.size(file);

        SpillDiskException failure = assertThrows(SpillDiskException.class,
                () -> run.append(new byte[0]));

        assertEquals(SpillFailureCode.SPILL_JOB_QUOTA_EXCEEDED, failure.code());
        assertEquals(MIB, job.reservedBytes());
        assertEquals(sizeBefore, Files.size(file), "rejected reservation must not extend the file");
        run.dispose();
        store.close();
        job.close();
        manager.close();
    }

    @Test
    void concurrentJobsLinearizeAtWorkerLimit() throws Exception {
        SpillWorkspaceManager manager = manager("worker-race", MIB, MIB, 0,
                Long.MAX_VALUE, INSTANCE, JOB_A, JOB_B);
        SpillJobWorkspace firstJob = manager.openJob();
        SpillJobWorkspace secondJob = manager.openJob();
        DiskSpillStore firstStore = firstJob.newSpillStore();
        DiskSpillStore secondStore = secondJob.newSpillStore();
        BoundedReplayRun firstRun = firstStore.createRun();
        BoundedReplayRun secondRun = secondStore.createRun();
        byte[] payload = new byte[(int) MIB - 4];
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger successes = new AtomicInteger();
        AtomicInteger workerRejections = new AtomicInteger();

        Thread first = new Thread(() -> appendAfter(start, firstRun, payload,
                successes, workerRejections), "spill-quota-first");
        Thread second = new Thread(() -> appendAfter(start, secondRun, payload,
                successes, workerRejections), "spill-quota-second");
        first.start();
        second.start();
        start.countDown();
        first.join(10_000);
        second.join(10_000);

        assertTrue(!first.isAlive() && !second.isAlive());
        assertEquals(1, successes.get());
        assertEquals(1, workerRejections.get());
        assertEquals(MIB, manager.reservedBytes());

        firstRun.dispose();
        secondRun.dispose();
        assertEquals(0L, manager.reservedBytes());
        firstStore.close();
        secondStore.close();
        firstJob.close();
        secondJob.close();
        manager.close();
    }

    @Test
    void concurrentReservationsDoNotSerializeSlowUsableSpaceProbes() throws Exception {
        Path base = temp.toRealPath().resolve("parallel-probe");
        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                base, 4 * MIB, 2 * MIB, 0, 0, 64, 32, 0);
        CountDownLatch bothProbing = new CountDownLatch(2);
        AtomicInteger activeProbes = new AtomicInteger();
        AtomicInteger maximumConcurrentProbes = new AtomicInteger();
        SpillWorkspaceManager manager = SpillWorkspaceManager.openForTests(
                config, () -> 100L, identities(INSTANCE, JOB_A, JOB_B), ignored -> {
                    int active = activeProbes.incrementAndGet();
                    maximumConcurrentProbes.accumulateAndGet(active, Math::max);
                    bothProbing.countDown();
                    try {
                        bothProbing.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IOException("probe interrupted", interrupted);
                    } finally {
                        activeProbes.decrementAndGet();
                    }
                    return Long.MAX_VALUE;
                });
        SpillJobWorkspace firstJob = manager.openJob();
        SpillJobWorkspace secondJob = manager.openJob();
        BoundedReplayRun firstRun = firstJob.newSpillStore().createRun();
        BoundedReplayRun secondRun = secondJob.newSpillStore().createRun();
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread first = new Thread(() -> appendCapturing(start, firstRun, failure), "spill-probe-first");
        Thread second = new Thread(() -> appendCapturing(start, secondRun, failure), "spill-probe-second");
        first.start();
        second.start();
        start.countDown();
        first.join(10_000);
        second.join(10_000);

        assertFalse(first.isAlive());
        assertFalse(second.isAlive());
        assertEquals(null, failure.get());
        assertEquals(2, maximumConcurrentProbes.get(),
                "slow filesystem capacity probes must not run under the global quota monitor");
        firstRun.dispose();
        secondRun.dispose();
        firstJob.close();
        secondJob.close();
        manager.close();
    }

    @Test
    void usableSpaceBoundaryAllowsEqualityAndRejectsOneByteLess() {
        SpillWorkspaceManager exact = manager("space-exact", 2 * MIB, MIB, 1000,
                1014, INSTANCE, JOB_A);
        SpillJobWorkspace exactJob = exact.openJob();
        DiskSpillStore exactStore = exactJob.newSpillStore();
        BoundedReplayRun exactRun = exactStore.createRun();
        exactRun.append(new byte[10]);
        assertEquals(14L, exact.reservedBytes());
        exactRun.dispose();
        exactStore.close();
        exactJob.close();
        exact.close();

        SpillWorkspaceManager shortByOne = manager("space-short", 2 * MIB, MIB, 1000,
                1013, INSTANCE, JOB_A);
        SpillJobWorkspace shortJob = shortByOne.openJob();
        DiskSpillStore shortStore = shortJob.newSpillStore();
        BoundedReplayRun shortRun = shortStore.createRun();
        SpillDiskException failure = assertThrows(SpillDiskException.class,
                () -> shortRun.append(new byte[10]));
        assertEquals(SpillFailureCode.SPILL_MIN_FREE_SPACE, failure.code());
        assertEquals(0L, shortByOne.reservedBytes());
        shortRun.dispose();
        shortStore.close();
        shortJob.close();
        shortByOne.close();
    }

    @Test
    void writeFailureKeepsChargeUntilRunFileIsActuallyDeleted() {
        SpillWorkspaceManager manager = manager("write-failure", 2 * MIB, MIB, 0,
                Long.MAX_VALUE, INSTANCE, JOB_A);
        SpillJobWorkspace job = manager.openJob();
        AtomicInteger writes = new AtomicInteger();
        DiskSpillStore store = job.newSpillStoreForTests(file -> new OutputStream() {
            @Override
            public void write(int value) throws IOException {
                if (writes.getAndIncrement() == 0) throw new IOException("/private/customer/secret");
            }

            @Override
            public void write(byte[] bytes, int offset, int length) throws IOException {
                if (writes.getAndIncrement() == 0) throw new IOException("/private/customer/secret");
            }
        });
        BoundedReplayRun run = store.createRun();

        SpillDiskException failure = assertThrows(SpillDiskException.class,
                () -> run.append(new byte[2048]));

        assertEquals(SpillFailureCode.SPILL_IO_WRITE_FAILED, failure.code());
        assertTrue(!failure.getMessage().contains("customer"));
        assertEquals(2052L, manager.reservedBytes(),
                "failed write remains conservatively charged while its file exists");
        assertEquals(0L, job.telemetrySnapshot().logicalBytesWritten(),
                "reservation is not a successfully appended frame");
        run.abort();
        assertEquals(0L, manager.reservedBytes(), "successful delete releases the failed write charge");
        store.close();
        job.close();
        manager.close();
    }

    @Test
    void runOpenFailureDeletesPartialFileAndDoesNotPoisonJobCleanup() {
        SpillWorkspaceManager manager = manager("open-failure", 2 * MIB, MIB, 0,
                Long.MAX_VALUE, INSTANCE, JOB_A);
        SpillJobWorkspace job = manager.openJob();
        DiskSpillStore store = job.newSpillStoreForTests(file -> {
            Files.createFile(file.toPath());
            throw new IOException("/private/customer/secret");
        });

        SpillDiskException failure = assertThrows(SpillDiskException.class, store::createRun);

        assertEquals(SpillFailureCode.SPILL_IO_WRITE_FAILED, failure.code());
        assertEquals(0L, manager.reservedBytes());
        assertEquals(0L, countRunFiles(job.directory()));
        store.close();
        job.close();
        manager.close();
    }

    @Test
    void telemetryOverflowCannotUndoASuccessfulAppend() {
        SpillWorkspaceManager manager = manager("telemetry-overflow", 2 * MIB, MIB, 0,
                Long.MAX_VALUE, INSTANCE, JOB_A);
        SpillJobWorkspace job = manager.openJob();
        DiskSpillStore store = job.newSpillStore();
        BoundedReplayRun run = store.createRun();
        job.written(Long.MAX_VALUE);

        assertDoesNotThrow(() -> run.append(new byte[]{7}));
        SpillJobTelemetrySnapshot snapshot = job.telemetrySnapshot();
        assertFalse(snapshot.complete());
        assertEquals(0L, snapshot.logicalBytesWritten(),
                "overflow must invalidate the fact rather than wrap it");

        run.dispose();
        store.close();
        job.close();
        manager.close();
    }

    private SpillWorkspaceManager manager(String name,
                                          long workerMax, long jobMax, long minFree,
                                          long usableSpace, String... identities) {
        Path base;
        try {
            base = temp.toRealPath().resolve(name);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        SpillWorkspaceConfig config = SpillWorkspaceConfig.forTests(
                base, workerMax, jobMax, minFree, 0, 64, 32, 0);
        return SpillWorkspaceManager.openForTests(
                config, () -> 100L, identities(identities), ignored -> usableSpace);
    }

    private static Supplier<String> identities(String... values) {
        Queue<String> queue = new ArrayDeque<>(Arrays.asList(values));
        return () -> {
            String value = queue.poll();
            if (value == null) throw new AssertionError("identity supplier exhausted");
            return value;
        };
    }

    private static Path onlyRunFile(Path jobDirectory) throws IOException {
        try (java.util.stream.Stream<Path> paths = Files.walk(jobDirectory)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".spill"))
                    .findFirst().orElseThrow(() -> new AssertionError("missing spill run"));
        }
    }

    private static long countRunFiles(Path jobDirectory) {
        try (java.util.stream.Stream<Path> paths = Files.walk(jobDirectory)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".spill")).count();
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static void appendAfter(CountDownLatch start, BoundedReplayRun run, byte[] payload,
                                    AtomicInteger successes, AtomicInteger workerRejections) {
        try {
            start.await();
            run.append(payload);
            successes.incrementAndGet();
        } catch (SpillDiskException failure) {
            if (failure.code() == SpillFailureCode.SPILL_WORKER_QUOTA_EXCEEDED) {
                workerRejections.incrementAndGet();
            } else {
                throw failure;
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }

    private static void appendCapturing(CountDownLatch start, BoundedReplayRun run,
                                        AtomicReference<Throwable> failure) {
        try {
            start.await();
            run.append(new byte[1024]);
        } catch (Throwable throwable) {
            failure.compareAndSet(null, throwable);
        }
    }
}
