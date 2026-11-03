package io.rowweave.cluster;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** cancellation work is assignment-bounded and concurrent across Workers. */
class StageCancellationCoordinatorTest {

    @Test
    @Timeout(value = 10, unit = TimeUnit.SECONDS)
    void acceptsOneSixtyFourAndMaximumAssignmentsWithoutUnboundedWork() {
        for (int assignments : new int[]{1, 64, 4096}) {
            AtomicInteger calls = new AtomicInteger();
            List<StageCancellationCoordinator.WorkerBatch> batches =
                    batches(assignments, Math.min(assignments, 64), calls, 0L, false);
            try (StageCancellationCoordinator coordinator =
                         new StageCancellationCoordinator(8)) {
                StageCancellationCoordinator.Result result = coordinator.cancel(
                        batches, deadlineAfterMillis(5_000L));

                assertTrue(result.complete(), "assignments=" + assignments);
                assertEquals(assignments, result.requestedAssignments());
                assertEquals(assignments, result.acknowledgedAssignments());
                assertEquals(0, result.incompleteWorkers());
                assertFalse(result.deadlineExceeded());
                assertEquals(assignments, calls.get());
            }
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void slowWorkersAreCanceledConcurrentlyUnderOneDeadline() {
        AtomicInteger calls = new AtomicInteger();
        List<StageCancellationCoordinator.WorkerBatch> batches =
                batches(4, 4, calls, 300L, false);
        long started = System.nanoTime();
        try (StageCancellationCoordinator coordinator =
                     new StageCancellationCoordinator(4)) {
            StageCancellationCoordinator.Result result = coordinator.cancel(
                    batches, deadlineAfterMillis(1_000L));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertTrue(result.complete());
            assertEquals(4, calls.get());
            assertTrue(elapsedMs >= 250L && elapsedMs < 700L,
                    "four Worker batches must overlap instead of accumulating: "
                            + elapsedMs + "ms");
        }
    }

    @Test
    void rejectionMarksTheWorkerIncompleteWithoutLeakingTheFailure() {
        AtomicInteger incomplete = new AtomicInteger();
        StageCancellationCoordinator.WorkerBatch batch =
                StageCancellationCoordinator.WorkerBatch.of(
                        "worker/incarnation",
                        Collections.singletonList(() -> {
                            throw new IllegalStateException("sensitive worker detail");
                        }),
                        failure -> incomplete.incrementAndGet());
        try (StageCancellationCoordinator coordinator =
                     new StageCancellationCoordinator(1)) {
            StageCancellationCoordinator.Result result = coordinator.cancel(
                    Collections.singletonList(batch), deadlineAfterMillis(1_000L));

            assertFalse(result.complete());
            assertEquals(1, result.requestedAssignments());
            assertEquals(0, result.acknowledgedAssignments());
            assertEquals(1, result.incompleteWorkers());
            assertFalse(result.deadlineExceeded());
            assertEquals(1, incomplete.get());
            assertFalse(result.toString().contains("sensitive worker detail"));
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void deadlineReturnsWithoutWaitingForEverySlowWorkerCall() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger incomplete = new AtomicInteger();
        List<StageCancellationCoordinator.WorkerBatch> batches = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            batches.add(StageCancellationCoordinator.WorkerBatch.of(
                    "worker-" + i + "/incarnation",
                    Collections.singletonList(() -> {
                        while (release.getCount() > 0) {
                            try {
                                release.await(20L, TimeUnit.MILLISECONDS);
                            } catch (InterruptedException ignored) {
                                // Simulate a control call that does not cooperate with interruption.
                            }
                        }
                    }),
                    failure -> incomplete.incrementAndGet()));
        }
        long started = System.nanoTime();
        try (StageCancellationCoordinator coordinator =
                     new StageCancellationCoordinator(4)) {
            StageCancellationCoordinator.Result result = coordinator.cancel(
                    batches, deadlineAfterMillis(200L));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertFalse(result.complete());
            assertTrue(result.deadlineExceeded());
            assertEquals(4, result.incompleteWorkers());
            assertEquals(4, incomplete.get());
            assertTrue(elapsedMs >= 150L && elapsedMs < 700L,
                    "terminal deadline must cap the coordinator wait: " + elapsedMs + "ms");
        } finally {
            release.countDown();
        }
    }

    @Test
    void rejectsMoreThanThePhysicalAssignmentHardLimit() {
        AtomicInteger calls = new AtomicInteger();
        List<StageCancellationCoordinator.WorkerBatch> batches =
                batches(4097, 64, calls, 0L, false);
        try (StageCancellationCoordinator coordinator =
                     new StageCancellationCoordinator(8)) {
            assertThrows(IllegalArgumentException.class, () -> coordinator.cancel(
                    batches, deadlineAfterMillis(1_000L)));
            assertEquals(0, calls.get());
        }
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.SECONDS)
    void blockedMaintenanceIoIsAbortedAtItsDeadlineAndReleasesTheBoundedLane() {
        CountDownLatch release = new CountDownLatch(1);
        AtomicInteger entered = new AtomicInteger();
        try (StageCancellationCoordinator coordinator = new StageCancellationCoordinator(1)) {
            long started = System.nanoTime();
            assertFalse(coordinator.runControlUntil(() -> {
                entered.incrementAndGet();
                while (release.getCount() > 0L) {
                    try {
                        release.await();
                    } catch (InterruptedException ignored) {
                        // Socket writes may ignore interruption; close must release the I/O.
                    }
                }
            }, release::countDown, deadlineAfterMillis(200L)));
            long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertEquals(1, entered.get());
            assertEquals(0L, release.getCount());
            assertTrue(elapsedMs >= 150L && elapsedMs < 700L);
            assertTrue(coordinator.runControlUntil(() -> entered.incrementAndGet(),
                    () -> { }, deadlineAfterMillis(1_000L)));
            assertEquals(2, entered.get());
        } finally {
            release.countDown();
        }
    }

    private static List<StageCancellationCoordinator.WorkerBatch> batches(
            int assignments, int workers, AtomicInteger calls,
            long delayMs, boolean reject) {
        List<List<StageCancellationCoordinator.CancelCall>> grouped = new ArrayList<>();
        for (int i = 0; i < workers; i++) grouped.add(new ArrayList<>());
        for (int i = 0; i < assignments; i++) {
            grouped.get(i % workers).add(() -> {
                calls.incrementAndGet();
                if (delayMs > 0L) Thread.sleep(delayMs);
                if (reject) throw new IllegalStateException("rejected");
            });
        }
        List<StageCancellationCoordinator.WorkerBatch> batches = new ArrayList<>();
        for (int i = 0; i < grouped.size(); i++) {
            batches.add(StageCancellationCoordinator.WorkerBatch.of(
                    "worker-" + i + "/incarnation", grouped.get(i), failure -> { }));
        }
        return batches;
    }

    private static long deadlineAfterMillis(long timeoutMs) {
        return System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
    }
}
