package io.rowweave.cluster;

import io.rowweave.run.RunAttemptFence;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** duplicate ACK and task start share one bounded admission terminal. */
class WorkerAssignmentAdmissionGateTest {
    private static final RunAttemptFence FENCE =
            new RunAttemptFence("run-admission", "attempt-admission", 83L);

    @Test
    void duplicateTimesOutWithoutRejectingTheSharedPreparingSlot() {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(2);
        byte[] command = new byte[]{1, 2, 3};
        WorkerAssignmentLedger.Registration first =
                ledger.registerSocket(1L, command, FENCE);
        WorkerAssignmentLedger.Registration duplicate =
                ledger.registerSocket(1L, command, FENCE);

        assertEquals(WorkerAssignmentLedger.Decision.NEW, first.decision);
        assertEquals(WorkerAssignmentLedger.Decision.DUPLICATE, duplicate.decision);
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(20L);
        assertEquals(WorkerAssignmentLedger.AdmissionWait.TIMED_OUT,
                ledger.awaitAdmissionUntil(duplicate.slot, deadline));
        assertTrue(ledger.isPreparing(first.slot));

        FutureTask<Void> future = new FutureTask<>(() -> null);
        assertTrue(ledger.attachFutureAndAdmit(first.slot, future));
        assertEquals(WorkerAssignmentLedger.AdmissionWait.ADMITTED,
                ledger.awaitAdmissionUntil(duplicate.slot, System.nanoTime()));
    }

    @Test
    void runnableCannotCrossGateBeforeFutureAttachmentAndAdmission() throws Exception {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(1);
        WorkerAssignmentLedger.Slot slot = ledger
                .registerSocket(2L, new byte[]{4}, FENCE).slot;
        AtomicBoolean crossed = new AtomicBoolean();
        Thread early = new Thread(() -> {
            if (ledger.awaitStartPermission(slot)) crossed.set(true);
        }, "early-stage-runnable");
        early.start();
        Thread.sleep(30L);
        assertFalse(crossed.get());

        FutureTask<Void> future = new FutureTask<>(() -> null);
        assertTrue(ledger.attachFutureAndAdmit(slot, future));
        early.join(1_000L);
        assertFalse(early.isAlive());
        assertTrue(crossed.get());
        assertTrue(ledger.tryStart(slot));
        ledger.complete(slot);
    }

    @Test
    void rejectionAndCancellationWakeEarlyRunnableWithoutBusinessPermission() {
        org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(2), () -> {
                    WorkerAssignmentLedger rejectedLedger = new WorkerAssignmentLedger(1);
                    WorkerAssignmentLedger.Slot rejected = rejectedLedger
                            .registerSocket(3L, new byte[]{5}, FENCE).slot;
                    AtomicBoolean rejectedPermission = new AtomicBoolean(true);
                    Thread rejectedThread = new Thread(() -> rejectedPermission.set(
                            rejectedLedger.awaitStartPermission(rejected)));
                    rejectedThread.start();
                    rejectedLedger.rejectAdmission(rejected, "prepare rejected");
                    rejectedThread.join();
                    assertFalse(rejectedPermission.get());
                    assertEquals(WorkerAssignmentLedger.AdmissionWait.REJECTED,
                            rejectedLedger.awaitAdmissionUntil(rejected, System.nanoTime()));

                    WorkerAssignmentLedger cancelledLedger = new WorkerAssignmentLedger(1);
                    WorkerAssignmentLedger.Slot cancelled = cancelledLedger
                            .registerSocket(4L, new byte[]{6}, FENCE).slot;
                    AtomicBoolean cancelledPermission = new AtomicBoolean(true);
                    Thread cancelledThread = new Thread(() -> cancelledPermission.set(
                            cancelledLedger.awaitStartPermission(cancelled)));
                    cancelledThread.start();
                    cancelledLedger.cancel(4L, FENCE);
                    cancelledThread.join();
                    assertFalse(cancelledPermission.get());
                });
    }

    @Test
    void preparePublicationAndSlotOwnershipAreOneCancellationLinearization()
            throws Exception {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(1);
        WorkerAssignmentLedger.Slot slot = ledger
                .registerSocket(5L, new byte[]{7}, FENCE).slot;
        java.util.concurrent.CountDownLatch published =
                new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch releasePrepare =
                new java.util.concurrent.CountDownLatch(1);
        AtomicInteger closes = new AtomicInteger();
        AtomicBoolean cancelledBeforeOwnership = new AtomicBoolean();

        Thread preparing = new Thread(() -> cancelledBeforeOwnership.set(
                ledger.prepareOwned(slot, () -> {
                    published.countDown();
                    try {
                        releasePrepare.await();
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException(interrupted);
                    }
                    return closes::incrementAndGet;
                })), "assignment-prepare-owner");
        preparing.start();
        assertTrue(published.await(1, TimeUnit.SECONDS));

        Thread cancelling = new Thread(() -> ledger.cancel(5L, FENCE),
                "assignment-cancel-racer");
        cancelling.start();
        Thread.sleep(30L);
        assertTrue(cancelling.isAlive(),
                "cancel must not observe a published resource before slot ownership");
        assertEquals(0, closes.get());

        releasePrepare.countDown();
        preparing.join(1_000L);
        cancelling.join(1_000L);
        assertFalse(preparing.isAlive());
        assertFalse(cancelling.isAlive());
        assertFalse(cancelledBeforeOwnership.get());
        assertEquals(1, closes.get(),
                "cancel owns and closes the resource exactly once after publication");
    }
}
