package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ControlResidentLeaseTest {

    @Test
    void ownerStateMachineRejectsStaleMutationAndReleasesExactlyOnce() {
        ControlResidentBudget budget = ControlResidentBudget.forTesting(128L);
        ControlResidentLease lease = budget.openLease();
        ControlResidentLease.OwnerToken decode = lease.ownerToken();
        lease.reserve(decode, 80L);
        assertEquals(80L, budget.snapshot().usedBytes());

        ControlResidentLease.OwnerToken handler = lease.transfer(
                decode, ControlResidentOwner.HANDLER_OWNED);
        assertFalse(lease.release(decode), "stale decode finally must be a no-op");
        assertThrows(IllegalStateException.class, () -> lease.reserve(decode, 1L));

        ControlResidentLease.OwnerToken run = lease.transfer(
                handler, ControlResidentOwner.RUN_OWNED);
        assertTrue(lease.release(run));
        assertFalse(lease.release(run), "duplicate terminal release must not underflow");
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void concurrentAdmissionRejectsWithStableCodeAndDoesNotWait() {
        ControlResidentBudget budget = ControlResidentBudget.forTesting(100L);
        ControlResidentLease first = budget.openLease();
        first.reserve(first.ownerToken(), 80L);
        ControlResidentLease second = budget.openLease();

        ControlResidentBudgetExceededException failure = assertThrows(
                ControlResidentBudgetExceededException.class,
                () -> second.reserve(second.ownerToken(), 21L));
        assertEquals("CONTROL_RESIDENT_BUDGET_EXCEEDED", failure.code());
        assertEquals(80L, budget.snapshot().usedBytes());
        assertTrue(first.release(first.ownerToken()));
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void successfulHandoffHappensBeforePublishedOwnerAndOldFinallyCannotRelease() throws Exception {
        ControlResidentBudget budget = ControlResidentBudget.forTesting(128L);
        ControlResidentLease lease = budget.openLease();
        ControlResidentLease.OwnerToken decode = lease.ownerToken();
        lease.reserve(decode, 64L);
        ControlResidentLease.OwnerToken handler = lease.transfer(
                decode, ControlResidentOwner.HANDLER_OWNED);
        AtomicReference<ControlResidentLease.OwnerToken> published = new AtomicReference<>();
        CountDownLatch transferred = new CountDownLatch(1);

        Thread handoff = new Thread(() -> {
            ControlResidentLease.OwnerToken assignment = lease.transfer(
                    handler, ControlResidentOwner.ASSIGNMENT_OWNED);
            published.set(assignment);
            transferred.countDown();
        });
        handoff.start();
        assertTrue(transferred.await(2, TimeUnit.SECONDS));
        assertFalse(lease.release(handler));
        ControlResidentLease.OwnerToken assignment = published.get();
        assertEquals(ControlResidentOwner.ASSIGNMENT_OWNED, assignment.owner());
        assertEquals(64L, lease.reservedBytes());
        assertTrue(lease.release(assignment));
        handoff.join(2_000L);
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void illegalOwnerTransitionFailsWithoutPublishingOrLosingCharge() {
        ControlResidentBudget budget = ControlResidentBudget.forTesting(64L);
        ControlResidentLease lease = budget.openLease();
        ControlResidentLease.OwnerToken decode = lease.ownerToken();
        lease.reserve(decode, 32L);

        assertThrows(IllegalStateException.class,
                () -> lease.transfer(decode, ControlResidentOwner.RUN_OWNED));
        assertEquals(ControlResidentOwner.DECODE_OWNED, lease.ownerToken().owner());
        assertEquals(32L, budget.snapshot().usedBytes());
        assertTrue(lease.release(decode));
    }
}

