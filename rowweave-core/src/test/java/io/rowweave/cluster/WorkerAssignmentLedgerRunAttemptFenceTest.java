package io.rowweave.cluster;

import io.rowweave.cluster.rpc.ControlResidentBudget;
import io.rowweave.cluster.rpc.ControlResidentLease;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageAssignmentToken;
import io.rowweave.run.StageTerminalOutcome;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WorkerAssignmentLedgerRunAttemptFenceTest {

    @Test
    void duplicateRequiresBothCommandFingerprintAndAttemptFence() {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(2);
        byte[] command = new byte[] { 1, 2, 3 };
        RunAttemptFence first = new RunAttemptFence("run", "attempt-1", 1L);
        RunAttemptFence second = new RunAttemptFence("run", "attempt-2", 2L);

        WorkerAssignmentLedger.Registration accepted =
                ledger.registerSocket(101L, command, first);
        assertEquals(WorkerAssignmentLedger.Decision.NEW, accepted.decision);
        assertEquals(first, accepted.slot.runAttemptFence);
        assertEquals(WorkerAssignmentLedger.Decision.DUPLICATE,
                ledger.registerSocket(101L, command, first).decision);
        assertEquals(WorkerAssignmentLedger.Decision.CONFLICT,
                ledger.registerSocket(101L, command, second).decision);
    }

    @Test
    void staleCancelCannotMutateActiveAssignment() {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(2);
        RunAttemptFence owner = new RunAttemptFence("run", "attempt-owner", 1L);
        RunAttemptFence stale = new RunAttemptFence("run", "attempt-stale", 2L);
        WorkerAssignmentLedger.Registration accepted =
                ledger.registerSocket(201L, new byte[] { 4 }, owner);

        assertThrows(IllegalArgumentException.class, () -> ledger.cancel(201L, stale));
        assertThrows(IllegalArgumentException.class, () -> ledger.cancel(201L, null));
        assertEquals(1, ledger.activeCount());
        assertEquals(WorkerAssignmentLedger.Decision.DUPLICATE,
                ledger.registerSocket(201L, new byte[] { 4 }, owner).decision);

        ledger.cancel(201L, owner);
        assertEquals(0, ledger.activeCount());
    }

    @Test
    void negativeAndTerminalTombstonesAreScopedToAttemptFence() {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(2);
        RunAttemptFence first = new RunAttemptFence("run", "attempt-1", 1L);
        RunAttemptFence second = new RunAttemptFence("run", "attempt-2", 2L);

        ledger.cancel(202L, first);
        assertEquals(WorkerAssignmentLedger.Decision.CANCELLED,
                ledger.registerSocket(202L, new byte[] { 5 }, first).decision);
        WorkerAssignmentLedger.Registration replacement =
                ledger.registerSocket(202L, new byte[] { 6 }, second);
        assertEquals(WorkerAssignmentLedger.Decision.NEW, replacement.decision);
        ledger.complete(replacement.slot);

        assertEquals(WorkerAssignmentLedger.Decision.NEW,
                ledger.registerSocket(202L, new byte[] { 7 }, first).decision,
                "terminal state from another attempt must not poison stageId reuse");
    }

    @Test
    void newSocketAssignmentTakesLeaseBeforePublishAndTerminalReleasesOnce() {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(2);
        ControlResidentBudget budget = ControlResidentBudget.forTesting(128L);
        ControlResidentLease lease = budget.openLease();
        ControlResidentLease.OwnerToken decode = lease.ownerToken();
        lease.reserve(decode, 64L);
        ControlResidentLease.OwnerToken handler = lease.transfer(
                decode, io.rowweave.cluster.rpc.ControlResidentOwner.HANDLER_OWNED);

        WorkerAssignmentLedger.Registration accepted = ledger.registerSocket(
                301L, new byte[] {8}, new RunAttemptFence("run", "attempt", 1L),
                false, lease, handler);
        assertEquals(WorkerAssignmentLedger.Decision.NEW, accepted.decision);
        assertFalse(lease.release(handler), "handler finally must be stale after publication");
        assertEquals(64L, budget.snapshot().usedBytes());

        ledger.complete(accepted.slot);
        ledger.complete(accepted.slot);
        assertEquals(0L, budget.snapshot().usedBytes());
    }

    @Test
    void duplicateDoesNotTakeReplacementLeaseAndCancelReleasesOriginal() {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(2);
        RunAttemptFence fence = new RunAttemptFence("run", "attempt", 1L);
        ControlResidentBudget budget = ControlResidentBudget.forTesting(256L);
        ControlResidentLease original = handlerOwnedLease(budget, 80L);
        ControlResidentLease replacement = handlerOwnedLease(budget, 40L);

        WorkerAssignmentLedger.Registration first = ledger.registerSocket(
                302L, new byte[] {9}, fence, false, original, original.ownerToken());
        assertEquals(WorkerAssignmentLedger.Decision.DUPLICATE,
                ledger.registerSocket(302L, new byte[] {9}, fence, false,
                        replacement, replacement.ownerToken()).decision);
        assertTrue(replacement.release(replacement.ownerToken()),
                "duplicate request must retain its handler-owned lease");
        assertEquals(80L, budget.snapshot().usedBytes());

        ledger.cancel(302L, fence);
        assertEquals(0L, budget.snapshot().usedBytes());
        assertFalse(original.release(original.ownerToken()));
        assertEquals(WorkerAssignmentLedger.Decision.NEW, first.decision);
    }

    @Test
    void durableIdentityIsBoundBeforeTheSlotCanBeCancelled() {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(2);
        RunAttemptFence fence = new RunAttemptFence("run", "attempt", 1L);
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, 1L, 0, 401L, 0, 0, StageAssignmentRole.WHOLE_GRAPH,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentToken token = StageAssignmentSnapshot.initial(
                new StageAssignmentManifest(fence, "artifact", 1L,
                        Collections.singletonList(record))).token(0);
        byte[] digest = new byte[StageTerminalReceipt.DIGEST_BYTES];

        WorkerAssignmentLedger.Registration registration = ledger.registerDurableSocket(
                401L, new byte[] {10}, fence, token, digest, null, null);
        WorkerAssignmentLedger.Cancellation cancellation = ledger.cancel(401L, fence);
        StageTerminalReceipt receipt = StageTerminalReceipt.canceled(
                token, digest, 0L, 0L, "CANCELED", digest, 1L);

        assertTrue(cancellation.reportCancelled);
        assertEquals(receipt, ledger.freezeTerminal(registration.slot, receipt));
        assertEquals(WorkerAssignmentLedger.ReconciliationDecision.TERMINAL_PENDING,
                ledger.reconcile(401L, token, digest).decision,
                "a cancel-visible durable slot must already have a usable terminal identity");
        assertEquals(StageTerminalOutcome.CANCELED,
                ledger.reconcile(401L, token, digest).receipt.outcome());
    }

    @Test
    void durableRegistrationCannotPublishWithoutItsTerminalIdentity() {
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(1);
        RunAttemptFence fence = new RunAttemptFence("run", "attempt", 1L);

        assertThrows(IllegalArgumentException.class, () -> ledger.registerDurableSocket(
                402L, new byte[] {11}, fence, null, null, null, null));
        assertEquals(0, ledger.activeCount());
    }

    private static ControlResidentLease handlerOwnedLease(ControlResidentBudget budget, long bytes) {
        ControlResidentLease lease = budget.openLease();
        ControlResidentLease.OwnerToken decode = lease.ownerToken();
        lease.reserve(decode, bytes);
        lease.transfer(decode, io.rowweave.cluster.rpc.ControlResidentOwner.HANDLER_OWNED);
        return lease;
    }
}
