package io.rowweave.cluster;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.run.PreparedSinkLocator;
import io.rowweave.run.PreparedSinkReceipt;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.SinkAttemptIdentity;
import io.rowweave.run.StageAssignmentManifest;
import io.rowweave.run.StageAssignmentRecord;
import io.rowweave.run.StageAssignmentRole;
import io.rowweave.run.StageAssignmentSnapshot;
import io.rowweave.run.StageTerminalReceipt;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** a durable Worker slot owns one immutable terminal report. */
class WorkerDurableTerminalLedgerTest {

    @Test
    void freezesAcknowledgesAndReconcilesTerminalWithPreparedEvidence()
            throws Exception {
        Fixture fixture = fixture();
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(1);
        WorkerAssignmentLedger.Registration registration = ledger.registerDurableSocket(
                101L, new byte[] {1, 2, 3}, fixture.fence,
                fixture.snapshot.token(0), fixture.commandDigest, null, null);
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                fixture.snapshot.token(0), fixture.commandDigest, 3L, 2L, 10L);
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1,
                SchemaFingerprint.of(new DataSchema(
                        new Field("id", DataType.LongType))),
                "domain:member", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        PreparedSinkReceipt prepared = new PreparedSinkReceipt(
                fixture.snapshot.token(0), fixture.commandDigest,
                new PreparedSinkLocator(PreparedSinkLocator.CURRENT_VERSION,
                        SinkAttemptIdentity.forAssignment(
                                fixture.manifest, fixture.assignment,
                                "sink-op", descriptor),
                        descriptor.capabilityProfile(), descriptor.commitDomainId(),
                        new byte[]{1, 2}, 2L, sha256("locator"), 7L));

        assertEquals(terminal, ledger.freezeTerminal(
                registration.slot, terminal, prepared));
        assertEquals(prepared, ledger.preparedSinkReceipt(registration.slot));
        WorkerAssignmentLedger.Reconciliation pending = ledger.reconcile(
                101L, fixture.snapshot.token(0), fixture.commandDigest);
        assertEquals(terminal, pending.receipt);
        assertEquals(prepared, pending.preparedSinkReceipt);
        assertTrue(ledger.acknowledgeTerminalReport(
                registration.slot, terminal, prepared));

        PreparedSinkReceipt conflicting = new PreparedSinkReceipt(
                fixture.snapshot.token(0), fixture.commandDigest,
                new PreparedSinkLocator(PreparedSinkLocator.CURRENT_VERSION,
                        prepared.locator().attemptIdentity(),
                        descriptor.capabilityProfile(), descriptor.commitDomainId(),
                        new byte[]{9}, 2L, sha256("locator"), 7L));
        assertThrows(IllegalArgumentException.class,
                () -> ledger.acknowledgeTerminalReport(
                        registration.slot, terminal, conflicting));
    }

    @Test
    void firstTerminalWinsAcrossExecutionAndLeaderChangeCancellation() throws Exception {
        Fixture fixture = fixture();
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(1);
        WorkerAssignmentLedger.Registration registration = ledger.registerDurableSocket(
                101L, new byte[] {1, 2, 3}, fixture.fence,
                fixture.snapshot.token(0), fixture.commandDigest, null, null);
        StageTerminalReceipt succeeded = StageTerminalReceipt.succeeded(
                fixture.snapshot.token(0), fixture.commandDigest, 9L, 7L, 100L);
        StageTerminalReceipt canceled = StageTerminalReceipt.canceled(
                fixture.snapshot.token(0), fixture.commandDigest, 9L, 0L,
                "LEADER_CHANGED", sha256("leader changed"), 101L);

        assertEquals(succeeded, ledger.freezeTerminal(registration.slot, succeeded));
        assertEquals(succeeded, ledger.freezeTerminal(registration.slot, canceled),
                "cancellation cannot overwrite a frozen execution outcome");
        assertTrue(ledger.hasPendingTerminalReport(registration.slot));
        assertTrue(ledger.claimReport(registration.slot));
        assertTrue(ledger.releaseTerminalReportClaim(registration.slot, succeeded),
                "interrupted old-leader delivery must hand report responsibility back");
        assertTrue(ledger.claimReport(registration.slot),
                "leader-change drain owner must be able to resend the frozen receipt");

        ledger.complete(registration.slot);
        assertEquals(succeeded, ledger.terminalReceipt(registration.slot),
                "terminal cleanup may release command/task but must retain report evidence");
        assertTrue(ledger.acknowledgeTerminalReport(registration.slot, succeeded));
        assertFalse(ledger.hasPendingTerminalReport(registration.slot));
        assertFalse(ledger.releaseTerminalReportClaim(registration.slot, succeeded),
                "ACKed evidence cannot reopen report responsibility");
        assertFalse(ledger.acknowledgeTerminalReport(registration.slot, succeeded),
                "exact duplicate ACK is idempotent");
    }

    @Test
    void durableIdentityAndAckMustMatchTheExactAcceptedAssignment() throws Exception {
        Fixture fixture = fixture();
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(1);
        WorkerAssignmentLedger.Registration registration = ledger.registerDurableSocket(
                101L, new byte[] {1, 2, 3}, fixture.fence,
                fixture.snapshot.token(0), fixture.commandDigest, null, null);
        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                fixture.snapshot.token(0), fixture.commandDigest, 1L, 1L, 10L);
        ledger.freezeTerminal(registration.slot, terminal);

        assertThrows(IllegalArgumentException.class, () -> ledger.freezeTerminal(
                registration.slot, StageTerminalReceipt.succeeded(
                        fixture.snapshot.token(0), sha256("other-command"), 1L, 1L, 10L)));
        assertThrows(IllegalArgumentException.class, () -> ledger.acknowledgeTerminalReport(
                registration.slot, StageTerminalReceipt.succeeded(
                        fixture.snapshot.token(0), fixture.commandDigest, 1L, 2L, 10L)));

        WorkerAssignmentLedger legacy = new WorkerAssignmentLedger(1);
        WorkerAssignmentLedger.Registration legacySlot = legacy.registerSocket(
                101L, new byte[] {1}, fixture.fence, false);
        assertThrows(IllegalStateException.class, () -> legacy.bindDurableIdentity(
                legacySlot.slot, fixture.snapshot.token(0), fixture.commandDigest));
    }

    @Test
    void reconciliationDistinguishesActivePendingTerminalAckedAndAbsentByExactIdentity()
            throws Exception {
        Fixture fixture = fixture();
        WorkerAssignmentLedger ledger = new WorkerAssignmentLedger(1);
        assertEquals(WorkerAssignmentLedger.ReconciliationDecision.ABSENT,
                ledger.reconcile(101L, fixture.snapshot.token(0), fixture.commandDigest).decision);

        WorkerAssignmentLedger.Registration registration = ledger.registerDurableSocket(
                101L, new byte[] {1, 2, 3}, fixture.fence,
                fixture.snapshot.token(0), fixture.commandDigest, null, null);
        assertEquals(WorkerAssignmentLedger.ReconciliationDecision.ACTIVE,
                ledger.reconcile(101L, fixture.snapshot.token(0), fixture.commandDigest).decision);
        assertEquals(WorkerAssignmentLedger.ReconciliationDecision.CONFLICT,
                ledger.reconcile(101L, fixture.snapshot.token(0), sha256("wrong-command")).decision,
                "a stageId match is insufficient without the accepted command digest");

        StageTerminalReceipt terminal = StageTerminalReceipt.succeeded(
                fixture.snapshot.token(0), fixture.commandDigest, 2L, 1L, 12L);
        ledger.freezeTerminal(registration.slot, terminal);
        ledger.complete(registration.slot);
        WorkerAssignmentLedger.Reconciliation pending = ledger.reconcile(
                101L, fixture.snapshot.token(0), fixture.commandDigest);
        assertEquals(WorkerAssignmentLedger.ReconciliationDecision.TERMINAL_PENDING,
                pending.decision);
        assertEquals(terminal, pending.receipt);

        ledger.acknowledgeTerminalReport(registration.slot, terminal);
        WorkerAssignmentLedger.Reconciliation acknowledged = ledger.reconcile(
                101L, fixture.snapshot.token(0), fixture.commandDigest);
        assertEquals(WorkerAssignmentLedger.ReconciliationDecision.TERMINAL_ACKNOWLEDGED,
                acknowledged.decision);
        assertEquals(terminal, acknowledged.receipt);
    }

    private static Fixture fixture() throws Exception {
        RunAttemptFence fence = new RunAttemptFence("run-ledger-terminal", "attempt-1", 1L);
        StageAssignmentRecord assignment = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.SINK,
                "worker", "127.0.0.1", 6100, -1, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, "artifact-ledger-terminal", 1L, Collections.singletonList(assignment));
        return new Fixture(fence, manifest, assignment,
                StageAssignmentSnapshot.initial(manifest), sha256("command"));
    }

    private static byte[] sha256(String text) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                text.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static final class Fixture {
        final RunAttemptFence fence;
        final StageAssignmentManifest manifest;
        final StageAssignmentRecord assignment;
        final StageAssignmentSnapshot snapshot;
        final byte[] commandDigest;

        Fixture(RunAttemptFence fence, StageAssignmentManifest manifest,
                StageAssignmentRecord assignment, StageAssignmentSnapshot snapshot,
                byte[] commandDigest) {
            this.fence = fence;
            this.manifest = manifest;
            this.assignment = assignment;
            this.snapshot = snapshot;
            this.commandDigest = commandDigest;
        }
    }
}
