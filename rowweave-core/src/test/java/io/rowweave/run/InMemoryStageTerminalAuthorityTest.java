package io.rowweave.run;

import io.rowweave.plan.StageExecutionTelemetry;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InMemoryStageTerminalAuthorityTest {

    @Test
    void terminalReceiptCommitsBeforeAckReplaysExactlyAndRejectsConflict() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-terminal-authority", 2);
        StageAssignmentSnapshot assignments = store.publish(manifest(run, 2)).snapshot();
        StageAssignmentAcceptance accepted = new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED, store.accept(accepted).code());

        StageTerminalReceipt receipt = StageTerminalReceipt.succeeded(
                accepted.token(), accepted.commandDigest(), 7L, 5L, 20L);
        StageTerminalDecision recorded = store.recordTerminal(receipt);
        assertEquals(StageTerminalDecision.Code.RECORDED, recorded.code());
        assertEquals(receipt, recorded.receipt());

        StageTerminalReceipt replay = StageTerminalReceipt.succeeded(
                accepted.token(), accepted.commandDigest(), 7L, 5L, 20L);
        assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED,
                store.recordTerminal(replay).code());
        StageTerminalReceipt conflict = StageTerminalReceipt.failed(
                accepted.token(), accepted.commandDigest(), 7L, 0L,
                "USER_CODE", digest(9), 20L);
        assertEquals(StageTerminalDecision.Code.TERMINAL_CONFLICT,
                store.recordTerminal(conflict).code());

        StageAssignmentSnapshot loaded = store.assignmentSnapshot(assignments.manifest().fence());
        assertEquals(1, loaded.terminalCount());
        assertEquals(receipt, loaded.terminal(0));
    }

    @Test
    void telemetryDifferenceDoesNotTurnTheSameBaseTerminalIntoAConflict() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-terminal-telemetry", 1);
        StageAssignmentSnapshot assignments = store.publish(manifest(run, 1)).snapshot();
        StageAssignmentAcceptance accepted = new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED, store.accept(accepted).code());

        StageTerminalReceipt first = StageTerminalReceipt.succeeded(
                accepted.token(), accepted.commandDigest(), 7L, 5L, 20L,
                StageExecutionTelemetry.unavailable());
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordTerminal(first).code());
        StageTerminalReceipt laterComplete = StageTerminalReceipt.succeeded(
                accepted.token(), accepted.commandDigest(), 7L, 5L, 20L,
                StageExecutionTelemetry.complete(1L, 2L, 3L, 4L, 5L, 6L));

        StageTerminalDecision duplicate = store.recordTerminal(laterComplete);
        assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED, duplicate.code());
        assertEquals(first, duplicate.receipt());
        assertEquals(first,
                store.assignmentSnapshot(assignments.manifest().fence()).terminal(0));
    }

    @Test
    void terminalRequiresExactAcceptanceAndLostRequiresRecoveryOwner() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-terminal-fences", 2);
        StageAssignmentSnapshot assignments = store.publish(manifest(run, 2)).snapshot();
        StageTerminalReceipt none = StageTerminalReceipt.succeeded(
                assignments.token(0), digest(1), 0L, 0L, 20L);
        assertEquals(StageTerminalDecision.Code.ACCEPTANCE_REQUIRED,
                store.recordTerminal(none).code());

        StageAssignmentAcceptance accepted = new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L);
        assertEquals(StageAcceptanceDecision.Code.ACCEPTED, store.accept(accepted).code());
        StageTerminalReceipt wrongDigest = StageTerminalReceipt.succeeded(
                accepted.token(), digest(2), 0L, 0L, 20L);
        assertEquals(StageTerminalDecision.Code.ASSIGNMENT_CONFLICT,
                store.recordTerminal(wrongDigest).code());

        StageTerminalReceipt lost = StageTerminalReceipt.lost(
                accepted.token(), accepted.commandDigest(),
                "WORKER_LOST", digest(3), 0L);
        assertEquals(StageTerminalDecision.Code.LOST_REQUIRES_RECOVERY,
                store.recordTerminal(lost).code());

        RecoveryClaim claim = store.claim(run.runId(), run.revision(), 0L,
                new RecoveryOwner("master-new", 11L));
        assertEquals(StageTerminalDecision.Code.RECOVERY_OWNED,
                store.recordTerminal(StageTerminalReceipt.succeeded(
                        accepted.token(), accepted.commandDigest(), 0L, 0L, 21L)).code());
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordTerminalRecovered(claim.token(), lost).code());
    }

    @Test
    void recoveryReplacementFencesFirstWriteButExactCommittedReplaySurvives() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-terminal-owner", 2);
        StageAssignmentSnapshot assignments = store.publish(manifest(run, 2)).snapshot();
        StageAssignmentAcceptance firstAcceptance = new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L);
        StageAssignmentAcceptance secondAcceptance = new StageAssignmentAcceptance(
                assignments.token(1), digest(2), 11L);
        store.accept(firstAcceptance);
        store.accept(secondAcceptance);

        RecoveryClaim first = store.claim(run.runId(), run.revision(), 0L,
                new RecoveryOwner("master-first", 20L));
        StageTerminalReceipt firstReceipt = StageTerminalReceipt.succeeded(
                firstAcceptance.token(), firstAcceptance.commandDigest(), 1L, 1L, 30L);
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordTerminalRecovered(first.token(), firstReceipt).code());

        RecoveryClaim replacement = store.claim(run.runId(), run.revision(),
                first.token().ownershipEpoch(), new RecoveryOwner("master-second", 21L));
        StageTerminalReceipt secondReceipt = StageTerminalReceipt.lost(
                secondAcceptance.token(), secondAcceptance.commandDigest(),
                "WORKER_REPLACED", digest(4), 0L);
        assertEquals(StageTerminalDecision.Code.RECOVERY_OWNED,
                store.recordTerminalRecovered(first.token(), secondReceipt).code());
        assertEquals(StageTerminalDecision.Code.RECORDED,
                store.recordTerminalRecovered(replacement.token(), secondReceipt).code());
        assertEquals(StageTerminalDecision.Code.ALREADY_RECORDED,
                store.recordTerminalRecovered(first.token(), firstReceipt).code());
        assertTrue(store.assignmentSnapshot(assignments.manifest().fence()).allTerminal());
    }

    @Test
    void recoveryClaimAndFirstTerminalWriteHaveOneLinearizationPoint() throws Exception {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-terminal-race", 2);
        StageAssignmentSnapshot assignments = store.publish(manifest(run, 1)).snapshot();
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L);
        store.accept(acceptance);
        StageTerminalReceipt receipt = StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 1L, 1L, 20L);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<RecoveryClaim> claim = new AtomicReference<>();
        AtomicReference<StageTerminalDecision> terminal = new AtomicReference<>();
        pool.submit(() -> {
            start.await();
            claim.set(store.claim(run.runId(), run.revision(), 0L,
                    new RecoveryOwner("master-new", 30L)));
            return null;
        });
        pool.submit(() -> {
            start.await();
            terminal.set(store.recordTerminal(receipt));
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        assertEquals(RecoveryClaim.Code.ACQUIRED, claim.get().code());
        assertTrue(terminal.get().code() == StageTerminalDecision.Code.RECORDED
                || terminal.get().code() == StageTerminalDecision.Code.RECOVERY_OWNED);
        StageAssignmentSnapshot after = store.assignmentSnapshot(assignments.manifest().fence());
        assertEquals(terminal.get().code() == StageTerminalDecision.Code.RECORDED ? 1 : 0,
                after.terminalCount());
    }

    @Test
    void terminalRunMutationAtomicallyRemovesTerminalReceipts() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunSnapshot run = attempt(store, "run-terminal-cleanup", 1);
        StageAssignmentSnapshot assignments = store.publish(manifest(run, 1)).snapshot();
        StageAssignmentAcceptance acceptance = new StageAssignmentAcceptance(
                assignments.token(0), digest(1), 10L);
        store.accept(acceptance);
        store.recordTerminal(StageTerminalReceipt.succeeded(
                acceptance.token(), acceptance.commandDigest(), 1L, 1L, 20L));

        RunDecision failed = store.apply(RunCommand.advanceAttempt(
                "fail-terminal", run.runId(), run.revision(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken(),
                AttemptStatus.FAILED, false, 30L, "FAILED", "terminal"));
        assertEquals(RunDecision.Code.ACCEPTED, failed.code());
        assertNull(store.assignmentSnapshot(assignments.manifest().fence()));
    }

    private static RunSnapshot attempt(InMemoryRunStateStore store, String runId,
                                       int maxAttempts) {
        RunDecision created = store.apply(RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact",
                maxAttempts, 0L, 1L));
        RunDecision attempted = store.apply(RunCommand.createAttempt(
                "attempt-" + runId, runId, created.snapshot().revision(),
                "attempt-1", "publication-1", 2L));
        return attempted.snapshot();
    }

    private static StageAssignmentManifest manifest(RunSnapshot run, int count) {
        RunAttemptFence fence = new RunAttemptFence(run.runId(),
                run.currentAttempt().attemptId(), run.currentAttempt().fenceToken());
        java.util.List<StageAssignmentRecord> records = new java.util.ArrayList<>();
        for (int i = 0; i < count; i++) {
            records.add(StageAssignmentRecord.create(
                    fence, 1L, i, 100L + i, i, 0,
                    i == count - 1 ? StageAssignmentRole.SINK : StageAssignmentRole.ROUTER,
                    "worker-" + i, "localhost", 7001 + i, -1,
                    "inc-" + i));
        }
        return new StageAssignmentManifest(fence, run.artifactId(), 1L, records);
    }

    private static byte[] digest(int value) {
        byte[] digest = new byte[StageTerminalReceipt.DIGEST_BYTES];
        Arrays.fill(digest, (byte) value);
        return digest;
    }
}
