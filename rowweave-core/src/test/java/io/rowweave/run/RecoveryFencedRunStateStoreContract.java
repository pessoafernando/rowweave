package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Must be passed by every backend that advertises recovery-fenced mutation. */
public abstract class RecoveryFencedRunStateStoreContract {
    protected abstract RunStateStore newStore();

    @Test
    void exactOwnerFencesOrdinaryMutationAndRetainsReceiptIdempotency() {
        RunStateStore store = newStore();
        RecoveryFencedRunStateStore recovery = (RecoveryFencedRunStateStore) store;
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create", "run", "definition", "artifact", 2, 0L, 1L));
        RecoveryToken token = acquired(ownership.claim(
                "run", run.revision(), 0L, new RecoveryOwner("master-a", 1L)));
        RunCommand attempt = RunCommand.createAttempt(
                "attempt", "run", run.revision(), "attempt-1", "publication-1", 2L);

        RunDecision ordinary = store.apply(attempt);
        assertEquals(RunDecision.Code.RECOVERY_OWNED, ordinary.code());
        assertEquals(run.revision(), ordinary.snapshot().revision());
        assertEquals(1, store.events("run", 0L, 10).size());

        RunDecision recovered = recovery.applyRecovered(token, attempt);
        assertEquals(RunDecision.Code.ACCEPTED, recovered.code());
        assertEquals(RunDecision.Code.ALREADY_APPLIED,
                recovery.applyRecovered(token, attempt).code());
        assertEquals(RunDecision.Code.RECOVERY_OWNED, store.apply(attempt).code());
        assertEquals(2, store.events("run", 0L, 10).size());
    }

    @Test
    void replacementFencesOldTokenAndLetsExactNewTokenContinue() {
        RunStateStore store = newStore();
        RecoveryFencedRunStateStore recovery = (RecoveryFencedRunStateStore) store;
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-replace", "replace", "definition", "artifact", 2, 0L, 1L));
        RecoveryToken first = acquired(ownership.claim(
                "replace", run.revision(), 0L, new RecoveryOwner("master-a", 1L)));
        run = accepted(recovery, first, RunCommand.createAttempt(
                "attempt-replace", "replace", run.revision(),
                "attempt-1", "publication-1", 2L));
        RecoveryToken second = acquired(ownership.claim(
                "replace", run.revision(), first.ownershipEpoch(),
                new RecoveryOwner("master-b", 2L)));
        RunCommand dispatch = RunCommand.advanceAttempt(
                "dispatch-replace", "replace", run.revision(), "attempt-1", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null);

        assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER,
                recovery.applyRecovered(first, dispatch).code());
        RunDecision advanced = recovery.applyRecovered(second, dispatch);
        assertEquals(RunDecision.Code.ACCEPTED, advanced.code());
        assertEquals(AttemptStatus.DISPATCHING,
                advanced.snapshot().currentAttempt().status());
        assertEquals(second, ownership.ownership("replace"));
    }

    @Test
    void terminalReceiptReplayStillRequiresExactRetainedOwner() {
        RunStateStore store = newStore();
        RecoveryFencedRunStateStore recovery = (RecoveryFencedRunStateStore) store;
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-terminal", "terminal", "definition", "artifact", 1, 0L, 1L));
        RecoveryToken token = acquired(ownership.claim(
                "terminal", run.revision(), 0L, new RecoveryOwner("master-a", 1L)));
        run = accepted(recovery, token, RunCommand.createAttempt(
                "attempt-terminal", "terminal", run.revision(),
                "attempt-1", "publication-1", 2L));
        RunCommand failed = RunCommand.advanceAttempt(
                "fail-terminal", "terminal", run.revision(), "attempt-1", 1L,
                AttemptStatus.FAILED, false, 3L, "FAILED", "terminal");
        RunDecision terminal = recovery.applyRecovered(token, failed);
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        assertTrue(terminal.snapshot().status().isTerminal());

        assertEquals(RunDecision.Code.RECOVERY_OWNED, store.apply(failed).code());
        assertEquals(RunDecision.Code.ALREADY_APPLIED,
                recovery.applyRecovered(token, failed).code());
        assertEquals(token, ownership.ownership("terminal"));
        assertTrue(ownership.current(token));
    }

    @Test
    void recoveryMutationRejectsNullWrongRunAndMissingOwnerWithoutSideEffects() {
        RunStateStore store = newStore();
        RecoveryFencedRunStateStore recovery = (RecoveryFencedRunStateStore) store;
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot firstRun = accepted(store, RunCommand.createRun(
                "create-first", "first", "definition", "artifact", 1, 0L, 1L));
        accepted(store, RunCommand.createRun(
                "create-second", "second", "definition", "artifact", 1, 0L, 1L));
        RecoveryToken token = acquired(ownership.claim(
                "first", firstRun.revision(), 0L, new RecoveryOwner("master-a", 1L)));
        RunCommand secondAttempt = RunCommand.createAttempt(
                "attempt-second", "second", 1L, "attempt-1", "publication-1", 2L);

        assertThrows(IllegalArgumentException.class,
                () -> recovery.applyRecovered(null, secondAttempt));
        assertThrows(IllegalArgumentException.class,
                () -> recovery.applyRecovered(token, secondAttempt));
        RecoveryToken unowned = RecoveryToken.create(
                "second", new RecoveryOwner("master-a", 1L), 1L, 1L);
        assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER,
                recovery.applyRecovered(unowned, secondAttempt).code());
        assertEquals(1L, store.get("second").revision());
    }

    @Test
    void firstClaimAndOrdinaryMutationHaveOneWinner() throws Exception {
        RunStateStore store = newStore();
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-race", "race", "definition", "artifact", 1, 0L, 1L));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<RecoveryClaim> claim = new AtomicReference<>();
        AtomicReference<RunDecision> mutation = new AtomicReference<>();
        pool.submit(() -> {
            start.await();
            claim.set(ownership.claim("race", run.revision(), 0L,
                    new RecoveryOwner("master-a", 1L)));
            return null;
        });
        pool.submit(() -> {
            start.await();
            mutation.set(store.apply(RunCommand.createAttempt(
                    "attempt-race", "race", run.revision(),
                    "attempt-1", "publication-1", 2L)));
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        if (claim.get().code() == RecoveryClaim.Code.ACQUIRED) {
            assertEquals(RunDecision.Code.RECOVERY_OWNED, mutation.get().code());
            assertEquals(RunStatus.READY, store.get("race").status());
        } else {
            assertTrue(Arrays.asList(RecoveryClaim.Code.STALE_RUN,
                    RecoveryClaim.Code.LOST_RACE).contains(claim.get().code()));
            assertEquals(RunDecision.Code.ACCEPTED, mutation.get().code());
            assertEquals(RunStatus.RUNNING, store.get("race").status());
        }
    }

    @Test
    void ownerReplacementAndRecoveredMutationHaveOneWinner() throws Exception {
        RunStateStore store = newStore();
        RecoveryFencedRunStateStore recovery = (RecoveryFencedRunStateStore) store;
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-owner-race", "owner-race", "definition", "artifact", 1, 0L, 1L));
        RecoveryToken first = acquired(ownership.claim(
                "owner-race", run.revision(), 0L, new RecoveryOwner("master-a", 1L)));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<RecoveryClaim> replacement = new AtomicReference<>();
        AtomicReference<RunDecision> mutation = new AtomicReference<>();
        pool.submit(() -> {
            start.await();
            replacement.set(ownership.claim("owner-race", run.revision(),
                    first.ownershipEpoch(), new RecoveryOwner("master-b", 2L)));
            return null;
        });
        pool.submit(() -> {
            start.await();
            mutation.set(recovery.applyRecovered(first, RunCommand.createAttempt(
                    "attempt-owner-race", "owner-race", run.revision(),
                    "attempt-1", "publication-1", 2L)));
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        if (replacement.get().code() == RecoveryClaim.Code.ACQUIRED) {
            assertEquals(RunDecision.Code.STALE_RECOVERY_OWNER, mutation.get().code());
            assertEquals(RunStatus.READY, store.get("owner-race").status());
        } else {
            assertTrue(Arrays.asList(RecoveryClaim.Code.STALE_RUN,
                    RecoveryClaim.Code.LOST_RACE).contains(replacement.get().code()));
            assertEquals(RunDecision.Code.ACCEPTED, mutation.get().code());
            assertEquals(RunStatus.RUNNING, store.get("owner-race").status());
        }
    }

    private static RecoveryToken acquired(RecoveryClaim claim) {
        assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());
        return claim.token();
    }

    private static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }

    private static RunSnapshot accepted(RecoveryFencedRunStateStore store,
                                        RecoveryToken token, RunCommand command) {
        RunDecision decision = store.applyRecovered(token, command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }
}
