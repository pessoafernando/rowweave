package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Must be passed by every backend that advertises durable recovery ownership. */
public abstract class RunRecoveryOwnershipContract {
    protected abstract RunStateStore newStore();

    @Test
    void claimReplayReplaceAndTerminalRetentionAreBackendIndependent() {
        RunStateStore store = newStore();
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RecoveryOwner firstOwner = new RecoveryOwner("master-a", 101L);
        RecoveryOwner secondOwner = new RecoveryOwner("master-b", 202L);

        assertEquals(RecoveryClaim.Code.STALE_RUN,
                ownership.claim("missing", 1L, 0L, firstOwner).code());
        assertNull(ownership.ownership("missing"));

        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create", "run", "definition", "artifact", 2, 0L, 1L));
        RecoveryClaim first = ownership.claim("run", run.revision(), 0L, firstOwner);
        assertEquals(RecoveryClaim.Code.ACQUIRED, first.code());
        assertEquals(1L, first.token().ownershipEpoch());
        assertEquals(1L, first.token().claimedRevision());
        assertTrue(ownership.current(first.token()));

        RecoveryClaim replay = ownership.claim("run", run.revision(), 0L, firstOwner);
        assertEquals(RecoveryClaim.Code.REPLAYED, replay.code());
        assertEquals(first.token(), replay.token());

        run = accepted((RecoveryFencedRunStateStore) store, first.token(),
                RunCommand.createAttempt(
                "attempt", "run", run.revision(), "attempt-1", "publication-1", 2L));
        RecoveryClaim laterRevision = ownership.claim(
                "run", run.revision(), first.token().ownershipEpoch(), firstOwner);
        assertEquals(RecoveryClaim.Code.REPLAYED, laterRevision.code());
        assertEquals(1L, laterRevision.token().claimedRevision());

        assertEquals(RecoveryClaim.Code.LOST_RACE,
                ownership.claim("run", run.revision(), 0L, secondOwner).code());
        assertEquals(first.token(), ownership.ownership("run"));
        RecoveryClaim replaced = ownership.claim(
                "run", run.revision(), first.token().ownershipEpoch(), secondOwner);
        assertEquals(RecoveryClaim.Code.ACQUIRED, replaced.code());
        assertEquals(2L, replaced.token().ownershipEpoch());
        assertFalse(ownership.current(first.token()));
        assertTrue(ownership.current(replaced.token()));

        run = accepted((RecoveryFencedRunStateStore) store, replaced.token(),
                RunCommand.advanceAttempt(
                "fail", "run", run.revision(), "attempt-1", 1L,
                AttemptStatus.FAILED, false, 3L, "FAILED", "terminal"));
        assertTrue(run.status().isTerminal());
        assertEquals(RecoveryClaim.Code.STALE_RUN,
                ownership.claim("run", run.revision(), 2L,
                        new RecoveryOwner("master-c", 303L)).code());
        assertEquals(replaced.token(), ownership.ownership("run"));
        assertTrue(ownership.current(replaced.token()));
    }

    @Test
    void competingClaimsFromTheSameObservedEpochPublishOnlyOneOwner() throws Exception {
        RunStateStore store = newStore();
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-race", "race", "definition", "artifact", 1, 0L, 1L));
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<RecoveryClaim> first = new AtomicReference<>();
        AtomicReference<RecoveryClaim> second = new AtomicReference<>();
        pool.submit(() -> {
            start.await();
            first.set(ownership.claim("race", run.revision(), 0L,
                    new RecoveryOwner("master-a", 1L)));
            return null;
        });
        pool.submit(() -> {
            start.await();
            second.set(ownership.claim("race", run.revision(), 0L,
                    new RecoveryOwner("master-b", 2L)));
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        long acquired = java.util.Arrays.asList(first.get(), second.get()).stream()
                .filter(claim -> claim.code() == RecoveryClaim.Code.ACQUIRED).count();
        long lost = java.util.Arrays.asList(first.get(), second.get()).stream()
                .filter(claim -> claim.code() == RecoveryClaim.Code.LOST_RACE).count();
        assertEquals(1L, acquired);
        assertEquals(1L, lost);
        assertEquals(1L, ownership.ownership("race").ownershipEpoch());
    }

    @Test
    void staleRevisionCannotPublishOrReplaceOwnership() {
        RunStateStore store = newStore();
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-stale", "stale", "definition", "artifact", 1, 0L, 1L));
        assertEquals(RecoveryClaim.Code.STALE_RUN,
                ownership.claim("stale", run.revision() + 1L, 0L,
                        new RecoveryOwner("master-a", 1L)).code());
        assertNull(ownership.ownership("stale"));
    }

    @Test
    void claimAndTerminalMutationHaveOneObservableLinearization() throws Exception {
        RunStateStore store = newStore();
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RunSnapshot run = accepted(store, RunCommand.createRun(
                "create-terminal-race", "terminal-race", "definition", "artifact", 1, 0L, 1L));
        run = accepted(store, RunCommand.createAttempt(
                "attempt-terminal-race", "terminal-race", run.revision(),
                "attempt-1", "publication-1", 2L));
        final long expectedRevision = run.revision();
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<RecoveryClaim> claim = new AtomicReference<>();
        AtomicReference<RunDecision> terminal = new AtomicReference<>();
        pool.submit(() -> {
            start.await();
            claim.set(ownership.claim("terminal-race", expectedRevision, 0L,
                    new RecoveryOwner("master-a", 1L)));
            return null;
        });
        pool.submit(() -> {
            start.await();
            terminal.set(store.apply(RunCommand.advanceAttempt(
                    "terminal", "terminal-race", expectedRevision, "attempt-1", 1L,
                    AttemptStatus.FAILED, false, 3L, "FAILED", "terminal")));
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        if (claim.get().code() == RecoveryClaim.Code.ACQUIRED) {
            assertEquals(RunDecision.Code.RECOVERY_OWNED, terminal.get().code());
            assertFalse(terminal.get().snapshot().status().isTerminal());
            assertEquals(claim.get().token(), ownership.ownership("terminal-race"));
            assertTrue(ownership.current(claim.get().token()));
        } else {
            assertTrue(claim.get().code() == RecoveryClaim.Code.STALE_RUN
                    || claim.get().code() == RecoveryClaim.Code.LOST_RACE);
            assertEquals(RunDecision.Code.ACCEPTED, terminal.get().code());
            assertTrue(terminal.get().snapshot().status().isTerminal());
            assertNull(ownership.ownership("terminal-race"));
        }
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
