package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Fencing contract that every retention authority backend must pass. */
public abstract class RunRetentionAuthorityContract {
    protected abstract RunStateStore newStore();

    @Test
    void claimUsesExactTerminalIdentityAndInclusiveAuthorityCutoff() {
        RunStateStore store = newStore();
        RunRetentionAuthority authority = (RunRetentionAuthority) store;
        RetentionOwner owner = new RetentionOwner("master-a", 101L);

        assertEquals(RunRetentionClaim.Code.STALE_RUN,
                authority.claim("missing", 1L, Long.MAX_VALUE, owner, 0L).code());
        RunSnapshot active = accepted(store, RunCommand.createRun(
                "create-active", "active", "definition", "artifact-active", 1, 0L, 1L));
        assertEquals(RunRetentionClaim.Code.STALE_RUN,
                authority.claim("active", active.revision(), Long.MAX_VALUE, owner, 0L).code());

        RunSnapshot terminal = terminal(store, "due");
        RunLifecycleRef ref = ((RunLifecycleCatalog) store).lifecycle("due");
        assertEquals(RunLifecycleState.TERMINAL, ref.state());
        assertEquals(RunRetentionClaim.Code.STALE_RUN,
                authority.claim("due", terminal.revision() + 1L,
                        Long.MAX_VALUE, owner, 0L).code());
        assertEquals(RunRetentionClaim.Code.NOT_DUE,
                authority.claim("due", terminal.revision(),
                        ref.terminalAtMillis() - 1L, owner, 0L).code());

        RunRetentionClaim acquired = authority.claim(
                "due", terminal.revision(), ref.terminalAtMillis(), owner, 0L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, acquired.code());
        assertEquals(1L, acquired.token().ownershipEpoch());
        assertEquals(ref.terminalAtMillis(), acquired.token().terminalAtMillis());
        assertTrue(authority.current(acquired.token()));
        RunLifecycleRef deleting = ((RunLifecycleCatalog) store).lifecycle("due");
        assertEquals(RunLifecycleState.DELETING, deleting.state());
        assertEquals(acquired.token(), deleting.retentionToken());

        RunRetentionClaim replay = authority.claim(
                "due", terminal.revision(), ref.terminalAtMillis(), owner, 0L);
        assertEquals(RunRetentionClaim.Code.REPLAYED, replay.code());
        assertEquals(acquired.token(), replay.token());
    }

    @Test
    void exactObservedEpochReplacesOwnerAndFencesOldToken() {
        RunStateStore store = newStore();
        RunRetentionAuthority authority = (RunRetentionAuthority) store;
        RunSnapshot terminal = terminal(store, "replace");
        RunLifecycleRef ref = ((RunLifecycleCatalog) store).lifecycle("replace");
        RetentionOwner firstOwner = new RetentionOwner("master-a", 101L);
        RetentionOwner secondOwner = new RetentionOwner("master-b", 202L);
        RunRetentionClaim first = authority.claim(
                "replace", terminal.revision(), ref.terminalAtMillis(), firstOwner, 0L);

        assertEquals(RunRetentionClaim.Code.LOST_RACE,
                authority.claim("replace", terminal.revision(),
                        ref.terminalAtMillis(), secondOwner, 0L).code());
        RunRetentionClaim replacement = authority.claim(
                "replace", terminal.revision(), ref.terminalAtMillis(), secondOwner, 1L);
        assertEquals(RunRetentionClaim.Code.ACQUIRED, replacement.code());
        assertEquals(2L, replacement.token().ownershipEpoch());
        assertFalse(authority.current(first.token()));
        assertTrue(authority.current(replacement.token()));
        assertEquals(replacement.token(),
                ((RunLifecycleCatalog) store).lifecycle("replace").retentionToken());
    }

    @Test
    void competingReplacementClaimsFromOneObservedEpochHaveOneWinner() throws Exception {
        RunStateStore store = newStore();
        RunRetentionAuthority authority = (RunRetentionAuthority) store;
        RunSnapshot terminal = terminal(store, "race");
        RunLifecycleRef ref = ((RunLifecycleCatalog) store).lifecycle("race");
        RunRetentionClaim first = authority.claim(
                "race", terminal.revision(), ref.terminalAtMillis(),
                new RetentionOwner("master-initial", 1L), 0L);

        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<RunRetentionClaim> left = new AtomicReference<>();
        AtomicReference<RunRetentionClaim> right = new AtomicReference<>();
        pool.submit(() -> {
            start.await();
            left.set(authority.claim("race", terminal.revision(),
                    ref.terminalAtMillis(), new RetentionOwner("master-left", 2L),
                    first.token().ownershipEpoch()));
            return null;
        });
        pool.submit(() -> {
            start.await();
            right.set(authority.claim("race", terminal.revision(),
                    ref.terminalAtMillis(), new RetentionOwner("master-right", 3L),
                    first.token().ownershipEpoch()));
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(1L, Arrays.asList(left.get(), right.get()).stream()
                .filter(value -> value.code() == RunRetentionClaim.Code.ACQUIRED).count());
        assertEquals(1L, Arrays.asList(left.get(), right.get()).stream()
                .filter(value -> value.code() == RunRetentionClaim.Code.LOST_RACE).count());
        assertFalse(authority.current(first.token()));
    }

    @Test
    void malformedClaimInputCannotReachTheAuthority() {
        RunRetentionAuthority authority = (RunRetentionAuthority) newStore();
        RetentionOwner owner = new RetentionOwner("master", 1L);
        assertThrows(IllegalArgumentException.class,
                () -> authority.claim("run", 0L, 1L, owner, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> authority.claim("run", 1L, -1L, owner, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> authority.claim("run", 1L, 1L, owner, -1L));
        assertThrows(IllegalArgumentException.class,
                () -> authority.claim("run", 1L, 1L, null, 0L));
        assertThrows(IllegalArgumentException.class, () -> authority.current(null));
    }

    @Test
    void terminalTransitionAndClaimHaveOneCoherentAuthorityView() throws Exception {
        RunStateStore store = newStore();
        RunRetentionAuthority authority = (RunRetentionAuthority) store;
        RunDecision created = store.apply(RunCommand.createRun(
                "create-transition-race", "transition-race", "definition",
                "artifact", 1, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());
        long terminalRevision = created.snapshot().revision() + 1L;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        AtomicReference<RunRetentionClaim> claim = new AtomicReference<>();
        AtomicReference<RunDecision> terminal = new AtomicReference<>();
        pool.submit(() -> {
            start.await();
            claim.set(authority.claim("transition-race", terminalRevision,
                    Long.MAX_VALUE, new RetentionOwner("master", 1L), 0L));
            return null;
        });
        pool.submit(() -> {
            start.await();
            terminal.set(store.apply(RunCommand.cancelReadyRun(
                    "cancel-transition-race", "transition-race",
                    created.snapshot().revision(), 2L, "CANCELED", "terminal")));
            return null;
        });
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.get().code());
        assertTrue(claim.get().code() == RunRetentionClaim.Code.STALE_RUN
                || claim.get().code() == RunRetentionClaim.Code.ACQUIRED);
        if (claim.get().code() == RunRetentionClaim.Code.ACQUIRED) {
            assertTrue(authority.current(claim.get().token()));
        }
    }

    private static RunSnapshot terminal(RunStateStore store, String runId) {
        RunSnapshot active = accepted(store, RunCommand.createRun(
                "create-" + runId, runId, "definition", "artifact-" + runId,
                1, 0L, Long.MAX_VALUE));
        return accepted(store, RunCommand.cancelReadyRun(
                "cancel-" + runId, runId, active.revision(), Long.MAX_VALUE,
                "CANCELED", "terminal"));
    }

    private static RunSnapshot accepted(RunStateStore store, RunCommand command) {
        RunDecision decision = store.apply(command);
        assertEquals(RunDecision.Code.ACCEPTED, decision.code());
        return decision.snapshot();
    }
}
