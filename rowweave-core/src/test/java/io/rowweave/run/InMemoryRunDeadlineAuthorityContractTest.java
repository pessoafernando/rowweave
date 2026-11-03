package io.rowweave.run;

import java.util.function.LongSupplier;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryRunDeadlineAuthorityContractTest
        extends RunDeadlineAuthorityContract {
    @Override
    protected RunStateStore newStore(LongSupplier authorityClock) {
        return new InMemoryRunStateStore(
                RunRecoveryCatalogKernel.MAX_ENTRIES_PER_SHARD,
                RunLifecycleCatalogKernel.MAX_ENTRIES_PER_SHARD,
                authorityClock);
    }

    @Test
    void deadlineIntentCrossesRecoveryOwnerWithoutRemovingIt() {
        MutableClock clock = new MutableClock(100L);
        RunStateStore store = newStore(clock);
        RunSnapshot running = running(store, "owned", 100L);
        RunRecoveryOwnership ownership = (RunRecoveryOwnership) store;
        RecoveryClaim claim = ownership.claim("owned", running.revision(), 0L,
                new RecoveryOwner("master", 1L));
        assertEquals(RecoveryClaim.Code.ACQUIRED, claim.code());

        assertEquals(RunDeadlineResult.Disposition.ACCEPTED,
                authority(store).evaluateDeadline("owned").disposition());
        assertEquals(claim.token(), ownership.ownership("owned"));
        assertEquals(RunDecision.Code.RECOVERY_OWNED,
                store.apply(RunCommand.advanceAttempt(
                        "terminal-ordinary", "owned", running.revision() + 1L,
                        "attempt", 1L, AttemptStatus.TIMED_OUT, false,
                        101L, "RUN_DEADLINE", null)).code());
        RunDecision terminal = ((RecoveryFencedRunStateStore) store).applyRecovered(
                claim.token(), RunCommand.advanceAttempt(
                        "terminal-recovered", "owned", running.revision() + 1L,
                        "attempt", 1L, AttemptStatus.TIMED_OUT, false,
                        101L, "RUN_DEADLINE", null));
        assertEquals(RunDecision.Code.ACCEPTED, terminal.code());
        assertEquals(RunStatus.FAILED, terminal.snapshot().status());
        assertEquals(AttemptStatus.TIMED_OUT,
                terminal.snapshot().currentAttempt().status());
    }
}
