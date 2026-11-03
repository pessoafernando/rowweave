package io.rowweave.run;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class RunCommandIdempotencyTest {

    @Test
    void replayIsRecognizedBeforeRevisionCasEvenAfterRunAdvanced() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunCommand create = RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L);
        RunDecision created = store.apply(create);
        assertEquals(RunDecision.Code.ACCEPTED, created.code());

        RunCommand attempt = RunCommand.createAttempt(
                "command-attempt", "run", 1L, "attempt-1", "publication-1", 2L);
        RunDecision attempted = store.apply(attempt);
        assertEquals(RunDecision.Code.ACCEPTED, attempted.code());
        assertEquals(2L, attempted.snapshot().revision());

        RunDecision replayed = store.apply(create);
        assertEquals(RunDecision.Code.ALREADY_APPLIED, replayed.code());
        assertFalse(replayed.accepted());
        assertEquals(2L, replayed.snapshot().revision());
        assertNotNull(replayed.event());
        assertEquals(1L, replayed.event().sequence());
        assertEquals("command-create", replayed.event().commandId());
        assertEquals(2, store.events("run", 0L, 100).size());
    }

    @Test
    void sameCommandIdWithDifferentCanonicalBodyFailsClosed() {
        InMemoryRunStateStore store = new InMemoryRunStateStore();
        RunDecision created = store.apply(RunCommand.createRun(
                "command-create", "run", "definition", "artifact", 2, 0L, 1L));
        assertEquals(RunDecision.Code.ACCEPTED, created.code());

        RunDecision conflict = store.apply(RunCommand.createRun(
                "command-create", "run", "different-definition", "artifact", 2, 0L, 1L));
        assertEquals(RunDecision.Code.COMMAND_ID_CONFLICT, conflict.code());
        assertEquals(1L, conflict.snapshot().revision());
        assertEquals(1, store.events("run", 0L, 100).size());
    }

    @Test
    void legacyFactoriesDeriveStableIdentityFromFullCanonicalBody() {
        RunCommand first = RunCommand.advanceAttempt(
                "run", 4L, "attempt-1", 1L,
                AttemptStatus.FAILED, false, 9L, "FAIL", "message");
        RunCommand same = RunCommand.advanceAttempt(
                "run", 4L, "attempt-1", 1L,
                AttemptStatus.FAILED, false, 9L, "FAIL", "message");
        RunCommand changed = RunCommand.advanceAttempt(
                "run", 4L, "attempt-1", 1L,
                AttemptStatus.FAILED, false, 9L, "FAIL_CHANGED", "message");

        assertEquals(first.commandId(), same.commandId());
        assertEquals("legacy-command-", first.commandId().substring(0, 15));
        assertFalse(first.commandId().equals(changed.commandId()));
        assertFalse(Arrays.equals(first.commandDigest(), changed.commandDigest()));

        byte[] exposed = first.commandDigest();
        exposed[0] ^= 1;
        assertFalse(Arrays.equals(exposed, first.commandDigest()));
        assertNotSame(first.commandDigest(), first.commandDigest());
    }

    @Test
    void explicitIdentityCoversEveryCommandFactory() {
        assertEquals("c1", RunCommand.createRun(
                "c1", "run", "definition", "artifact", 2, 0L, 1L).commandId());
        assertEquals("c2", RunCommand.createAttempt(
                "c2", "run", 1L, "attempt", "publication", 2L).commandId());
        assertEquals("c3", RunCommand.advanceAttempt(
                "c3", "run", 2L, "attempt", 1L,
                AttemptStatus.DISPATCHING, false, 3L, null, null).commandId());
        assertEquals("c4", RunCommand.requestCancellation(
                "c4", "run", 2L, "attempt", 1L,
                CancellationCause.USER, 3L, "CANCEL", "requested").commandId());
        assertEquals("c5", RunCommand.cancelReadyRun(
                "c5", "run", 2L, 3L, "CANCEL", "requested").commandId());
    }
}
