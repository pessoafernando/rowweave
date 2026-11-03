package io.rowweave.run;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunCancellationResultTest {

    @Test
    void foundAndAbsentResultsHaveClosedValidatedShapes() {
        RunCancellationResult accepted = new RunCancellationResult(
                "run", RunCancellationResult.Disposition.ACCEPTED,
                RunStatus.CANCELING, 4L);
        assertEquals("run", accepted.runId());
        assertEquals(RunCancellationResult.Disposition.ACCEPTED,
                accepted.disposition());
        assertEquals(RunStatus.CANCELING, accepted.status());
        assertEquals(4L, accepted.revision());

        RunCancellationResult missing = new RunCancellationResult(
                "run", RunCancellationResult.Disposition.NOT_FOUND, null, 0L);
        assertNull(missing.status());
        assertEquals(0L, missing.revision());

        assertThrows(IllegalArgumentException.class, () -> new RunCancellationResult(
                "run", RunCancellationResult.Disposition.ACCEPTED, null, 1L));
        assertThrows(IllegalArgumentException.class, () -> new RunCancellationResult(
                "run", RunCancellationResult.Disposition.NOT_FOUND,
                RunStatus.READY, 1L));
        assertThrows(IllegalArgumentException.class, () -> new RunCancellationResult(
                "run", RunCancellationResult.Disposition.RETIRING, null, 1L));
        assertThrows(IllegalArgumentException.class, () -> new RunCancellationResult(
                "run", RunCancellationResult.Disposition.ACCEPTED,
                RunStatus.SUCCEEDED, 2L));
        assertThrows(IllegalArgumentException.class, () -> new RunCancellationResult(
                "run", RunCancellationResult.Disposition.ALREADY_CANCELED,
                RunStatus.FAILED, 2L));
        assertThrows(IllegalArgumentException.class, () -> new RunCancellationResult(
                "run", RunCancellationResult.Disposition.ALREADY_TERMINAL,
                RunStatus.CANCELING, 2L));
        assertThrows(IllegalArgumentException.class, () -> new RunCancellationResult(
                "", RunCancellationResult.Disposition.NOT_FOUND, null, 0L));
    }
}
