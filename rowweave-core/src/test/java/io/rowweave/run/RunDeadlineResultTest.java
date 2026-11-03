package io.rowweave.run;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RunDeadlineResultTest {
    @Test
    void validatesClosedShapes() {
        RunDeadlineResult due = new RunDeadlineResult(
                "run", RunDeadlineResult.Disposition.ACCEPTED,
                RunStatus.CANCELING, 4L, 100L, 100L);
        assertEquals("run", due.runId());
        assertEquals(RunDeadlineResult.Disposition.ACCEPTED, due.disposition());
        assertEquals(RunStatus.CANCELING, due.status());
        assertEquals(4L, due.revision());
        assertEquals(100L, due.deadlineEpochMillis());
        assertEquals(100L, due.observedAuthorityTimeMillis());

        RunDeadlineResult missing = new RunDeadlineResult(
                "run", RunDeadlineResult.Disposition.NOT_FOUND,
                null, 0L, 0L, 0L);
        assertNull(missing.status());

        assertThrows(IllegalArgumentException.class, () -> new RunDeadlineResult(
                "run", RunDeadlineResult.Disposition.ACCEPTED,
                RunStatus.READY, 1L, 100L, 100L));
        assertThrows(IllegalArgumentException.class, () -> new RunDeadlineResult(
                "run", RunDeadlineResult.Disposition.NOT_DUE,
                RunStatus.READY, 1L, 100L, 100L));
        assertThrows(IllegalArgumentException.class, () -> new RunDeadlineResult(
                "run", RunDeadlineResult.Disposition.NOT_FOUND,
                RunStatus.READY, 1L, 0L, 0L));
        assertThrows(IllegalArgumentException.class, () -> new RunDeadlineResult(
                "", RunDeadlineResult.Disposition.NOT_FOUND,
                null, 0L, 0L, 0L));
    }
}
