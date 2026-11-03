package io.rowweave.run;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class RunDeadlineCommandCompatibilityTest {
    @Test
    void timeoutReadyCommandAppendsNumericIdAndRoundTripsCanonically() {
        assertEquals(1, RunWire.kindId(RunCommand.Kind.CREATE_RUN));
        assertEquals(2, RunWire.kindId(RunCommand.Kind.CREATE_ATTEMPT));
        assertEquals(3, RunWire.kindId(RunCommand.Kind.ADVANCE_ATTEMPT));
        assertEquals(4, RunWire.kindId(RunCommand.Kind.REQUEST_CANCELLATION));
        assertEquals(5, RunWire.kindId(RunCommand.Kind.CANCEL_READY_RUN));
        assertEquals(6, RunWire.kindId(RunCommand.Kind.TIMEOUT_READY_RUN));

        RunCommand command = RunCommand.timeoutReadyRun(
                "deadline", "run", 1L, 100L, "RUN_DEADLINE", null);
        byte[] body = RunCommandCanonical.encodeBody(command);
        assertEquals(6, body[4] & 0xff);
        RunCommand decoded = RunCommandCanonical.decodeBody(
                command.commandId(), body);
        assertArrayEquals(command.commandDigest(), decoded.commandDigest());
        assertArrayEquals(body, RunCommandCanonical.encodeBody(decoded));
    }
}
