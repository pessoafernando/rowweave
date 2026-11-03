package io.rowweave.run;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RunStateStoreKernelTest {

    @Test
    void externalBackendUsesTheSingleStateMachineAndControlledDecisionFactories() {
        RunCommand command = RunCommand.createRun(
                "command", "run", "definition", "artifact", 2, 0L, 1L);
        assertEquals("run", command.runId());

        RunDecision accepted = RunStateStoreKernel.evaluate(null, command);
        assertEquals(RunDecision.Code.ACCEPTED, accepted.code());
        assertEquals(RunDecision.Code.ALREADY_APPLIED,
                RunStateStoreKernel.replayed(accepted.snapshot(), accepted.event()).code());
        assertEquals(RunDecision.Code.COMMAND_ID_CONFLICT,
                RunStateStoreKernel.commandIdConflict(accepted.snapshot()).code());
    }
}
