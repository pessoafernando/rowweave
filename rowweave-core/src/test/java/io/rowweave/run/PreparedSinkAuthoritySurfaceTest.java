package io.rowweave.run;

import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** terminal and prepared locator evidence are separate CAS records. */
class PreparedSinkAuthoritySurfaceTest {

    @Test
    void authorityAndRpcExposeIndependentPreparedSinkEvidence() throws Exception {
        Class<?> receipt = Class.forName(
                "io.rowweave.run.PreparedSinkReceipt");
        Class<?> decision = Class.forName(
                "io.rowweave.run.PreparedSinkDecision");
        assertEquals(decision, DurableStageAssignmentAuthority.class.getMethod(
                "recordPreparedSink", receipt).getReturnType());
        assertEquals(RpcMessage.class, RpcMessage.class.getMethod(
                "durableStageReport", String.class,
                StageTerminalReceipt.class, receipt).getReturnType());
        assertEquals(byte[].class, RpcMessage.class.getMethod(
                "preparedSinkReceiptBytes").getReturnType());
        assertEquals(receipt, StageAssignmentSnapshot.class.getMethod(
                "preparedSink", int.class).getReturnType());
    }
}
