package io.rowweave.cluster.rpc;

import io.rowweave.incremental.IncrementalRunSubmissionCodec;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** only clients may submit bounded incremental intent to Master. */
class IncrementalSubmissionRpcContractTest {

    @Test
    void incrementalSubmissionUsesTheLargeLaneAndRoundTripsExactly()
            throws Exception {
        byte[] envelope = new byte[1024 * 1024 + 1];
        envelope[0] = 7;
        RpcMessage request = RpcMessage.submitIncrementalRun(envelope);

        byte[] encoded = RpcCodec.encode(request);
        RpcMessage decoded = RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(encoded)));

        assertEquals(RpcMessage.Type.SUBMIT_INCREMENTAL_RUN, decoded.type());
        assertArrayEquals(envelope, decoded.incrementalSubmissionBytes());
        assertTrue(RpcRequestPolicy.MASTER.allows(
                RpcMessage.Type.SUBMIT_INCREMENTAL_RUN));
        assertTrue(ControlRequestAuthorization.allows(
                RpcRequestPolicy.MASTER, EnumSet.of(ControlRole.CLIENT),
                RpcMessage.Type.SUBMIT_INCREMENTAL_RUN));
        assertFalse(ControlRequestAuthorization.allows(
                RpcRequestPolicy.MASTER, EnumSet.of(ControlRole.WORKER),
                RpcMessage.Type.SUBMIT_INCREMENTAL_RUN));
    }

    @Test
    void nullEmptyAndOversizeIncrementalEnvelopesFailBeforeTransport() {
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.submitIncrementalRun(null));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.submitIncrementalRun(new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.submitIncrementalRun(new byte[
                        IncrementalRunSubmissionCodec.MAX_FRAME_BYTES + 1]));
    }
}
