package io.rowweave.cluster.rpc;

import io.rowweave.cluster.Endpoint;
import io.rowweave.cluster.IncrementalSourceActionCommandCodec;
import io.rowweave.cluster.IncrementalSourceActionResultCodec;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** source actions use one Master-to-Worker large RPC lane. */
class IncrementalSourceActionRpcContractTest {

    @Test
    void commandAndResultUseDedicatedLargeFramesAndExactRoles()
            throws Exception {
        byte[] command = new byte[IncrementalSourceActionCommandCodec
                .MAX_FRAME_BYTES];
        command[0] = 1;
        RpcMessage request = RpcMessage.executeIncrementalSourceAction(
                "worker-1", command, new Endpoint("127.0.0.1", 9000),
                0x0000_0002_0000_0003L, "incarnation-1");
        RpcMessage decodedRequest = roundTrip(request);
        assertEquals(RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION,
                decodedRequest.type());
        assertArrayEquals(command, decodedRequest.incrementalSourceActionBytes());
        assertEquals("worker-1", decodedRequest.workerId());
        assertEquals("incarnation-1", decodedRequest.incarnationId());
        assertTrue(RpcRequestPolicy.WORKER.allows(request.type()));
        assertTrue(ControlRequestAuthorization.allows(
                RpcRequestPolicy.WORKER, EnumSet.of(ControlRole.MASTER),
                request.type()));
        assertFalse(ControlRequestAuthorization.allows(
                RpcRequestPolicy.WORKER, EnumSet.of(ControlRole.CLIENT),
                request.type()));

        byte[] result = new byte[IncrementalSourceActionResultCodec
                .MAX_FRAME_BYTES];
        result[0] = 2;
        RpcMessage decodedResult = roundTrip(
                RpcMessage.incrementalSourceActionResult(result));
        assertEquals(RpcMessage.Type.INCREMENTAL_SOURCE_ACTION_RESULT,
                decodedResult.type());
        assertArrayEquals(result, decodedResult.incrementalSourceActionResultBytes());
    }

    @Test
    void emptyAndOversizeActionPayloadsFailBeforeTransport() {
        Endpoint leader = new Endpoint("127.0.0.1", 9000);
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.executeIncrementalSourceAction(
                        "worker-1", new byte[0], leader, 1L, "incarnation-1"));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.executeIncrementalSourceAction(
                        "worker-1", new byte[IncrementalSourceActionCommandCodec
                                .MAX_FRAME_BYTES + 1],
                        leader, 1L, "incarnation-1"));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.incrementalSourceActionResult(new byte[
                        IncrementalSourceActionResultCodec.MAX_FRAME_BYTES + 1]));
    }

    private static RpcMessage roundTrip(RpcMessage message) throws Exception {
        byte[] encoded = RpcCodec.encode(message);
        return RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(encoded)));
    }
}
