package io.rowweave.cluster.rpc;

import io.rowweave.run.RunCancellationResult;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.EnumSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunCancellationRpcContractTest {
    private static final String OLD_QUERY_BATCH_SINK_OUTCOME =
            "000000870000001bffffffffffffffff0000000000000000ffffffff0000000000000000"
                    + "ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffff"
                    + "ffffffffffffffffffffffff0000000000000000ffffffffffffffffffffffffffffffff"
                    + "ffffffffffffffff00000000000000000000000a72756e2d676f6c64656e";

    @Test
    void newTypesAppendWithoutChangingRepresentativeOldFrame() {
        assertEquals(29, RpcMessage.Type.QUERY_RUN_EVENTS.ordinal());
        assertEquals(30, RpcMessage.Type.RUN_EVENTS_PAGE.ordinal());
        assertEquals(31, RpcMessage.Type.CANCEL_RUN.ordinal());
        assertEquals(32, RpcMessage.Type.RUN_CANCELLATION_RESULT.ordinal());
        assertEquals(33, RpcMessage.Type.WORKER_REGISTERED.ordinal());
        assertEquals(34, RpcMessage.Type.QUERY_QUALITY_GATE_DECISION.ordinal());
        assertEquals(35, RpcMessage.Type.QUALITY_GATE_DECISION.ordinal());
        assertEquals(36, RpcMessage.Type.EXECUTE_PROVIDER_GROUP.ordinal());
        assertEquals(37, RpcMessage.Type.PROVIDER_GROUP_RESULT.ordinal());
        assertEquals(40, RpcMessage.Type.SUBMIT_INCREMENTAL_RUN.ordinal());
        assertEquals(41, RpcMessage.Type.EXECUTE_INCREMENTAL_SOURCE_ACTION.ordinal());
        assertEquals(42, RpcMessage.Type.INCREMENTAL_SOURCE_ACTION_RESULT.ordinal());
        assertEquals(43, RpcMessage.Type.INSTALL_INCREMENTAL_CONTROL_MEMBER.ordinal());
        assertEquals(44, RpcMessage.Type.PUBLISH_INCREMENTAL_MANIFEST.ordinal());
        assertEquals(45, RpcMessage.Type.INITIALIZE_INCREMENTAL_FEED.ordinal());
        assertEquals(46, RpcMessage.Type.RENEW_PREPARED_OUTPUT.ordinal());
        assertEquals(47, RpcMessage.Type.values().length);
        assertEquals(OLD_QUERY_BATCH_SINK_OUTCOME,
                hex(RpcCodec.encode(RpcMessage.queryBatchSinkOutcome("run-golden"))));
    }

    @Test
    void requestAndEveryResultShapeRoundTripThroughBoundedUnionFields() throws Exception {
        RpcMessage request = roundTrip(RpcMessage.cancelRun("run", "request-1"));
        assertEquals(RpcMessage.Type.CANCEL_RUN, request.type());
        assertEquals("run", request.runId());
        assertEquals("request-1", request.cancelRequestId());

        for (RunCancellationResult.Disposition disposition
                : RunCancellationResult.Disposition.values()) {
            boolean absent = disposition == RunCancellationResult.Disposition.NOT_FOUND
                    || disposition == RunCancellationResult.Disposition.RETIRING;
            RunStatus status = absent ? null
                    : disposition == RunCancellationResult.Disposition.ALREADY_TERMINAL
                    ? RunStatus.FAILED
                    : disposition == RunCancellationResult.Disposition.ALREADY_CANCELED
                    ? RunStatus.CANCELED
                    : disposition == RunCancellationResult.Disposition.ACCEPTED
                    ? RunStatus.CANCELING : RunStatus.RUNNING;
            long revision = absent ? 0L : 7L;
            RpcMessage response = roundTrip(RpcMessage.runCancellationResult(
                    new RunCancellationResult("run", disposition, status, revision)));
            assertEquals(RpcMessage.Type.RUN_CANCELLATION_RESULT, response.type());
            assertTrue(response.success());
            assertEquals("run", response.runId());
            assertEquals(disposition.name(), response.runCancellationDisposition());
            assertEquals(status == null ? null : status.name(),
                    response.runCancellationStatus());
            assertEquals(revision, response.runCancellationRevision());
            assertNull(response.errorMessage());
        }

        RpcMessage failure = roundTrip(RpcMessage.runCancellationFailure(
                "run", "authority unavailable"));
        assertFalse(failure.success());
        assertEquals("authority unavailable", failure.errorMessage());
        assertNull(failure.runCancellationDisposition());
    }

    @Test
    void malformedIdentityAndAuthorizationFailClosed() {
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.cancelRun("", "request"));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.cancelRun("run", ""));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.cancelRun("run", repeat('x', 129)));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.runCancellationFailure("run", ""));

        assertTrue(RpcRequestPolicy.MASTER.allows(RpcMessage.Type.CANCEL_RUN));
        assertFalse(RpcRequestPolicy.WORKER.allows(RpcMessage.Type.CANCEL_RUN));
        assertTrue(ControlRequestAuthorization.allows(
                RpcRequestPolicy.MASTER, EnumSet.of(ControlRole.CLIENT),
                RpcMessage.Type.CANCEL_RUN));
        assertFalse(ControlRequestAuthorization.allows(
                RpcRequestPolicy.MASTER, EnumSet.of(ControlRole.WORKER),
                RpcMessage.Type.CANCEL_RUN));
    }

    private static RpcMessage roundTrip(RpcMessage message) throws Exception {
        return RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(RpcCodec.encode(message))));
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte element : bytes) value.append(String.format("%02x", element & 0xff));
        return value.toString();
    }
}

