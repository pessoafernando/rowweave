package io.rowweave.cluster.rpc;

import io.rowweave.run.AttemptStatus;
import io.rowweave.run.RunExecutionEvent;
import io.rowweave.run.RunExecutionEventPage;
import io.rowweave.run.RunExecutionEventPageCodec;
import io.rowweave.run.RunStatus;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RunExecutionEventRpcContractTest {

    private static final String OLD_QUERY_BATCH_SINK_OUTCOME =
            "000000870000001bffffffffffffffff0000000000000000ffffffff0000000000000000"
                    + "ffffffff000000000000000000ffffffff0000000000000000ffffffffffffffffffffffff"
                    + "ffffffffffffffffffffffff0000000000000000ffffffffffffffffffffffffffffffff"
                    + "ffffffffffffffff00000000000000000000000a72756e2d676f6c64656e";

    @Test
    void newTypesAppendOrdinalsAndRepresentativeOldFrameIsByteIdentical() {
        assertEquals(28, RpcMessage.Type.BATCH_SINK_OUTCOME.ordinal());
        assertEquals(29, RpcMessage.Type.QUERY_RUN_EVENTS.ordinal());
        assertEquals(30, RpcMessage.Type.RUN_EVENTS_PAGE.ordinal());
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
    void queryAndSuccessOrFailureResponsesRoundTripThroughExistingUnionFields() throws Exception {
        RpcMessage query = roundTrip(RpcMessage.queryRunEvents("run-events-1", 41L, 256));
        assertEquals(RpcMessage.Type.QUERY_RUN_EVENTS, query.type());
        assertEquals("run-events-1", query.runId());
        assertEquals(41L, query.eventAfterSequence());
        assertEquals(256, query.eventLimit());
        assertNull(query.runEventPageBytes());

        byte[] pageBytes = RunExecutionEventPageCodec.encode(
                RunExecutionEventPage.found(41L,
                        Collections.singletonList(event(42L)), false));
        RpcMessage success = roundTrip(
                RpcMessage.runEventsPage("run-events-1", pageBytes));
        assertEquals(RpcMessage.Type.RUN_EVENTS_PAGE, success.type());
        assertEquals("run-events-1", success.runId());
        assertTrue(success.success());
        assertArrayEquals(pageBytes, success.runEventPageBytes());
        assertNull(success.errorMessage());

        RpcMessage failure = roundTrip(
                RpcMessage.runEventsPageFailure("run-events-1", "authority unavailable"));
        assertEquals(RpcMessage.Type.RUN_EVENTS_PAGE, failure.type());
        assertEquals("run-events-1", failure.runId());
        assertFalse(failure.success());
        assertNull(failure.runEventPageBytes());
        assertEquals("authority unavailable", failure.errorMessage());
    }

    @Test
    void requestAndResponseFactoriesRejectOutOfBoundsBeforeEncoding() {
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.queryRunEvents("run", -1L, 1));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.queryRunEvents("run", 0L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.queryRunEvents("run", 0L, 257));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.runEventsPage("run", null));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.runEventsPage(
                        "run", new byte[RunExecutionEventPageCodec.MAX_FRAME_BYTES + 1]));
        assertThrows(IllegalArgumentException.class,
                () -> RpcMessage.runEventsPageFailure("run", ""));
    }

    @Test
    void eventPagesRemainNormalFramesAndSecureSuffixRemainsLast() throws Exception {
        byte[] oversizedHeader = java.nio.ByteBuffer.allocate(8)
                .putInt(1024 * 1024 + 1)
                .putInt(RpcMessage.Type.RUN_EVENTS_PAGE.ordinal())
                .array();
        java.io.IOException tooLarge = assertThrows(java.io.IOException.class, () ->
                RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(oversizedHeader))));
        assertTrue(tooLarge.getMessage().contains("max=1048576"), tooLarge.getMessage());

        RpcSecurityMetadata security = RpcSecurityMetadata.of(repeat('a'), repeat('b'));
        byte[] encoded = RpcCodec.encode(RpcMessage.queryRunEvents("run", 0L, 1)
                .withSecurityMetadata(security));
        int suffixStart = encoded.length - RpcCodec.SECURE_SUFFIX_BYTES;
        assertEquals("RWXXRPCS1", new String(encoded, suffixStart,
                RpcCodec.SECURE_MAGIC_BYTES, java.nio.charset.StandardCharsets.US_ASCII));
        RpcMessage decoded = RpcCodec.decode(
                new DataInputStream(new ByteArrayInputStream(encoded)));
        assertEquals(security.deploymentSetId(), decoded.securityMetadata().deploymentSetId());
    }

    private static RunExecutionEvent event(long sequence) {
        return RunExecutionEvent.of(sequence, RunExecutionEvent.Type.ATTEMPT_ADVANCED,
                RunStatus.RUNNING, RunStatus.RUNNING,
                AttemptStatus.DISPATCHING, AttemptStatus.RUNNING,
                1000L + sequence, RunExecutionEvent.DiagnosticCategory.NONE,
                "0001020304050607");
    }

    private static RpcMessage roundTrip(RpcMessage message) throws Exception {
        return RpcCodec.decode(new DataInputStream(
                new ByteArrayInputStream(RpcCodec.encode(message))));
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte element : bytes) value.append(String.format("%02x", element & 0xff));
        return value.toString();
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }
}

