package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.StreamSourceAssignment;
import io.rowweave.stream.contract.StreamSourceSplit;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** GDF3: operator-task deployment is a node/lane protocol, not a role shape. */
class StreamRemoteWorkerWireTest {
    @Test
    void operatorTaskPrepareAndStartCarryIdentityAndPhysicalLanes()
            throws Exception {
        StreamTaskLaneSpec input = StreamTaskLaneSpec.input(
                "edge#source#0->map#0", 4_096, 16_384L);
        StreamTaskLaneSpec output = StreamTaskLaneSpec.endpoint(
                "edge#map#0->sink#0", "worker-sink", 19_001,
                4_096, 16_384L);
        StreamRemoteWorkerWire.Request original =
                StreamRemoteWorkerWire.Request.startOperatorTask(
                        new byte[] {1, 2}, new byte[] {3, 4}, 128,
                        "map", 0, Arrays.asList(input),
                        Arrays.asList(output));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(new DataOutputStream(bytes),
                original);
        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(StreamRemoteWorkerWire.Type.START_OPERATOR_TASK,
                decoded.type);
        assertEquals("map", decoded.operatorUid);
        assertEquals(0, decoded.subtaskIndex);
        assertEquals(1, decoded.inputLanes.size());
        assertEquals(input.laneId(), decoded.inputLanes.get(0).laneId());
        assertEquals(1, decoded.outputLanes.size());
        assertEquals("worker-sink", decoded.outputLanes.get(0).host());
        assertEquals(19_001, decoded.outputLanes.get(0).port());
        assertTrue(decoded.maximumEvents == 128);
    }

    @Test
    void operatorCheckpointCarriesParticipantIdentityWithoutChangingLegacyForm()
            throws Exception {
        StreamRemoteWorkerWire.Request original =
                StreamRemoteWorkerWire.Request.checkpoint(
                        new byte[] {9}, "aggregate", 3);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(new DataOutputStream(bytes),
                original);
        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(StreamRemoteWorkerWire.Type.CHECKPOINT, decoded.type);
        assertEquals("aggregate", decoded.operatorUid);
        assertEquals(3, decoded.subtaskIndex);
    }

    @Test
    void boundedTaskLifecycleIsAttemptScopedAndV2Only() throws Exception {
        StreamRemoteWorkerWire.Request request =
                StreamRemoteWorkerWire.Request.taskLifecycle(
                        new byte[] {4}, "source", 2);
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(
                new DataOutputStream(requestBytes), request);
        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(requestBytes.toByteArray())));
        assertEquals(StreamRemoteWorkerWire.Type.TASK_LIFECYCLE,
                decoded.type);
        assertEquals("source", decoded.operatorUid);
        assertEquals(2, decoded.subtaskIndex);
        assertEquals('2', requestBytes.toByteArray()[11]);

        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeResponse(
                new DataOutputStream(responseBytes),
                StreamRemoteWorkerWire.Response.taskLifecycle(false, true));
        StreamRemoteWorkerWire.Response response =
                StreamRemoteWorkerWire.readResponse(new DataInputStream(
                        new ByteArrayInputStream(responseBytes.toByteArray())));
        assertTrue(response.taskCompleted);
        assertTrue(!response.taskFailed);
        assertThrows(IllegalArgumentException.class, () ->
                StreamRemoteWorkerWire.Response.taskLifecycle(true, true));
    }

    @Test
    void taskProgressUsesChecksummedBoundedV3AndCannotMasqueradeAsV2()
            throws Exception {
        StreamTaskProgress progress = new StreamTaskProgress(
                "run-1", "attempt-1", 3L, 2L, 4L,
                new StreamTaskIdentity("source", 1), 5L, true,
                10L, 9L, 100L, 90L, 42L, false,
                2L, 20L, 3, 30L, 40L, 4, 1, 5_000L);
        byte[] progressFrame = StreamTaskProgressCodec.encode(progress);
        assertEquals(progress, StreamTaskProgressCodec.decode(progressFrame));
        byte[] corrupt = progressFrame.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> StreamTaskProgressCodec.decode(corrupt));

        StreamRemoteWorkerWire.Request request =
                StreamRemoteWorkerWire.Request.taskProgress(
                        new byte[] {4}, "source", 1);
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(
                new DataOutputStream(requestBytes), request);
        StreamRemoteWorkerWire.Request decodedRequest =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(requestBytes.toByteArray())));
        assertEquals(StreamRemoteWorkerWire.Type.TASK_PROGRESS,
                decodedRequest.type);
        assertEquals("RWXXSWC3", new String(
                requestBytes.toByteArray(), 4, 8,
                StandardCharsets.US_ASCII));

        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeResponse(
                new DataOutputStream(responseBytes),
                StreamRemoteWorkerWire.Response.taskProgress(progress));
        StreamRemoteWorkerWire.Response decodedResponse =
                StreamRemoteWorkerWire.readResponse(new DataInputStream(
                        new ByteArrayInputStream(responseBytes.toByteArray())));
        assertEquals(progress, decodedResponse.taskProgress);
        assertEquals("RWXXSWC3", new String(
                responseBytes.toByteArray(), 4, 8,
                StandardCharsets.US_ASCII));

        byte[] disguised = requestBytes.toByteArray().clone();
        disguised[11] = '2';
        ByteBuffer.wrap(disguised, 12, 4).putInt(2);
        assertThrows(IllegalArgumentException.class,
                () -> StreamRemoteWorkerWire.readRequest(
                        new DataInputStream(
                                new ByteArrayInputStream(disguised))));
    }

    @Test
    void transactionalCompletionTargetsOnePhysicalSinkTask()
            throws Exception {
        StreamRemoteWorkerWire.Request original =
                StreamRemoteWorkerWire.Request.checkpointComplete(
                        new byte[] {7}, "sink", 5);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(new DataOutputStream(bytes),
                original);
        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(bytes.toByteArray())));

        assertEquals(StreamRemoteWorkerWire.Type.CHECKPOINT_COMPLETE,
                decoded.type);
        assertEquals("sink", decoded.operatorUid);
        assertEquals(5, decoded.subtaskIndex);
    }

    @Test
    void checkpointPayloadChunkRoundTripsWithinTheAuthenticatedFrame()
            throws Exception {
        String digest = repeat('a');
        byte[] chunk = new byte[] {3, 1, 4, 1, 5};
        StreamRemoteWorkerWire.Request original =
                StreamRemoteWorkerWire.Request.storeCheckpointPayload(
                        new byte[] {8, 9},
                        StreamCheckpointPayloadCommandCodec.store(
                                digest, 11L, 6L, chunk));
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(
                new DataOutputStream(requestBytes), original);
        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(
                                requestBytes.toByteArray())));
        StreamCheckpointPayloadCommandCodec.Command command =
                StreamCheckpointPayloadCommandCodec.decode(
                        decoded.artifactFrame);
        assertEquals(StreamRemoteWorkerWire.Type.STORE_CHECKPOINT_PAYLOAD,
                decoded.type);
        assertEquals(6L, command.offset());
        assertArrayEquals(chunk, command.chunk());

        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeResponse(
                new DataOutputStream(responseBytes),
                StreamRemoteWorkerWire.Response
                        .checkpointPayloadChunk(chunk));
        StreamRemoteWorkerWire.Response response =
                StreamRemoteWorkerWire.readResponse(new DataInputStream(
                        new ByteArrayInputStream(
                                responseBytes.toByteArray())));
        assertArrayEquals(chunk, response.checkpointPayloadChunk);
    }

    @Test
    void frozenV1CheckpointGoldenRemainsReadable() throws Exception {
        byte[] golden = hex("0000004a525758585357433100000001"
                + "0000001a00000008000000010900000009616767726567617465"
                + "00000003889ca0ea9bc4bb9a97428e43cca5964fdfe083d2d9cd"
                + "4ac8d488da5a978a1000");

        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(golden)));

        assertEquals(StreamRemoteWorkerWire.Type.CHECKPOINT, decoded.type);
        assertEquals("aggregate", decoded.operatorUid);
        assertEquals(3, decoded.subtaskIndex);
    }

    @Test
    void payloadOpcodeWritesV2AndCannotMasqueradeAsV1() throws Exception {
        StreamRemoteWorkerWire.Request payload =
                StreamRemoteWorkerWire.Request.storeCheckpointPayload(
                        new byte[] {8, 9},
                        StreamCheckpointPayloadCommandCodec.store(
                                repeat('b'), 5L, 0L,
                                new byte[] {1, 2, 3, 4, 5}));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(
                new DataOutputStream(bytes), payload);
        byte[] encoded = bytes.toByteArray();
        assertEquals("RWXXSWC2", new String(encoded, 4, 8,
                StandardCharsets.US_ASCII));
        assertEquals(2, ByteBuffer.wrap(encoded, 12, 4).getInt());

        byte[] disguised = encoded.clone();
        disguised[11] = '1';
        ByteBuffer.wrap(disguised, 12, 4).putInt(1);
        assertThrows(IllegalArgumentException.class,
                () -> StreamRemoteWorkerWire.readRequest(
                        new DataInputStream(
                                new ByteArrayInputStream(disguised))));
    }

    @Test
    void dynamicDiscoveryCarriesAttemptSourceCursorAndBoundedResult()
            throws Exception {
        StreamRemoteWorkerWire.Request request =
                StreamRemoteWorkerWire.Request.discoverSource(
                        new byte[] {1}, new byte[] {2}, "source",
                        new byte[] {3, 4}, 17);
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(
                new DataOutputStream(requestBytes), request);
        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(requestBytes.toByteArray())));

        assertEquals(StreamRemoteWorkerWire.Type.DISCOVER_SOURCE,
                decoded.type);
        assertEquals("source", decoded.operatorUid);
        assertArrayEquals(new byte[] {3, 4}, decoded.discoveryCursorFrame);
        assertEquals(17, decoded.discoveryLimit);
        assertEquals("RWXXSWC2", new String(requestBytes.toByteArray(), 4, 8,
                StandardCharsets.US_ASCII));

        StreamRemoteWorkerWire.Response response =
                StreamRemoteWorkerWire.Response.sourceDiscovery(
                        new byte[] {5, 6, 7});
        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeResponse(
                new DataOutputStream(responseBytes), response);
        StreamRemoteWorkerWire.Response decodedResponse =
                StreamRemoteWorkerWire.readResponse(new DataInputStream(
                        new ByteArrayInputStream(responseBytes.toByteArray())));
        assertEquals(StreamRemoteWorkerWire.ResponseKind.SOURCE_DISCOVERY,
                decodedResponse.kind);
        assertArrayEquals(new byte[] {5, 6, 7},
                decodedResponse.sourceDiscoveryFrame);
    }

    @Test
    void liveSplitAssignmentCarriesExactTaskAndWorkerFence()
            throws Exception {
        StreamSourceAssignment assignment = new StreamSourceAssignment(
                "run-1", "attempt-1", "source", repeat('c'), 7L, 3,
                "worker-incarnation-1", 11L);
        byte[] assignmentFrame = StreamSourceTaskAssignmentCodec.encode(
                assignment, new StreamSourceSplit(repeat('c'),
                        "position-v1", new byte[] {4, 5}));
        StreamRemoteWorkerWire.Request original =
                StreamRemoteWorkerWire.Request.assignSourceSplit(
                        new byte[] {1, 2, 3}, "source", 3,
                        assignmentFrame);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(new DataOutputStream(bytes),
                original);

        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(bytes.toByteArray())));
        StreamSourceTaskAssignmentCodec.Value decodedAssignment =
                StreamSourceTaskAssignmentCodec.decode(
                        decoded.sourceAssignmentFrame);

        assertEquals(StreamRemoteWorkerWire.Type.ASSIGN_SOURCE_SPLIT,
                decoded.type);
        assertEquals("source", decoded.operatorUid);
        assertEquals(3, decoded.subtaskIndex);
        assertEquals(assignment, decodedAssignment.assignment());
        assertArrayEquals(new byte[] {4, 5},
                decodedAssignment.split().descriptorFrame());
        assertEquals("RWXXSWC2", new String(bytes.toByteArray(), 4, 8,
                StandardCharsets.US_ASCII));
    }

    @Test
    void splitRevocationCarriesTheExactAssignmentAndRequiresV2()
            throws Exception {
        StreamSourceAssignment assignment = new StreamSourceAssignment(
                "run-1", "attempt-1", "source", repeat('e'), 2L, 1,
                "worker-incarnation-1", 9L);
        byte[] assignmentFrame = StreamSourceTaskAssignmentCodec.encode(
                assignment, new StreamSourceSplit(repeat('e'),
                        "position-v1", new byte[] {6}));
        StreamRemoteWorkerWire.Request original =
                StreamRemoteWorkerWire.Request.revokeSourceSplit(
                        new byte[] {3, 4}, "source", 1,
                        assignmentFrame);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(new DataOutputStream(bytes),
                original);
        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(bytes.toByteArray())));
        assertEquals(StreamRemoteWorkerWire.Type.REVOKE_SOURCE_SPLIT,
                decoded.type);
        assertEquals("source", decoded.operatorUid);
        assertEquals(1, decoded.subtaskIndex);
        assertEquals(assignment,
                StreamSourceTaskAssignmentCodec.decode(
                        decoded.sourceAssignmentFrame).assignment());
        byte[] disguised = bytes.toByteArray().clone();
        disguised[11] = '1';
        ByteBuffer.wrap(disguised, 12, 4).putInt(1);
        assertThrows(IllegalArgumentException.class,
                () -> StreamRemoteWorkerWire.readRequest(
                        new DataInputStream(
                                new ByteArrayInputStream(disguised))));
    }

    @Test
    void completedSplitProgressCarriesTheFencedAssignment()
            throws Exception {
        StreamRemoteWorkerWire.Request request =
                StreamRemoteWorkerWire.Request.sourceSplitProgress(
                        new byte[] {7, 8}, "source", 2);
        ByteArrayOutputStream requestBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(
                new DataOutputStream(requestBytes), request);
        StreamRemoteWorkerWire.Request decodedRequest =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(requestBytes.toByteArray())));
        assertEquals(StreamRemoteWorkerWire.Type.SOURCE_SPLIT_PROGRESS,
                decodedRequest.type);
        assertEquals("source", decodedRequest.operatorUid);
        assertEquals(2, decodedRequest.subtaskIndex);

        StreamSourceAssignment assignment = new StreamSourceAssignment(
                "run-1", "attempt-1", "source", repeat('d'), 3L, 2,
                "worker-incarnation-1", 5L);
        byte[] frame = StreamSourceTaskAssignmentCodec.encode(assignment,
                new StreamSourceSplit(repeat('d'), "position-v1",
                        new byte[] {9}));
        ByteArrayOutputStream responseBytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeResponse(
                new DataOutputStream(responseBytes),
                StreamRemoteWorkerWire.Response.sourceSplitProgress(frame));
        StreamRemoteWorkerWire.Response decodedResponse =
                StreamRemoteWorkerWire.readResponse(new DataInputStream(
                        new ByteArrayInputStream(responseBytes.toByteArray())));
        assertEquals(StreamRemoteWorkerWire.ResponseKind
                .SOURCE_SPLIT_PROGRESS, decodedResponse.kind);
        assertEquals(assignment, StreamSourceTaskAssignmentCodec.decode(
                decodedResponse.sourceSplitProgressFrame).assignment());
    }

    @Test
    void emptyAssignmentFrameMarksACoordinatorManagedIdleSource()
            throws Exception {
        StreamRemoteWorkerWire.Request request =
                StreamRemoteWorkerWire.Request.startOperatorTask(
                        new byte[] {1}, new byte[] {2}, 8, "source", 0,
                        java.util.Collections.<StreamTaskLaneSpec>emptyList(),
                        java.util.Collections.singletonList(
                                StreamTaskLaneSpec.endpoint(
                                        "source#0->sink#0", "worker", 19_001,
                                        4_096, 16_384L)), null, new byte[0]);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        StreamRemoteWorkerWire.writeRequest(new DataOutputStream(bytes),
                request);
        StreamRemoteWorkerWire.Request decoded =
                StreamRemoteWorkerWire.readRequest(new DataInputStream(
                        new ByteArrayInputStream(bytes.toByteArray())));

        assertNotNull(decoded.sourceAssignmentFrame);
        assertEquals(0, decoded.sourceAssignmentFrame.length);
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) Integer.parseInt(
                    value.substring(index * 2, index * 2 + 2), 16);
        }
        return bytes;
    }
}
