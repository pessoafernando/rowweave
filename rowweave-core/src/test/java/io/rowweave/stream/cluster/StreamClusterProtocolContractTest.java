package io.rowweave.stream.cluster;

import io.rowweave.stream.contract.PortableStreamPlan;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The separate control wire and the worker capability pool must close before any cluster side effect. */
class StreamClusterProtocolContractTest {

    private static final String DIGEST = repeat('a', 64);

    @Test
    void progressObservationRequiresBothV3AndItsCapability() {
        List<String> baseline = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        List<String> progress = new ArrayList<>(baseline);
        progress.add(StreamWorkerCapabilityProfile.PROGRESS_CAPABILITY);

        assertFalse(profile("v2", "v2-inc", 2, 1, progress,
                1, 1, 1_024, 1_024L).supportsProgressObservation());
        assertFalse(profile("v3-missing", "v3-missing-inc", 3, 1,
                baseline, 1, 1, 1_024, 1_024L)
                .supportsProgressObservation());
        assertTrue(profile("v3", "v3-inc", 3, 1, progress,
                1, 1, 1_024, 1_024L).supportsProgressObservation());
    }

    @Test
    void controlKindsHaveStableIdsAndCanonicalBoundedWire() {
        assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12),
                wireIds());

        byte[] payload = new byte[] {1, 2, 3, 4};
        StreamControlMessage message = new StreamControlMessage(
                StreamControlMessage.Kind.CHECKPOINT_TRIGGER,
                7L, "coordinator-7", "run-1", "attempt-2",
                3L, 5L, "worker-inc-9", 11L, DIGEST, payload);
        byte[] encoded = StreamControlMessageCodec.encode(message);

        assertArrayEquals("RWXXSCV1".getBytes(java.nio.charset.StandardCharsets.US_ASCII),
                StreamControlMessageCodec.magic());
        assertEquals(message, StreamControlMessageCodec.decode(encoded));
        assertArrayEquals(encoded, StreamControlMessageCodec.encode(
                StreamControlMessageCodec.decode(encoded)));
        payload[0] = 99;
        assertArrayEquals(new byte[] {1, 2, 3, 4}, message.payload());

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertCode(StreamClusterProtocolException.Code.CHECKSUM_MISMATCH,
                () -> StreamControlMessageCodec.decode(corrupt));
        assertCode(StreamClusterProtocolException.Code.UNKNOWN_CONTROL_KIND,
                () -> StreamControlMessage.Kind.fromWireId(99));
        assertThrows(IllegalArgumentException.class, () -> new StreamControlMessage(
                StreamControlMessage.Kind.START, 7L, "coordinator-7",
                "run-1", "attempt-2", 3L, 5L, "worker-inc-9",
                11L, DIGEST, new byte[StreamControlMessage.MAX_PAYLOAD_BYTES + 1]));
        String malformedIdentity = new String(new char[] {'\ud800'});
        assertThrows(IllegalArgumentException.class, () -> new StreamControlMessage(
                StreamControlMessage.Kind.START, 7L, malformedIdentity,
                "run-1", "attempt-2", 3L, 5L, "worker-inc-9",
                11L, DIGEST, new byte[0]));
        assertNotEquals(new String(StreamControlMessageCodec.magic(),
                        java.nio.charset.StandardCharsets.US_ASCII),
                "RWXXSEV1", "control and data protocol domains must stay distinct");
    }

    @Test
    void capabilityPoolPlacesOnlyExactEligibleIncarnationsWithoutOversubscription() {
        List<String> required = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        StreamWorkerCapabilityProfile workerB = profile(
                "worker-b", "inc-b", required, 2, 2, 2_048, 2_000L);
        StreamWorkerCapabilityProfile workerA = profile(
                "worker-a", "inc-a", required, 2, 2, 2_048, 2_000L);
        StreamWorkerCapabilityProfile incompatible = profile(
                "worker-c", "inc-c",
                Collections.singletonList(PortableStreamPlan.STREAM_CAPABILITY),
                8, 8, 8_192, 80_000L);

        StreamTaskResourceRequest request = new StreamTaskResourceRequest(
                4, 1, 1, 1_024, 1_000L, 2, required);
        StreamWorkerPlacement placement = StreamWorkerCapabilityPool.place(
                Arrays.asList(workerB, incompatible, workerA), request);

        assertEquals(4, placement.assignments().size());
        assertAssignment(placement, 0, "worker-a", "inc-a");
        assertAssignment(placement, 1, "worker-b", "inc-b");
        assertAssignment(placement, 2, "worker-a", "inc-a");
        assertAssignment(placement, 3, "worker-b", "inc-b");
        assertEquals(2, placement.reservedTasks("worker-a", "inc-a"));
        assertEquals(2, placement.reservedTasks("worker-b", "inc-b"));
        assertEquals(0, placement.reservedTasks("worker-c", "inc-c"));

        StreamClusterProtocolException insufficient = assertThrows(
                StreamClusterProtocolException.class,
                () -> StreamWorkerCapabilityPool.place(
                        Arrays.asList(workerA, workerB),
                        new StreamTaskResourceRequest(
                                5, 1, 1, 1_024, 1_000L, 2, required)));
        assertEquals(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                insufficient.code());

        StreamWorkerCapabilityProfile replacement = profile(
                "worker-a", "inc-a-new", required, 2, 2, 2_048, 2_000L);
        assertCode(StreamClusterProtocolException.Code.AMBIGUOUS_WORKER_INCARNATION,
                () -> StreamWorkerCapabilityPool.place(
                        Arrays.asList(workerA, replacement),
                        new StreamTaskResourceRequest(
                                1, 1, 1, 1_024, 1_000L, 2, required)));
        assertThrows(IllegalArgumentException.class, () -> profile(
                "worker-d", "inc-d",
                Arrays.asList(PortableStreamPlan.STREAM_CAPABILITY,
                        new String(new char[] {'\ud800'})),
                1, 1, 1_024, 1_000L));
    }

    @Test
    void unknownRequiredCapabilityRejectsButWorkerExtensionsStayOptional() {
        List<String> baseline = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        List<String> workerExtensions = new ArrayList<>(baseline);
        workerExtensions.add("rowweave.stream.optional.future.v2");
        StreamWorkerCapabilityProfile worker = profile(
                "worker-a", "inc-a", workerExtensions,
                1, 1, 2_048, 2_000L);

        StreamWorkerPlacement accepted = StreamWorkerCapabilityPool.place(
                Collections.singletonList(worker),
                new StreamTaskResourceRequest(
                        1, 1, 1, 1_024, 1_000L, 1, baseline));
        assertEquals(1, accepted.assignments().size());

        List<String> unknownRequired = new ArrayList<>(baseline);
        unknownRequired.add("rowweave.stream.required.future.v2");
        assertCode(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                () -> StreamWorkerCapabilityPool.place(
                        Collections.singletonList(worker),
                        new StreamTaskResourceRequest(
                                1, 1, 1, 1_024, 1_000L, 1,
                                unknownRequired)));
    }

    @Test
    void protocolNegotiationAcceptsNMinusOneOnNButRejectsNOnNMinusOne() {
        List<String> v1 = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        List<String> v2 = new ArrayList<>(v1);
        v2.add(StreamWorkerCapabilityProfile
                .CHECKPOINT_TRANSFER_CAPABILITY);
        StreamWorkerCapabilityProfile current = profile(
                "worker-current", "inc-current", 2, 1, v2,
                1, 1, 2_048, 2_000L);
        StreamWorkerCapabilityProfile previous = profile(
                "worker-previous", "inc-previous", 1, 1, v1,
                1, 1, 2_048, 2_000L);

        assertEquals(1, StreamWorkerCapabilityPool.place(
                Collections.singletonList(current),
                new StreamTaskResourceRequest(
                        1, 1, 1, 1_024, 1_000L, 1, v1))
                .assignments().size(),
                "an N Worker must retain the frozen N-1 reader");
        assertCode(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                () -> StreamWorkerCapabilityPool.place(
                        Collections.singletonList(previous),
                        new StreamTaskResourceRequest(
                                1, 2, 1, 1_024, 1_000L, 1, v2)));
    }

    @Test
    void v2CheckpointTransferRequiresExplicitCapabilityBeforePlacement() {
        List<String> v1 = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        StreamWorkerCapabilityProfile versionOnly = profile(
                "worker-version-only", "inc-version-only", 2, 1, v1,
                1, 1, 2_048, 2_000L);
        List<String> required = new ArrayList<>(v1);
        required.add(StreamWorkerCapabilityProfile
                .CHECKPOINT_TRANSFER_CAPABILITY);

        assertCode(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                () -> StreamWorkerCapabilityPool.place(
                        Collections.singletonList(versionOnly),
                        new StreamTaskResourceRequest(
                                1, 2, 1, 1_024, 1_000L, 1, required)));
    }

    @Test
    void fanoutChargesEveryResidentChannelBeforePlacement() {
        List<String> required = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        StreamWorkerCapabilityProfile oneChannel = profile(
                "worker-a", "inc-a", required, 1, 1,
                2_048, 8_000L);
        StreamTaskResourceRequest twoChannels = new StreamTaskResourceRequest(
                1, 1, 1, 1_024, 2_000L, 2, 2, required);

        assertCode(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                () -> StreamWorkerCapabilityPool.place(
                        Collections.singletonList(oneChannel), twoChannels));

        StreamWorkerCapabilityProfile enoughChannels = profile(
                "worker-b", "inc-b", required, 1, 2,
                2_048, 8_000L);
        assertEquals(1, StreamWorkerCapabilityPool.place(
                Collections.singletonList(enoughChannels), twoChannels)
                .assignments().size());
    }

    @Test
    void heterogeneousNodeSubtasksArePlacedByLocalResourcesAndEligibility() {
        List<String> required = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        List<String> mapCapabilities = new ArrayList<>(required);
        mapCapabilities.add("rowweave.function.deployment.map-v1");
        StreamWorkerCapabilityProfile source = extendedProfile(
                "source-worker", "source-inc", required,
                1, 2, 2_048, 1_000L, 2, 0, 0, 64L, 0L);
        StreamWorkerCapabilityProfile computeA = extendedProfile(
                "compute-a", "compute-a-inc", mapCapabilities,
                1, 4, 4_096, 700L, 2, 10, 10, 128L, 256L);
        StreamWorkerCapabilityProfile computeB = extendedProfile(
                "compute-b", "compute-b-inc", mapCapabilities,
                1, 4, 4_096, 700L, 2, 10, 10, 128L, 256L);
        StreamWorkerCapabilityProfile sink = extendedProfile(
                "sink-worker", "sink-inc", required,
                1, 2, 2_048, 1_000L, 2, 0, 0, 64L, 0L);
        List<StreamTaskPlacementRequest> tasks = Arrays.asList(
                task("source", 0, 1_024, 400L, 1, 1,
                        0, 0, 32L, 0L, required),
                task("map", 0, 2_048, 600L, 2, 3,
                        4, 2, 64L, 128L, mapCapabilities),
                task("map", 1, 2_048, 600L, 2, 3,
                        4, 2, 64L, 128L, mapCapabilities),
                task("sink", 0, 1_024, 400L, 1, 1,
                        0, 0, 32L, 0L, required));
        BiPredicate<StreamTaskPlacementRequest,
                StreamWorkerCapabilityProfile> localProviders =
                (task, worker) -> {
                    if (task.identity().operatorUid().equals("source")) {
                        return worker.workerId().equals("source-worker");
                    }
                    if (task.identity().operatorUid().equals("sink")) {
                        return worker.workerId().equals("sink-worker");
                    }
                    return worker.workerId().startsWith("compute-");
                };

        StreamWorkerPlacement placement = StreamWorkerCapabilityPool.placeTasks(
                Arrays.asList(sink, computeB, source, computeA), tasks,
                localProviders);

        assertNodeAssignment(placement, 0, "source", 0, "source-worker");
        assertNodeAssignment(placement, 1, "map", 0, "compute-a");
        assertNodeAssignment(placement, 2, "map", 1, "compute-b");
        assertNodeAssignment(placement, 3, "sink", 0, "sink-worker");
    }

    @Test
    void heterogeneousPlacementRejectsOneOversizedNodeWithoutPartialResult() {
        List<String> required = Arrays.asList(
                PortableStreamPlan.STREAM_CAPABILITY,
                StreamWorkerCapabilityProfile.CONTROL_CAPABILITY,
                StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        StreamWorkerCapabilityProfile worker = extendedProfile(
                "worker-a", "inc-a", required,
                4, 8, 4_096, 1_000L, 8, 8, 8, 128L, 128L);
        List<StreamTaskPlacementRequest> tasks = Arrays.asList(
                task("small", 0, 1_024, 200L, 1, 1,
                        1, 1, 32L, 32L, required),
                task("oversized", 0, 8_192, 200L, 1, 1,
                        1, 1, 32L, 32L, required));

        assertCode(StreamClusterProtocolException.Code.INSUFFICIENT_CAPACITY,
                () -> StreamWorkerCapabilityPool.placeTasks(
                        Collections.singletonList(worker), tasks,
                        (task, candidate) -> true));
    }

    private static List<Integer> wireIds() {
        List<Integer> ids = new ArrayList<>();
        for (StreamControlMessage.Kind kind : StreamControlMessage.Kind.values()) {
            ids.add(kind.wireId());
        }
        return ids;
    }

    private static StreamWorkerCapabilityProfile profile(
            String workerId, String incarnation, List<String> capabilities,
            int slots, int channels, int frameBytes, long residentBytes) {
        return profile(workerId, incarnation, 1, 1, capabilities,
                slots, channels, frameBytes, residentBytes);
    }

    private static StreamWorkerCapabilityProfile profile(
            String workerId, String incarnation, int controlVersion,
            int dataVersion, List<String> capabilities,
            int slots, int channels, int frameBytes, long residentBytes) {
        return new StreamWorkerCapabilityProfile(
                workerId, incarnation, controlVersion, dataVersion,
                capabilities,
                slots, channels, frameBytes, residentBytes, slots * 2);
    }

    private static StreamWorkerCapabilityProfile extendedProfile(
            String workerId, String incarnation, List<String> capabilities,
            int slots, int channels, int frameBytes, long residentBytes,
            int threads, int timers, int windows, long checkpointBytes,
            long spillBytes) {
        return new StreamWorkerCapabilityProfile(
                workerId, incarnation, 1, 1, capabilities,
                slots, channels, frameBytes, residentBytes, threads,
                timers, windows, checkpointBytes, spillBytes);
    }

    private static StreamTaskPlacementRequest task(
            String operatorUid, int subtaskIndex, int frameBytes,
            long residentBytes, int threads, int channels, int timers,
            int windows, long checkpointBytes, long spillBytes,
            List<String> capabilities) {
        return new StreamTaskPlacementRequest(
                new StreamTaskIdentity(operatorUid, subtaskIndex),
                1, 1, frameBytes, residentBytes, threads, channels,
                timers, windows, checkpointBytes, spillBytes, capabilities);
    }

    private static void assertNodeAssignment(
            StreamWorkerPlacement placement, int ordinal,
            String operatorUid, int subtaskIndex, String workerId) {
        StreamWorkerPlacement.Assignment assignment =
                placement.assignments().get(ordinal);
        assertEquals(ordinal, assignment.taskOrdinal());
        assertEquals(operatorUid, assignment.operatorUid());
        assertEquals(subtaskIndex, assignment.subtaskIndex());
        assertEquals(workerId, assignment.workerId());
    }

    private static void assertAssignment(
            StreamWorkerPlacement placement, int taskOrdinal,
            String workerId, String incarnation) {
        StreamWorkerPlacement.Assignment assignment =
                placement.assignments().get(taskOrdinal);
        assertEquals(taskOrdinal, assignment.taskOrdinal());
        assertEquals(workerId, assignment.workerId());
        assertEquals(incarnation, assignment.workerIncarnation());
    }

    private static void assertCode(
            StreamClusterProtocolException.Code code,
            org.junit.jupiter.api.function.Executable executable) {
        StreamClusterProtocolException failure = assertThrows(
                StreamClusterProtocolException.class, executable);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
