package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.StreamJobClient;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamCoProcessSpec;
import io.rowweave.stream.api.StreamBroadcastStateSpec;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.PortableStreamNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** symmetric keyed co-process reaches product admission. */
class StreamProductKeyedCoProcessPlanAdmissionTest {

    @Test
    void admitsTwoCanonicalKeyedInputsAsOneStatefulOperator() {
        StreamProductExecutionGraph admitted = assertDoesNotThrow(
                () -> StreamProductExecutionGraph.from(session()
                        .toPlanArtifact()));

        assertTrue(admitted.plan.nodes().stream().anyMatch(node ->
                node.opcode() == StreamOperatorCode.KEYED_CO_PROCESS));
        assertEquals(2, admitted.sources.size());
        PortableStreamNode coProcess = admitted.nodes(
                StreamOperatorCode.KEYED_CO_PROCESS).get(0);
        assertEquals(Arrays.asList("left-key", "right-key"),
                coProcess.inputOperatorUids());
        assertEquals(2, admitted.executionGraph.node(
                coProcess.operatorUid()).inputEdges().size());
        assertEquals(StreamEdge.Partitioning.HASH,
                admitted.executionGraph.node(coProcess.operatorUid())
                        .inputEdges().get(0).partitioning());
        assertEquals(StreamEdge.Partitioning.HASH,
                admitted.executionGraph.node(coProcess.operatorUid())
                        .inputEdges().get(1).partitioning());
    }

    @Test
    void placementClaimsEveryPhysicalBroadcastAndForwardLane() {
        StreamTestGraph admitted = StreamTestGraph.from(
                configureBroadcast(RowWeave.createStreamSession(
                        "broadcast-physical-resources"), 3,
                        immutableBroadcast()).toPlanArtifact());
        List<StreamTaskPlacementRequest> requests =
                StreamNodeTaskPlacementPlanner.requests(
                        admitted.executionGraph, 1, 1,
                        Arrays.asList(
                                io.rowweave.stream.contract.PortableStreamPlan
                                        .STREAM_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .DATA_CAPABILITY));
        List<StreamWorkerPlacement.Assignment> assignments =
                new ArrayList<>();
        for (int index = 0; index < requests.size(); index++) {
            StreamTaskIdentity identity = requests.get(index).identity();
            assignments.add(new StreamWorkerPlacement.Assignment(
                    index, identity.operatorUid(), identity.subtaskIndex(),
                    "worker", "incarnation"));
        }
        StreamPhysicalExecutionPlan physical =
                StreamPhysicalExecutionPlan.from(
                        admitted.executionGraph,
                        new StreamWorkerPlacement(assignments));
        int broadcastLanes = 0;
        for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
            if (lane.partitioning() == StreamEdge.Partitioning.BROADCAST) {
                broadcastLanes++;
            }
        }
        assertEquals(9, broadcastLanes);
        for (StreamTaskPlacementRequest request : requests) {
            int inbound = 0;
            int outbound = 0;
            long credits = 0L;
            for (StreamPhysicalExecutionPlan.Lane lane : physical.lanes()) {
                if (lane.target().equals(request.identity())) {
                    inbound++;
                    credits += lane.logicalEdge().initialCreditBytes();
                }
                if (lane.source().equals(request.identity())) {
                    outbound++;
                    credits += lane.logicalEdge().initialCreditBytes();
                }
            }
            StreamExecutionGraph.ResourceRequest node =
                    admitted.executionGraph.node(
                            request.identity().operatorUid())
                            .resourceRequest();
            assertEquals(Math.max(1, inbound + outbound),
                    request.channels(), request.identity().toString());
            assertEquals(node.maximumResidentBytes() + credits
                            + 8_192L + request.channels() * 4_096L,
                    request.residentBytes(), request.identity().toString());
            assertEquals(node.threadCount() + 1 + outbound,
                    request.threads(), request.identity().toString());
        }
    }

    @Test
    void placementLeasesTheMaximumCumulativeCheckpointPayloadsPerTask() {
        StreamProductExecutionGraph admitted = StreamProductExecutionGraph
                .from(broadcastSession().toPlanArtifact());
        PortableStreamNode coProcess = admitted.nodes(
                StreamOperatorCode.KEYED_CO_PROCESS).get(0);
        StreamExecutionGraph.ResourceRequest node = admitted.executionGraph
                .node(coProcess.operatorUid()).resourceRequest();
        List<StreamTaskPlacementRequest> requests =
                StreamNodeTaskPlacementPlanner.requests(
                        admitted.executionGraph, 1, 1,
                        Arrays.asList(
                                io.rowweave.stream.contract.PortableStreamPlan
                                        .STREAM_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .DATA_CAPABILITY));
        StreamTaskPlacementRequest process = null;
        for (StreamTaskPlacementRequest request : requests) {
            if (request.identity().operatorUid().equals(
                    coProcess.operatorUid())) {
                process = request;
                break;
            }
        }
        assertTrue(process != null);
        assertEquals(Math.multiplyExact(node.checkpointBytes(), 4L),
                process.checkpointBytes(),
                "state, timer/broadcast and gate payload maxima must be cumulative");
    }

    static StreamSession session() {
        return configure(RowWeave.createStreamSession(
                "keyed-co-process-admission"));
    }

    static StreamSession session(StreamJobClient client) {
        return configure(RowWeave.createStreamSession(
                "keyed-co-process-admission",
                "keyed-co-process-admission", client));
    }

    static StreamSession broadcastSession() {
        return configureBroadcast(RowWeave.createStreamSession(
                "keyed-broadcast-process-admission"), 1,
                immutableBroadcast());
    }

    static StreamSession broadcastSession(StreamJobClient client,
            int parallelism) {
        return configureBroadcast(RowWeave.createStreamSession(
                "keyed-broadcast-process-admission",
                "keyed-broadcast-process-admission", client), parallelism,
                immutableBroadcast());
    }

    static StreamSession changelogBroadcastSession() {
        return configureBroadcast(RowWeave.createStreamSession(
                "keyed-broadcast-changelog-admission"), 1,
                StreamBroadcastStateSpec.replayableChangelog(
                        "rules", "version-1", digest("version-1"),
                        "broadcast-map-v1", 1,
                        16L, 1_048_576L, 4_096L, 2));
    }

    private static StreamSession configureBroadcast(StreamSession session,
            int parallelism, StreamBroadcastStateSpec broadcast) {
        session.setParallelism(parallelism);
        session.setMaxParallelism(128);
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 32_768L, 32, 0,
                1_048_576L, 8, 256, 1_048_576L));
        DataStream left = session.fromSource(source("left", leftSchema()))
                .uid("left")
                .calc(leftIdentityCalc(), leftSchema()).uid("left-calc")
                .keyBy("id").uid("left-key");
        StreamSourceSpec rightSource = broadcast.authorityKind()
                == StreamBroadcastStateSpec.AuthorityKind.IMMUTABLE_SNAPSHOT
                ? boundedSource("right", rightSchema())
                : source("right", rightSchema());
        DataStream right = session.fromSource(rightSource)
                .uid("right")
                .calc(rightIdentityCalc(), rightSchema()).uid("right-calc");
        left.keyedCoProcess(right,
                        specification().withBroadcastRight(broadcast))
                .uid("co-process")
                .calc(StreamCalcSpec.of(null, Arrays.asList(
                                StreamCalcSpec.projection("id",
                                        StreamExpression.input("id",
                                                PortableDataType.LONG,
                                                false)),
                                StreamCalcSpec.projection("result",
                                        StreamExpression.input("result",
                                                PortableDataType.STRING,
                                                true)))),
                        outputSchema())
                .uid("post-co-process-calc")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink-contract"),
                        new byte[] {3}));
        return session;
    }

    private static StreamCalcSpec leftIdentityCalc() {
        return StreamCalcSpec.of(null, Arrays.asList(
                StreamCalcSpec.projection("id", StreamExpression.input(
                        "id", PortableDataType.LONG, false)),
                StreamCalcSpec.projection("left-value",
                        StreamExpression.input("left-value",
                                PortableDataType.STRING, true))));
    }

    private static StreamCalcSpec rightIdentityCalc() {
        return StreamCalcSpec.of(null, Arrays.asList(
                StreamCalcSpec.projection("adaptor-id",
                        StreamExpression.input("adaptor-id",
                                PortableDataType.LONG, false)),
                StreamCalcSpec.projection("right-value",
                        StreamExpression.input("right-value",
                                PortableDataType.STRING, true))));
    }

    private static StreamBroadcastStateSpec immutableBroadcast() {
        return StreamBroadcastStateSpec.immutableSnapshot(
                "rules", "snapshot-1", digest("snapshot-1"),
                "broadcast-map-v1", 1,
                16L, 1_048_576L, 4_096L, 1);
    }

    private static StreamSession configure(StreamSession session) {
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 32_768L, 32, 0,
                1_048_576L, 8, 64, 1_048_576L));
        DataStream left = session.fromSource(source("left", leftSchema()))
                .uid("left").keyBy("id").uid("left-key");
        DataStream right = session.fromSource(source("right", rightSchema()))
                .uid("right").keyBy("adaptor-id").uid("right-key");
        left.keyedCoProcess(right, specification()).uid("co-process")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink-contract"),
                        new byte[] {3}));
        return session;
    }

    private static StreamCoProcessSpec specification() {
        StreamManagedStateSpec state = StreamManagedStateSpec.bounded(
                new StreamStateSpec("shared",
                        StreamStateSpec.Kind.KEYED_MAP,
                        "co-state-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                16, 1_000L, 1_048_576L, 4_096L);
        return StreamCoProcessSpec.keyed(function(), outputSchema(),
                Collections.singletonList("id"),
                Collections.singletonList(state), Collections.emptyList(),
                StreamEmissionLimit.of(2, 4_096L),
                StreamEmissionLimit.of(2, 4_096L),
                StreamEmissionLimit.of(1, 4_096L));
    }

    private static FunctionDeploymentReference function() {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(digest("deployment-set"),
                "stream-functions", digest("co-process-jar"),
                "example.CoProcess", FunctionAbi.KEYED_CO_PROCESS_V1,
                "stream-function-config-v1", digest(config), config,
                new FunctionResourceProfile(1_024L, 0L, 8));
    }

    private static StreamSourceSpec source(String side,
            StreamSchema schema) {
        return StreamSourceSpec.exactOrdered(schema, side + "-source-v1",
                digest(side + "-contract"),
                side.getBytes(StandardCharsets.UTF_8),
                Collections.singletonList(new StreamSplitSpec(
                        digest(side + "-split"), "sequence-v1",
                        new byte[] {2})));
    }

    private static StreamSourceSpec boundedSource(String side,
            StreamSchema schema) {
        return StreamSourceSpec.boundedExactOrdered(schema,
                side + "-source-v1", digest(side + "-contract"),
                side.getBytes(StandardCharsets.UTF_8),
                Collections.singletonList(new StreamSplitSpec(
                        digest(side + "-split"), "sequence-v1",
                        new byte[] {2})));
    }

    private static StreamSchema leftSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.LongType, false),
                new StreamField("left-value", "value", "value",
                        DataType.StringType, true)));
    }

    private static StreamSchema rightSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.LongType, false),
                new StreamField("right-value", "value", "value",
                        DataType.StringType, true)));
    }

    private static StreamSchema outputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.LongType, false),
                new StreamField("result", "result", "result",
                        DataType.StringType, true)));
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(hash.length * 2);
            for (byte current : hash) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
