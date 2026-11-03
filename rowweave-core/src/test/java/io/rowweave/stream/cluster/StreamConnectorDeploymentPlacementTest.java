package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** A signed connector binding must survive generic node-local placement. */
class StreamConnectorDeploymentPlacementTest {
    @Test
    void sourceAndSinkRequireTheBoundSetOnTheirOwnPlacementRequests() {
        String setId = digest("signed-connector-set");
        StreamSession session = session();
        session.setDeploymentSetId(setId);
        StreamProductExecutionGraph admitted =
                StreamProductExecutionGraph.from(session.toPlanArtifact());

        List<StreamTaskPlacementRequest> requests =
                StreamNodeTaskPlacementPlanner.requests(
                        admitted.executionGraph,
                        admitted.controlProtocolVersion(), 1,
                        admitted.executionCapabilities());

        for (StreamTaskPlacementRequest request : requests) {
            assertTrue(request.requiredCapabilities().contains(
                    PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1));
            assertTrue(request.requiredCapabilities().contains(
                    PlanArtifact.DEPLOYMENT_FEATURE_PREFIX + setId));
        }
    }

    @Test
    void legacyUnboundGraphKeepsItsOriginalPlacementContract() {
        StreamProductExecutionGraph admitted =
                StreamProductExecutionGraph.from(
                        session().toPlanArtifact());

        for (StreamTaskPlacementRequest request :
                StreamNodeTaskPlacementPlanner.requests(
                        admitted.executionGraph,
                        admitted.controlProtocolVersion(), 1,
                        admitted.executionCapabilities())) {
            assertFalse(request.requiredCapabilities().contains(
                    PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1));
        }
    }

    @Test
    void workerWithoutSignedConnectorCapabilityIsRejectedBeforeDeployment() {
        String setId = digest("signed-connector-set");
        StreamSession session = session();
        session.setDeploymentSetId(setId);
        StreamProductExecutionGraph admitted =
                StreamProductExecutionGraph.from(session.toPlanArtifact());
        List<String> legacyCapabilities = new ArrayList<>(
                admitted.executionCapabilities());
        legacyCapabilities.remove(
                PlanArtifact.FEATURE_STREAM_CONNECTOR_DEPLOYMENT_V1);
        StreamWorkerCapabilityProfile oldWorker = profile(
                legacyCapabilities);

        StreamClusterProtocolException rejected = assertThrows(
                StreamClusterProtocolException.class,
                () -> StreamNodeTaskPlacementPlanner.place(
                        admitted.executionGraph,
                        Collections.singletonList(oldWorker),
                        admitted.controlProtocolVersion(), 1,
                        admitted.executionCapabilities(),
                        (request, worker) -> worker.capabilities().containsAll(
                                request.requiredCapabilities())));
        assertEquals(StreamClusterProtocolException.Code
                .INSUFFICIENT_CAPACITY, rejected.code());

        StreamWorkerCapabilityProfile currentWorker = profile(
                admitted.executionCapabilities());
        assertEquals(2, StreamNodeTaskPlacementPlanner.place(
                admitted.executionGraph,
                Collections.singletonList(currentWorker),
                admitted.controlProtocolVersion(), 1,
                admitted.executionCapabilities(),
                (request, worker) -> worker.capabilities().containsAll(
                        request.requiredCapabilities()))
                .assignments().size());
    }

    @Test
    void processFunctionAndConnectorMustBindTheSameSignedSet() {
        PlanArtifact functionGraph =
                StreamProductKeyedCoProcessPlanAdmissionTest
                        .broadcastSession().toPlanArtifact();
        String functionSet = digest("deployment-set");

        assertEquals(functionSet,
                PlanArtifactDeploymentBinding.deploymentSetId(
                        functionGraph));
        assertThrows(RuntimeException.class,
                () -> PlanArtifactDeploymentBinding.bindStreamConnector(
                        functionGraph, digest("different-set")));
    }

    private static StreamWorkerCapabilityProfile profile(
            List<String> capabilities) {
        return new StreamWorkerCapabilityProfile("worker", "incarnation",
                2, 1, capabilities, 8, 64, 4 * 1024 * 1024,
                1024L * 1024L * 1024L, 128);
    }

    private static StreamSession session() {
        StreamSession session = RowWeave.createStreamSession("connector-placement");
        StreamSchema schema = new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType,
                        false)));
        session.fromSource(StreamSourceSpec.exactOrdered(schema,
                "source-v1", digest("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split"), "position-v1", new byte[] {2}))))
                .uid("source").sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink-contract"),
                        new byte[] {3}));
        return session;
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte current : bytes) {
                result.append(Character.forDigit((current >>> 4) & 0xf, 16));
                result.append(Character.forDigit(current & 0xf, 16));
            }
            return result.toString();
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException(impossible);
        }
    }
}
