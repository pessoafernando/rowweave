package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.api.StreamWatermarkPolicy;
import io.rowweave.stream.contract.PortableStreamPlan;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Provider-event-time sources lease both their reader and idle watchdog. */
class StreamSourceWatchdogResourceAdmissionTest {

    @Test
    void providerEventTimeSourceDeclaresAndPlacesItsWatchdogThread() {
        StreamTestGraph graph = StreamTestGraph.from(session().toPlanArtifact());
        StreamExecutionGraph.Node source = graph.executionGraph.node("source");
        assertEquals(2, source.resourceRequest().threadCount());

        List<String> capabilities = new ArrayList<>(
                graph.plan.requiredCapabilities());
        capabilities.add(StreamWorkerCapabilityProfile.CONTROL_CAPABILITY);
        capabilities.add(StreamWorkerCapabilityProfile.DATA_CAPABILITY);
        List<StreamTaskPlacementRequest> requests =
                StreamNodeTaskPlacementPlanner.requests(
                        graph.executionGraph, 1, 1, capabilities);
        StreamTaskPlacementRequest sourceRequest = null;
        for (StreamTaskPlacementRequest request : requests) {
            if (request.identity().operatorUid().equals("source")) {
                sourceRequest = request;
                break;
            }
        }
        assertEquals(4, sourceRequest.threads(),
                "reader + watchdog + task bootstrap + outbound lane");
    }

    @Test
    void workerLeaseAcceptsExactWatchdogCapacityAndRejectsOneThreadLess() {
        StreamTestGraph graph = StreamTestGraph.from(session().toPlanArtifact());
        StreamTaskPlacementRequest sourceRequest = sourceRequest(graph);
        StreamWorkerCapabilityProfile exact = profile(
                graph, sourceRequest, sourceRequest.threads(), "exact");
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                exact, Collections.emptyMap(), Collections.emptyMap())) {
            RemoteStreamWorkerEndpoint.inProcess(server).prepareOperatorTask(
                    command(exact, graph),
                    PlanArtifactCodec.encode(graph.artifact),
                    graph.maximumEvents(), "source", 0,
                    Collections.emptyList(), Collections.emptyList());
        }

        StreamWorkerCapabilityProfile shortByOne = profile(
                graph, sourceRequest, sourceRequest.threads() - 1, "short");
        try (StreamRemoteWorkerServer server = new StreamRemoteWorkerServer(
                shortByOne, Collections.emptyMap(), Collections.emptyMap())) {
            IllegalStateException rejected = assertThrows(
                    IllegalStateException.class, () ->
                    RemoteStreamWorkerEndpoint.inProcess(server)
                            .prepareOperatorTask(command(shortByOne, graph),
                                    PlanArtifactCodec.encode(graph.artifact),
                                    graph.maximumEvents(), "source", 0,
                                    Collections.emptyList(),
                                    Collections.emptyList()));
            assertTrue(rejected.getMessage().contains("ADMISSION_REJECTED"));
        }
    }

    private static StreamTaskPlacementRequest sourceRequest(
            StreamTestGraph graph) {
        List<StreamTaskPlacementRequest> requests =
                StreamNodeTaskPlacementPlanner.requests(
                        graph.executionGraph, 1, 1,
                        graph.product.executionCapabilities());
        for (StreamTaskPlacementRequest request : requests) {
            if (request.identity().operatorUid().equals("source")) {
                return request;
            }
        }
        throw new AssertionError("source placement request is missing");
    }

    private static StreamWorkerCapabilityProfile profile(
            StreamTestGraph graph, StreamTaskPlacementRequest request,
            int threads, String suffix) {
        return new StreamWorkerCapabilityProfile(
                "watchdog-worker-" + suffix,
                "watchdog-incarnation-" + suffix, 1, 1,
                graph.product.executionCapabilities(), 1,
                request.channels(), request.maximumFrameBytes(),
                request.residentBytes(), threads,
                request.timerCount(), request.windowCount(),
                request.checkpointBytes(), request.spillDiskBytes());
    }

    private static byte[] command(StreamWorkerCapabilityProfile profile,
            StreamTestGraph graph) {
        return StreamControlMessageCodec.encode(new StreamControlMessage(
                StreamControlMessage.Kind.START, 1L, "watchdog-master",
                "watchdog-run", "attempt-1", 1L, 1L,
                profile.workerIncarnation(), 1L, graph.commandDigest,
                ByteBuffer.allocate(8).putLong(1L).array()));
    }

    private static StreamSession session() {
        StreamSession session = RowWeave.createStreamSession("source-watchdog");
        session.setTimeMode(StreamTimeMode.EVENT_TIME);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 8_192L, 32, 0,
                65_536L, 8, 64, 1_048_576L));
        session.fromSource(StreamSourceSpec.exactOrderedEventTime(
                        schema(), "source-v1", digest("source"),
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(digest("split"),
                                "position-v1", new byte[] {2})),
                        StreamWatermarkPolicy.providerEvents(1_000L)))
                .uid("source").sinkTo("sink",
                StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink"), new byte[] {3}));
        return session;
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.LongType, false)));
    }

    private static String digest(String value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte one : bytes) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", one & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
