package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.StreamSession;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** the product planner admits supervised async wait. */
class StreamAsyncWaitProductAdmissionTest {

    @Test
    void orderedLookupIsAnAdmittedUnaryNodeWithDeploymentCapability() {
        StreamTestGraph plan = plan();

        assertEquals(1, plan.product.nodes(
                StreamOperatorCode.ASYNC_WAIT).size());
        assertEquals(StreamOperatorCode.ASYNC_WAIT,
                plan.onlyNode(StreamOperatorCode.ASYNC_WAIT).opcode());
        StreamAsyncBinding binding = (StreamAsyncBinding)
                plan.onlyNode(StreamOperatorCode.ASYNC_WAIT).binding();
        assertEquals(StreamAsyncBinding.Ordering.ORDERED,
                binding.ordering());
        assertEquals(4, binding.resources().maximumConcurrency());
        assertTrue(plan.product.executionCapabilities().contains(
                PlanArtifact.DEPLOYMENT_FEATURE_PREFIX
                        + function().deploymentSetId()));
    }

    @Test
    void chainedAsyncWaitsRemainIndependentNodeResourceClaims() {
        StreamTestGraph admitted = chainedPlan(8, 65_536L);
        assertEquals(2, admitted.product.nodes(
                StreamOperatorCode.ASYNC_WAIT).size());
        for (String uid : Arrays.asList("lookup-1", "lookup-2")) {
            StreamExecutionGraph.ResourceRequest resource = admitted.product
                    .executionGraph.node(uid).resourceRequest();
            assertTrue(resource.maximumResidentBytes() >= 32_768L,
                    uid + " must reserve its node-local pending-byte budget");
            assertTrue(resource.threadCount() >= 6,
                    uid + " must reserve its bounded request threads");
        }
    }

    @Test
    void placementChargesRunnerTimeoutBootstrapAndSocketLane() {
        StreamTestGraph admitted = plan();
        java.util.List<StreamTaskPlacementRequest> requests =
                StreamNodeTaskPlacementPlanner.requests(
                        admitted.executionGraph, 1, 1,
                        Arrays.asList(
                                io.rowweave.stream.contract.PortableStreamPlan
                                        .STREAM_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .CONTROL_CAPABILITY,
                                StreamWorkerCapabilityProfile
                                        .DATA_CAPABILITY));
        StreamTaskPlacementRequest lookup = null;
        for (StreamTaskPlacementRequest request : requests) {
            if (request.identity().operatorUid().equals("lookup")) {
                lookup = request;
                break;
            }
        }
        assertTrue(lookup != null);
        assertEquals(2, lookup.channels());
        assertEquals(8, lookup.threads());
        assertEquals(86_016L, lookup.residentBytes());
    }

    static StreamTestGraph plan() {
        return plan(StreamAsyncOrdering.ORDERED);
    }

    static StreamTestGraph plan(StreamAsyncOrdering ordering) {
        return plan(ordering, 1_000L, 3, 10L, 100L);
    }

    static StreamTestGraph plan(StreamAsyncOrdering ordering,
            long timeoutMillis, int maximumAttempts,
            long initialBackoffMillis, long maximumBackoffMillis) {
        return plan(ordering, timeoutMillis, maximumAttempts,
                initialBackoffMillis, maximumBackoffMillis,
                StreamEmissionLimit.of(2, 8_192L));
    }

    static StreamTestGraph plan(StreamAsyncOrdering ordering,
            long timeoutMillis, int maximumAttempts,
            long initialBackoffMillis, long maximumBackoffMillis,
            StreamEmissionLimit emissionLimit) {
        StreamSession session = RowWeave.createStreamSession(
                "async-wait-admission");
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 16_384, 65_536L, 0, 0,
                65_536L, 8, 32, 0L));
        session.fromSource(StreamSourceSpec.exactOrdered(inputSchema(),
                        "source-v1", digest("source"), new byte[] {1},
                        Collections.singletonList(new StreamSplitSpec(
                                digest("split"), "sequence-v1",
                                new byte[] {2}))))
                .uid("source")
                .asyncWait(StreamAsyncWaitSpec.of(function(), outputSchema(),
                        Collections.singletonList("id"),
                        ordering,
                        StreamAsyncReplaySpec.identityKeyed(
                                "request-id-v1", digest("replay")),
                        StreamAsyncResourceSpec.of(4, 32_768L,
                                timeoutMillis, maximumAttempts,
                                initialBackoffMillis, maximumBackoffMillis,
                                emissionLimit)))
                .uid("lookup")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink"), new byte[] {3}));
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    private static StreamTestGraph chainedPlan(
            int threads, long managedStateBytes) {
        StreamSession session = RowWeave.createStreamSession(
                "async-wait-chain");
        session.setResourceProfile(new StreamResourceProfile(
                16, 4_096, 16_384, managedStateBytes, 0, 0,
                65_536L, threads, 32, 0L));
        DataStream input = session.fromSource(StreamSourceSpec.exactOrdered(
                        inputSchema(), "source-v1", digest("source"),
                        new byte[] {1}, Collections.singletonList(
                        new StreamSplitSpec(digest("split"),
                                "sequence-v1", new byte[] {2}))))
                .uid("source");
        StreamAsyncWaitSpec first = StreamAsyncWaitSpec.of(
                function(), outputSchema(), Collections.singletonList("id"),
                StreamAsyncOrdering.ORDERED,
                StreamAsyncReplaySpec.identityKeyed(
                        "request-id-v1", digest("replay")),
                StreamAsyncResourceSpec.of(4, 32_768L,
                        1_000L, 3, 10L, 100L,
                        StreamEmissionLimit.of(2, 8_192L)));
        StreamAsyncWaitSpec second = StreamAsyncWaitSpec.of(
                function(), outputSchema(), Collections.singletonList("id"),
                StreamAsyncOrdering.UNORDERED,
                StreamAsyncReplaySpec.identityKeyed(
                        "request-id-v1", digest("replay")),
                StreamAsyncResourceSpec.of(4, 32_768L,
                        1_000L, 3, 10L, 100L,
                        StreamEmissionLimit.of(2, 8_192L)));
        input.asyncWait(first).uid("lookup-1")
                .asyncWait(second).uid("lookup-2")
                .sinkTo("sink", StreamSinkSpec.eagerAtLeastOnce(
                        "sink-v1", digest("sink"), new byte[] {3}));
        return StreamTestGraph.from(session.toPlanArtifact());
    }

    static FunctionDeploymentReference function() {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(digest("deployment-set"),
                "async-function", digest("jar"),
                "example.AsyncFunction", FunctionAbi.STREAM_ASYNC_V1,
                "config-v1", digest(config), config,
                new FunctionResourceProfile(4_096L, 0L, 2));
    }

    static StreamSchema inputSchema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id",
                        DataType.LongType, false)));
    }

    static StreamSchema outputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id",
                        DataType.LongType, false),
                new StreamField("name", "name", "name",
                        DataType.StringType, true)));
    }

    static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    static String digest(byte[] value) {
        try {
            byte[] bytes = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : bytes) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
