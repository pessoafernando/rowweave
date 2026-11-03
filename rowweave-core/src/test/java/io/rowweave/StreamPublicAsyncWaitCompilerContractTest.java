package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamAsyncOrdering;
import io.rowweave.stream.api.StreamAsyncReplaySpec;
import io.rowweave.stream.api.StreamAsyncResourceSpec;
import io.rowweave.stream.api.StreamAsyncWaitSpec;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamAsyncBinding;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** async wait is capacity-bounded and replay-authoritative. */
class StreamPublicAsyncWaitCompilerContractTest {

    @Test
    void exactlyOnceOrderedLookupRoundTripsReplayAndCapacityContract() {
        StreamSession session = RowWeave.createStreamSession("async-lookup");
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        session.setCheckpointConfig(StreamCheckpointConfig.periodic(
                10_000L, 5_000L, 1));
        session.fromSource(source()).uid("source")
                .asyncWait(specification(StreamAsyncOrdering.ORDERED,
                        StreamAsyncReplaySpec.immutableVersion(
                                "customer-snapshot-7", sha256("snapshot"))))
                .uid("lookup").sinkTo("sink", transactionalSink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        assertEquals(StreamOperatorCode.ASYNC_WAIT,
                job.plan().nodes().get(1).opcode());
        StreamAsyncBinding binding = (StreamAsyncBinding)
                job.plan().nodes().get(1).binding();
        assertEquals(StreamAsyncBinding.Ordering.ORDERED, binding.ordering());
        assertEquals(StreamAsyncBinding.ReplayMode.IMMUTABLE_VERSION,
                binding.replayMode());
        assertEquals(16, binding.resources().maximumConcurrency());
        assertEquals(3, binding.resources().maximumAttempts());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.ASYNC_WAIT_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_ASYNC_WAIT_V1));
    }

    @Test
    void atLeastOnceAllowsUnorderedNonReplayStableProvider() {
        StreamSession session = RowWeave.createStreamSession("async-best-effort");
        session.fromSource(source()).uid("source")
                .asyncWait(specification(StreamAsyncOrdering.UNORDERED,
                        StreamAsyncReplaySpec.atLeastOnce()))
                .uid("lookup").sinkTo("sink", eagerSink());
        StreamAsyncBinding binding = (StreamAsyncBinding)
                PortableStreamJobArtifact.decodeValidated(
                        session.toPlanArtifact()).plan().nodes().get(1).binding();
        assertEquals(StreamAsyncBinding.Ordering.UNORDERED,
                binding.ordering());
        assertEquals(StreamAsyncBinding.ReplayMode.AT_LEAST_ONCE,
                binding.replayMode());
    }

    @Test
    void exactOnceWrongAbiAndUnboundedResourcesFailClosed() {
        StreamSession exact = RowWeave.createStreamSession("unsafe-async");
        exact.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        exact.setCheckpointConfig(StreamCheckpointConfig.periodic(
                10_000L, 5_000L, 1));
        exact.fromSource(source()).uid("source")
                .asyncWait(specification(StreamAsyncOrdering.ORDERED,
                        StreamAsyncReplaySpec.atLeastOnce()))
                .uid("lookup").sinkTo("sink", transactionalSink());
        assertCode(StreamApiException.Code.DELIVERY_GUARANTEE_MISMATCH,
                exact::toPlanArtifact);

        assertThrows(IllegalArgumentException.class, () ->
                StreamAsyncWaitSpec.of(reference(FunctionAbi.MAP_V1),
                        outputSchema(), Collections.singletonList("id"),
                        StreamAsyncOrdering.ORDERED,
                        StreamAsyncReplaySpec.identityKeyed(
                                "request-id-v1", sha256("identity")),
                        resources()));
        assertThrows(IllegalArgumentException.class, () ->
                StreamAsyncResourceSpec.of(0, 1024L, 1000L, 3,
                        10L, 100L, StreamEmissionLimit.of(1, 1024L)));
        assertThrows(IllegalArgumentException.class, () ->
                StreamAsyncResourceSpec.of(16, 1024L, 1000L, 3,
                        100L, 10L, StreamEmissionLimit.of(1, 1024L)));
    }

    private static StreamAsyncWaitSpec specification(
            StreamAsyncOrdering ordering, StreamAsyncReplaySpec replay) {
        return StreamAsyncWaitSpec.of(reference(FunctionAbi.STREAM_ASYNC_V1),
                outputSchema(), Collections.singletonList("id"), ordering,
                replay, resources());
    }

    private static StreamAsyncResourceSpec resources() {
        return StreamAsyncResourceSpec.of(16, 1024L * 1024L, 5_000L, 3,
                100L, 1_000L, StreamEmissionLimit.of(2, 16L * 1024L));
    }

    private static StreamSchema inputSchema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
    }

    private static StreamSchema outputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("name", "name", "name",
                        DataType.StringType, true)));
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.exactOrdered(inputSchema(), "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec eagerSink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static StreamSinkSpec transactionalSink() {
        return StreamSinkSpec.checkpointTransactional(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference reference(FunctionAbi abi) {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "async-function", sha256("jar"), "example.AsyncLookup",
                abi, "config-v1", sha256(config), config,
                new FunctionResourceProfile(1024L * 1024L, 0L, 4));
    }

    private static void assertCode(StreamApiException.Code code,
            Runnable action) {
        StreamApiException failure = assertThrows(
                StreamApiException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static String sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) result.append(String.format(
                    java.util.Locale.ROOT, "%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
