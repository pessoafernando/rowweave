package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamCoProcessSpec;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.api.StreamTimerSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamProcessBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** keyed co-process is a bounded symmetric two-input atom. */
class StreamPublicKeyedCoProcessCompilerContractTest {

    @Test
    void compatibleKeyedInputsCompileToCanonicalTwoInputProcess() {
        StreamSession session = RowWeave.createStreamSession("co-process");
        DataStream left = session.fromSource(source("left", leftSchema()))
                .uid("left").keyBy("id").uid("left-key");
        DataStream right = session.fromSource(source("right", rightSchema()))
                .uid("right").keyBy("adaptor-id").uid("right-key");
        left.keyedCoProcess(right, spec()).uid("co-process")
                .sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        PortableStreamNode node = job.plan().nodes().get(4);
        StreamProcessBinding binding = (StreamProcessBinding) node.binding();
        assertEquals(StreamOperatorCode.KEYED_CO_PROCESS, node.opcode());
        assertEquals(Arrays.asList("left-key", "right-key"),
                node.inputOperatorUids());
        assertEquals(StreamProcessBinding.FunctionKind.KEYED_CO_PROCESS,
                binding.functionKind());
        assertEquals(3, binding.maximumLeftOutputs());
        assertEquals(4, binding.maximumRightOutputs());
        assertEquals(StreamEdge.Partitioning.HASH,
                edge(job, "co-process", 0).partitioning());
        assertEquals(StreamEdge.Partitioning.HASH,
                edge(job, "co-process", 1).partitioning());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.KEYED_CO_PROCESS_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_KEYED_CO_PROCESS_V1));
    }

    @Test
    void bothInputsMustBeKeyedWithCompatibleCanonicalKeyTypes() {
        StreamSession missingKey = RowWeave.createStreamSession("missing-key");
        DataStream left = missingKey.fromSource(
                source("left", leftSchema())).uid("left")
                .keyBy("id").uid("left-key");
        DataStream right = missingKey.fromSource(
                source("right", rightSchema())).uid("right");
        left.keyedCoProcess(right, spec()).uid("co")
                .sinkTo("sink", sink());
        assertCode(StreamApiException.Code.MISSING_STABLE_KEY,
                missingKey::toPlanArtifact);

        StreamSession mismatch = RowWeave.createStreamSession("key-mismatch");
        DataStream longKey = mismatch.fromSource(
                source("left", leftSchema())).uid("left")
                .keyBy("id").uid("left-key");
        DataStream stringKey = mismatch.fromSource(
                source("bad", stringKeySchema())).uid("right")
                .keyBy("adaptor-id").uid("right-key");
        longKey.keyedCoProcess(stringKey, spec()).uid("co")
                .sinkTo("sink", sink());
        assertCode(StreamApiException.Code.INCOMPATIBLE_KEY_CODEC,
                mismatch::toPlanArtifact);
    }

    @Test
    void coProcessRejectsWrongAbiAndCrossSessionInput() {
        StreamSession wrong = RowWeave.createStreamSession("wrong-co-abi");
        DataStream left = wrong.fromSource(source("left", leftSchema()))
                .uid("left").keyBy("id").uid("left-key");
        DataStream right = wrong.fromSource(source("right", rightSchema()))
                .uid("right").keyBy("adaptor-id").uid("right-key");
        left.keyedCoProcess(right, spec(FunctionAbi.KEYED_PROCESS_V1))
                .uid("co").sinkTo("sink", sink());
        assertCode(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                wrong::toPlanArtifact);

        StreamSession first = RowWeave.createStreamSession("first");
        StreamSession second = RowWeave.createStreamSession("second");
        assertThrows(IllegalArgumentException.class, () ->
                first.fromSource(source("first", leftSchema()))
                        .keyedCoProcess(second.fromSource(
                                source("second", rightSchema())), spec()));
    }

    @Test
    void coProcessTaggedOutputIsAConsumedNamedEdgePort() {
        StreamOutputTag unmatched = StreamOutputTag.of("unmatched",
                rightSchema(), StreamChangelogMode.APPEND,
                Collections.emptyList(),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamSession session = RowWeave.createStreamSession("co-tag");
        DataStream left = session.fromSource(source("left", leftSchema()))
                .uid("left").keyBy("id").uid("left-key");
        DataStream right = session.fromSource(source("right", rightSchema()))
                .uid("right").keyBy("adaptor-id").uid("right-key");
        StreamProcessResult result = left.keyedCoProcessWithOutputs(right,
                spec().withOutputTags(Collections.singletonList(unmatched)),
                "co-process");
        result.main().sinkTo("main-sink", sink());
        result.output("unmatched").sinkTo("tag-sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        assertEquals(1, ((StreamProcessBinding) job.plan().nodes().get(4)
                .binding()).outputTags().size());
        assertEquals("unmatched", edge(job, "tag-sink", 0)
                .sourceOutputTagId());
    }

    @Test
    void taggedPortContractIsAuthoritativeAsBinaryInput() {
        StreamOutputTag rightTag = StreamOutputTag.of("right-tag",
                rightSchema(), StreamChangelogMode.APPEND,
                Collections.singletonList("adaptor-id"),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamSession session = RowWeave.createStreamSession("tagged-co-input");
        DataStream tagged = session.fromSource(source("producer", leftSchema()))
                .uid("producer").keyBy("id").uid("producer-key")
                .keyedProcessWithOutputs(StreamProcessSpec.keyed(
                                reference(FunctionAbi.KEYED_PROCESS_V1),
                                leftSchema(), Collections.singletonList("id"),
                                Collections.emptyList(), Collections.emptyList(),
                                StreamEmissionLimit.of(1, 4096L),
                                StreamEmissionLimit.of(1, 4096L))
                                .withOutputTags(
                                        Collections.singletonList(rightTag)),
                        "producer-process")
                .output("right-tag");
        DataStream left = session.fromSource(source("left", leftSchema()))
                .uid("left").keyBy("id").uid("left-key");
        left.keyedCoProcess(tagged, spec()).uid("tagged-co")
                .sinkTo("sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        assertEquals("right-tag", edge(job, "tagged-co", 1)
                .sourceOutputTagId());
        assertEquals(rightSchema().fingerprint(), node(job, "tagged-co")
                .inputSchemaFingerprints().get(1));
    }

    private static StreamCoProcessSpec spec() {
        return spec(FunctionAbi.KEYED_CO_PROCESS_V1);
    }

    private static StreamCoProcessSpec spec(FunctionAbi abi) {
        StreamManagedStateSpec state = StreamManagedStateSpec.bounded(
                new StreamStateSpec("shared", StreamStateSpec.Kind.KEYED_MAP,
                        "co-state-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                10, 100_000L, 1024L * 1024L, 4096L);
        return StreamCoProcessSpec.keyed(reference(abi), outputSchema(),
                Collections.singletonList("id"),
                Collections.singletonList(state),
                Collections.singletonList(StreamTimerSpec.processingTime(
                        "expiry", 2, 100_000L, 1024L * 1024L)),
                StreamEmissionLimit.of(3, 32_768L),
                StreamEmissionLimit.of(4, 32_768L),
                StreamEmissionLimit.of(1, 16_384L));
    }

    private static StreamEdge edge(PortableStreamJob job, String target,
            int input) {
        for (StreamEdge edge : job.plan().edges()) {
            if (edge.targetOperatorUid().equals(target)
                    && edge.targetInputIndex() == input) return edge;
        }
        throw new AssertionError("missing edge");
    }

    private static PortableStreamNode node(PortableStreamJob job,
            String uid) {
        for (PortableStreamNode node : job.plan().nodes()) {
            if (node.operatorUid().equals(uid)) return node;
        }
        throw new AssertionError("missing node");
    }

    private static void assertCode(StreamApiException.Code code,
            Runnable action) {
        StreamApiException failure = assertThrows(
                StreamApiException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static StreamSchema leftSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
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

    private static StreamSchema stringKeySchema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, false)));
    }

    private static StreamSchema outputSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("result", "result", "result",
                        DataType.StringType, true)));
    }

    private static StreamSourceSpec source(String identity,
            StreamSchema schema) {
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"),
                new byte[] {(byte) identity.length()},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split-" + identity), "sequence-v1",
                        new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference reference(FunctionAbi abi) {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "stream-functions", sha256("jar"), "example.CoProcess", abi,
                "stream-function-config-v1", sha256(config), config,
                new FunctionResourceProfile(1024L, 0L, 8));
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
