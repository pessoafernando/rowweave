package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.portable.PortableDataType;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamCalcSpec;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamEventTimeSpec;
import io.rowweave.stream.api.StreamExpression;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamProcessBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** tagged outputs are typed process ports carried by edges. */
class StreamPublicTaggedOutputCompilerContractTest {

    @Test
    void consumedTagCompilesAsNamedEdgePortWithoutSyntheticOperator() {
        StreamOutputTag rejects = StreamOutputTag.of(
                "rejects", rejectSchema(), StreamChangelogMode.APPEND,
                Collections.emptyList(),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamSession session = RowWeave.createStreamSession("tagged-process");
        DataStream keyed = session.fromSource(source()).uid("source")
                .keyBy("id").uid("key");
        StreamProcessResult result = keyed.keyedProcessWithOutputs(
                process().withOutputTags(Collections.singletonList(rejects)),
                "process");
        result.main().sinkTo("main-sink", sink());
        result.output("rejects").sinkTo("reject-sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        assertEquals(5, job.plan().nodes().size());
        PortableStreamNode process = job.plan().nodes().get(2);
        assertEquals(1, ((StreamProcessBinding) process.binding())
                .outputTags().size());
        StreamEdge rejectEdge = edge(job, "reject-sink", 0);
        assertEquals("rejects", rejectEdge.sourceOutputTagId());
        assertEquals(rejectSchema().fingerprint(),
                job.plan().nodes().get(4).inputSchemaFingerprints().get(0));
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.TAGGED_OUTPUT_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_TAGGED_OUTPUT_V1));
    }

    @Test
    void downstreamCalcUsesTaggedPortSchemaAndStableKeys() {
        StreamOutputTag rejects = StreamOutputTag.of(
                "rejects", rejectSchema(), StreamChangelogMode.APPEND,
                Collections.singletonList("code"),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamSession session = RowWeave.createStreamSession("tagged-calc");
        StreamProcessResult result = session.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcessWithOutputs(process().withOutputTags(
                        Collections.singletonList(rejects)), "process");
        result.main().sinkTo("main-sink", sink());
        result.output("rejects").calc(StreamCalcSpec.of(null, Arrays.asList(
                        StreamCalcSpec.projection("code", StreamExpression.input(
                                "code", PortableDataType.STRING, false)),
                        StreamCalcSpec.projection("message", StreamExpression.input(
                                "message", PortableDataType.STRING, true)))),
                        rejectSchema())
                .uid("reject-calc").sinkTo("reject-sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        PortableStreamNode calc = node(job, "reject-calc");
        assertEquals(Collections.singletonList(rejectSchema().fingerprint()),
                calc.inputSchemaFingerprints());
        assertEquals(Collections.singletonList("code"),
                calc.stableKeyFields());
        assertEquals(Collections.singletonList("code"),
                node(job, "reject-sink").stableKeyFields());
    }

    @Test
    void taggedPortsRemainAuthoritativeThroughUnionAndTimeAssignment() {
        StreamOutputTag rejects = StreamOutputTag.of(
                "rejects", timedRejectSchema(), StreamChangelogMode.APPEND,
                Collections.singletonList("code"),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamSession session = RowWeave.createStreamSession("tagged-union-time");
        session.setTimeMode(io.rowweave.stream.api.StreamTimeMode.EVENT_TIME);
        StreamProcessResult result = session.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcessWithOutputs(process().withOutputTags(
                        Collections.singletonList(rejects)), "process");
        DataStream tagged = result.output("rejects");
        tagged.union(tagged).uid("reject-union")
                .assignEventTime(StreamEventTimeSpec.monotonic(
                        StreamExpression.input("event-time",
                                PortableDataType.TIMESTAMP, false),
                        1_000L,
                        StreamEventTimeSpec.InvalidTimestampPolicy.FAIL))
                .uid("reject-time").sinkTo("reject-sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        PortableStreamNode union = node(job, "reject-union");
        assertEquals(Arrays.asList(timedRejectSchema().fingerprint(),
                        timedRejectSchema().fingerprint()),
                union.inputSchemaFingerprints());
        assertEquals(Collections.singletonList("code"),
                union.stableKeyFields());
        assertEquals("rejects", edge(job, "reject-union", 0)
                .sourceOutputTagId());
        assertEquals("rejects", edge(job, "reject-union", 1)
                .sourceOutputTagId());
        assertEquals(timedRejectSchema().fingerprint(),
                node(job, "reject-time").inputSchemaFingerprints().get(0));
    }

    @Test
    void unionRejectsMixedActualTaggedPortChangelogModes() {
        StreamOutputTag append = StreamOutputTag.of(
                "append", rejectSchema(), StreamChangelogMode.APPEND,
                Collections.singletonList("code"),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamOutputTag upsert = StreamOutputTag.of(
                "upsert", rejectSchema(), StreamChangelogMode.UPSERT,
                Collections.singletonList("code"),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamSession session = RowWeave.createStreamSession(
                "tagged-mixed-changelog-union");
        StreamProcessResult result = session.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcessWithOutputs(process().withOutputTags(
                        Arrays.asList(append, upsert)), "process");
        result.output("append").union(result.output("upsert"))
                .uid("mixed-union").sinkTo("sink", sink());

        assertCode(StreamApiException.Code.CHANGELOG_MODE_MISMATCH,
                session::toPlanArtifact);
    }

    @Test
    void taggedPortKeyIsAuthoritativeForDownstreamStatefulProcess() {
        StreamOutputTag rejects = StreamOutputTag.of(
                "rejects", rejectSchema(), StreamChangelogMode.APPEND,
                Collections.singletonList("code"),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
        StreamSession session = RowWeave.createStreamSession("tagged-stateful");
        StreamProcessResult result = session.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcessWithOutputs(process().withOutputTags(
                        Collections.singletonList(rejects)), "process");
        result.main().sinkTo("main-sink", sink());
        result.output("rejects").keyedProcess(StreamProcessSpec.keyed(
                        reference(), rejectSchema(),
                        Collections.singletonList("code"),
                        Collections.emptyList(), Collections.emptyList(),
                        StreamEmissionLimit.of(1, 4096L),
                        StreamEmissionLimit.of(1, 4096L)))
                .uid("reject-stateful").sinkTo("reject-sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        assertEquals("rejects", edge(job, "reject-stateful", 0)
                .sourceOutputTagId());
        assertEquals(Collections.singletonList("code"),
                node(job, "reject-stateful").stableKeyFields());
    }

    @Test
    void unknownDuplicateAndUnconsumedTagsFailClosed() {
        StreamOutputTag tag = StreamOutputTag.of(
                "rejects", rejectSchema(), StreamChangelogMode.APPEND,
                Collections.emptyList(),
                StreamEmissionLimit.of(1, 1024L),
                StreamEmissionLimit.of(1, 1024L),
                StreamEmissionLimit.of(1, 1024L));
        assertThrows(IllegalArgumentException.class, () ->
                process().withOutputTags(Arrays.asList(tag, tag)));

        StreamSession session = RowWeave.createStreamSession("unused-tag");
        StreamProcessResult result = session.fromSource(source()).uid("source")
                .keyBy("id").uid("key")
                .keyedProcessWithOutputs(process().withOutputTags(
                        Collections.singletonList(tag)), "process");
        result.main().sinkTo("sink", sink());
        assertThrows(IllegalArgumentException.class,
                () -> result.output("missing"));
        assertCode(StreamApiException.Code.UNUSED_OUTPUT_TAG,
                session::toPlanArtifact);
    }

    private static StreamProcessSpec process() {
        return StreamProcessSpec.keyed(reference(), mainSchema(),
                Collections.singletonList("id"), Collections.emptyList(),
                Collections.emptyList(), StreamEmissionLimit.of(1, 4096L),
                StreamEmissionLimit.of(1, 4096L));
    }

    private static StreamEdge edge(PortableStreamJob job, String target,
            int input) {
        for (StreamEdge edge : job.plan().edges()) {
            if (edge.targetOperatorUid().equals(target)
                    && edge.targetInputIndex() == input) return edge;
        }
        throw new AssertionError("edge not found");
    }

    private static PortableStreamNode node(PortableStreamJob job, String uid) {
        for (PortableStreamNode node : job.plan().nodes()) {
            if (node.operatorUid().equals(uid)) return node;
        }
        throw new AssertionError("node not found");
    }

    private static void assertCode(StreamApiException.Code code,
            Runnable action) {
        StreamApiException failure = assertThrows(
                StreamApiException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static StreamSchema mainSchema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
    }

    private static StreamSchema rejectSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("code", "code", "code",
                        DataType.StringType, false),
                new StreamField("message", "message", "message",
                        DataType.StringType, true)));
    }

    private static StreamSchema timedRejectSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("code", "code", "code",
                        DataType.StringType, false),
                new StreamField("event-time", "event-time", "event-time",
                        DataType.TimestampType, false)));
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.exactOrdered(mainSchema(), "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference reference() {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "functions", sha256("jar"), "example.TaggedProcess",
                FunctionAbi.KEYED_PROCESS_V1, "config-v1", sha256(config),
                config, new FunctionResourceProfile(1024L, 0L, 4));
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
