package io.rowweave.stream.cluster;

import io.rowweave.RowWeave;
import io.rowweave.DataStream;
import io.rowweave.DataType;
import io.rowweave.StreamProcessResult;
import io.rowweave.StreamSession;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamManagedStateSpec;
import io.rowweave.stream.api.StreamOutputTag;
import io.rowweave.stream.api.StreamProcessSpec;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamStateSpec;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** A task input is the selected output port contract, not its producer main port. */
class StreamTaggedEdgeInputContractTest {

    @Test
    void taggedEdgeCarriesItsOwnSchemaKeyAndChangelogIntoTaskContext() {
        StreamOutputTag tag = StreamOutputTag.of(
                "rejects", tagSchema(), StreamChangelogMode.UPSERT,
                Collections.singletonList("code"),
                StreamEmissionLimit.of(1, 4_096L),
                StreamEmissionLimit.of(1, 4_096L),
                StreamEmissionLimit.of(1, 4_096L));
        StreamSession session = RowWeave.createStreamSession("tag-input-contract");
        session.setChangelogMode(StreamChangelogMode.APPEND);
        session.setResourceProfile(new StreamResourceProfile(
                8, 4_096, 8_192, 8_192L, 32, 0,
                65_536L, 8, 64, 1_048_576L));
        DataStream keyed = session.fromSource(source()).uid("source")
                .keyBy("id").uid("key");
        StreamProcessResult result = keyed.keyedProcessWithOutputs(
                StreamProcessSpec.keyed(reference(FunctionAbi.KEYED_PROCESS_V1),
                        mainSchema(),
                        Collections.singletonList("id"),
                        Collections.singletonList(state()),
                        Collections.emptyList(),
                        StreamEmissionLimit.of(1, 4_096L),
                        StreamEmissionLimit.of(1, 4_096L))
                        .withOutputTags(Collections.singletonList(tag)),
                "process");
        result.main().sinkTo("main-sink", sink());
        result.output("rejects").filter(reference(FunctionAbi.FILTER_V1))
                .uid("tag-filter")
                .sinkTo("tag-sink", sink());

        StreamTestGraph graph = StreamTestGraph.from(session.toPlanArtifact());
        StreamNodeTaskContext context = StreamNodeTaskContext.from(
                graph.artifact.artifactId(), graph.executionGraph,
                graph.onlyNode(StreamOperatorCode.FILTER).operatorUid());
        StreamNodeTaskContext.Source input = context.inputs().get(0);

        assertEquals(tagSchema().toPortableSchema(), input.schema());
        assertEquals(Collections.singletonList("code"),
                input.stableKeyFields());
        assertEquals(PortableStreamPlan.ChangelogMode.UPSERT,
                input.changelogMode());
        assertTrue(input.edge().sourceOutputTagId().equals("rejects"));
        assertFalse(graph.product.appendRecords(
                graph.product.sink("tag-sink")),
                "Sink framing must follow the inherited edge changelog");
    }

    private static StreamSchema mainSchema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType,
                        false)));
    }

    private static StreamSchema tagSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("code", "code", "code",
                        DataType.StringType, false),
                new StreamField("message", "message", "message",
                        DataType.StringType, true)));
    }

    private static StreamSourceSpec source() {
        return StreamSourceSpec.exactOrdered(mainSchema(), "source-v1",
                digest("source"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        digest("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", digest("sink"), new byte[] {3});
    }

    private static StreamManagedStateSpec state() {
        return StreamManagedStateSpec.bounded(new StreamStateSpec(
                        "latest", StreamStateSpec.Kind.KEYED_VALUE,
                        "signed-long-v1", 1,
                        StreamStateSpec.TtlMode.PROCESSING_TIME, 60_000L),
                1, 10L, 4_096L, Long.BYTES);
    }

    private static FunctionDeploymentReference reference(FunctionAbi abi) {
        byte[] configuration = new byte[] {4};
        return new FunctionDeploymentReference(digest("deployment-set"),
                "functions", digest("jar"), "example.TaggedFunction",
                abi, "config-v1", digest(configuration),
                configuration, new FunctionResourceProfile(1_024L, 0L, 4));
    }

    private static String digest(String value) {
        return digest(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String digest(byte[] value) {
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
