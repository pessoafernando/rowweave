package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamOperatorCode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** binary UNION is an engine primitive, not a Pipeline transform. */
class StreamPublicUnionCompilerContractTest {

    @Test
    void sameContractInputsCompileToCanonicalBinaryUnion() {
        StreamSession session = RowWeave.createStreamSession("union");
        DataStream left = session.fromSource(source("left", schema())).uid("left");
        DataStream right = session.fromSource(source("right", schema())).uid("right");
        left.union(right).uid("union").sinkTo("sink", sink());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(artifact);
        PortableStreamNode union = job.plan().nodes().get(2);
        assertEquals(StreamOperatorCode.UNION, union.opcode());
        assertEquals(Arrays.asList("left", "right"),
                union.inputOperatorUids());
        assertEquals(schema().toPortableSchema(), union.outputSchema());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.UNION_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_UNION_V1));
        assertEquals(StreamEdge.Partitioning.FORWARD,
                edge(job, "union", 0).partitioning());
        assertEquals(StreamEdge.Partitioning.FORWARD,
                edge(job, "union", 1).partitioning());
    }

    @Test
    void unionRejectsSchemaKeyAndSessionDriftBeforeArtifactCreation() {
        StreamSession schemaSession = RowWeave.createStreamSession("schema-drift");
        DataStream left = schemaSession.fromSource(
                source("left", schema())).uid("left");
        DataStream right = schemaSession.fromSource(
                source("right", widerSchema())).uid("right");
        left.union(right).uid("union").sinkTo("sink", sink());
        assertCode(StreamApiException.Code.SCHEMA_MISMATCH,
                schemaSession::toPlanArtifact);

        StreamSession keySession = RowWeave.createStreamSession("key-drift");
        DataStream keyed = keySession.fromSource(source("left", schema()))
                .uid("left").keyBy("id").uid("left-key");
        DataStream unkeyed = keySession.fromSource(source("right", schema()))
                .uid("right");
        keyed.union(unkeyed).uid("union").sinkTo("sink", sink());
        assertCode(StreamApiException.Code.INVALID_STABLE_KEY,
                keySession::toPlanArtifact);

        StreamSession first = RowWeave.createStreamSession("first");
        StreamSession second = RowWeave.createStreamSession("second");
        DataStream firstStream = first.fromSource(source("first", schema()));
        DataStream secondStream = second.fromSource(source("second", schema()));
        assertThrows(IllegalArgumentException.class,
                () -> firstStream.union(secondStream));
    }

    private static StreamEdge edge(PortableStreamJob job, String target,
            int input) {
        for (StreamEdge edge : job.plan().edges()) {
            if (edge.targetOperatorUid().equals(target)
                    && edge.targetInputIndex() == input) return edge;
        }
        throw new AssertionError("edge not found");
    }

    private static void assertCode(StreamApiException.Code code,
            Runnable action) {
        StreamApiException failure = assertThrows(
                StreamApiException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static StreamSchema schema() {
        return new StreamSchema(Collections.singletonList(
                new StreamField("id", "id", "id", DataType.LongType, false)));
    }

    private static StreamSchema widerSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("id", "id", "id", DataType.LongType, false),
                new StreamField("value", "value", "value",
                        DataType.StringType, true)));
    }

    private static StreamSourceSpec source(String identity, StreamSchema schema) {
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"), new byte[] {(byte) identity.length()},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split-" + identity), "sequence-v1",
                        new byte[] {1})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {2});
    }

    private static String sha256(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(
                    value.getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(64);
            for (byte current : digest) {
                result.append(String.format(java.util.Locale.ROOT,
                        "%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
