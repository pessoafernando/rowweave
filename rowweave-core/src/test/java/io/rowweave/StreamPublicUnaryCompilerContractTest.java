package io.rowweave;

import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamEmissionLimit;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.StreamEdge;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamFunctionBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** stateless/key partition API compiles without schema inference. */
class StreamPublicUnaryCompilerContractTest {

    @Test
    void mapAndKeyByCompileExplicitSchemasAndHashPartition() {
        StreamSchema sourceSchema = schema(true);
        StreamSchema mappedSchema = schema(false);
        StreamSession session = RowWeave.createStreamSession("unary");
        DataStream keyed = session.fromSource(source(sourceSchema)).uid("source")
                .map(reference(FunctionAbi.MAP_V1, "Map"), mappedSchema).uid("map")
                .keyBy("adaptor-id").uid("key")
                .filter(reference(FunctionAbi.FILTER_V1, "Filter")).uid("filter");
        keyed.sinkTo("sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        List<PortableStreamNode> nodes = job.plan().nodes();
        assertEquals(Arrays.asList(StreamOperatorCode.SOURCE,
                        StreamOperatorCode.MAP, StreamOperatorCode.KEY_BY,
                        StreamOperatorCode.FILTER, StreamOperatorCode.SINK),
                Arrays.asList(nodes.get(0).opcode(), nodes.get(1).opcode(),
                        nodes.get(2).opcode(), nodes.get(3).opcode(),
                        nodes.get(4).opcode()));
        assertEquals(mappedSchema.fingerprint(), nodes.get(1).outputSchema().fingerprint());
        assertEquals(Collections.singletonList("adaptor-id"),
                nodes.get(2).stableKeyFields());
        assertEquals(Collections.singletonList("adaptor-id"),
                nodes.get(4).stableKeyFields());
        assertEquals(StreamEdge.Partitioning.HASH,
                edgeTo(job, "key").partitioning());
        assertEquals(StreamEdge.Partitioning.FORWARD,
                edgeTo(job, "filter").partitioning());
    }

    @Test
    void keyByRejectsMissingNullableAndDuplicateStableFieldIds() {
        assertKeyFailure(schema(true), new String[] {"missing"},
                StreamApiException.Code.INVALID_STABLE_KEY);
        assertKeyFailure(schema(true), new String[] {"adaptor-id"},
                StreamApiException.Code.INVALID_STABLE_KEY);
        assertKeyFailure(schema(false), new String[] {"adaptor-id", "adaptor-id"},
                StreamApiException.Code.INVALID_STABLE_KEY);
    }

    @Test
    void mapRejectsWrongAbiBeforeArtifactCreation() {
        StreamSession session = RowWeave.createStreamSession("wrong-map");
        session.fromSource(source(schema(false))).uid("source")
                .map(reference(FunctionAbi.FILTER_V1, "Filter"), schema(false))
                .uid("map").sinkTo("sink", sink());
        StreamApiException failure = assertThrows(
                StreamApiException.class, session::toPlanArtifact);
        assertEquals(StreamApiException.Code.FUNCTION_ABI_MISMATCH, failure.code());
    }

    @Test
    void flatMapCompilesAQuotaBoundedPushContract() {
        StreamSchema schema = schema(false);
        StreamSession session = RowWeave.createStreamSession("flat-map");
        session.fromSource(source(schema)).uid("source")
                .flatMap(reference(FunctionAbi.FLAT_MAP_V1, "FlatMap"), schema,
                        StreamEmissionLimit.of(4, 4096L))
                .uid("flat-map").sinkTo("sink", sink());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                session.toPlanArtifact());
        PortableStreamNode node = job.plan().nodes().get(1);
        assertEquals(StreamOperatorCode.FLAT_MAP, node.opcode());
        assertEquals("4", node.scalarParameters().get(
                "flatMap.maxOutputsPerInput"));
        assertEquals("4096", node.scalarParameters().get(
                "flatMap.maxOutputBytesPerInput"));
        assertEquals(StreamFunctionBinding.FunctionKind.FLAT_MAP,
                ((StreamFunctionBinding) node.binding()).functionKind());
        assertEquals(true, job.plan().requiredCapabilities().contains(
                io.rowweave.stream.contract.PortableStreamPlan
                        .FLAT_MAP_CAPABILITY));

        assertThrows(IllegalArgumentException.class,
                () -> StreamEmissionLimit.of(0, 4096L));
        assertThrows(IllegalArgumentException.class,
                () -> StreamEmissionLimit.of(1, 0L));
    }

    @Test
    void flatMapRejectsWrongAbiBeforeArtifactCreation() {
        StreamSession session = RowWeave.createStreamSession("wrong-flat-map");
        session.fromSource(source(schema(false))).uid("source")
                .flatMap(reference(FunctionAbi.MAP_V1, "Map"), schema(false),
                        StreamEmissionLimit.of(1, 1024L))
                .uid("flat-map").sinkTo("sink", sink());
        StreamApiException failure = assertThrows(
                StreamApiException.class, session::toPlanArtifact);
        assertEquals(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                failure.code());
    }

    private static void assertKeyFailure(StreamSchema schema, String[] keys,
                                         StreamApiException.Code code) {
        StreamSession session = RowWeave.createStreamSession("invalid-key-" + keys[0]);
        session.fromSource(source(schema)).uid("source")
                .keyBy(keys).uid("key").sinkTo("sink", sink());
        StreamApiException failure = assertThrows(
                StreamApiException.class, session::toPlanArtifact);
        assertEquals(code, failure.code());
    }

    private static StreamEdge edgeTo(PortableStreamJob job, String uid) {
        for (StreamEdge edge : job.plan().edges()) {
            if (edge.targetOperatorUid().equals(uid)) return edge;
        }
        throw new AssertionError("edge not found: " + uid);
    }

    private static StreamSchema schema(boolean nullableKey) {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, nullableKey),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static StreamSourceSpec source(StreamSchema schema) {
        return StreamSourceSpec.exactOrdered(schema, "source-v1",
                sha256("source-contract"), new byte[] {1},
                Collections.singletonList(new StreamSplitSpec(
                        sha256("split"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sink() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static FunctionDeploymentReference reference(
            FunctionAbi abi, String name) {
        byte[] config = new byte[] {4};
        return new FunctionDeploymentReference(sha256("deployment-set"),
                "stream-functions", sha256("jar"), "example." + name, abi,
                "stream-function-config-v1", sha256(config), config,
                new FunctionResourceProfile(1_024L, 0L, 16));
    }

    private static String sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) result.append(String.format("%02x", current & 0xff));
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
