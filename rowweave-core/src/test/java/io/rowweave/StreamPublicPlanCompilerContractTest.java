package io.rowweave;

import io.rowweave.artifact.PlanArtifact;
import io.rowweave.portable.FunctionAbi;
import io.rowweave.portable.FunctionDeploymentReference;
import io.rowweave.portable.FunctionResourceProfile;
import io.rowweave.stream.api.StreamApiException;
import io.rowweave.stream.api.StreamCheckpointConfig;
import io.rowweave.stream.api.StreamChangelogMode;
import io.rowweave.stream.api.StreamDeliveryGuarantee;
import io.rowweave.stream.api.StreamField;
import io.rowweave.stream.api.StreamResourceProfile;
import io.rowweave.stream.api.StreamRestartPolicy;
import io.rowweave.stream.api.StreamSchema;
import io.rowweave.stream.api.StreamSinkSpec;
import io.rowweave.stream.api.StreamSourceSpec;
import io.rowweave.stream.api.StreamSplitSpec;
import io.rowweave.stream.api.StreamTimeMode;
import io.rowweave.stream.contract.PortableStreamJob;
import io.rowweave.stream.contract.PortableStreamJobArtifact;
import io.rowweave.stream.contract.PortableStreamNode;
import io.rowweave.stream.contract.PortableStreamPlan;
import io.rowweave.stream.contract.StreamOperatorCode;
import io.rowweave.stream.contract.StreamSinkBinding;
import io.rowweave.stream.contract.StreamSourceBinding;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public Stream DSL must compile a canonical product job envelope. */
class StreamPublicPlanCompilerContractTest {

    @Test
    void publicDslCompilesCanonicalImmutableProductJob() {
        byte[] sourceConfiguration = new byte[] {1, 2, 3};
        byte[] splitDescriptor = new byte[] {4, 5, 6};
        byte[] sinkConfiguration = new byte[] {7, 8, 9};
        List<StreamSplitSpec> splits = new ArrayList<>();
        splits.add(new StreamSplitSpec(
                sha256("split-0"), "sequence-v1", splitDescriptor));

        StreamSchema schema = payloadSchema();
        StreamSourceSpec sourceSpec = StreamSourceSpec.exactOrdered(
                schema, "source-v1", sha256("source-contract"),
                sourceConfiguration, splits);
        StreamSinkSpec sinkSpec = StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), sinkConfiguration);

        sourceConfiguration[0] = 99;
        splitDescriptor[0] = 99;
        sinkConfiguration[0] = 99;
        splits.clear();

        StreamSession session = RowWeave.createStreamSession(
                "stream-public-contract", "Stream public contract");
        session.setDefinitionId("stream-public-contract-v1");
        session.setParallelism(2);
        session.setMaxParallelism(128);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.AT_LEAST_ONCE);
        session.setChangelogMode(StreamChangelogMode.APPEND);
        session.setTimeMode(StreamTimeMode.PROCESSING_TIME);
        session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(30_000L, 2));
        session.setRestartPolicy(StreamRestartPolicy.fixedDelay(3, 1_000L));
        session.setResourceProfile(StreamResourceProfile.defaults());

        DataStream source = session.fromSource(sourceSpec).uid("source");
        DataStream filtered = source.filter(filterReference()).uid("filter");
        assertNotSame(source, filtered);
        assertEquals(source.schema(), filtered.schema());
        filtered.sinkTo("sink", sinkSpec);

        PlanArtifact first = session.toPlanArtifact();
        PlanArtifact second = session.toPlanArtifact();
        assertEquals(first.artifactId(), second.artifactId());
        assertArrayEquals(first.payload(), second.payload());
        assertEquals(PlanArtifact.RuntimeTarget.STREAM, first.runtimeTarget());
        assertEquals(PlanArtifact.Boundedness.UNBOUNDED, first.boundedness());
        assertEquals(PlanArtifact.PayloadFormat.PORTABLE_STREAM_JOB_V1,
                first.payloadFormat());

        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(first);
        assertEquals(StreamCheckpointConfig.manualOnly(30_000L, 2),
                job.checkpointConfig());
        assertEquals(StreamRestartPolicy.fixedDelay(3, 1_000L),
                job.restartPolicy());
        assertEquals(StreamResourceProfile.defaults(), job.resourceProfile());
        assertEquals(3, job.plan().nodes().size());
        assertEquals(2, job.plan().edges().size());
        assertEquals(Collections.singletonList("sink"),
                job.plan().rootSinkOperatorUids());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.NODE_EXECUTION_CONTRACT_CAPABILITY));
        assertTrue(first.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_NODE_EXECUTION_CONTRACT_V1));
        for (PortableStreamNode node : job.plan().nodes()) {
            assertNotNull(node.executionDescriptor());
            assertEquals(node.opcode() == StreamOperatorCode.SOURCE ? 1 : 2,
                    node.executionDescriptor().parallelism());
            assertEquals(128,
                    node.executionDescriptor().maximumParallelism());
            assertEquals(job.plan().changelogMode(),
                    node.executionDescriptor().changelogMode());
            assertEquals(job.plan().timeMode(),
                    node.executionDescriptor().timeMode());
        }

        PortableStreamNode sourceNode = job.plan().nodes().get(0);
        PortableStreamNode filterNode = job.plan().nodes().get(1);
        PortableStreamNode sinkNode = job.plan().nodes().get(2);
        assertEquals(StreamOperatorCode.SOURCE, sourceNode.opcode());
        assertEquals(StreamOperatorCode.FILTER, filterNode.opcode());
        assertEquals(StreamOperatorCode.SINK, sinkNode.opcode());
        assertEquals(schema.fingerprint(), sourceNode.outputSchema().fingerprint());
        assertEquals(schema.fingerprint(), filterNode.outputSchema().fingerprint());
        assertArrayEquals(new byte[] {1, 2, 3},
                ((StreamSourceBinding) sourceNode.binding()).configurationFrame());
        assertArrayEquals(new byte[] {4, 5, 6},
                ((StreamSourceBinding) sourceNode.binding()).fixedSplits()
                        .get(0).descriptorFrame());
        assertArrayEquals(new byte[] {7, 8, 9},
                ((StreamSinkBinding) sinkNode.binding()).configurationFrame());
    }

    @Test
    void schemaKeepsStableIdentityAndProjectsFieldsWithoutAliasing() {
        List<StreamField> fields = new ArrayList<>(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
        StreamSchema schema = new StreamSchema(fields);
        fields.clear();

        assertEquals(2, schema.fieldCount());
        assertEquals("adaptor-id", schema.field(0).fieldId());
        assertEquals(false, schema.field(0).nullable());
        assertEquals(2, schema.toDataSchema().getFieldCount());
        assertEquals("adaptorId", schema.toDataSchema().getField(0).getName());
        assertEquals(schema, payloadSchema());
    }

    @Test
    void compilerRejectsMissingAndDuplicateOperatorIdentity() {
        StreamSession missing = baseSession("missing");
        missing.fromSource(sourceSpec()).sinkTo("sink", sinkSpec());
        StreamApiException missingFailure = assertThrows(
                StreamApiException.class, missing::toPlanArtifact);
        assertEquals(StreamApiException.Code.MISSING_OPERATOR_UID,
                missingFailure.code());

        StreamSession duplicate = baseSession("duplicate");
        duplicate.fromSource(sourceSpec()).uid("same")
                .filter(filterReference()).uid("same")
                .sinkTo("sink", sinkSpec());
        StreamApiException duplicateFailure = assertThrows(
                StreamApiException.class, duplicate::toPlanArtifact);
        assertEquals(StreamApiException.Code.DUPLICATE_OPERATOR_UID,
                duplicateFailure.code());

        StreamSession noSink = baseSession("no-sink");
        noSink.fromSource(sourceSpec()).uid("source");
        StreamApiException noSinkFailure = assertThrows(
                StreamApiException.class, noSink::toPlanArtifact);
        assertEquals(StreamApiException.Code.MISSING_SINK, noSinkFailure.code());
    }

    @Test
    void compilerRejectsGuaranteeAndFunctionAbiMismatchBeforeArtifact() {
        StreamSession exact = baseSession("exact-eager");
        exact.setDeliveryGuarantee(StreamDeliveryGuarantee.EXACTLY_ONCE);
        exact.setCheckpointConfig(StreamCheckpointConfig.periodic(
                1_000L, 30_000L, 2));
        exact.fromSource(sourceSpec()).uid("source").sinkTo("sink", sinkSpec());
        StreamApiException guaranteeFailure = assertThrows(
                StreamApiException.class, exact::toPlanArtifact);
        assertEquals(StreamApiException.Code.DELIVERY_GUARANTEE_MISMATCH,
                guaranteeFailure.code());

        StreamSession abi = baseSession("abi");
        abi.fromSource(sourceSpec()).uid("source")
                .filter(mapReference());
        StreamApiException abiFailure = assertThrows(
                StreamApiException.class, abi::toPlanArtifact);
        assertEquals(StreamApiException.Code.FUNCTION_ABI_MISMATCH,
                abiFailure.code());
    }

    @Test
    void publicDslDeclaresUnalignedCapabilityAndV2JobEnvelope() {
        StreamSession session = baseSession("unaligned");
        session.setCheckpointConfig(
                StreamCheckpointConfig.manualOnlyUnaligned(30_000L, 2));
        session.setResourceProfile(new StreamResourceProfile(
                8, 1_024, 4_096, 0L, 0, 0,
                64 * 1024L, 4, 16, 0L));
        session.fromSource(sourceSpec()).uid("source")
                .sinkTo("sink", sinkSpec());

        PlanArtifact artifact = session.toPlanArtifact();
        PortableStreamJob job = PortableStreamJobArtifact.decodeValidated(
                artifact);

        assertEquals(io.rowweave.stream.api.StreamCheckpointMode.UNALIGNED,
                job.checkpointConfig().mode());
        assertTrue(job.plan().requiredCapabilities().contains(
                PortableStreamPlan.UNALIGNED_CHECKPOINT_CAPABILITY));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_STREAM_UNALIGNED_CHECKPOINT_V2));
        assertArrayEquals("RWXXPSJ2".getBytes(StandardCharsets.US_ASCII),
                Arrays.copyOf(artifact.payload(), 8));
    }

    private static StreamSession baseSession(String suffix) {
        StreamSession session = RowWeave.createStreamSession("stream-" + suffix);
        session.setDefinitionId("stream-" + suffix + "-v1");
        session.setParallelism(1);
        session.setMaxParallelism(128);
        session.setDeliveryGuarantee(StreamDeliveryGuarantee.AT_LEAST_ONCE);
        session.setChangelogMode(StreamChangelogMode.APPEND);
        session.setTimeMode(StreamTimeMode.PROCESSING_TIME);
        session.setCheckpointConfig(StreamCheckpointConfig.manualOnly(30_000L, 2));
        session.setRestartPolicy(StreamRestartPolicy.fixedDelay(1, 0L));
        session.setResourceProfile(StreamResourceProfile.defaults());
        return session;
    }

    private static StreamSourceSpec sourceSpec() {
        return StreamSourceSpec.exactOrdered(
                payloadSchema(), "source-v1", sha256("source-contract"),
                new byte[] {1}, Collections.singletonList(new StreamSplitSpec(
                        sha256("split-0"), "sequence-v1", new byte[] {2})));
    }

    private static StreamSinkSpec sinkSpec() {
        return StreamSinkSpec.eagerAtLeastOnce(
                "sink-v1", sha256("sink-contract"), new byte[] {3});
    }

    private static StreamSchema payloadSchema() {
        return new StreamSchema(Arrays.asList(
                new StreamField("adaptor-id", "adaptorId", "adaptorId",
                        DataType.StringType, false),
                new StreamField("amount", "amount", "amount",
                        DataType.BigDecimalType, true)));
    }

    private static FunctionDeploymentReference filterReference() {
        return functionReference(FunctionAbi.FILTER_V1, "ContractFilter");
    }

    private static FunctionDeploymentReference mapReference() {
        return functionReference(FunctionAbi.MAP_V1, "ContractMap");
    }

    private static FunctionDeploymentReference functionReference(
            FunctionAbi abi, String simpleName) {
        byte[] configuration = new byte[] {11};
        return new FunctionDeploymentReference(
                sha256("deployment-set"), "stream-functions",
                sha256("deployment-jar"), "example." + simpleName, abi,
                "stream-function-config-v1", sha256(configuration), configuration,
                new FunctionResourceProfile(1_024L, 0L, 1));
    }

    private static String sha256(String value) {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static String sha256(byte[] value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(value);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte current : digest) {
                result.append(String.format("%02x", current & 0xff));
            }
            return result.toString();
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }
}
