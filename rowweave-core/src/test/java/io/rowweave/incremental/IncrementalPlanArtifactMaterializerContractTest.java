package io.rowweave.incremental;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.Field;
import io.rowweave.IncrementalSourceSplit;
import io.rowweave.ProviderBatchSink;
import io.rowweave.DataSchema;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactDeploymentBinding;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.BatchSourcePlanValidator;
import io.rowweave.plan.ObjectTrustContext;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** an interval Artifact may differ from its template only at exact SOURCE bindings. */
class IncrementalPlanArtifactMaterializerContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("op", DataType.StringType),
            new Field("position", DataType.LongType));

    @Test
    void derivesStableTemplateFingerprintAndMaterializesExactArtifact()
            throws Exception {
        Fixture fixture = fixture("sink-config", "partitions");
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "ledger-definition", fixture.template, trust,
                        ignored -> { });
        CapturedTargetReceipt receipt = receipt(
                definition.fingerprint(), fixture.contract, "snapshot");
        IncrementalMaterializationResponse response = response(
                fixture, receipt, "partitions");

        PlanArtifact first = IncrementalPlanArtifactMaterializer.materialize(
                definition, fixture.template, fixture.contract,
                fixture.configuration, response, trust, ignored -> { });
        PlanArtifact replay = IncrementalPlanArtifactMaterializer.materialize(
                definition, fixture.template, fixture.contract,
                fixture.configuration, response, trust, ignored -> { });
        assertEquals(first.artifactId(), replay.artifactId());
        assertEquals(PlanArtifact.ChangelogMode.UPSERT, first.changelogMode());
        assertEquals(hex(definition.fingerprint()), first.definitionId());
        assertArrayEquals(PlanArtifactCodec.encode(first),
                PlanArtifactCodec.encode(replay));

        PlanGraph graph = PlanGraph.fromBytes(
                first.payload(), trust, ignored -> { });
        BatchSourcePlanValidator.validateDurable(graph, trust);
        assertEquals(2, graph.nodeCount());
        assertEquals(1, graph.node(0).sourceSplitBindings().size());
        IncrementalSourceSplit input = (IncrementalSourceSplit)
                graph.node(0).sourceSplitBindings().get(0).input();
        assertEquals(receipt, input.capturedTarget());
        assertArrayEquals(response.partitionSetDigest(),
                input.partitionSetDigest());
        assertEquals(fixture.output.batchSinkDescriptor(),
                graph.node(1).batchSinkDescriptor());
    }

    @Test
    void materializedArtifactPreservesTheSecureDeploymentIdentity()
            throws Exception {
        Fixture fixture = fixture("sink-config", "partitions");
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "ledger-definition", fixture.template, trust,
                        ignored -> { });
        CapturedTargetReceipt receipt = receipt(
                definition.fingerprint(), fixture.contract, "snapshot");
        String deploymentSetId = String.join("", Collections.nCopies(64, "a"));

        PlanArtifact materialized = IncrementalPlanArtifactMaterializer.materialize(
                definition, fixture.template, fixture.contract,
                fixture.configuration, response(fixture, receipt, "partitions"),
                deploymentSetId, trust, ignored -> { });

        assertEquals(deploymentSetId,
                PlanArtifactDeploymentBinding.deploymentSetId(materialized));
    }

    @Test
    void templateFingerprintChangesWithAnyDurableSinkBinding()
            throws Exception {
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        Fixture first = fixture("sink-config-a", "partitions");
        Fixture second = fixture("sink-config-b", "partitions");

        IncrementalDefinition left = IncrementalPlanTemplateFingerprint.derive(
                "ledger-definition", first.template, trust, ignored -> { });
        IncrementalDefinition same = IncrementalPlanTemplateFingerprint.derive(
                "ledger-definition", first.template, trust, ignored -> { });
        IncrementalDefinition right = IncrementalPlanTemplateFingerprint.derive(
                "ledger-definition", second.template, trust, ignored -> { });

        assertArrayEquals(left.fingerprint(), same.fingerprint());
        assertNotEquals(hex(left.fingerprint()), hex(right.fingerprint()));
    }

    @Test
    void definitionReceiptPartitionOrTemplateDriftFailsBeforeArtifact()
            throws Exception {
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        Fixture fixture = fixture("sink-config", "partitions");
        IncrementalDefinition definition =
                IncrementalPlanTemplateFingerprint.derive(
                        "ledger-definition", fixture.template, trust,
                        ignored -> { });
        CapturedTargetReceipt receipt = receipt(
                definition.fingerprint(), fixture.contract, "snapshot");
        IncrementalMaterializationResponse response = response(
                fixture, receipt, "partitions");

        Fixture changedTemplate = fixture("changed-sink-config", "partitions");
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalPlanArtifactMaterializer.materialize(
                        definition, changedTemplate.template, fixture.contract,
                        fixture.configuration, response, trust, ignored -> { }));

        CapturedTargetReceipt wrongDefinition = receipt(
                digest("wrong-definition"), fixture.contract, "snapshot");
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalPlanArtifactMaterializer.materialize(
                        definition, fixture.template, fixture.contract,
                        fixture.configuration,
                        response(fixture, wrongDefinition, "partitions"),
                        trust, ignored -> { }));

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalPlanArtifactMaterializer.materialize(
                        definition, fixture.template, fixture.contract,
                        fixture.configuration,
                        response(fixture, receipt, "other-partitions"),
                        trust, ignored -> { }));
    }

    private static Fixture fixture(String sinkConfig, String partition)
            throws Exception {
        byte[] partitionDigest = digest(partition);
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "ledger-source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), schemaDigest(), false,
                partitionDigest,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                .withIncrementalPlanDescriptor(descriptor);
        ProviderBatchSink output = output(sinkConfig);
        PlanNode sink = PlanNode.sink(META, 0, output)
                .param(PlanNode.OPERATOR_UID_PARAM, "ledger-sink").build();
        PlanGraph template = new PlanGraph(Arrays.asList(source, sink), 1);
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("source-config"));
        return new Fixture(template, output, contract, configuration,
                partitionDigest);
    }

    private static ProviderBatchSink output(String config) {
        BatchSinkDescriptor descriptor = new BatchSinkDescriptor(
                BatchSinkDescriptor.CURRENT_VERSION, "test.manifest", 1,
                SchemaFingerprint.of(META), "destination", "domain",
                BatchSinkDescriptor.CapabilityProfile
                        .MANIFEST_ATOMIC_GROUP_V1, "binding");
        return new ProviderBatchSink(
                META, descriptor,
                new DurableSinkCodecDescriptor("test.sink-config", 1, 1024),
                bytes(config));
    }

    private static CapturedTargetReceipt receipt(
            byte[] definition, IncrementalSourceProviderContract contract,
            String snapshot) throws Exception {
        return CapturedTargetReceipt.normal(
                1L, definition,
                IncrementalSourceProviderContractCodec.digest(contract),
                0L, IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes(snapshot));
    }

    private static IncrementalMaterializationResponse response(
            Fixture fixture, CapturedTargetReceipt receipt, String partitions)
            throws Exception {
        BatchSourceSplitDescriptor descriptor =
                new BatchSourceSplitDescriptor(
                        BatchSourceSplitDescriptor.CURRENT_VERSION,
                        fixture.contract.providerKey().connectorType(),
                        fixture.contract.providerKey().connectorVersion(),
                        "p0", SchemaFingerprint.of(META),
                        hex(digest(new String(receipt.snapshotIdentity(),
                                StandardCharsets.UTF_8))),
                        BatchSourceSplitDescriptor.ReplayGuarantee
                                .EXACT_ORDERED_FROM_START);
        return new IncrementalMaterializationResponse(
                fixture.contract.providerKey(), receipt, digest(partitions),
                Collections.singletonList(new IncrementalMaterializedSplit(
                        descriptor, bytes("open-request"))));
    }

    private static IncrementalSourceProviderContract contract() {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey("test.embedded", 1),
                new IncrementalSourceCodecDescriptor("test.config", 1, 64),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.CAPTURE_CODEC_ID, 1,
                        IncrementalCaptureResponseCodec.MAX_FRAME_BYTES),
                new IncrementalSourceCodecDescriptor(
                        IncrementalSourceActionExecutor.MATERIALIZATION_CODEC_ID,
                        1, IncrementalMaterializationResponseCodec.MAX_FRAME_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES,
                IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                false,
                new IncrementalSourceActionBudget(
                        1024L * 1024L, 24L * 1024L * 1024L,
                        1, 60_000L),
                IncrementalSourceProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    private static byte[] schemaDigest() {
        String value = SchemaFingerprint.of(META);
        byte[] result = new byte[value.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(
                    value.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    private static byte[] digest(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(bytes(value));
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) {
            result.append(Character.forDigit((b >>> 4) & 0xf, 16));
            result.append(Character.forDigit(b & 0xf, 16));
        }
        return result.toString();
    }

    private static final class Fixture {
        final PlanGraph template;
        final ProviderBatchSink output;
        final IncrementalSourceProviderContract contract;
        final IncrementalSourceConfig configuration;
        final byte[] partitionDigest;

        Fixture(PlanGraph template, ProviderBatchSink output,
                IncrementalSourceProviderContract contract,
                IncrementalSourceConfig configuration,
                byte[] partitionDigest) {
            this.template = template;
            this.output = output;
            this.contract = contract;
            this.configuration = configuration;
            this.partitionDigest = partitionDigest;
        }
    }
}
