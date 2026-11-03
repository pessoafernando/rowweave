package io.rowweave.incremental;

import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.IncrementalSourceSplit;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.ObjectTrustContext;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** materialized split metadata remains inert until exact Worker pre-open. */
class IncrementalSourceSplitBindingContractTest {
    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("op", DataType.StringType),
            new Field("position", DataType.LongType));

    @Test
    void materializesCanonicalInertBindingsThatSurvivePlanWire() throws Exception {
        Fixture fixture = fixture();

        List<BatchSourceSplitBinding> bindings =
                IncrementalSourceBindingMaterializer.materialize(
                        META, fixture.configuration, fixture.response);
        assertEquals(1, bindings.size());
        IncrementalSourceSplit input =
                (IncrementalSourceSplit) bindings.get(0).input();
        assertEquals(fixture.split.descriptor(), input.batchSourceSplit());
        assertEquals(fixture.configuration, input.configuration());
        assertEquals(fixture.receipt, input.capturedTarget());
        assertArrayEquals(fixture.response.partitionSetDigest(),
                input.partitionSetDigest());
        assertArrayEquals(fixture.split.openRequest(), input.openRequest());
        assertThrows(IllegalStateException.class, input::iterator);

        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                .withIncrementalPlanDescriptor(descriptor(fixture))
                .withSourceSplitBindings(bindings);
        ObjectTrustContext trust = ObjectTrustContext.mtlsBuiltIns();
        byte[] wire = new PlanGraph(Collections.singletonList(source), 1)
                .toBytes(trust, ignored -> { });
        PlanGraph decoded = PlanGraph.fromBytes(
                wire, trust, ignored -> { });
        IncrementalSourceSplit roundTrip = (IncrementalSourceSplit)
                decoded.node(0).sourceSplitBindings().get(0).input();
        assertEquals(fixture.receipt, roundTrip.capturedTarget());
        assertArrayEquals(fixture.split.openRequest(), roundTrip.openRequest());
    }

    @Test
    void preOpenRequiresEveryFrozenIdentityToMatch() throws Exception {
        Fixture fixture = fixture();
        BatchSourceSplitBinding binding =
                IncrementalSourceBindingMaterializer.materialize(
                        META, fixture.configuration, fixture.response).get(0);

        IncrementalSourceSplit verified =
                IncrementalSourceBindingMaterializer.verifyPreOpen(
                        binding, fixture.contract, fixture.configuration,
                        fixture.receipt, fixture.response.partitionSetDigest());
        assertEquals(binding.input(), verified);

        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceBindingMaterializer.verifyPreOpen(
                        binding, fixture.contract, fixture.configuration,
                        fixture.receipt, digest("other-partitions")));
        IncrementalSourceConfig driftedConfig = new IncrementalSourceConfig(
                fixture.configuration.providerKey(),
                fixture.configuration.contractDigest(), bytes("drift"));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceBindingMaterializer.verifyPreOpen(
                        binding, fixture.contract, driftedConfig,
                        fixture.receipt, fixture.response.partitionSetDigest()));
        CapturedTargetReceipt driftedReceipt = CapturedTargetReceipt.normal(
                1L, digest("definition"),
                IncrementalSourceProviderContractCodec.digest(fixture.contract),
                0L, IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(11L), bytes("snapshot-2"));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceBindingMaterializer.verifyPreOpen(
                        binding, fixture.contract, fixture.configuration,
                        driftedReceipt, fixture.response.partitionSetDigest()));
    }

    @Test
    void schemaOrContractDriftCannotProduceAnyBinding() throws Exception {
        Fixture fixture = fixture();
        DataSchema wrongMeta = new DataSchema(new Field("other", DataType.StringType));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceBindingMaterializer.materialize(
                        wrongMeta, fixture.configuration, fixture.response));

        IncrementalSourceConfig wrongContract = new IncrementalSourceConfig(
                fixture.configuration.providerKey(), digest("wrong-contract"),
                fixture.configuration.configurationBytes());
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceBindingMaterializer.materialize(
                        META, wrongContract, fixture.response));
    }

    @Test
    void durableProxyIndependentlyRejectsSnapshotIdentityDrift()
            throws Exception {
        Fixture fixture = fixture();
        BatchSourceSplitDescriptor drifted =
                new BatchSourceSplitDescriptor(
                        BatchSourceSplitDescriptor.CURRENT_VERSION,
                        fixture.contract.providerKey().connectorType(),
                        fixture.contract.providerKey().connectorVersion(),
                        fixture.split.descriptor().splitId(),
                        fixture.split.descriptor().schemaFingerprint(),
                        hex(digest("other-snapshot")),
                        BatchSourceSplitDescriptor.ReplayGuarantee
                                .EXACT_ORDERED_FROM_START);

        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceSplit(
                        META, fixture.configuration, fixture.receipt,
                        fixture.response.partitionSetDigest(),
                        new IncrementalMaterializedSplit(
                                drifted, fixture.split.openRequest())));
    }

    private static Fixture fixture() throws Exception {
        IncrementalSourceProviderContract contract = contract();
        IncrementalSourceConfig configuration = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract),
                bytes("config"));
        CapturedTargetReceipt receipt = CapturedTargetReceipt.normal(
                1L, digest("definition"), configuration.contractDigest(),
                0L, IncrementalFrontier.begin(), "trigger",
                IncrementalRunMode.SNAPSHOT_V1,
                IncrementalFrontier.int64Sequence(10L), bytes("snapshot"));
        BatchSourceSplitDescriptor splitDescriptor =
                new BatchSourceSplitDescriptor(
                        BatchSourceSplitDescriptor.CURRENT_VERSION,
                        contract.providerKey().connectorType(),
                        contract.providerKey().connectorVersion(), "p0",
                        SchemaFingerprint.of(META),
                        hex(digest("snapshot")),
                        BatchSourceSplitDescriptor.ReplayGuarantee
                                .EXACT_ORDERED_FROM_START);
        IncrementalMaterializedSplit split =
                new IncrementalMaterializedSplit(
                        splitDescriptor, bytes("open-request"));
        IncrementalMaterializationResponse response =
                new IncrementalMaterializationResponse(
                        contract.providerKey(), receipt,
                        digest("partitions"), Collections.singletonList(split));
        return new Fixture(contract, configuration, receipt, split, response);
    }

    private static IncrementalPlanDescriptor descriptor(Fixture fixture)
            throws Exception {
        return new IncrementalPlanDescriptor(
                "ledger-source", digest("frontier-contract"),
                Collections.singletonList(0), 1,
                Collections.singletonList(2), digestSchema(), false,
                fixture.response.partitionSetDigest(),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
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

    private static byte[] digestSchema() {
        return hexToBytes(SchemaFingerprint.of(META));
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

    private static byte[] hexToBytes(String value) {
        byte[] result = new byte[value.length() / 2];
        for (int i = 0; i < result.length; i++) {
            result[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return result;
    }

    private static final class Fixture {
        final IncrementalSourceProviderContract contract;
        final IncrementalSourceConfig configuration;
        final CapturedTargetReceipt receipt;
        final IncrementalMaterializedSplit split;
        final IncrementalMaterializationResponse response;

        Fixture(IncrementalSourceProviderContract contract,
                IncrementalSourceConfig configuration,
                CapturedTargetReceipt receipt,
                IncrementalMaterializedSplit split,
                IncrementalMaterializationResponse response) {
            this.contract = contract;
            this.configuration = configuration;
            this.receipt = receipt;
            this.split = split;
            this.response = response;
        }
    }
}
