package io.rowweave.artifact;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.portable.PortableBatchPlan;
import io.rowweave.portable.PortableDataType;
import io.rowweave.portable.PortableField;
import io.rowweave.portable.PortableOperatorCode;
import io.rowweave.portable.PortablePlanNode;
import io.rowweave.portable.PortableRowSchema;
import io.rowweave.portable.PortableSinkBinding;
import io.rowweave.portable.PortableSourceBinding;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortablePlanArtifactTest {

    private static final String DIGEST_A = repeat('a', 64);
    private static final String DIGEST_B = repeat('b', 64);

    @Test
    void portablePlanUsesStablePayloadIdAndSurvivesArtifactRoundTrip() {
        PortableBatchPlan plan = plan();
        PlanArtifact artifact = PortablePlanArtifact.wrap(
                "daily-ledger", plan);

        assertEquals(PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1,
                artifact.payloadFormat());
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1));
        assertTrue(artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS));
        assertEquals(2, artifact.operators().size());
        assertEquals("rowweave.portable.opcode.1",
                artifact.operators().get(0).typeId());

        PlanArtifact decodedArtifact = PlanArtifactCodec.decode(
                PlanArtifactCodec.encode(artifact));
        PlanArtifactValidator.validateReadable(decodedArtifact);
        assertEquals(plan, PortablePlanArtifact.decodeValidated(
                decodedArtifact));
    }

    @Test
    void portablePayloadWireIdIsThreeAndLegacyReaderPathDoesNotClaimIt() {
        PlanArtifact artifact = PortablePlanArtifact.wrap(
                "daily-ledger", plan());
        byte[] encoded = PlanArtifactCodec.encode(artifact);
        PlanArtifact decoded = PlanArtifactCodec.decode(encoded);
        assertEquals(artifact.artifactId(), decoded.artifactId());
        assertEquals(PlanArtifact.PayloadFormat.PORTABLE_BATCH_PLAN_V1,
                decoded.payloadFormat());

        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class,
                () -> LegacyPlanArtifactAdapter.unwrapValidated(artifact));
        assertEquals("PORTABLE_ADAPTER_REQUIRED", failure.code());
    }

    @Test
    void outerDescriptorDriftIsRejectedBeforeRuntimeAdmission() {
        PlanArtifact valid = PortablePlanArtifact.wrap("daily-ledger", plan());
        OperatorDescriptor first = valid.operators().get(0);
        OperatorDescriptor drift = new OperatorDescriptor(
                first.operatorId(), first.kind(), "legacy-source",
                first.schemaFingerprint(), first.inputOperatorIds());
        PlanArtifact tampered = new PlanArtifact(
                valid.definitionId(), valid.planSchemaVersion(),
                valid.minimumReaderVersion(), valid.runtimeTarget(),
                valid.boundedness(), valid.changelogMode(), valid.timeMode(),
                valid.requiredFeatures(), valid.optionalFeatures(),
                Arrays.asList(drift, valid.operators().get(1)),
                valid.payloadFormat(), valid.payload());

        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class,
                () -> PlanArtifactValidator.validateReadable(tampered));
        assertEquals("PORTABLE_DESCRIPTOR_MISMATCH", failure.code());
    }

    @Test
    void outerPayloadFeatureAndBatchSemanticsCannotReinterpretPortableBody() {
        PlanArtifact valid = PortablePlanArtifact.wrap("daily-ledger", plan());
        PlanArtifact duplicatePayloadFeature = copy(
                valid, valid.runtimeTarget(), Arrays.asList(
                        PlanArtifact.FEATURE_PORTABLE_BATCH_PLAN_V1,
                        PlanArtifact.FEATURE_TRUSTED_LEGACY_PLAN_GRAPH,
                        PlanArtifact.FEATURE_EXPLICIT_OPERATOR_IDS));
        assertFailure("PAYLOAD_FEATURE_MISMATCH", duplicatePayloadFeature);

        PlanArtifact streamOuter = copy(
                valid, PlanArtifact.RuntimeTarget.STREAM,
                valid.requiredFeatures());
        assertFailure("PORTABLE_SEMANTIC_MISMATCH", streamOuter);
    }

    @Test
    void publicV1ExecutionAdmissionAcceptsOnlyPortablePayloads() {
        PlanArtifact portable = PortablePlanArtifact.wrap(
                "daily-ledger", plan());
        PlanArtifactValidator.validateForPublicV1Execution(portable);

        DataSchema meta = new DataSchema(new Field("id", DataType.LongType));
        PlanArtifact legacy = LegacyPlanArtifactAdapter.wrap(
                "legacy-ledger", new PlanGraph(Arrays.asList(
                        PlanNode.placeholderSource(meta),
                        PlanNode.sink(meta, 0, new DiscardSink(meta)).build())));
        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class,
                () -> PlanArtifactValidator.validateForPublicV1Execution(legacy));
        assertEquals("LEGACY_NOT_PUBLIC_V1", failure.code());
    }

    private static PlanArtifact copy(
            PlanArtifact source, PlanArtifact.RuntimeTarget runtimeTarget,
            java.util.List<String> requiredFeatures) {
        return new PlanArtifact(
                source.definitionId(), source.planSchemaVersion(),
                source.minimumReaderVersion(), runtimeTarget,
                source.boundedness(), source.changelogMode(), source.timeMode(),
                requiredFeatures, source.optionalFeatures(), source.operators(),
                source.payloadFormat(), source.payload());
    }

    private static void assertFailure(String code, PlanArtifact artifact) {
        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class,
                () -> PlanArtifactValidator.validateReadable(artifact));
        assertEquals(code, failure.code());
    }

    private static PortableBatchPlan plan() {
        PortableRowSchema schema = new PortableRowSchema(Arrays.asList(
                new PortableField("id", "id", "id",
                        PortableDataType.LONG, false),
                new PortableField("amount", "amount", "amount",
                        PortableDataType.BIG_DECIMAL, true)));
        PortablePlanNode source = new PortablePlanNode(
                "source", PortableOperatorCode.SOURCE,
                Collections.emptyList(), Collections.emptyList(), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSourceBinding(
                        "jdbc-v1", DIGEST_A, new byte[]{1},
                        Collections.singletonList(new byte[]{2})));
        PortablePlanNode sink = new PortablePlanNode(
                "sink", PortableOperatorCode.SINK,
                Collections.singletonList("source"),
                Collections.singletonList(schema.fingerprint()), schema,
                Collections.emptyMap(), Collections.emptyMap(),
                new PortableSinkBinding(
                        "manifest-file-v1", DIGEST_B,
                        new byte[]{3}, new byte[]{4}));
        return new PortableBatchPlan(
                1, 2, Collections.singletonList("sink"),
                Arrays.asList(source, sink),
                Collections.singletonList("rowweave.runtime.batch.v1"));
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
