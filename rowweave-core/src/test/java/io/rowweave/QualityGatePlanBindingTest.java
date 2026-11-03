package io.rowweave;

import io.rowweave.ops.RowValidation;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.TypedQualityPublicationPlanValidator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** policy is frozen into all three non-forgeable quality Sinks. */
class QualityGatePlanBindingTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));

    @Test
    void oldApiBindsAllowAllAndExplicitPolicyBindsEverySinkAcrossRoundTrip(
            @TempDir Path tmp) {
        PlanGraph legacy = graph(tmp.resolve("allow-all"), null);
        assertPolicy(legacy, QualityGatePolicy.allowAll());

        QualityGatePolicy policy = QualityGatePolicy.builder()
                .maxRejectedRows(0)
                .maxRejectedPartsPerMillion(0)
                .reasonThreshold(QualityReasonKey.of(
                        "QUALITY_RULE_VIOLATION", "adaptor.required", 0,
                        "NOT_NULL"), 0)
                .exemplarLimit(4)
                .build();
        PlanGraph original = graph(tmp.resolve("explicit"), policy);
        TypedQualityPublicationPlanValidator.validateFullGraph(original);
        PlanGraph copy = PlanGraph.fromBytes(original.toBytes());
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap("quality-gate", original);
        PlanGraph artifactCopy = LegacyPlanArtifactAdapter.unwrapValidated(
                PlanArtifactCodec.decode(PlanArtifactCodec.encode(artifact)));

        assertPolicy(original, policy);
        assertPolicy(copy, policy);
        assertPolicy(artifactCopy, policy);
        assertFalse(Files.exists(tmp.resolve("explicit")));
    }

    @Test
    void rejectsMissingOrMixedBindingBeforeOpeningCommitDomain(@TempDir Path tmp) {
        QualityGatePolicy policy = QualityGatePolicy.builder()
                .maxRejectedRows(1).exemplarLimit(1).build();
        Path domain = tmp.resolve("tampered");
        PlanGraph canonical = graph(domain, policy);
        int rejectedSink = sinkByRole(canonical, TypedQualityManifest.REJECTED_SINK_UID);
        PlanNode rejected = canonical.node(rejectedSink);
        ManifestFileSink oldUngated = new ManifestFileSink(
                rejected.outMeta(), domain.toString(), "rejected");
        PlanNode replacement = PlanNode.sink(
                        rejected.outMeta(), rejected.child(0), oldUngated)
                .param(PlanNode.OPERATOR_UID_PARAM,
                        TypedQualityManifest.REJECTED_SINK_UID).build();
        List<PlanNode> nodes = new ArrayList<>(canonical.nodes());
        nodes.set(rejectedSink, replacement);

        assertThrows(IllegalArgumentException.class,
                () -> TypedQualityPublicationPlanValidator.validateFullGraph(
                        new PlanGraph(nodes, canonical.parallelism())));

        QualityGatePolicy differentPolicy = QualityGatePolicy.builder()
                .maxRejectedRows(2).exemplarLimit(1).build();
        PlanGraph different = graph(domain, differentPolicy);
        PlanNode mixedRejected = different.node(sinkByRole(
                different, TypedQualityManifest.REJECTED_SINK_UID));
        List<PlanNode> mixedNodes = new ArrayList<>(canonical.nodes());
        mixedNodes.set(rejectedSink, mixedRejected);
        assertThrows(IllegalArgumentException.class,
                () -> TypedQualityPublicationPlanValidator.validateFullGraph(
                        new PlanGraph(mixedNodes, canonical.parallelism())));
        assertFalse(Files.exists(domain));
    }

    @Test
    void rejectsQualityBindingOutsideCanonicalPublication(@TempDir Path tmp) {
        Path domain = tmp.resolve("ordinary");
        QualityGatePolicy policy = QualityGatePolicy.builder()
                .maxRejectedRows(0).build();
        ManifestFileSink gated = ManifestFileSink.qualityGate(
                META, domain.toString(), "ordinary",
                TypedQualityManifest.VALID_SINK_UID, policy);
        PlanNode source = PlanNode.placeholderSource(META);
        PlanNode sink = PlanNode.sink(META, 0, gated)
                .param(PlanNode.OPERATOR_UID_PARAM, "custom.output").build();
        PlanGraph noncanonical = new PlanGraph(Arrays.asList(source, sink));

        assertThrows(IllegalArgumentException.class,
                () -> TypedQualityPublicationPlanValidator.validateFullGraph(noncanonical));
        assertFalse(Files.exists(domain));
    }

    private static PlanGraph graph(Path domain, QualityGatePolicy policy) {
        ExecutionEnvironment session = new ExecutionEnvironment("quality-gate-plan", "quality gate plan");
        RowValidation validation = RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .build();
        DataRowClassification classification = session.fromSource(new CollectionSource(
                        META, "quality-split", Collections.emptyList()))
                .uid("quality.source")
                .classifyRowsWithSourcePosition(validation);
        TypedQualityManifest target = TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary");
        if (policy != null) target = target.withQualityGate(policy);
        classification.writeToManifest(target);
        return session.toPlanGraph();
    }

    private static void assertPolicy(PlanGraph graph, QualityGatePolicy expected) {
        int sinks = 0;
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() != PlanNode.Kind.SINK) continue;
            sinks++;
            ManifestFileSink output = (ManifestFileSink) node.output();
            assertEquals(expected, output.qualityGatePolicy());
            assertArrayEquals(QualityGatePolicyCodec.digest(expected),
                    output.qualityGatePolicyDigest());
            assertEquals(node.param(PlanNode.OPERATOR_UID_PARAM),
                    output.qualityGateRole());
            String identity = output.batchSinkDescriptor().bindingIdentity();
            assertTrue(identity.startsWith("quality-gate-v1:"
                    + output.qualityGateRole() + ":"));
            assertEquals(64, identity.substring(identity.lastIndexOf(':') + 1).length());
        }
        assertEquals(3, sinks);
    }

    private static int sinkByRole(PlanGraph graph, String role) {
        for (int i = 0; i < graph.nodeCount(); i++) {
            PlanNode node = graph.node(i);
            if (node.kind() == PlanNode.Kind.SINK
                    && role.equals(node.param(PlanNode.OPERATOR_UID_PARAM))) return i;
        }
        throw new AssertionError("missing sink role " + role);
    }
}
