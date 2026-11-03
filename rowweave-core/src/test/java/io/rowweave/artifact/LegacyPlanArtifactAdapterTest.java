package io.rowweave.artifact;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyPlanArtifactAdapterTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5})
    void oneToFiveOutputsRoundTripWithDescriptors(int sinkCount) {
        List<PlanNode> nodes = new ArrayList<>();
        nodes.add(PlanNode.sourceFromInputs(META, Collections.singletonList(new ProbeInput())));
        nodes.add(PlanNode.unary("filter", META, 0).param("expr", "id >= 0")
                .param("__fused_chain_len__", "2").build());
        for (int i = 0; i < sinkCount; i++) {
            nodes.add(PlanNode.sink(META, 1, new ProbeOutput()).build());
        }
        PlanGraph graph = new PlanGraph(nodes, 4);

        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap("definition-" + sinkCount, graph);
        PlanArtifact decoded = PlanArtifactCodec.decode(PlanArtifactCodec.encode(artifact));
        PlanGraph back = LegacyPlanArtifactAdapter.unwrapValidated(decoded);

        assertEquals(graph.nodeCount(), back.nodeCount());
        assertEquals(graph.sinkCount(), back.sinkCount());
        assertEquals(4, back.parallelism());
        assertEquals("2", back.node(1).param("__fused_chain_len__"));
        for (int i = 0; i < graph.nodeCount(); i++) {
            assertEquals(graph.node(i).kind(), back.node(i).kind());
            assertEquals(graph.node(i).typeId(), back.node(i).typeId());
            assertArrayEquals(graph.node(i).childIndices(), back.node(i).childIndices());
        }
    }

    @Test
    void wrappingSerializesDescriptorsWithoutOpeningInputOrOutput() {
        ProbeInput.iterations.set(0);
        ProbeOutput.collects.set(0);
        PlanGraph graph = new PlanGraph(java.util.Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(new ProbeInput())),
                PlanNode.sink(META, 0, new ProbeOutput()).build()));

        LegacyPlanArtifactAdapter.wrap("no-side-effects", graph);

        assertEquals(0, ProbeInput.iterations.get());
        assertEquals(0, ProbeOutput.collects.get());
    }

    @Test
    void serializableUserFunctionRemainsAnOpaquePayloadUntilExplicitUnwrap() {
        ProbeInput.iterations.set(0);
        ProbeOutput.collects.set(0);
        io.rowweave.FilterFunction function =
                (java.io.Serializable & io.rowweave.FilterFunction) row -> true;
        PlanGraph graph = new PlanGraph(java.util.Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(new ProbeInput())),
                PlanNode.unaryFn("filterFn", META, 0, function),
                PlanNode.sink(META, 1, new ProbeOutput()).build()));

        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap("function-payload", graph);
        PlanArtifact decoded = PlanArtifactCodec.decode(PlanArtifactCodec.encode(artifact));
        assertEquals(3, decoded.operators().size());
        PlanGraph unwrapped = LegacyPlanArtifactAdapter.unwrapValidated(decoded);

        assertNotNull(unwrapped.node(1).userFunction());
        assertEquals("filterFn", unwrapped.node(1).typeId());
        assertEquals(0, ProbeInput.iterations.get());
        assertEquals(0, ProbeOutput.collects.get());
    }

    @Test
    void descriptorTamperingIsRejectedBeforeExecution() {
        ProbeInput.iterations.set(0);
        ProbeOutput.collects.set(0);
        PlanGraph graph = new PlanGraph(java.util.Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(new ProbeInput())),
                PlanNode.sink(META, 0, new ProbeOutput()).build()));
        PlanArtifact original = LegacyPlanArtifactAdapter.wrap("tamper", graph);
        List<OperatorDescriptor> altered = new ArrayList<>(original.operators());
        OperatorDescriptor sink = altered.get(1);
        altered.set(1, new OperatorDescriptor(sink.operatorId(), sink.kind(), "not-a-sink",
                sink.schemaFingerprint(), sink.inputOperatorIds()));
        PlanArtifact tampered = new PlanArtifact(
                original.definitionId(), original.planSchemaVersion(), original.minimumReaderVersion(),
                original.runtimeTarget(), original.boundedness(), original.changelogMode(), original.timeMode(),
                original.requiredFeatures(), original.optionalFeatures(), altered,
                original.payloadFormat(), original.payload());

        PlanArtifactValidationException failure = assertThrows(PlanArtifactValidationException.class,
                () -> LegacyPlanArtifactAdapter.unwrapValidated(tampered));
        assertEquals("LEGACY_DESCRIPTOR_MISMATCH", failure.code());
        assertEquals("operators[1]", failure.path());
        assertEquals(0, ProbeInput.iterations.get());
        assertEquals(0, ProbeOutput.collects.get());
    }

    @Test
    void descriptorTamperingIsRejectedBeforePayloadReadHooks() {
        HookedInput.reads.set(0);
        PlanGraph graph = new PlanGraph(java.util.Arrays.asList(
                PlanNode.sourceFromInputs(META,
                        Collections.singletonList(new HookedInput())),
                PlanNode.sink(META, 0, new ProbeOutput()).build()));
        PlanArtifact original = LegacyPlanArtifactAdapter.wrap("hook-tamper", graph);
        List<OperatorDescriptor> altered = new ArrayList<>(original.operators());
        OperatorDescriptor source = altered.get(0);
        altered.set(0, new OperatorDescriptor(
                source.operatorId(), source.kind(), "drifted-source",
                source.schemaFingerprint(), source.inputOperatorIds()));
        PlanArtifact tampered = new PlanArtifact(
                original.definitionId(), original.planSchemaVersion(),
                original.minimumReaderVersion(), original.runtimeTarget(),
                original.boundedness(), original.changelogMode(), original.timeMode(),
                original.requiredFeatures(), original.optionalFeatures(), altered,
                original.payloadFormat(), original.payload());

        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class,
                () -> LegacyPlanArtifactAdapter.unwrapValidated(tampered));
        assertEquals("LEGACY_DESCRIPTOR_MISMATCH", failure.code());
        assertEquals(0, HookedInput.reads.get(),
                "operator metadata drift must reject before ObjectInputStream");
    }

    @Test
    void explicitAndPositionalOperatorIdsCanCoexistWithoutRewritingTopology() {
        PlanGraph graph = new PlanGraph(java.util.Arrays.asList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(new ProbeInput()))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source"),
                PlanNode.unary("filter", META, 0).param("expr", "id >= 0").build(),
                PlanNode.sink(META, 1, new ProbeOutput()).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-output")));

        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap("mixed-identity", graph);

        assertEquals("ledger-source", artifact.operators().get(0).operatorId());
        assertEquals("legacy-op-000001", artifact.operators().get(1).operatorId());
        assertEquals(Collections.singletonList("ledger-source"),
                artifact.operators().get(1).inputOperatorIds());
        assertEquals("ledger-output", artifact.operators().get(2).operatorId());
        assertEquals(Collections.singletonList("legacy-op-000001"),
                artifact.operators().get(2).inputOperatorIds());
        assertTrue(artifact.requiredFeatures().contains("rowweave.operator-id.explicit.v1"));
        assertTrue(artifact.requiredFeatures().contains(PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS));
        LegacyPlanArtifactAdapter.unwrapValidated(artifact);
    }

    @Test
    void allExplicitIdsDoNotClaimLegacyPositionalIdentity() {
        PlanGraph graph = new PlanGraph(java.util.Arrays.asList(
                PlanNode.placeholderSource(META).withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new ProbeOutput()).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));

        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap("explicit-identity", graph);

        assertTrue(artifact.requiredFeatures().contains("rowweave.operator-id.explicit.v1"));
        assertEquals(false, artifact.requiredFeatures().contains(
                PlanArtifact.FEATURE_LEGACY_POSITIONAL_OPERATOR_IDS));
        LegacyPlanArtifactAdapter.unwrapValidated(artifact);
    }

    @Test
    void duplicateNonCanonicalAndOversizedExplicitIdsFailAtArtifactBoundary() {
        PlanGraph duplicate = new PlanGraph(java.util.Arrays.asList(
                PlanNode.placeholderSource(META).withParam(PlanNode.OPERATOR_UID_PARAM, "same"),
                PlanNode.sink(META, 0, new ProbeOutput()).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "same")));
        PlanArtifactValidationException duplicateFailure = assertThrows(PlanArtifactValidationException.class,
                () -> LegacyPlanArtifactAdapter.wrap("duplicate", duplicate));
        assertEquals("DUPLICATE_OPERATOR_ID", duplicateFailure.code());

        PlanGraph nonCanonical = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(META).withParam(PlanNode.OPERATOR_UID_PARAM, "e\u0301")));
        assertEquals("NON_CANONICAL_TEXT", assertThrows(PlanArtifactValidationException.class,
                () -> LegacyPlanArtifactAdapter.wrap("noncanonical", nonCanonical)).code());

        char[] chars = new char[257];
        java.util.Arrays.fill(chars, 'x');
        PlanGraph oversized = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(META).withParam(PlanNode.OPERATOR_UID_PARAM, new String(chars))));
        assertEquals("LIMIT_EXCEEDED", assertThrows(PlanArtifactValidationException.class,
                () -> LegacyPlanArtifactAdapter.wrap("oversized", oversized)).code());
    }

    private static final class ProbeInput implements Source {
        private static final long serialVersionUID = 1L;
        private static final AtomicInteger iterations = new AtomicInteger();

        @Override
        public DataSchema getSchema() {
            return META;
        }

        @Override
        public Iterator<DataRow> iterator() {
            iterations.incrementAndGet();
            throw new AssertionError("artifact wrapping must not open Source");
        }
    }

    private static final class ProbeOutput implements Sink {
        private static final long serialVersionUID = 1L;
        private static final AtomicInteger collects = new AtomicInteger();

        @Override
        public DataSchema getSchema() {
            return META;
        }

        @Override
        public void collect(DataRow row) {
            collects.incrementAndGet();
            throw new AssertionError("artifact wrapping must not collect Sink");
        }
    }

    private static final class HookedInput implements Source {
        private static final long serialVersionUID = 1L;
        private static final AtomicInteger reads = new AtomicInteger();

        private void readObject(java.io.ObjectInputStream in) throws Exception {
            in.defaultReadObject();
            reads.incrementAndGet();
        }

        @Override public DataSchema getSchema() { return META; }
        @Override public Iterator<DataRow> iterator() {
            return Collections.emptyIterator();
        }
    }
}
