package io.rowweave.plan;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitBinding;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.ExecutionEnvironment;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** freeze Source + canonical split descriptors into SOURCE Plan nodes. */
class BatchSourcePlanBindingTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", "identifier", DataType.LongType));
    private static final DataSchema OTHER_ALIAS = new DataSchema(
            new Field("id", "other_identifier", DataType.LongType));
    private static final String SCHEMA = SchemaFingerprint.of(META);

    @Test
    void exactBindingsFreezeInInputOrderAndRoundTripWithArtifactPayload() {
        DescribedInput first = exact("split-b", "snapshot-b", META);
        DescribedInput second = exact("split-a", "snapshot-a", META);
        PlanGraph graph = graph(first, second);
        PlanNode source = graph.node(0);

        assertEquals(Arrays.asList("split-b", "split-a"), splitIds(source),
                "descriptor identity must not reorder the application Source[]");
        assertSame(first, source.sourceSplitBindings().get(0).input());
        assertSame(second, source.sourceSplitBindings().get(1).input());
        assertEquals(2, source.listParam(PlanNode.SOURCE_SPLIT_DESCRIPTORS_PARAM).size());
        assertFalse(source.listParam(PlanNode.SOURCE_SPLIT_DESCRIPTORS_PARAM).get(0)
                        .contains("snapshot-b"),
                "reserved metadata is canonical encoded data, not raw snapshot text");

        PlanGraph decoded = PlanGraph.fromBytes(graph.toBytes());
        assertEquals(splitIds(source), splitIds(decoded.node(0)));
        assertEquals(source.sourceSplitDescriptors(), decoded.node(0).sourceSplitDescriptors());
        assertEquals(2, decoded.node(0).inputs().size());
    }

    @Test
    void legacyInputsDowngradeExplicitlyWithoutOpeningThem() {
        LegacyInput first = new LegacyInput(META);
        LegacyInput second = new LegacyInput(META);
        PlanNode source = graph(first, second).node(0);

        assertEquals(Arrays.asList("legacy-position:0", "legacy-position:1"), splitIds(source));
        for (BatchSourceSplitDescriptor descriptor : source.sourceSplitDescriptors()) {
            assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT,
                    descriptor.replayGuarantee());
            assertEquals(SCHEMA, descriptor.schemaFingerprint());
        }
        assertEquals(0, first.iterations);
        assertEquals(0, second.iterations);
    }

    @Test
    void oldWireWithoutReservedMetadataCannotSilentlyUpgradeToExactReplay() {
        DescribedInput capableInput = exact("capable", "snapshot-current", META);
        PlanNode decodedOldSource = PlanNode.sourceFromWire(
                META, Collections.<Source>singletonList(capableInput),
                new LinkedHashMap<String, String>(),
                new LinkedHashMap<String, List<String>>());

        BatchSourceSplitDescriptor descriptor =
                decodedOldSource.sourceSplitDescriptors().get(0);
        assertEquals("legacy-position:0", descriptor.splitId());
        assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT,
                descriptor.replayGuarantee());
        assertEquals(0, capableInput.iterations);
    }

    @Test
    void duplicateIdsSchemaMismatchAndNullDescriptorsFailBeforeIteration() {
        DescribedInput first = exact("duplicate", "snapshot-1", META);
        DescribedInput duplicate = exact("duplicate", "snapshot-2", META);
        IllegalArgumentException duplicateFailure = assertThrows(
                IllegalArgumentException.class, () -> graph(first, duplicate));
        assertTrue(duplicateFailure.getMessage().contains("duplicate splitId"));

        DescribedInput aliasMismatch = exact("alias", "snapshot-3", OTHER_ALIAS);
        IllegalArgumentException schemaFailure = assertThrows(
                IllegalArgumentException.class, () -> graph(first, aliasMismatch));
        assertTrue(schemaFailure.getMessage().contains("schemaFingerprint"));

        DescribedInput missing = new DescribedInput(META, null);
        IllegalArgumentException nullFailure = assertThrows(
                IllegalArgumentException.class, () -> graph(missing));
        assertTrue(nullFailure.getMessage().contains("descriptor"));
        assertEquals(0, first.iterations);
        assertEquals(0, duplicate.iterations);
        assertEquals(0, aliasMismatch.iterations);
        assertEquals(0, missing.iterations);
    }

    @Test
    void descriptorMutationCannotChangeAnAlreadyFrozenPlan() {
        DescribedInput mutable = exact("stable", "snapshot-1", META);
        PlanGraph graph = graph(mutable);
        BatchSourceSplitDescriptor frozen = graph.node(0).sourceSplitDescriptors().get(0);
        mutable.descriptor = descriptor("stable", "snapshot-2", META,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);

        assertEquals("snapshot-1", frozen.snapshotId());
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class, graph::toBytes);
        assertTrue(failure.getMessage().contains("changed after plan binding"));
    }

    @Test
    void subgraphAndBindingAwareReplacementPreserveIdentityMetadata() {
        DescribedInput first = exact("first", "snapshot-1", META);
        DescribedInput second = exact("second", "snapshot-2", META);
        PlanNode source = PlanNode.sourceFromInputs(META, Arrays.asList(first, second))
                .withName("ledger source")
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source");
        PlanGraph graph = new PlanGraph(Collections.singletonList(source), 3);

        PlanGraph subgraph = graph.subGraphRootedAt(0);
        assertEquals(splitIds(source), splitIds(subgraph.node(0)));
        assertEquals("ledger-source", subgraph.node(0).param(PlanNode.OPERATOR_UID_PARAM));

        BatchSourceSplitBinding selected = source.sourceSplitBindings().get(1);
        PlanGraph exactSubset = graph.replaceSourceBindings(
                0, Collections.singletonList(selected));
        assertEquals(Collections.singletonList("second"), splitIds(exactSubset.node(0)));
        assertSame(second, exactSubset.node(0).inputs().get(0));
        assertEquals("ledger source", exactSubset.node(0).name());
        assertEquals("ledger-source", exactSubset.node(0).param(PlanNode.OPERATOR_UID_PARAM));

        LegacyInput replacement = new LegacyInput(META);
        PlanGraph legacyReplacement = graph.replaceSourceInputs(
                0, Collections.singletonList(replacement));
        assertEquals(Collections.singletonList("legacy-position:0"),
                splitIds(legacyReplacement.node(0)));
    }

    @Test
    void reservedMetadataAndAggregateBytesAreBoundedAndCanonical() {
        PlanNode source = PlanNode.sourceFromInputs(META,
                Collections.<Source>singletonList(new LegacyInput(META)));
        IllegalArgumentException malformed = assertThrows(
                IllegalArgumentException.class,
                () -> source.withListParam(
                        PlanNode.SOURCE_SPLIT_DESCRIPTORS_PARAM,
                        Collections.singletonList("%%%not-base64%%%")));
        assertTrue(malformed.getMessage().contains("canonical"));
        IllegalArgumentException count = assertThrows(
                IllegalArgumentException.class,
                () -> source.withListParam(
                        PlanNode.SOURCE_SPLIT_DESCRIPTORS_PARAM,
                        Collections.emptyList()));
        assertTrue(count.getMessage().contains("count"));

        List<Source> tooLarge = new ArrayList<>();
        String largeSnapshot = repeat('s',
                BatchSourceSplitDescriptor.MAX_SNAPSHOT_ID_BYTES);
        for (int i = 0; i < PlanGraphEnvelopeScanner.MAX_INPUTS; i++) {
            tooLarge.add(exact("split-" + i, largeSnapshot, META));
        }
        IllegalArgumentException aggregate = assertThrows(
                IllegalArgumentException.class,
                () -> PlanNode.sourceFromInputs(META, tooLarge));
        assertTrue(aggregate.getMessage().contains("descriptor bytes"));
    }

    private static PlanGraph graph(Source... inputs) {
        ExecutionEnvironment session = new ExecutionEnvironment("source-binding", "source-binding");
        io.rowweave.DataSet source = session.fromSource(inputs);
        source.sinkTo(new DiscardSink(META));
        return session.toPlanGraph();
    }

    private static List<String> splitIds(PlanNode source) {
        List<String> ids = new ArrayList<>();
        for (BatchSourceSplitDescriptor descriptor : source.sourceSplitDescriptors()) {
            ids.add(descriptor.splitId());
        }
        return ids;
    }

    private static DescribedInput exact(String splitId, String snapshotId, DataSchema meta) {
        return new DescribedInput(meta, descriptor(
                splitId, snapshotId, meta,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START));
    }

    private static BatchSourceSplitDescriptor descriptor(
            String splitId, String snapshotId, DataSchema meta,
            BatchSourceSplitDescriptor.ReplayGuarantee replay) {
        return new BatchSourceSplitDescriptor(
                1, "test-source", 1, splitId, SchemaFingerprint.of(meta), snapshotId, replay);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static final class DescribedInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final DataSchema meta;
        private transient int iterations;
        private BatchSourceSplitDescriptor descriptor;

        private DescribedInput(DataSchema meta, BatchSourceSplitDescriptor descriptor) {
            this.meta = meta;
            this.descriptor = descriptor;
        }

        @Override public DataSchema getSchema() { return meta; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() {
            iterations++;
            return Collections.emptyIterator();
        }
    }

    private static final class LegacyInput implements Source {
        private static final long serialVersionUID = 1L;
        private final DataSchema meta;
        private transient int iterations;
        private LegacyInput(DataSchema meta) { this.meta = meta; }
        @Override public DataSchema getSchema() { return meta; }
        @Override public Iterator<DataRow> iterator() {
            iterations++;
            return Collections.emptyIterator();
        }
    }
}
