package io.rowweave.plan;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.BatchSink;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.Sink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** freeze Sink + canonical Sink descriptor into SINK nodes. */
class BatchSinkPlanBindingTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));
    private static final DataSchema OTHER = new DataSchema(new Field("other", DataType.LongType));
    private static final String SCHEMA = SchemaFingerprint.of(META);

    @Test
    void builtInBindingFreezesAndRoundTripsWithPlanAndSubgraph() {
        DiscardSink output = new DiscardSink(META);
        PlanGraph graph = graph(output);
        PlanNode sink = graph.node(1);

        assertSame(output, sink.batchSinkBinding().output());
        assertEquals(BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1,
                sink.batchSinkDescriptor().capabilityProfile());
        assertEquals(1, sink.listParam(PlanNode.SINK_DESCRIPTOR_PARAM).size());
        assertFalse(sink.listParam(PlanNode.SINK_DESCRIPTOR_PARAM).get(0)
                .contains(output.getClass().getName()));

        PlanGraph decoded = PlanGraph.fromBytes(graph.toBytes());
        assertEquals(sink.batchSinkDescriptor(), decoded.node(1).batchSinkDescriptor());
        assertEquals(sink.batchSinkDescriptor(),
                graph.subGraphRootedAt(1).node(1).batchSinkDescriptor());
    }

    @Test
    void customDescriptorMutationAndSchemaMismatchFailBeforeCollect() {
        MutableSink mutable = new MutableSink(META, descriptor(
                SCHEMA, BatchSinkDescriptor.CapabilityProfile.LEGACY_PROCESS_LOCAL));
        PlanGraph graph = graph(mutable);
        mutable.descriptor = descriptor(
                SCHEMA, BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1);

        IllegalArgumentException drift = assertThrows(
                IllegalArgumentException.class, graph::toBytes);
        assertTrue(drift.getMessage().contains("changed after plan binding"));
        assertEquals(0, mutable.collects);

        MutableSink wrongSchema = new MutableSink(META, descriptor(
                SchemaFingerprint.of(OTHER),
                BatchSinkDescriptor.CapabilityProfile.LEGACY_PROCESS_LOCAL));
        IllegalArgumentException mismatch = assertThrows(
                IllegalArgumentException.class, () -> graph(wrongSchema));
        assertTrue(mismatch.getMessage().contains("schemaFingerprint"));
        assertEquals(0, wrongSchema.collects);
    }

    @Test
    void oldWireOnlyBridgesExactBuiltInsAndDowngradesCustomCapability() {
        MutableSink selfClaiming = new MutableSink(META, descriptor(
                SCHEMA, BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        PlanNode custom = PlanNode.sinkFromWire(
                META, 0, selfClaiming,
                new LinkedHashMap<String, String>(),
                new LinkedHashMap<String, List<String>>());
        assertEquals(BatchSinkDescriptor.CapabilityProfile.LEGACY_PROCESS_LOCAL,
                custom.batchSinkDescriptor().capabilityProfile());

        DiscardSink discard = new DiscardSink(META);
        PlanNode builtIn = PlanNode.sinkFromWire(
                META, 0, discard,
                new LinkedHashMap<String, String>(),
                new LinkedHashMap<String, List<String>>());
        assertEquals(BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1,
                builtIn.batchSinkDescriptor().capabilityProfile());
    }

    @Test
    void reservedMetadataMustBeSingleCanonicalFrame() {
        PlanNode sink = graph(new DiscardSink(META)).node(1);
        IllegalArgumentException malformed = assertThrows(
                IllegalArgumentException.class,
                () -> sink.withListParam(
                        PlanNode.SINK_DESCRIPTOR_PARAM,
                        Collections.singletonList("%%%not-base64%%%")));
        assertTrue(malformed.getMessage().contains("canonical"));

        IllegalArgumentException count = assertThrows(
                IllegalArgumentException.class,
                () -> sink.withListParam(
                        PlanNode.SINK_DESCRIPTOR_PARAM, Collections.emptyList()));
        assertTrue(count.getMessage().contains("exactly one"));
    }

    private static PlanGraph graph(Sink output) {
        PlanNode source = PlanNode.sourceFromInputs(META,
                Collections.singletonList(new CollectionSource(
                        META, Collections.<DataRow>emptyList())));
        PlanNode sink = PlanNode.sink(META, 0, output).build();
        return new PlanGraph(java.util.Arrays.asList(source, sink), 1);
    }

    private static BatchSinkDescriptor descriptor(
            String schema, BatchSinkDescriptor.CapabilityProfile profile) {
        return new BatchSinkDescriptor(
                1, "test-sink", 1, schema, "test-destination", "test-domain", profile);
    }

    private static final class MutableSink implements BatchSink {
        private static final long serialVersionUID = 1L;
        private final DataSchema meta;
        private BatchSinkDescriptor descriptor;
        private transient int collects;

        private MutableSink(DataSchema meta, BatchSinkDescriptor descriptor) {
            this.meta = meta;
            this.descriptor = descriptor;
        }

        @Override public BatchSinkDescriptor batchSinkDescriptor() { return descriptor; }
        @Override public DataSchema getSchema() { return meta; }
        @Override public void collect(DataRow row) { collects++; }
    }
}
