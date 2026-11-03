package io.rowweave.incremental;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.artifact.LegacyPlanArtifactAdapter;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.artifact.PlanArtifactCodec;
import io.rowweave.artifact.PlanArtifactValidationException;
import io.rowweave.cluster.JobClient;
import io.rowweave.cluster.LocalCluster;
import io.rowweave.cluster.Master;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PushDagExecutor;
import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** incremental descriptor, reserved SOURCE wire and feature admission. */
class IncrementalPlanDescriptorContractTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("op", DataType.StringType),
            new Field("position", DataType.StringType),
            new Field("amount", DataType.BigDecimalType));

    @Test
    void descriptorRoundTripsCanonicallyAndDefensively() throws Exception {
        List<Integer> keys = new ArrayList<>(Collections.singletonList(0));
        List<Integer> positions = new ArrayList<>(Collections.singletonList(2));
        byte[] frontierContract = sha256("frontier-contract");
        byte[] partitionSet = sha256("partition-set");
        IncrementalPlanDescriptor descriptor = new IncrementalPlanDescriptor(
                "ledger-source", frontierContract, keys, 1, positions,
                schemaDigest(META), true, partitionSet,
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);

        byte[] encoded = IncrementalPlanDescriptorCodec.encode(descriptor);
        IncrementalPlanDescriptor decoded = IncrementalPlanDescriptorCodec.decode(encoded);
        assertEquals(descriptor, decoded);
        assertArrayEquals(encoded, IncrementalPlanDescriptorCodec.encode(decoded));
        assertArrayEquals(sha256(encoded), IncrementalPlanDescriptorCodec.digest(descriptor));
        assertTrue(encoded.length <= IncrementalPlanDescriptorCodec.MAX_FRAME_BYTES);

        keys.set(0, 3);
        positions.set(0, 3);
        frontierContract[0] ^= 1;
        partitionSet[0] ^= 1;
        assertEquals(Collections.singletonList(0), descriptor.primaryKeyFieldIndexes());
        assertEquals(Collections.singletonList(2), descriptor.recordPositionFieldIndexes());
        assertArrayEquals(sha256("frontier-contract"), descriptor.frontierContractDigest());
        assertArrayEquals(sha256("partition-set"), descriptor.partitionSetDigest());
        assertThrows(UnsupportedOperationException.class,
                () -> descriptor.primaryKeyFieldIndexes().add(3));
    }

    @Test
    void descriptorRejectsAmbiguousFieldsAndMalformedFrames() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> descriptor(Collections.<Integer>emptyList(), 1,
                        Collections.singletonList(2)));
        assertThrows(IllegalArgumentException.class,
                () -> descriptor(Arrays.asList(0, 0), 1,
                        Collections.singletonList(2)));
        assertThrows(IllegalArgumentException.class,
                () -> descriptor(Collections.singletonList(0), 0,
                        Collections.singletonList(2)));
        assertThrows(IllegalArgumentException.class,
                () -> descriptor(Collections.singletonList(0), 1,
                        Arrays.asList(2, 2)));
        assertThrows(IllegalArgumentException.class,
                () -> descriptor(Collections.singletonList(0), 1,
                        Collections.singletonList(-1)));

        byte[] encoded = IncrementalPlanDescriptorCodec.encode(descriptor());
        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalPlanDescriptorCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalPlanDescriptorCodec.decode(
                        new byte[IncrementalPlanDescriptorCodec.MAX_FRAME_BYTES + 1]));
    }

    @Test
    void sourceReservedMetadataIsSingleCanonicalFrameAndSurvivesWire() throws Exception {
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                .withIncrementalPlanDescriptor(descriptor());
        PlanGraph graph = new PlanGraph(Collections.singletonList(source), 1);

        List<String> wireValue = source.listParam(PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM);
        assertEquals(1, wireValue.size());
        assertFalse(wireValue.get(0).contains("="), "reserved frame must be base64url without padding");
        assertEquals(descriptor(), source.incrementalPlanDescriptor());

        PlanNode decoded = PlanGraph.fromBytes(graph.toBytes()).node(0);
        assertEquals(wireValue, decoded.listParam(PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM));
        assertEquals(descriptor(), decoded.incrementalPlanDescriptor());
        assertEquals(wireValue,
                graph.subGraphRootedAt(0).node(0)
                        .listParam(PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM));

        assertThrows(IllegalArgumentException.class,
                () -> source.withListParam(PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM,
                        Arrays.asList(wireValue.get(0), wireValue.get(0))));
        assertThrows(IllegalArgumentException.class,
                () -> source.withListParam(PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM,
                        Collections.singletonList("%%%not-base64%%%")));
        assertThrows(IllegalArgumentException.class,
                () -> source.withListParam(PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM,
                        Collections.singletonList(repeat('A',
                                (IncrementalPlanDescriptorCodec.MAX_FRAME_BYTES * 8 + 5) / 6 + 1))));

        List<String> hostileValues = new AccessFailingList(2);
        assertThrows(IllegalArgumentException.class,
                () -> source.withListParam(
                        PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM, hostileValues));
        PlanNode unary = PlanNode.unary("filter", META, 0).build();
        assertThrows(IllegalArgumentException.class,
                () -> unary.withListParam(
                        PlanNode.INCREMENTAL_PLAN_DESCRIPTOR_PARAM, hostileValues));
    }

    @Test
    void incrementalArtifactRequiresFeatureDescriptorAndUpsertAsOneClosedContract() throws Exception {
        PlanGraph incrementalGraph = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(META)
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                        .withIncrementalPlanDescriptor(descriptor())), 1);
        PlanArtifact incremental = LegacyPlanArtifactAdapter.wrap("ledger-feed", incrementalGraph);

        assertEquals(PlanArtifact.ChangelogMode.UPSERT, incremental.changelogMode());
        assertTrue(incremental.requiredFeatures().contains(
                PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER));
        PlanArtifact decodedArtifact = PlanArtifactCodec.decode(
                PlanArtifactCodec.encode(incremental));
        assertEquals(PlanArtifact.ChangelogMode.UPSERT, decodedArtifact.changelogMode());
        assertCode("INCREMENTAL_RUNTIME_LOCKED",
                () -> LegacyPlanArtifactAdapter.unwrapValidated(incremental));

        PlanArtifact missingFeature = copy(incremental, PlanArtifact.ChangelogMode.UPSERT,
                withoutIncrementalFeature(incremental.requiredFeatures()), incremental.payload());
        assertCode("INVALID_INCREMENTAL_PLAN",
                () -> LegacyPlanArtifactAdapter.unwrapValidated(missingFeature));

        PlanArtifact appendWithFeature = copy(incremental, PlanArtifact.ChangelogMode.APPEND,
                incremental.requiredFeatures(), incremental.payload());
        assertCode("INVALID_INCREMENTAL_PLAN",
                () -> LegacyPlanArtifactAdapter.unwrapValidated(appendWithFeature));

        PlanGraph ordinaryGraph = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(META)
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")), 1);
        PlanArtifact ordinary = LegacyPlanArtifactAdapter.wrap("ordinary", ordinaryGraph);
        assertEquals(PlanArtifact.ChangelogMode.APPEND, ordinary.changelogMode());
        assertFalse(ordinary.requiredFeatures().contains(
                PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER));

        PlanArtifact featureWithoutDescriptor = copy(
                ordinary, PlanArtifact.ChangelogMode.UPSERT,
                withIncrementalFeature(ordinary.requiredFeatures()), ordinary.payload());
        assertCode("INVALID_INCREMENTAL_PLAN",
                () -> LegacyPlanArtifactAdapter.unwrapValidated(featureWithoutDescriptor));
    }

    @Test
    void genericGraphSubmissionCannotBypassTheFrontierAwareRuntimeGate() throws Exception {
        PlanGraph incrementalGraph = new PlanGraph(Collections.singletonList(
                PlanNode.placeholderSource(META)
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                        .withIncrementalPlanDescriptor(descriptor())), 1);
        RecordingGraphClient client = new RecordingGraphClient();

        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> client.submit(incrementalGraph));
        assertEquals(0, client.graphSubmits);
    }

    @Test
    void directGenericExecutionEnginesRemainLocked() throws Exception {
        PlanGraph graph = incrementalSinkGraph();
        LocalCluster localCluster = new LocalCluster(1);
        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> localCluster.submitToSink(graph));
        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> localCluster.submitKeyed(graph));
        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> localCluster.submitBinaryKeyed(graph));

        Master master = new Master(1);
        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> master.run(graph));
        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> master.runKeyed(graph));
        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> master.runBinaryKeyed(graph));

        PushDagExecutor executor = new PushDagExecutor(1);
        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> executor.runSinkGraph(graph));
        assertCode("INCREMENTAL_RUNTIME_LOCKED", () -> executor.runWholeGraph(graph));
    }

    @Test
    void descriptorRequiresExactlyOneExplicitlyIdentifiedSource() throws Exception {
        PlanNode first = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source-a")
                .withIncrementalPlanDescriptor(new IncrementalPlanDescriptor(
                        "source-a", sha256("frontier"), Collections.singletonList(0), 1,
                        Collections.singletonList(2), schemaDigest(META), false,
                        sha256("partitions"),
                        IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1));
        PlanNode second = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "source-b")
                .withIncrementalPlanDescriptor(new IncrementalPlanDescriptor(
                        "source-b", sha256("frontier"), Collections.singletonList(0), 1,
                        Collections.singletonList(2), schemaDigest(META), false,
                        sha256("partitions"),
                        IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1));
        PlanGraph twoSources = new PlanGraph(Arrays.asList(first, second), 1);
        PlanArtifact artifact = LegacyPlanArtifactAdapter.wrap("two-sources", twoSources);

        assertCode("INVALID_INCREMENTAL_PLAN",
                () -> LegacyPlanArtifactAdapter.unwrapValidated(artifact));

        PlanNode ordinary = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ordinary-source");
        PlanGraph mixedSources = new PlanGraph(Arrays.asList(first, ordinary), 1);
        PlanArtifact mixed = LegacyPlanArtifactAdapter.wrap("mixed-sources", mixedSources);
        assertCode("INVALID_INCREMENTAL_PLAN",
                () -> LegacyPlanArtifactAdapter.unwrapValidated(mixed));

        PlanNode uidDrift = first.withParam(PlanNode.OPERATOR_UID_PARAM, "other-source");
        PlanArtifact drifted = LegacyPlanArtifactAdapter.wrap(
                "uid-drift", new PlanGraph(Collections.singletonList(uidDrift), 1));
        assertCode("INVALID_INCREMENTAL_PLAN",
                () -> LegacyPlanArtifactAdapter.unwrapValidated(drifted));
    }

    private static IncrementalPlanDescriptor descriptor() throws Exception {
        return descriptor(Collections.singletonList(0), 1, Collections.singletonList(2));
    }

    private static PlanGraph incrementalSinkGraph() throws Exception {
        PlanNode source = PlanNode.placeholderSource(META)
                .withParam(PlanNode.OPERATOR_UID_PARAM, "ledger-source")
                .withIncrementalPlanDescriptor(descriptor());
        PlanNode sink = PlanNode.sink(META, 0, new DiscardSink(META)).build();
        return new PlanGraph(Arrays.asList(source, sink), 1);
    }

    private static IncrementalPlanDescriptor descriptor(
            List<Integer> keys, int operation, List<Integer> positions) throws Exception {
        return new IncrementalPlanDescriptor(
                "ledger-source", sha256("frontier-contract"), keys, operation, positions,
                schemaDigest(META), true, sha256("partition-set"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
    }

    private static PlanArtifact copy(
            PlanArtifact source, PlanArtifact.ChangelogMode changelog,
            List<String> requiredFeatures, byte[] payload) {
        return new PlanArtifact(
                source.definitionId(), source.planSchemaVersion(), source.minimumReaderVersion(),
                source.runtimeTarget(), source.boundedness(), changelog, source.timeMode(),
                requiredFeatures, source.optionalFeatures(), source.operators(),
                source.payloadFormat(), payload);
    }

    private static List<String> withoutIncrementalFeature(List<String> features) {
        List<String> copy = new ArrayList<>(features);
        copy.remove(PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER);
        return copy;
    }

    private static List<String> withIncrementalFeature(List<String> features) {
        List<String> copy = new ArrayList<>(features);
        copy.add(PlanArtifact.FEATURE_BOUNDED_INCREMENTAL_FRONTIER);
        return copy;
    }

    private static void assertCode(String code, Runnable action) {
        PlanArtifactValidationException failure = assertThrows(
                PlanArtifactValidationException.class, action::run);
        assertEquals(code, failure.code(), failure.getMessage());
    }

    private static final class RecordingGraphClient implements JobClient {
        private int graphSubmits;

        @Override
        public long submitToSink(PlanGraph sinkGraph) {
            graphSubmits++;
            return 0L;
        }

        @Override
        public ExecutionReport submitToSinkReported(PlanGraph sinkGraph) {
            graphSubmits++;
            return new ExecutionReport(0L, 0L, 0L, 0L, null);
        }
    }

    private static final class AccessFailingList extends AbstractList<String> {
        private final int size;

        private AccessFailingList(int size) {
            this.size = size;
        }

        @Override
        public String get(int index) {
            throw new AssertionError("reserved list was accessed before its count was rejected");
        }

        @Override
        public int size() {
            return size;
        }
    }

    private static byte[] sha256(String value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(
                value.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] schemaDigest(DataSchema meta) {
        String value = SchemaFingerprint.of(meta);
        byte[] decoded = new byte[value.length() / 2];
        for (int i = 0; i < decoded.length; i++) {
            decoded[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return decoded;
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
