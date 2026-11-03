package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded acyclic definition template and fingerprint. */
class IncrementalDefinitionContractTest {

    @Test
    void canonicalTemplateRoundTripsAndFingerprintIsRecomputed() throws Exception {
        IncrementalDefinition definition = definition(
                descriptor(), operators("map-implementation"), sinkDigests("sink-a"));
        byte[] encoded = IncrementalDefinitionCodec.encode(definition);
        IncrementalDefinition decoded = IncrementalDefinitionCodec.decode(encoded);

        assertEquals(definition, decoded);
        assertArrayEquals(encoded, IncrementalDefinitionCodec.encode(decoded));
        assertArrayEquals(sha256(encoded), definition.fingerprint());
        assertArrayEquals(definition.fingerprint(), decoded.fingerprint());
        assertEquals(32, definition.fingerprint().length);
        assertTrue(encoded.length <= IncrementalDefinitionCodec.MAX_FRAME_BYTES);
    }

    @Test
    void descriptorTopologySchemaImplementationAndSinkBindingsAllAffectFingerprint() throws Exception {
        IncrementalDefinition baseline = definition(
                descriptor(), operators("map-implementation"), sinkDigests("sink-a"));

        IncrementalPlanDescriptor changedDescriptor = new IncrementalPlanDescriptor(
                "ledger-source", sha256("other-frontier"), Collections.singletonList(0), 1,
                Collections.singletonList(2), sha256("schema"), true,
                sha256("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        assertDifferent(baseline, definition(
                changedDescriptor, operators("map-implementation"), sinkDigests("sink-a")));

        List<IncrementalDefinition.OperatorBinding> changedImplementation =
                new ArrayList<>(operators("map-implementation"));
        changedImplementation.set(1, operator(
                "map", IncrementalDefinition.OperatorKind.MAP_V1, "incremental-map",
                sha256("schema"), Collections.singletonList("ledger-source"),
                sha256("other-map-implementation")));
        assertDifferent(baseline, definition(
                descriptor(), changedImplementation, sinkDigests("sink-a")));

        List<IncrementalDefinition.OperatorBinding> changedTopology = Arrays.asList(
                operator("ledger-source", IncrementalDefinition.OperatorKind.SOURCE_V1,
                        "incremental-source", sha256("schema"),
                        Collections.<String>emptyList(), sha256("source-implementation")),
                operator("map-a", IncrementalDefinition.OperatorKind.MAP_V1,
                        "incremental-map", sha256("schema"),
                        Collections.singletonList("ledger-source"), sha256("map-implementation")),
                operator("map-b", IncrementalDefinition.OperatorKind.MAP_V1,
                        "incremental-map", sha256("schema"),
                        Collections.singletonList("map-a"), sha256("map-implementation")),
                operator("sink", IncrementalDefinition.OperatorKind.SINK_V1,
                        "manifest-sink", sha256("schema"),
                        Collections.singletonList("map-b"), sha256("sink-implementation")));
        assertDifferent(baseline, definition(
                descriptor(), changedTopology, sinkDigests("sink-a")));

        byte[] otherSchema = sha256("other-schema");
        IncrementalPlanDescriptor otherSchemaDescriptor = new IncrementalPlanDescriptor(
                "ledger-source", sha256("frontier"), Collections.singletonList(0), 1,
                Collections.singletonList(2), otherSchema, true,
                sha256("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
        List<IncrementalDefinition.OperatorBinding> changedSchema = Arrays.asList(
                operator("ledger-source", IncrementalDefinition.OperatorKind.SOURCE_V1,
                        "incremental-source", otherSchema, Collections.<String>emptyList(),
                        sha256("source-implementation")),
                operator("map", IncrementalDefinition.OperatorKind.MAP_V1,
                        "incremental-map", otherSchema, Collections.singletonList("ledger-source"),
                        sha256("map-implementation")),
                operator("sink", IncrementalDefinition.OperatorKind.SINK_V1,
                        "manifest-sink", otherSchema, Collections.singletonList("map"),
                        sha256("sink-implementation")));
        assertDifferent(baseline, definition(
                otherSchemaDescriptor, changedSchema, sinkDigests("sink-a")));
        assertDifferent(baseline, definition(
                descriptor(), operators("map-implementation"), sinkDigests("sink-b")));
    }

    @Test
    void captureReceiptAndIntervalIdentityCannotEnterDefinitionPreimage() throws Exception {
        IncrementalDefinition definition = definition(
                descriptor(), operators("map-implementation"), sinkDigests("sink-a"));
        byte[] before = definition.fingerprint();
        CapturedTargetReceipt first = receipt(10L, "snapshot-a");
        CapturedTargetReceipt recaptured = receipt(11L, "snapshot-b");

        assertNotEquals(IncrementalIntervalIdentity.from(first).intervalId(),
                IncrementalIntervalIdentity.from(recaptured).intervalId());
        assertArrayEquals(before, definition.fingerprint());
        assertArrayEquals(before,
                IncrementalDefinitionCodec.decode(
                        IncrementalDefinitionCodec.encode(definition)).fingerprint());
    }

    @Test
    void topologyMustBeCanonicalAcyclicAndMatchTheDescriptorSource() throws Exception {
        List<IncrementalDefinition.OperatorBinding> duplicate =
                new ArrayList<>(operators("map-implementation"));
        duplicate.set(1, operator(
                "ledger-source", IncrementalDefinition.OperatorKind.MAP_V1, "incremental-map",
                sha256("schema"), Collections.singletonList("ledger-source"),
                sha256("map-implementation")));
        assertThrows(IllegalArgumentException.class,
                () -> definition(descriptor(), duplicate, sinkDigests("sink-a")));

        List<IncrementalDefinition.OperatorBinding> forwardReference =
                new ArrayList<>(operators("map-implementation"));
        forwardReference.set(1, operator(
                "map", IncrementalDefinition.OperatorKind.MAP_V1, "incremental-map",
                sha256("schema"), Collections.singletonList("sink"),
                sha256("map-implementation")));
        assertThrows(IllegalArgumentException.class,
                () -> definition(descriptor(), forwardReference, sinkDigests("sink-a")));

        List<IncrementalDefinition.OperatorBinding> wrongSource =
                new ArrayList<>(operators("map-implementation"));
        wrongSource.set(0, operator(
                "other-source", IncrementalDefinition.OperatorKind.SOURCE_V1, "incremental-source",
                sha256("schema"), Collections.<String>emptyList(), sha256("source")));
        assertThrows(IllegalArgumentException.class,
                () -> definition(descriptor(), wrongSource, sinkDigests("sink-a")));

        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalDefinition(
                        "ledger-definition", descriptor(),
                        operators("map-implementation"), Collections.<byte[]>emptyList()));

        List<IncrementalDefinition.OperatorBinding> mapAfterSink = Arrays.asList(
                operator("ledger-source", IncrementalDefinition.OperatorKind.SOURCE_V1,
                        "incremental-source", sha256("schema"),
                        Collections.<String>emptyList(), sha256("source")),
                operator("early-sink", IncrementalDefinition.OperatorKind.SINK_V1,
                        "manifest-sink", sha256("schema"),
                        Collections.singletonList("ledger-source"), sha256("early-sink")),
                operator("late-map", IncrementalDefinition.OperatorKind.MAP_V1,
                        "incremental-map", sha256("schema"),
                        Collections.singletonList("early-sink"), sha256("late-map")),
                operator("late-sink", IncrementalDefinition.OperatorKind.SINK_V1,
                        "manifest-sink", sha256("schema"),
                        Collections.singletonList("late-map"), sha256("late-sink")));
        assertThrows(IllegalArgumentException.class,
                () -> definition(descriptor(), mapAfterSink,
                        Arrays.asList(sha256("early"), sha256("late"))));

        List<IncrementalDefinition.OperatorBinding> sinkChaining = Arrays.asList(
                operator("ledger-source", IncrementalDefinition.OperatorKind.SOURCE_V1,
                        "incremental-source", sha256("schema"),
                        Collections.<String>emptyList(), sha256("source")),
                operator("sink-a", IncrementalDefinition.OperatorKind.SINK_V1,
                        "manifest-sink", sha256("schema"),
                        Collections.singletonList("ledger-source"), sha256("sink-a")),
                operator("sink-b", IncrementalDefinition.OperatorKind.SINK_V1,
                        "manifest-sink", sha256("schema"),
                        Collections.singletonList("sink-a"), sha256("sink-b")));
        assertThrows(IllegalArgumentException.class,
                () -> definition(descriptor(), sinkChaining,
                        Arrays.asList(sha256("sink-a"), sha256("sink-b"))));

        List<IncrementalDefinition.OperatorBinding> branchedMaps = Arrays.asList(
                operator("ledger-source", IncrementalDefinition.OperatorKind.SOURCE_V1,
                        "incremental-source", sha256("schema"),
                        Collections.<String>emptyList(), sha256("source")),
                operator("map-a", IncrementalDefinition.OperatorKind.MAP_V1,
                        "incremental-map", sha256("schema"),
                        Collections.singletonList("ledger-source"), sha256("map-a")),
                operator("map-b", IncrementalDefinition.OperatorKind.MAP_V1,
                        "incremental-map", sha256("schema"),
                        Collections.singletonList("ledger-source"), sha256("map-b")),
                operator("sink", IncrementalDefinition.OperatorKind.SINK_V1,
                        "manifest-sink", sha256("schema"),
                        Collections.singletonList("map-b"), sha256("sink")));
        assertThrows(IllegalArgumentException.class,
                () -> definition(descriptor(), branchedMaps, sinkDigests("sink")));
    }

    @Test
    void modelDefensivelyCopiesAndCodecRejectsCorruptOrOversizedFrames() throws Exception {
        byte[] schema = sha256("schema");
        byte[] implementation = sha256("implementation");
        byte[] sink = sha256("sink");
        IncrementalDefinition.OperatorBinding source = operator(
                "ledger-source", IncrementalDefinition.OperatorKind.SOURCE_V1,
                "incremental-source", schema, Collections.<String>emptyList(), implementation);
        IncrementalDefinition definition = new IncrementalDefinition(
                "ledger-definition", descriptor(),
                Arrays.asList(source, operator(
                        "sink", IncrementalDefinition.OperatorKind.SINK_V1, "manifest-sink",
                        sha256("schema"), Collections.singletonList("ledger-source"),
                        sha256("sink-implementation"))),
                Collections.singletonList(sink));
        schema[0] ^= 1;
        implementation[0] ^= 1;
        sink[0] ^= 1;
        assertArrayEquals(sha256("schema"), definition.operators().get(0).schemaFingerprint());
        assertArrayEquals(sha256("implementation"),
                definition.operators().get(0).implementationDigest());
        assertArrayEquals(sha256("sink"), definition.sinkBindingDigests().get(0));
        assertThrows(UnsupportedOperationException.class,
                () -> definition.operators().add(source));

        byte[] encoded = IncrementalDefinitionCodec.encode(definition);
        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalDefinitionCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalDefinitionCodec.decode(
                        new byte[IncrementalDefinitionCodec.MAX_FRAME_BYTES + 1]));
    }

    @Test
    void logicalOperatorCapacityIsIndependentFromPhysicalAssignmentCapacity() throws Exception {
        List<IncrementalDefinition.OperatorBinding> logicalOperators = new ArrayList<>();
        logicalOperators.add(operator(
                "ledger-source", IncrementalDefinition.OperatorKind.SOURCE_V1, "source",
                sha256("schema"), Collections.<String>emptyList(), sha256("source")));
        logicalOperators.add(operator(
                "map-a", IncrementalDefinition.OperatorKind.MAP_V1, "map",
                sha256("schema"), Collections.singletonList("ledger-source"), sha256("map-a")));
        logicalOperators.add(operator(
                "map-b", IncrementalDefinition.OperatorKind.MAP_V1, "map",
                sha256("schema"), Collections.singletonList("map-a"), sha256("map-b")));
        List<byte[]> sinkBindings = new ArrayList<>();
        for (int sink = 0; sink < IncrementalDefinition.MAX_SINK_BINDINGS; sink++) {
            logicalOperators.add(operator(
                    "s" + sink, IncrementalDefinition.OperatorKind.SINK_V1, "sink",
                    sha256("schema"), Collections.singletonList("map-b"), sha256("sink")));
            sinkBindings.add(sha256("binding"));
        }

        IncrementalDefinition definition = definition(
                descriptor(), logicalOperators, sinkBindings);

        assertEquals(1 + 2 + IncrementalDefinition.MAX_SINK_BINDINGS,
                definition.operators().size());
        assertTrue(IncrementalDefinitionCodec.encode(definition).length
                < IncrementalDefinitionCodec.MAX_FRAME_BYTES);
    }

    private static IncrementalDefinition definition(
            IncrementalPlanDescriptor descriptor,
            List<IncrementalDefinition.OperatorBinding> operators,
            List<byte[]> sinks) {
        return new IncrementalDefinition("ledger-definition", descriptor, operators, sinks);
    }

    private static List<IncrementalDefinition.OperatorBinding> operators(
            String mapImplementation) throws Exception {
        return Arrays.asList(
                operator("ledger-source", IncrementalDefinition.OperatorKind.SOURCE_V1,
                        "incremental-source", sha256("schema"),
                        Collections.<String>emptyList(), sha256("source-implementation")),
                operator("map", IncrementalDefinition.OperatorKind.MAP_V1,
                        "incremental-map", sha256("schema"),
                        Collections.singletonList("ledger-source"), sha256(mapImplementation)),
                operator("sink", IncrementalDefinition.OperatorKind.SINK_V1,
                        "manifest-sink", sha256("schema"),
                        Collections.singletonList("map"), sha256("sink-implementation")));
    }

    private static IncrementalDefinition.OperatorBinding operator(
            String uid, IncrementalDefinition.OperatorKind kind, String type,
            byte[] schema, List<String> inputs, byte[] implementation) {
        return new IncrementalDefinition.OperatorBinding(
                uid, kind, type, schema, inputs, implementation);
    }

    private static List<byte[]> sinkDigests(String value) throws Exception {
        return Collections.singletonList(sha256(value));
    }

    private static IncrementalPlanDescriptor descriptor() throws Exception {
        return new IncrementalPlanDescriptor(
                "ledger-source", sha256("frontier"), Collections.singletonList(0), 1,
                Collections.singletonList(2), sha256("schema"), true,
                sha256("partitions"),
                IncrementalPlanDescriptor.SinkCommitProfile.MANIFEST_ATOMIC_V1);
    }

    private static CapturedTargetReceipt receipt(long target, String token) throws Exception {
        return CapturedTargetReceipt.normal(
                1L, sha256("definition"), sha256("source-contract"), 3L,
                IncrementalFrontier.int64Sequence(3L), "trigger",
                IncrementalRunMode.INCREMENTAL_V1,
                IncrementalFrontier.int64Sequence(target), bytes(token));
    }

    private static void assertDifferent(
            IncrementalDefinition left, IncrementalDefinition right) {
        assertNotEquals(hex(left.fingerprint()), hex(right.fingerprint()));
    }

    private static byte[] sha256(String value) throws Exception {
        return sha256(bytes(value));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String hex(byte[] value) {
        StringBuilder out = new StringBuilder(value.length * 2);
        for (byte current : value) {
            int unsigned = current & 0xff;
            out.append(Character.forDigit(unsigned >>> 4, 16));
            out.append(Character.forDigit(unsigned & 0xf, 16));
        }
        return out.toString();
    }
}
