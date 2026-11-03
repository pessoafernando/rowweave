package io.rowweave;

import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.ops.RowValidation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Object-only classification facade and side-effect-free plan model. */
class ObjectStorageRowClassificationModelTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));
    private static final String SHA_A = repeat('a');
    private static final String SHA_B = repeat('b');

    @AfterEach
    void resetProvider() {
        ModelProvider.opens = 0;
        OtherProvider.opens = 0;
    }

    @Test
    void facadeExposesOnlyAtomicManifestPublicationAndCannotBeForged() {
        ObjectStorageRowClassification classification = classification(
                new ExecutionEnvironment("object-quality-api", "object quality api"));

        List<String> publicMethods = new ArrayList<>();
        for (Method method : ObjectStorageRowClassification.class.getDeclaredMethods()) {
            if (Modifier.isPublic(method.getModifiers())) publicMethods.add(method.getName());
        }
        assertEquals(Arrays.asList("writeToManifest"), publicMethods);
        for (Constructor<?> constructor
                : ObjectStorageRowClassification.class.getDeclaredConstructors()) {
            assertFalse(Modifier.isPublic(constructor.getModifiers()),
                    "callers must not forge or extract classification branches");
        }
        assertEquals(0, ModelProvider.opens);
    }

    @Test
    void buildsOneTenNodeThreeMemberPlanWithoutOpeningProviderOrOutput(
            @TempDir Path temp) {
        Path domain = temp.resolve("not-created").resolve("quality");
        ExecutionEnvironment session = new ExecutionEnvironment("object-quality-plan", "object quality plan");
        ObjectStorageSnapshotSource first = input(
                META, new ModelProvider(), "part-a.tsv", "version-1", 10L, SHA_A);
        ObjectStorageSnapshotSource second = input(
                META, new ModelProvider(), "part-b.tsv", "version-2", 20L, SHA_B);
        String firstOriginalDigest = digest(first.batchSourceSplit());

        session.classifyObjectStorageRows("ledger.object.source", first, second)
                .writeToManifest(TypedQualityManifest.of(
                        domain.toString(), "valid", "rejected", "summary"));
        PlanGraph graph = session.toPlanGraph();

        assertEquals(10, graph.nodeCount());
        assertEquals(1, nodesOfKind(graph, PlanNode.Kind.SOURCE).size());
        assertEquals(2, countType(graph, "objectQualityRoute"));
        assertEquals(1, countType(graph, "typedQualityAcceptedSummary"));
        assertEquals(1, countType(graph, "typedQualityRejectedSummary"));
        assertEquals(1, countType(graph, "union"));
        assertEquals(1, countType(graph, "orderBy"));
        assertEquals(3, nodesOfKind(graph, PlanNode.Kind.SINK).size());

        PlanNode source = graph.node(graph.singleSourceIndex());
        assertEquals("ledger.object.source", source.param(PlanNode.OPERATOR_UID_PARAM));
        assertEquals(META.getFieldCount() + 8, source.outMeta().getFieldCount());
        assertCarrierTail(source.outMeta(), META.getFieldCount());
        assertEquals(2, source.inputs().size());
        assertTrue(source.inputs().get(0) instanceof ObjectStorageQualitySource);
        ObjectStorageQualitySource wrapped =
                (ObjectStorageQualitySource) source.inputs().get(0);
        assertEquals(firstOriginalDigest, wrapped.originalSplitIdentitySha256());
        assertTrue(wrapped.batchSourceSplit().connectorType()
                .startsWith("rowweave.object-storage-row-quality/"));
        assertNotEquals(first.batchSourceSplit().schemaFingerprint(),
                wrapped.batchSourceSplit().schemaFingerprint());
        assertEquals(source.outMeta(), wrapped.getSchema());

        Set<String> members = new HashSet<>();
        for (PlanNode sink : nodesOfKind(graph, PlanNode.Kind.SINK)) {
            assertEquals(ManifestFileSink.class, sink.output().getClass());
            members.add(((ManifestFileSink) sink.output()).memberName());
        }
        assertEquals(new HashSet<>(Arrays.asList("valid", "rejected", "summary")), members);
        assertEquals(META, sinkMeta(graph, "valid"));
        assertRejectedMeta(sinkMeta(graph, "rejected"));
        assertEquals(TypedQualityManifest.summarySchema(), sinkMeta(graph, "summary"));
        assertFalse(Files.exists(domain));
        assertEquals(0, ModelProvider.opens);
    }

    @Test
    void rejectsInvalidUidInputsSchemaProviderGroupAndCarrierWidth() {
        ExecutionEnvironment session = new ExecutionEnvironment("object-quality-invalid", "object quality invalid");
        ObjectStorageSnapshotSource good = input(
                META, new ModelProvider(), "part-a.tsv", "version-1", 10L, SHA_A);
        assertInvalid(() -> session.classifyObjectStorageRows(null, good));
        assertInvalid(() -> session.classifyObjectStorageRows("", good));
        assertInvalid(() -> session.classifyObjectStorageRows("source"));
        assertInvalid(() -> session.classifyObjectStorageRows(
                "source", (ObjectStorageSnapshotSource[]) null));
        assertInvalid(() -> session.classifyObjectStorageRows(
                "source", good, null));

        DataSchema drift = new DataSchema(new Field("id", DataType.LongType));
        assertInvalid(() -> session.classifyObjectStorageRows(
                "source", good,
                input(drift, new ModelProvider(), "part-b.tsv", "version-1", 10L, SHA_B)));
        assertInvalid(() -> session.classifyObjectStorageRows(
                "source", good,
                input(META, new OtherProvider(), "part-b.tsv", "version-1", 10L, SHA_B)));

        Field[] fields = new Field[io.rowweave.ops.RowValidation.MAX_FIELDS - 7];
        for (int i = 0; i < fields.length; i++) {
            fields[i] = new Field("f" + i, DataType.IntegerType);
        }
        assertInvalid(() -> session.classifyObjectStorageRows(
                "source", input(new DataSchema(fields), new ModelProvider(),
                        "wide.tsv", "version-1", 10L, SHA_A)));
        assertEquals(0, ModelProvider.opens);
        assertEquals(0, OtherProvider.opens);
    }

    @Test
    void invalidTargetLeavesNoPartiallyRegisteredSink(@TempDir Path temp) {
        ExecutionEnvironment session = new ExecutionEnvironment("object-quality-atomic", "object quality atomic");
        ObjectStorageRowClassification classification = classification(session);

        assertPublicationInvalid(() -> classification.writeToManifest(null));
        assertThrows(IllegalStateException.class, session::toPlanGraph);
        assertFalse(Files.exists(temp.resolve("unused")));
        assertEquals(0, ModelProvider.opens);
    }

    @Test
    void reservesTheWholeJobAgainstOrdinaryTypedAndRepeatedPublications(
            @TempDir Path temp) {
        ObjectStorageSnapshotSource object = input(
                META, new ModelProvider(), "part-a.tsv", "version-1", 10L, SHA_A);

        ExecutionEnvironment existingSink = new ExecutionEnvironment("existing", "existing");
        existingSink.fromSource(new CollectionSource(
                        META, "collection", java.util.Collections.emptyList()))
                .sinkTo(new DiscardSink(META));
        assertInvalid(() -> existingSink.classifyObjectStorageRows("object.source", object));

        ExecutionEnvironment ordinaryAfter = new ExecutionEnvironment("ordinary-after", "ordinary after");
        ordinaryAfter.classifyObjectStorageRows("object.source", object);
        assertInvalid(() -> ordinaryAfter.fromSource(new CollectionSource(
                        META, "collection", java.util.Collections.emptyList()))
                .sinkTo(new DiscardSink(META)));

        ExecutionEnvironment typedAfter = new ExecutionEnvironment("typed-after", "typed after");
        typedAfter.classifyObjectStorageRows("object.source", object);
        DataRowClassification typed = typedAfter.fromSource(new CollectionSource(
                        META, "collection", java.util.Collections.emptyList()))
                .uid("typed.source")
                .classifyRowsWithSourcePosition(RowValidation.builder(META)
                        .notNull("adaptor.required", 0).build());
        assertPublicationInvalid(() -> typed.writeToManifest(TypedQualityManifest.of(
                temp.resolve("typed").toString(), "valid", "rejected", "summary")));

        ExecutionEnvironment repeated = new ExecutionEnvironment("repeated", "repeated");
        ObjectStorageRowClassification publication =
                repeated.classifyObjectStorageRows("object.source", object);
        publication.writeToManifest(TypedQualityManifest.of(
                temp.resolve("object").toString(), "valid", "rejected", "summary"));
        assertPublicationInvalid(() -> publication.writeToManifest(TypedQualityManifest.of(
                temp.resolve("object-2").toString(), "valid", "rejected", "summary")));
        assertInvalid(() -> repeated.classifyObjectStorageRows("object.source.2", object));
        assertEquals(0, ModelProvider.opens);
    }

    @Test
    void wrapperRoundTripPreservesDerivedIdentityAndRedactsOriginalLocator()
            throws Exception {
        ObjectStorageSnapshotSource original = input(
                META, new ModelProvider(), "domain/private-ledger.tsv",
                "secret-version-7", 10L, SHA_A);
        ExecutionEnvironment session = new ExecutionEnvironment("object-quality-copy", "object quality copy");
        session.classifyObjectStorageRows("ledger.object.source", original)
                .writeToManifest(TypedQualityManifest.of(
                        Paths.get("target", "object-quality").toAbsolutePath().toString(),
                        "valid", "rejected", "summary"));
        ObjectStorageQualitySource wrapped = (ObjectStorageQualitySource)
                session.toPlanGraph().node(0).inputs().get(0);

        ObjectStorageQualitySource copy = roundTrip(wrapped);

        assertEquals(wrapped.batchSourceSplit(), copy.batchSourceSplit());
        assertEquals(wrapped.originalSplitIdentitySha256(),
                copy.originalSplitIdentitySha256());
        assertFalse(copy.toString().contains("private-ledger"));
        assertFalse(copy.toString().contains("secret-version"));
        assertFalse(copy.toString().contains("credential-prod"));
        assertEquals(0, ModelProvider.opens);
    }

    @Test
    void wholePlanRoundTripPreservesQualitySourceWithoutOpeningProvider(
            @TempDir Path temp) {
        ExecutionEnvironment session = new ExecutionEnvironment("object-quality-plan-copy", "plan copy");
        session.classifyObjectStorageRows("ledger.object.source", input(
                        META, new ModelProvider(), "domain/private-ledger.tsv",
                        "secret-version-7", 10L, SHA_A))
                .writeToManifest(TypedQualityManifest.of(
                        temp.resolve("quality").toString(),
                        "valid", "rejected", "summary"));

        PlanGraph copy = PlanGraph.fromBytes(session.toPlanGraph().toBytes());

        assertEquals(10, copy.nodeCount());
        ObjectStorageQualitySource wrapped = (ObjectStorageQualitySource)
                copy.node(copy.singleSourceIndex()).inputs().get(0);
        assertTrue(wrapped.batchSourceSplit().connectorType()
                .startsWith("rowweave.object-storage-row-quality/"));
        assertFalse(wrapped.toString().contains("private-ledger"));
        assertFalse(wrapped.toString().contains("secret-version"));
        assertEquals(0, ModelProvider.opens);
        assertFalse(Files.exists(temp.resolve("quality")));
    }

    @Test
    void serializedWrapperRejectsTamperedDerivedIdentity() throws Exception {
        ObjectStorageQualitySource wrapped = new ObjectStorageQualitySource(
                "ledger.object.source", input(
                        META, new ModelProvider(), "part-a.tsv", "version-1", 10L, SHA_A));
        java.lang.reflect.Field identity = ObjectStorageQualitySource.class
                .getDeclaredField("originalSplitIdentitySha256");
        identity.setAccessible(true);
        identity.set(wrapped, SHA_B);

        Exception failure = assertThrows(Exception.class, () -> roundTrip(wrapped));

        assertTrue(hasCause(failure, InvalidObjectException.class), failure::toString);
        assertEquals(0, ModelProvider.opens);
    }

    private static ObjectStorageRowClassification classification(ExecutionEnvironment session) {
        return session.classifyObjectStorageRows(
                "ledger.object.source",
                input(META, new ModelProvider(), "part-a.tsv", "version-1", 10L, SHA_A));
    }

    private static ObjectStorageSnapshotSource input(
            DataSchema meta, ObjectStorageSourceProvider provider, String key,
            String version, long length, String sha) {
        return new ObjectStorageSnapshotSource(
                meta, provider, "endpoint-prod", "credential-prod", "domain",
                key, version, length, sha, ObjectStorageSnapshotSource.MIN_ROW_BYTES);
    }

    private static int countType(PlanGraph graph, String typeId) {
        int count = 0;
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (typeId.equals(graph.node(i).typeId())) count++;
        }
        return count;
    }

    private static List<PlanNode> nodesOfKind(PlanGraph graph, PlanNode.Kind kind) {
        List<PlanNode> result = new ArrayList<>();
        for (int i = 0; i < graph.nodeCount(); i++) {
            if (graph.node(i).kind() == kind) result.add(graph.node(i));
        }
        return result;
    }

    private static DataSchema sinkMeta(PlanGraph graph, String member) {
        for (PlanNode sink : nodesOfKind(graph, PlanNode.Kind.SINK)) {
            ManifestFileSink output = (ManifestFileSink) sink.output();
            if (member.equals(output.memberName())) return sink.outMeta();
        }
        throw new AssertionError("missing sink member " + member);
    }

    private static void assertCarrierTail(DataSchema meta, int offset) {
        assertField(meta, offset, "quality_route_tag", DataType.IntegerType);
        assertField(meta, offset + 1, "source_operator_uid", DataType.StringType);
        assertField(meta, offset + 2, "split_identity_sha256", DataType.StringType);
        assertField(meta, offset + 3, "split_local_ordinal", DataType.LongType);
        assertField(meta, offset + 4, "quality_error_code", DataType.StringType);
        assertField(meta, offset + 5, "quality_rule_id", DataType.StringType);
        assertField(meta, offset + 6, "quality_field_index", DataType.IntegerType);
        assertField(meta, offset + 7, "quality_detail", DataType.StringType);
    }

    private static void assertRejectedMeta(DataSchema meta) {
        assertEquals(7, meta.getFieldCount());
        assertField(meta, 0, "source_operator_uid", DataType.StringType);
        assertField(meta, 1, "split_identity_sha256", DataType.StringType);
        assertField(meta, 2, "split_local_ordinal", DataType.LongType);
        assertField(meta, 3, "quality_error_code", DataType.StringType);
        assertField(meta, 4, "quality_rule_id", DataType.StringType);
        assertField(meta, 5, "quality_field_index", DataType.IntegerType);
        assertField(meta, 6, "quality_detail", DataType.StringType);
    }

    private static void assertField(
            DataSchema meta, int index, String name, DataType type) {
        assertEquals(name, meta.getField(index).getName());
        assertEquals(type, meta.getField(index).getDataType());
    }

    private static String digest(BatchSourceSplitDescriptor descriptor) {
        byte[] bytes = BatchSourceSplitDescriptorCodec.digest(descriptor);
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(Character.forDigit((value >>> 4) & 0xf, 16));
            result.append(Character.forDigit(value & 0xf, 16));
        }
        return result.toString();
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    private static ObjectStorageQualitySource roundTrip(ObjectStorageQualitySource input)
            throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(input);
        }
        try (ObjectInputStream objectInput = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            return (ObjectStorageQualitySource) objectInput.readObject();
        }
    }

    private static boolean hasCause(Throwable failure, Class<? extends Throwable> type) {
        Throwable current = failure;
        while (current != null) {
            if (type.isInstance(current)) return true;
            current = current.getCause();
        }
        return false;
    }

    private static void assertInvalid(org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().startsWith("OBJECT_QUALITY_INVALID:"),
                failure::getMessage);
    }

    private static void assertPublicationInvalid(
            org.junit.jupiter.api.function.Executable executable) {
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class, executable);
        assertTrue(failure.getMessage().startsWith("QUALITY_PUBLICATION_INVALID:"),
                failure::getMessage);
    }

    public static final class ModelProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static int opens;
        public ModelProvider() { }
        @Override public String providerType() { return "quality-model"; }
        @Override public int providerVersion() { return 3; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            opens++;
            return null;
        }
    }

    public static final class OtherProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static int opens;
        public OtherProvider() { }
        @Override public String providerType() { return "other-quality-model"; }
        @Override public int providerVersion() { return 3; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            opens++;
            return null;
        }
    }
}
