package io.rowweave.plan;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSource;
import io.rowweave.Source;
import io.rowweave.ManifestFileSource;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** exact built-ins and durable source admission. */
class BatchSourceBuiltInAdmissionTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("name", DataType.StringType));

    @Test
    void explicitCollectionIdBindsCanonicalOrderedSnapshotWhileOldConstructorStaysLegacy() {
        CollectionSource first = new CollectionSource(
                META, "orders-0", Arrays.asList(row(1, "a"), row(2, "b")));
        CollectionSource same = new CollectionSource(
                META, "orders-0", Arrays.asList(row(1, "a"), row(2, "b")));
        CollectionSource changed = new CollectionSource(
                META, "orders-0", Arrays.asList(row(1, "a"), row(2, "changed")));
        CollectionSource reordered = new CollectionSource(
                META, "orders-0", Arrays.asList(row(2, "b"), row(1, "a")));

        BatchSourceSplitDescriptor descriptor = descriptor(first);
        assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START,
                descriptor.replayGuarantee());
        assertEquals("orders-0", descriptor.splitId());
        assertEquals(SchemaFingerprint.of(META), descriptor.schemaFingerprint());
        assertEquals(descriptor, descriptor(same));
        assertNotEquals(descriptor.snapshotId(), descriptor(changed).snapshotId());
        assertNotEquals(descriptor.snapshotId(), descriptor(reordered).snapshotId());
        assertDoesNotThrow(() -> BatchSourcePlanValidator.validateDurable(graph(first)));
        assertThrows(IllegalArgumentException.class,
                () -> new CollectionSource(META, null, Collections.emptyList()));
        assertThrows(IllegalArgumentException.class,
                () -> new CollectionSource(META, "", Collections.emptyList()));

        CollectionSource legacy = new CollectionSource(
                META, Collections.singletonList(row(1, "a")));
        BatchSourceSplitDescriptor legacyDescriptor = graph(legacy)
                .node(0).sourceSplitDescriptors().get(0);
        assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT,
                legacyDescriptor.replayGuarantee());
        assertUnsupported(legacy);
    }

    @Test
    void onlyPinnedManifestGenerationIsExact(@TempDir Path tempDir) {
        Path domain = tempDir.resolve("manifest-domain");
        ManifestFileSource latest = new ManifestFileSource(
                META, domain.toString(), "ledger");
        ManifestFileSource pinned = new ManifestFileSource(
                META, domain.toString(), "ledger", 7L);
        ManifestFileSource same = new ManifestFileSource(
                META, domain.toString(), "ledger", 7L);
        ManifestFileSource next = new ManifestFileSource(
                META, domain.toString(), "ledger", 8L);

        assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT,
                graph(latest).node(0).sourceSplitDescriptors().get(0).replayGuarantee());
        BatchSourceSplitDescriptor pinnedDescriptor = descriptor(pinned);
        assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START,
                pinnedDescriptor.replayGuarantee());
        assertEquals(pinnedDescriptor, descriptor(same));
        assertEquals(pinnedDescriptor.splitId(), descriptor(next).splitId());
        assertNotEquals(pinnedDescriptor.snapshotId(), descriptor(next).snapshotId());
        assertDoesNotThrow(() -> BatchSourcePlanValidator.validateDurable(graph(pinned)));
        assertUnsupported(latest);
    }

    @Test
    void plainFileAndUndeclaredCustomExactConnectorAreRejected(@TempDir Path tempDir) {
        assertUnsupported(new FileSource(META, tempDir.resolve("plain.tsv").toString()));
        assertUnsupported(new CustomExactInput());
        assertDoesNotThrow(() -> BatchSourcePlanValidator.validateDurable(
                new PlanGraph(Collections.singletonList(PlanNode.placeholderSource(META)))));
    }

    @Test
    void customInputCannotClaimReservedObjectStorageCapability() {
        IllegalArgumentException rejected = assertThrows(
                IllegalArgumentException.class,
                () -> BatchSourcePlanValidator.validateDurable(
                        graph(new SpoofedObjectStorageInput()),
                        ObjectTrustContext.localTrusted(getClass().getClassLoader())));
        assertTrue(rejected.getMessage().contains("RESERVED_OBJECT_STORAGE_SOURCE"),
                rejected.getMessage());
    }

    @Test
    void javaSerializationCannotForgeBuiltInSnapshotIdentity(@TempDir Path tempDir)
            throws Exception {
        CollectionSource collection = new CollectionSource(
                META, "immutable-0", Collections.singletonList(row(1, "before")));
        java.lang.reflect.Field rows = CollectionSource.class.getDeclaredField("rows");
        rows.setAccessible(true);
        ((DataRow[]) rows.get(collection))[0].set(1, "after");
        assertThrows(InvalidObjectException.class, () -> roundTrip(collection));

        ManifestFileSource manifest = new ManifestFileSource(
                META, tempDir.resolve("tampered-manifest").toString(), "ledger", 3L);
        java.lang.reflect.Field frozen =
                ManifestFileSource.class.getDeclaredField("batchSourceSplit");
        frozen.setAccessible(true);
        frozen.set(manifest, new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "forged-member",
                SchemaFingerprint.of(META), "forged-snapshot",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START));
        assertThrows(InvalidObjectException.class, () -> roundTrip(manifest));

        ManifestFileSource oldPinned = new ManifestFileSource(
                META, tempDir.resolve("old-pinned").toString(), "ledger", 4L);
        frozen.set(oldPinned, null);
        ManifestFileSource decodedLegacy = (ManifestFileSource) roundTrip(oldPinned);
        assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT,
                graph(decodedLegacy).node(0).sourceSplitDescriptors().get(0).replayGuarantee());
        assertUnsupported(decodedLegacy);
    }

    @Test
    void exactBuiltInDescriptorIsInTheMtlsCoreTrustClosure() {
        CollectionSource input = new CollectionSource(
                META, "trusted-0", Collections.singletonList(row(9, "trusted")));
        PlanGraph graph = graph(input);

        byte[] bytes = graph.toBytes(ObjectTrustContext.mtlsBuiltIns(), ignored -> { });
        PlanGraph decoded = PlanGraph.fromBytes(
                bytes, ObjectTrustContext.mtlsBuiltIns(), ignored -> { });
        assertEquals(graph.node(0).sourceSplitDescriptors(),
                decoded.node(0).sourceSplitDescriptors());
    }

    private static BatchSourceSplitDescriptor descriptor(BatchSource input) {
        return input.batchSourceSplit();
    }

    private static void assertUnsupported(Source input) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> BatchSourcePlanValidator.validateDurable(graph(input)));
        assertTrue(failure.getMessage().contains("UNSUPPORTED_SOURCE_REPLAY"),
                failure.getMessage());
    }

    private static PlanGraph graph(Source input) {
        return new PlanGraph(Collections.singletonList(
                PlanNode.sourceFromInputs(META, Collections.singletonList(input))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source-ledger")));
    }

    private static DataRow row(long id, String name) {
        DataRow row = new DataRow(2);
        row.set(0, id);
        row.set(1, name);
        return row;
    }

    private static Object roundTrip(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        try (ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))) {
            return in.readObject();
        }
    }

    private static final class CustomExactInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor =
                new BatchSourceSplitDescriptor(
                        1, "custom-exact", 1, "custom-0",
                        SchemaFingerprint.of(META), "custom-snapshot",
                        BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }

    private static final class SpoofedObjectStorageInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor =
                new BatchSourceSplitDescriptor(
                        1, "rowweave.object-storage/forged", 1, "forged-object",
                        SchemaFingerprint.of(META), "forged-snapshot",
                        BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }
}
