package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** connector execution SPI and immutable object snapshot model. */
class ObjectStorageSnapshotSourceModelTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));
    private static final String SHA_A = repeat('a', 64);
    private static final String SHA_B = repeat('b', 64);

    @AfterEach
    void restoreMutableProviderIdentity() {
        MutableIdentityProvider.type = "mutable-provider";
    }

    @Test
    void selfExecutingInputWinsOverLegacyRegistryWithoutOpening() {
        SelfExecutingInput input = new SelfExecutingInput();
        SourceExecutorFactory.register(SelfExecutingInput.class,
                ignored -> new MarkerExecutor<SelfExecutingInput>(
                        ignored, "legacy-registry"));

        SourceExecutor<?> executor = SourceExecutorFactory.create(input);

        assertTrue(executor instanceof MarkerExecutor);
        assertEquals("self-executing", ((MarkerExecutor) executor).marker);
        assertEquals(1, input.createCalls);
        assertFalse(((MarkerExecutor) executor).opened,
                "factory construction must remain side-effect free");
        assertSame(input, executor.input);
    }

    @Test
    void selfExecutingInputMustReturnAnExecutorBoundToItself() {
        assertThrows(IllegalArgumentException.class,
                () -> SourceExecutorFactory.create(new NullExecutorInput()));
        IllegalArgumentException mismatch = assertThrows(
                IllegalArgumentException.class,
                () -> SourceExecutorFactory.create(new WrongExecutorInput()));
        assertTrue(mismatch.getMessage().contains("same Source"), mismatch.getMessage());
    }

    @Test
    void objectIdentityIsStableAndSnapshotChangesOnlyForFrozenContent() {
        ObjectStorageSnapshotSource first = input(
                new GoodProvider(), "version-7", 123L, SHA_A);
        ObjectStorageSnapshotSource same = input(
                new GoodProvider(), "version-7", 123L, SHA_A);
        ObjectStorageSnapshotSource next = input(
                new GoodProvider(), "version-8", 124L, SHA_B);

        assertEquals(first.batchSourceSplit(), same.batchSourceSplit());
        assertEquals(first.batchSourceSplit().splitId(), next.batchSourceSplit().splitId());
        assertNotEquals(first.batchSourceSplit().snapshotId(),
                next.batchSourceSplit().snapshotId());
        assertEquals("rowweave.object-storage/test-provider",
                first.batchSourceSplit().connectorType());
        assertEquals(3, first.batchSourceSplit().connectorVersion());
        assertEquals(SchemaFingerprint.of(META),
                first.batchSourceSplit().schemaFingerprint());
        assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START,
                first.batchSourceSplit().replayGuarantee());
        assertEquals("credential-prod", first.readRequest().credentialRef());
        assertEquals("version-7", first.readRequest().versionId());
        assertEquals(123L, first.readRequest().contentLength());
        assertEquals(SHA_A, first.readRequest().contentSha256());
        assertTrue(first.toString().contains("test-provider"));
        assertFalse(first.toString().contains("domain/ledger.tsv"));
        assertFalse(first.toString().contains("credential-prod"));
        assertFalse(first.toString().contains("version-7"));
    }

    @Test
    void providerMustBeStatelessPublicAndIdentityStable() {
        IllegalArgumentException stateful = assertThrows(
                IllegalArgumentException.class,
                () -> input(new StatefulProvider(), "v1", 0L, SHA_A));
        assertTrue(stateful.getMessage().contains("instance fields"), stateful.getMessage());

        IllegalArgumentException constructor = assertThrows(
                IllegalArgumentException.class,
                () -> input(PrivateConstructorProvider.INSTANCE, "v1", 0L, SHA_A));
        assertTrue(constructor.getMessage().contains("public no-arg"), constructor.getMessage());

        ObjectStorageSnapshotSource mutable = input(
                new MutableIdentityProvider(), "v1", 0L, SHA_A);
        MutableIdentityProvider.type = "changed-provider";
        IllegalArgumentException drift = assertThrows(
                IllegalArgumentException.class, mutable::batchSourceSplit);
        assertTrue(drift.getMessage().contains("identity changed"), drift.getMessage());
    }

    @Test
    void requestBoundsRejectLocatorShapedOrOversizedValuesWithoutEchoingThem() {
        assertInvalid("endpointRef", "https://storage.invalid/bucket", "https://storage.invalid");
        assertInvalid("credentialRef", "token?secret=value", "secret=value");
        assertInvalid("namespace", "domain\nsecret-namespace", "secret-namespace");
        assertInvalid("objectKey", "domain/../secret-ledger", "secret-ledger");
        assertInvalid("versionId", "", "");
        assertThrows(IllegalArgumentException.class,
                () -> input(new GoodProvider(), "v1", -1L, SHA_A));
        assertThrows(IllegalArgumentException.class,
                () -> input(new GoodProvider(), "v1",
                        ObjectStorageReadRequest.MAX_OBJECT_BYTES + 1L, SHA_A));
        assertThrows(IllegalArgumentException.class,
                () -> input(new GoodProvider(), "v1", 0L, "etag-is-not-a-digest"));
        assertThrows(IllegalArgumentException.class,
                () -> new ObjectStorageSnapshotSource(
                        META, new GoodProvider(), "endpoint-prod", "credential-prod",
                        "domain", "ledger.tsv", "v1", 0L, SHA_A,
                        ObjectStorageSnapshotSource.MIN_ROW_BYTES - 1));
        assertThrows(IllegalArgumentException.class,
                () -> new ObjectStorageSnapshotSource(
                        META, new GoodProvider(), "endpoint-prod", "credential-prod",
                        "domain", "ledger.tsv", "v1", 0L, SHA_A,
                        ObjectStorageSnapshotSource.MAX_ROW_BYTES + 1));
    }

    @Test
    void serializationRoundTripRevalidatesDerivedDescriptorAndProvider() throws Exception {
        ObjectStorageSnapshotSource original = input(
                new GoodProvider(), "version-7", 123L, SHA_A);
        ObjectStorageSnapshotSource decoded =
                (ObjectStorageSnapshotSource) roundTrip(original);
        assertEquals(original.batchSourceSplit(), decoded.batchSourceSplit());
        assertEquals(original.readRequest(), decoded.readRequest());
        assertTrue(SourceExecutorFactory.create(decoded)
                instanceof ObjectStorageSnapshotInputExecutor);

        java.lang.reflect.Field descriptor = ObjectStorageSnapshotSource.class
                .getDeclaredField("batchSourceSplit");
        descriptor.setAccessible(true);
        descriptor.set(original, new BatchSourceSplitDescriptor(
                1, "rowweave.object-storage/test-provider", 3,
                "forged-split", SchemaFingerprint.of(META), "forged-snapshot",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START));
        assertThrows(InvalidObjectException.class, () -> roundTrip(original));
    }

    private static ObjectStorageSnapshotSource input(
            ObjectStorageSourceProvider provider, String version,
            long contentLength, String sha256) {
        return new ObjectStorageSnapshotSource(
                META, provider, "endpoint-prod", "credential-prod",
                "domain", "domain/ledger.tsv", version, contentLength, sha256,
                ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES);
    }

    private static void assertInvalid(String field, String value, String secretFragment) {
        IllegalArgumentException failure;
        switch (field) {
            case "endpointRef":
                failure = assertThrows(IllegalArgumentException.class,
                        () -> new ObjectStorageSnapshotSource(
                                META, new GoodProvider(), value, "credential-prod", "domain",
                                "ledger.tsv", "v1", 0L, SHA_A,
                                ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES));
                break;
            case "credentialRef":
                failure = assertThrows(IllegalArgumentException.class,
                        () -> new ObjectStorageSnapshotSource(
                                META, new GoodProvider(), "endpoint-prod", value, "domain",
                                "ledger.tsv", "v1", 0L, SHA_A,
                                ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES));
                break;
            case "namespace":
                failure = assertThrows(IllegalArgumentException.class,
                        () -> new ObjectStorageSnapshotSource(
                                META, new GoodProvider(), "endpoint-prod", "credential-prod", value,
                                "ledger.tsv", "v1", 0L, SHA_A,
                                ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES));
                break;
            case "objectKey":
                failure = assertThrows(IllegalArgumentException.class,
                        () -> new ObjectStorageSnapshotSource(
                                META, new GoodProvider(), "endpoint-prod", "credential-prod", "domain",
                                value, "v1", 0L, SHA_A,
                                ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES));
                break;
            case "versionId":
                failure = assertThrows(IllegalArgumentException.class,
                        () -> new ObjectStorageSnapshotSource(
                                META, new GoodProvider(), "endpoint-prod", "credential-prod", "domain",
                                "ledger.tsv", value, 0L, SHA_A,
                                ObjectStorageSnapshotSource.DEFAULT_MAX_ROW_BYTES));
                break;
            default:
                throw new AssertionError(field);
        }
        assertTrue(failure.getMessage().contains(field), failure.getMessage());
        if (!secretFragment.isEmpty()) {
            assertFalse(failure.getMessage().contains(secretFragment), failure.getMessage());
        }
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

    private static String repeat(char value, int count) {
        return String.join("", Collections.nCopies(count, String.valueOf(value)));
    }

    public static final class GoodProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        public GoodProvider() { }
        @Override public String providerType() { return "test-provider"; }
        @Override public int providerVersion() { return 3; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) {
            throw new AssertionError("CS1 factory must not open provider");
        }
    }

    public static final class StatefulProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        private final String secret = "must-not-be-serialized";
        public StatefulProvider() { }
        @Override public String providerType() { return "stateful-provider"; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) { return null; }
    }

    public static final class PrivateConstructorProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static final PrivateConstructorProvider INSTANCE = new PrivateConstructorProvider();
        private PrivateConstructorProvider() { }
        @Override public String providerType() { return "private-provider"; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) { return null; }
    }

    public static final class MutableIdentityProvider implements ObjectStorageSourceProvider {
        private static final long serialVersionUID = 1L;
        static String type = "mutable-provider";
        public MutableIdentityProvider() { }
        @Override public String providerType() { return type; }
        @Override public int providerVersion() { return 1; }
        @Override public ObjectStorageReadHandle open(ObjectStorageReadRequest request) { return null; }
    }

    private static final class SelfExecutingInput implements BatchExecutableSource {
        private static final long serialVersionUID = 1L;
        private int createCalls;
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() {
            return descriptor("self-executing");
        }
        @Override public SourceExecutor<?> createSourceExecutor() {
            createCalls++;
            return new MarkerExecutor(this, "self-executing");
        }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }

    private static final class NullExecutorInput implements BatchExecutableSource {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor("null"); }
        @Override public SourceExecutor<?> createSourceExecutor() { return null; }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }

    private static final class WrongExecutorInput implements BatchExecutableSource {
        private static final long serialVersionUID = 1L;
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor("wrong"); }
        @Override public SourceExecutor<?> createSourceExecutor() {
            return new MarkerExecutor(new SelfExecutingInput(), "wrong");
        }
        @Override public Iterator<DataRow> iterator() { return Collections.emptyIterator(); }
    }

    private static BatchSourceSplitDescriptor descriptor(String splitId) {
        return new BatchSourceSplitDescriptor(
                1, "test-self-executing", 1, splitId,
                SchemaFingerprint.of(META), "snapshot-" + splitId,
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
    }

    private static final class MarkerExecutor<I extends Source> extends SourceExecutor<I> {
        private final String marker;
        private boolean opened;
        private MarkerExecutor(I input, String marker) {
            super(input, input.getSchema());
            this.marker = marker;
        }
        @Override public void open() { opened = true; }
        @Override public boolean hasNext() { return false; }
        @Override public DataRow next() { throw new IllegalStateException("empty"); }
        @Override public void close() { }
    }

}
