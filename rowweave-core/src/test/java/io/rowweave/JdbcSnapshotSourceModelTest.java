package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InvalidObjectException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.ResultSet;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** immutable JDBC snapshot model and signed stateless provider SPI. */
class JdbcSnapshotSourceModelTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));
    private static final String DIGEST_A = repeat('a', 64);
    private static final String DIGEST_B = repeat('b', 64);
    private static final String DIGEST_C = repeat('c', 64);
    private static final String DIGEST_D = repeat('d', 64);

    @AfterEach
    void restoreMutableProviderIdentity() {
        MutableIdentityProvider.type = "mutable-jdbc";
        GoodProvider.openCalls = 0;
    }

    @Test
    void identitySeparatesLogicalPartitionFromDatabaseSnapshot() {
        JdbcSnapshotSource first = input(
                new GoodProvider(), "partition-00", DIGEST_B, DIGEST_C);
        JdbcSnapshotSource same = input(
                new GoodProvider(), "partition-00", DIGEST_B, DIGEST_C);
        JdbcSnapshotSource nextSnapshot = input(
                new GoodProvider(), "partition-00", DIGEST_B, DIGEST_D);
        JdbcSnapshotSource nextPartition = input(
                new GoodProvider(), "partition-01", DIGEST_D, DIGEST_C);

        assertEquals(first.batchSourceSplit(), same.batchSourceSplit());
        assertEquals(first.batchSourceSplit().splitId(),
                nextSnapshot.batchSourceSplit().splitId());
        assertNotEquals(first.batchSourceSplit().snapshotId(),
                nextSnapshot.batchSourceSplit().snapshotId());
        assertNotEquals(first.batchSourceSplit().splitId(),
                nextPartition.batchSourceSplit().splitId());
        assertEquals("rowweave.jdbc-snapshot/test-jdbc",
                first.batchSourceSplit().connectorType());
        assertEquals(2, first.batchSourceSplit().connectorVersion());
        assertEquals(SchemaFingerprint.of(META),
                first.batchSourceSplit().schemaFingerprint());
        assertEquals(BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START,
                first.batchSourceSplit().replayGuarantee());
    }

    @Test
    void requestContainsOnlyBoundedReferencesAndDigestsAndFactoryDoesNotOpen() {
        JdbcSnapshotSource input = input(
                new GoodProvider(), "partition-00", DIGEST_B, DIGEST_C);

        JdbcSnapshotReadRequest request = input.readRequest();
        assertEquals("endpoint-prod", request.endpointRef());
        assertEquals("credential-prod", request.credentialRef());
        assertEquals("ledger", request.datasetRef());
        assertEquals("ledger-by-range", request.queryRef());
        assertEquals("partition-00", request.partitionRef());
        assertEquals("snapshot-prod", request.snapshotRef());
        assertEquals(DIGEST_A, request.queryDigest());
        assertEquals(DIGEST_B, request.partitionDigest());
        assertEquals(DIGEST_C, request.snapshotId());
        assertEquals(JdbcSnapshotSource.DEFAULT_FETCH_SIZE, request.fetchSize());
        assertEquals(JdbcSnapshotSource.DEFAULT_QUERY_TIMEOUT_SECONDS,
                request.queryTimeoutSeconds());
        assertEquals(JdbcSnapshotSource.DEFAULT_MAX_CELL_BYTES,
                request.maxCellBytes());

        SourceExecutor<?> executor = SourceExecutorFactory.create(input);
        assertTrue(executor instanceof JdbcSnapshotInputExecutor);
        assertSame(input, executor.input);
        assertEquals(0, GoodProvider.openCalls,
                "factory construction must not resolve credentials or open JDBC");

        String inputSummary = input.toString();
        String requestSummary = request.toString();
        for (String sensitive : new String[]{"endpoint-prod", "credential-prod",
                "ledger-by-range", "partition-00", "snapshot-prod"}) {
            assertFalse(inputSummary.contains(sensitive), inputSummary);
            assertFalse(requestSummary.contains(sensitive), requestSummary);
        }
        for (java.lang.reflect.Field field : JdbcSnapshotReadRequest.class
                .getDeclaredFields()) {
            String name = field.getName().toLowerCase();
            assertFalse(name.contains("url") || name.equals("sql")
                            || name.contains("password") || name.equals("user")
                            || name.contains("bind") || name.contains("token"),
                    "request must not serialize raw JDBC material: " + field.getName());
        }
    }

    @Test
    void providerMustBePublicStatelessConstructibleAndIdentityStable() {
        IllegalArgumentException stateful = assertThrows(
                IllegalArgumentException.class,
                () -> input(new StatefulProvider(), "p0", DIGEST_B, DIGEST_C));
        assertTrue(stateful.getMessage().contains("instance fields"),
                stateful.getMessage());

        IllegalArgumentException constructor = assertThrows(
                IllegalArgumentException.class,
                () -> input(PrivateConstructorProvider.INSTANCE,
                        "p0", DIGEST_B, DIGEST_C));
        assertTrue(constructor.getMessage().contains("public no-arg"),
                constructor.getMessage());

        JdbcSnapshotSource mutable = input(
                new MutableIdentityProvider(), "p0", DIGEST_B, DIGEST_C);
        MutableIdentityProvider.type = "changed-jdbc";
        IllegalArgumentException drift = assertThrows(
                IllegalArgumentException.class, mutable::batchSourceSplit);
        assertTrue(drift.getMessage().contains("identity changed"),
                drift.getMessage());
    }

    @Test
    void referencesDigestsAndExecutionHintsHaveHardBoundsWithoutEchoingValues() {
        assertInvalidRef("endpointRef", "jdbc:h2:mem:secret", "jdbc:h2");
        assertInvalidRef("credentialRef", "password?value=secret", "secret");
        assertInvalidRef("datasetRef", "ledger data", "ledger data");
        assertInvalidRef("queryRef", "select * from ledger", "select *");
        assertInvalidRef("partitionRef", "p/../../secret", "secret");
        assertInvalidRef("snapshotRef", "snapshot#token", "token");

        assertInvalidDigest("queryDigest", "not-a-digest");
        assertInvalidDigest("partitionDigest", DIGEST_A.toUpperCase());
        assertInvalidDigest("snapshotId", DIGEST_A.substring(1));

        assertInvalidHints(0, 1, JdbcSnapshotSource.MIN_CELL_BYTES);
        assertInvalidHints(JdbcSnapshotSource.MAX_FETCH_SIZE + 1,
                1, JdbcSnapshotSource.MIN_CELL_BYTES);
        assertInvalidHints(1, 0, JdbcSnapshotSource.MIN_CELL_BYTES);
        assertInvalidHints(1,
                JdbcSnapshotSource.MAX_QUERY_TIMEOUT_SECONDS + 1,
                JdbcSnapshotSource.MIN_CELL_BYTES);
        assertInvalidHints(1, 1, JdbcSnapshotSource.MIN_CELL_BYTES - 1);
        assertInvalidHints(1, 1, JdbcSnapshotSource.MAX_CELL_BYTES + 1);
    }

    @Test
    void serializationRoundTripRevalidatesDerivedIdentityAndProvider() throws Exception {
        JdbcSnapshotSource original = input(
                new GoodProvider(), "partition-00", DIGEST_B, DIGEST_C);
        JdbcSnapshotSource decoded = (JdbcSnapshotSource) roundTrip(original);

        assertEquals(original.batchSourceSplit(), decoded.batchSourceSplit());
        assertEquals(original.readRequest(), decoded.readRequest());
        assertTrue(SourceExecutorFactory.create(decoded)
                instanceof JdbcSnapshotInputExecutor);

        java.lang.reflect.Field descriptor = JdbcSnapshotSource.class
                .getDeclaredField("batchSourceSplit");
        descriptor.setAccessible(true);
        descriptor.set(original, new BatchSourceSplitDescriptor(
                1, "rowweave.jdbc-snapshot/test-jdbc", 2,
                "forged-split", SchemaFingerprint.of(META), "forged-snapshot",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START));
        assertThrows(InvalidObjectException.class, () -> roundTrip(original));
    }

    private static JdbcSnapshotSource input(
            JdbcSnapshotSourceProvider provider, String partitionRef,
            String partitionDigest, String snapshotId) {
        return new JdbcSnapshotSource(
                META, provider, "endpoint-prod", "credential-prod", "ledger",
                "ledger-by-range", partitionRef, "snapshot-prod",
                DIGEST_A, partitionDigest, snapshotId,
                JdbcSnapshotSource.DEFAULT_FETCH_SIZE,
                JdbcSnapshotSource.DEFAULT_QUERY_TIMEOUT_SECONDS,
                JdbcSnapshotSource.DEFAULT_MAX_CELL_BYTES);
    }

    private static void assertInvalidRef(
            String field, String value, String sensitiveFragment) {
        String[] refs = {"endpoint-prod", "credential-prod", "ledger",
                "ledger-by-range", "partition-00", "snapshot-prod"};
        int index;
        switch (field) {
            case "endpointRef": index = 0; break;
            case "credentialRef": index = 1; break;
            case "datasetRef": index = 2; break;
            case "queryRef": index = 3; break;
            case "partitionRef": index = 4; break;
            case "snapshotRef": index = 5; break;
            default: throw new AssertionError(field);
        }
        refs[index] = value;
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new JdbcSnapshotSource(
                        META, new GoodProvider(), refs[0], refs[1], refs[2],
                        refs[3], refs[4], refs[5], DIGEST_A, DIGEST_B, DIGEST_C,
                        1, 1, JdbcSnapshotSource.MIN_CELL_BYTES));
        assertTrue(failure.getMessage().contains(field), failure.getMessage());
        assertFalse(failure.getMessage().contains(sensitiveFragment),
                failure.getMessage());
    }

    private static void assertInvalidDigest(String field, String value) {
        String query = DIGEST_A;
        String partition = DIGEST_B;
        String snapshot = DIGEST_C;
        if (field.equals("queryDigest")) query = value;
        if (field.equals("partitionDigest")) partition = value;
        if (field.equals("snapshotId")) snapshot = value;
        final String finalQuery = query;
        final String finalPartition = partition;
        final String finalSnapshot = snapshot;
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> new JdbcSnapshotSource(
                        META, new GoodProvider(), "endpoint-prod", "credential-prod",
                        "ledger", "ledger-by-range", "partition-00", "snapshot-prod",
                        finalQuery, finalPartition, finalSnapshot,
                        1, 1, JdbcSnapshotSource.MIN_CELL_BYTES));
        assertTrue(failure.getMessage().contains(field), failure.getMessage());
        assertFalse(failure.getMessage().contains(value), failure.getMessage());
    }

    private static void assertInvalidHints(
            int fetchSize, int timeoutSeconds, int maxCellBytes) {
        assertThrows(IllegalArgumentException.class,
                () -> new JdbcSnapshotSource(
                        META, new GoodProvider(), "endpoint-prod", "credential-prod",
                        "ledger", "ledger-by-range", "partition-00", "snapshot-prod",
                        DIGEST_A, DIGEST_B, DIGEST_C,
                        fetchSize, timeoutSeconds, maxCellBytes));
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

    public static final class GoodProvider implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        static int openCalls;
        public GoodProvider() { }
        @Override public String providerType() { return "test-jdbc"; }
        @Override public int providerVersion() { return 2; }
        @Override public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            openCalls++;
            throw new AssertionError("JS1 factory must not open provider");
        }
    }

    public static final class StatefulProvider implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        private final String secret = "must-not-be-serialized";
        public StatefulProvider() { }
        @Override public String providerType() { return "stateful-jdbc"; }
        @Override public int providerVersion() { return 1; }
        @Override public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            return null;
        }
    }

    public static final class PrivateConstructorProvider
            implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        static final PrivateConstructorProvider INSTANCE =
                new PrivateConstructorProvider();
        private PrivateConstructorProvider() { }
        @Override public String providerType() { return "private-jdbc"; }
        @Override public int providerVersion() { return 1; }
        @Override public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            return null;
        }
    }

    public static final class MutableIdentityProvider
            implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        static String type = "mutable-jdbc";
        public MutableIdentityProvider() { }
        @Override public String providerType() { return type; }
        @Override public int providerVersion() { return 1; }
        @Override public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            return null;
        }
    }

    private static final class EmptyHandle implements JdbcSnapshotReadHandle {
        @Override public String providerType() { return "test-jdbc"; }
        @Override public int providerVersion() { return 2; }
        @Override public String queryDigest() { return DIGEST_A; }
        @Override public String partitionDigest() { return DIGEST_B; }
        @Override public String snapshotId() { return DIGEST_C; }
        @Override public String schemaFingerprint() {
            return SchemaFingerprint.of(META);
        }
        @Override public ResultSet resultSet() { return null; }
        @Override public void close() { }
    }
}
