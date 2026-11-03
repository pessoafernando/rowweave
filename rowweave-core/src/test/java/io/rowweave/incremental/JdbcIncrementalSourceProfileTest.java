package io.rowweave.incremental;

import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotReadRequest;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** JDBC accepts only immutable, exactly reopenable cuts. */
class JdbcIncrementalSourceProfileTest {
    private static final String A = repeat('a');
    private static final String B = repeat('b');
    private static final String C = repeat('c');

    @Test
    void workerOpenRejectsSnapshotRequestSwapBeforeLocalAccess() throws Exception {
        JdbcIncrementalSourceProfile profile =
                JdbcIncrementalSourceProfile.sequence(sequence());
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        JdbcIncrementalSourceProfile.Snapshot snapshot =
                new JdbcIncrementalSourceProfile.Snapshot("snapshot-a", A);
        CapturedTargetReceipt receipt = request.bind(
                profile.sourceConfig().contractDigest(),
                profile.capture(request, IncrementalFrontier.int64Sequence(2L),
                        snapshot));
        IncrementalMaterializationResponse correct = profile.materialize(
                receipt, snapshot, Collections.singletonList(
                        new JdbcIncrementalSourceProfile.Partition("p0", B)));
        JdbcIncrementalSourceProfile.Snapshot otherSnapshot =
                new JdbcIncrementalSourceProfile.Snapshot("snapshot-b", C);
        CapturedTargetReceipt otherReceipt = request.bind(
                profile.sourceConfig().contractDigest(),
                profile.capture(request, IncrementalFrontier.int64Sequence(2L),
                        otherSnapshot));
        IncrementalMaterializationResponse other = profile.materialize(
                otherReceipt, otherSnapshot, Collections.singletonList(
                        new JdbcIncrementalSourceProfile.Partition("p0", B)));
        IncrementalMaterializedSplit swapped = new IncrementalMaterializedSplit(
                correct.splits().get(0).descriptor(),
                other.splits().get(0).openRequest());
        io.rowweave.IncrementalSourceSplit input =
                new io.rowweave.IncrementalSourceSplit(
                        sequence().dataSchema(), profile.sourceConfig(), receipt,
                        correct.partitionSetDigest(), swapped);
        final boolean[] opened = {false};
        JdbcIncrementalSourceAccess access = access(opened);

        assertThrows(IllegalArgumentException.class,
                () -> new JdbcSequenceIncrementalSourceProvider(access)
                        .openSplit(input));
        assertFalse(opened[0]);
    }

    @Test
    void coreProviderDelegatesOnlyThroughWorkerLocalAccess() throws Exception {
        JdbcIncrementalSourceProfile profile =
                JdbcIncrementalSourceProfile.sequence(sequence());
        JdbcIncrementalSourceAccess access = new JdbcIncrementalSourceAccess() {
            @Override
            public boolean supports(JdbcIncrementalSourceProfile candidate) {
                return "endpoint-prod".equals(candidate.endpointRef());
            }

            @Override
            public Capture capture(JdbcIncrementalSourceProfile candidate,
                                   IncrementalCaptureRequest request) {
                return new Capture(IncrementalFrontier.int64Sequence(5L),
                        new JdbcIncrementalSourceProfile.Snapshot(
                                "snapshot-5", B));
            }

            @Override
            public java.util.List<JdbcIncrementalSourceProfile.Partition>
            materialize(JdbcIncrementalSourceProfile candidate,
                        CapturedTargetReceipt receipt,
                        JdbcIncrementalSourceProfile.Snapshot snapshot) {
                return Collections.singletonList(
                        new JdbcIncrementalSourceProfile.Partition("p0", C));
            }

            @Override
            public io.rowweave.SourceExecutor<io.rowweave
                    .IncrementalSourceSplit> open(
                    JdbcIncrementalSourceProfile candidate,
                    io.rowweave.IncrementalSourceSplit input,
                    JdbcSnapshotReadRequest request) {
                throw new AssertionError("metadata test must not open rows");
            }
        };
        JdbcSequenceIncrementalSourceProvider provider =
                new JdbcSequenceIncrementalSourceProvider(access);
        IncrementalSourceActionExecutor actions =
                new IncrementalSourceActionExecutor(
                        provider.contract(), profile.sourceConfig(), provider,
                        io.rowweave.internal.ProviderActionSupervisor
                                .processWide(2));
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        CapturedTargetReceipt receipt = actions.capture(
                request, System.nanoTime() + 5_000_000_000L);

        assertEquals(IncrementalFrontier.int64Sequence(5L), receipt.target());
        assertEquals(1, actions.materialize(receipt,
                System.nanoTime() + 5_000_000_000L,
                ignored -> { }).splits().size());
    }

    private static JdbcIncrementalSourceAccess access(boolean[] opened) {
        return new JdbcIncrementalSourceAccess() {
            @Override public boolean supports(JdbcIncrementalSourceProfile p) {
                return true;
            }
            @Override public Capture capture(JdbcIncrementalSourceProfile p,
                                              IncrementalCaptureRequest r) {
                throw new AssertionError();
            }
            @Override public java.util.List<JdbcIncrementalSourceProfile.Partition>
            materialize(JdbcIncrementalSourceProfile p,
                        CapturedTargetReceipt r,
                        JdbcIncrementalSourceProfile.Snapshot s) {
                throw new AssertionError();
            }
            @Override public io.rowweave.SourceExecutor<io.rowweave
                    .IncrementalSourceSplit> open(
                    JdbcIncrementalSourceProfile p,
                    io.rowweave.IncrementalSourceSplit i,
                    JdbcSnapshotReadRequest r) {
                opened[0] = true;
                return null;
            }
        };
    }

    @Test
    void sequenceProfileFreezesSnapshotAndBuildsExactPartitionRequests()
            throws Exception {
        JdbcIncrementalSourceProfile.Configuration configuration = sequence();
        JdbcIncrementalSourceProfile profile =
                JdbcIncrementalSourceProfile.sequence(configuration);
        IncrementalSourceConfig inert = profile.sourceConfig();

        assertArrayEquals(inert.configurationBytes(),
                profile.canonicalizeConfiguration(inert));
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        IncrementalCaptureResponse response = profile.capture(
                request, IncrementalFrontier.int64Sequence(42L),
                new JdbcIncrementalSourceProfile.Snapshot(
                        "snapshot-42", C));
        CapturedTargetReceipt receipt = request.bind(
                inert.contractDigest(), response);
        IncrementalMaterializationResponse materialized = profile.materialize(
                receipt, new JdbcIncrementalSourceProfile.Snapshot(
                        "snapshot-42", C),
                Arrays.asList(
                        new JdbcIncrementalSourceProfile.Partition("p0", A),
                        new JdbcIncrementalSourceProfile.Partition("p1", B)));

        assertEquals(2, materialized.splits().size());
        assertEquals(profile.contract().providerKey(),
                materialized.providerKey());
        JdbcSnapshotReadRequest first =
                JdbcIncrementalSourceProfile.decodeOpenRequest(
                        materialized.splits().get(0).openRequest());
        assertEquals("endpoint-prod", first.endpointRef());
        assertEquals("credential-prod", first.credentialRef());
        assertEquals("ledger", first.datasetRef());
        assertEquals("ledger-sequence-range", first.queryRef());
        assertEquals("p0", first.partitionRef());
        assertEquals("snapshot-42", first.snapshotRef());
        assertEquals(A, first.partitionDigest());
        assertEquals(C, first.snapshotId());
        assertEquals(SchemaFingerprint.of(configuration.dataSchema()),
                materialized.splits().get(0).descriptor().schemaFingerprint());
        assertEquals(hex(sha256(response.snapshotIdentity())),
                materialized.splits().get(0).descriptor().snapshotId());
    }

    @Test
    void timestampProfileRequiresStableTieBreakAndNoLateLeftProof() {
        assertThrows(IllegalArgumentException.class,
                () -> timestamp(false, true));
        assertThrows(IllegalArgumentException.class,
                () -> timestamp(true, false));

        JdbcIncrementalSourceProfile profile =
                JdbcIncrementalSourceProfile.timestampKey(
                        timestamp(true, true));
        assertEquals(IncrementalFrontier.Kind.TIMESTAMP_KEY_V1,
                profile.contract().frontierKind());
    }

    @Test
    void mutableSourceUnreopenableSnapshotAndSnapshotDriftFailClosed()
            throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> configuration(false, true, true, true));
        assertThrows(IllegalArgumentException.class,
                () -> configuration(true, false, true, true));

        JdbcIncrementalSourceProfile profile =
                JdbcIncrementalSourceProfile.sequence(sequence());
        IncrementalCaptureRequest request = IncrementalCaptureRequest.normal(
                1L, sha256("definition"), 0L, IncrementalFrontier.begin(),
                "trigger", IncrementalRunMode.SNAPSHOT_V1);
        CapturedTargetReceipt receipt = request.bind(
                profile.sourceConfig().contractDigest(),
                profile.capture(request, IncrementalFrontier.int64Sequence(7L),
                        new JdbcIncrementalSourceProfile.Snapshot(
                                "snapshot-7", A)));

        assertThrows(IllegalArgumentException.class,
                () -> profile.materialize(receipt,
                        new JdbcIncrementalSourceProfile.Snapshot(
                                "snapshot-7", B),
                        Arrays.asList(
                                new JdbcIncrementalSourceProfile.Partition(
                                        "p0", C))));
    }

    private static JdbcIncrementalSourceProfile.Configuration sequence() {
        return configuration(true, true, true, true);
    }

    private static JdbcIncrementalSourceProfile.Configuration timestamp(
            boolean stableTieBreak, boolean noLateLeft) {
        return new JdbcIncrementalSourceProfile.Configuration(
                meta(), "endpoint-prod", "credential-prod", "ledger",
                "ledger-timestamp-key-range", A,
                JdbcIncrementalSourceProfile.SourceKind.IMMUTABLE_CHANGE_TABLE,
                true, true, stableTieBreak, noLateLeft,
                JdbcSnapshotSource.DEFAULT_FETCH_SIZE,
                JdbcSnapshotSource.DEFAULT_QUERY_TIMEOUT_SECONDS,
                JdbcSnapshotSource.DEFAULT_MAX_CELL_BYTES);
    }

    private static JdbcIncrementalSourceProfile.Configuration configuration(
            boolean immutable, boolean reopenable, boolean stableKey,
            boolean noLateLeft) {
        return new JdbcIncrementalSourceProfile.Configuration(
                meta(), "endpoint-prod", "credential-prod", "ledger",
                "ledger-sequence-range", A,
                JdbcIncrementalSourceProfile.SourceKind.IMMUTABLE_CHANGE_TABLE,
                immutable, reopenable, stableKey, noLateLeft,
                JdbcSnapshotSource.DEFAULT_FETCH_SIZE,
                JdbcSnapshotSource.DEFAULT_QUERY_TIMEOUT_SECONDS,
                JdbcSnapshotSource.DEFAULT_MAX_CELL_BYTES);
    }

    private static io.rowweave.DataSchema meta() {
        return new io.rowweave.DataSchema(
                new io.rowweave.Field("id", io.rowweave.DataType.LongType),
                new io.rowweave.Field("amount",
                        io.rowweave.DataType.BigDecimalType));
    }

    private static byte[] sha256(String value) throws Exception {
        return sha256(value.getBytes(StandardCharsets.UTF_8));
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }

    private static String hex(byte[] value) {
        StringBuilder text = new StringBuilder(value.length * 2);
        for (byte item : value) {
            text.append(Character.forDigit((item >>> 4) & 0xf, 16));
            text.append(Character.forDigit(item & 0xf, 16));
        }
        return text.toString();
    }

    private static String repeat(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
