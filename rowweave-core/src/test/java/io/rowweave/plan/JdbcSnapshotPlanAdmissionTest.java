package io.rowweave.plan;

import io.rowweave.AssignedSourceBatch;
import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.JdbcSnapshotSource;
import io.rowweave.JdbcSnapshotReadHandle;
import io.rowweave.JdbcSnapshotReadRequest;
import io.rowweave.JdbcSnapshotSourceProvider;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** trusted JDBC capability and coherent exact-snapshot source groups. */
class JdbcSnapshotPlanAdmissionTest {

    private static final DataSchema META =
            new DataSchema(new Field("id", DataType.LongType));
    private static final String QUERY_A = digest('a');
    private static final String QUERY_B = digest('b');
    private static final String PARTITION_A = digest('c');
    private static final String PARTITION_B = digest('d');
    private static final String SNAPSHOT_A = digest('e');
    private static final String SNAPSHOT_B = digest('f');

    @AfterEach
    void resetOpens() {
        ProviderA.openCalls = 0;
        ProviderB.openCalls = 0;
    }

    @Test
    void coherentGroupSurvivesTrustedPlanAndAssignedBatchWithoutOpeningProvider() {
        PlanGraph original = graph(
                input(new ProviderA(), "endpoint", "dataset", QUERY_A,
                        "partition-0", PARTITION_A, SNAPSHOT_A),
                input(new ProviderA(), "endpoint", "dataset", QUERY_A,
                        "partition-1", PARTITION_B, SNAPSHOT_A));
        ObjectTrustContext trust = ObjectTrustContext.localTrusted(
                getClass().getClassLoader());

        byte[] planBytes = original.toBytes(trust, ignored -> { });
        PlanGraph decoded = PlanGraph.fromBytes(
                planBytes, trust, ignored -> { });
        BatchSourcePlanValidator.validateDurable(decoded, trust);
        AssignedSourceBatch assigned = new AssignedSourceBatch(
                decoded.node(0).sourceSplitBindings());
        byte[] assignmentBytes = TrustedObjectCodec.write(
                assigned, Source.class, trust, ignored -> { });
        AssignedSourceBatch materialized = (AssignedSourceBatch)
                TrustedObjectCodec.readSlice(
                        assignmentBytes, 0, assignmentBytes.length,
                        Source.class, trust, ignored -> { });

        assertEquals(2, materialized.bindings().size());
        JdbcSnapshotSource second =
                (JdbcSnapshotSource) materialized.sources().get(1);
        assertEquals(PARTITION_B, second.readRequest().partitionDigest());
        assertEquals(SNAPSHOT_A, second.readRequest().snapshotId());
        assertEquals(0, ProviderA.openCalls);
    }

    @Test
    void providerRequiresTrustAndCustomInputCannotClaimReservedCapability() {
        JdbcSnapshotSource jdbc = input(
                new ProviderA(), "endpoint", "dataset", QUERY_A,
                "partition-0", PARTITION_A, SNAPSHOT_A);
        IllegalArgumentException untrusted = assertThrows(
                IllegalArgumentException.class,
                () -> BatchSourcePlanValidator.validateDurable(graph(jdbc)));
        assertTrue(untrusted.getMessage().contains(
                "UNTRUSTED_JDBC_SNAPSHOT_PROVIDER"), untrusted.getMessage());

        IllegalArgumentException spoofed = assertThrows(
                IllegalArgumentException.class,
                () -> BatchSourcePlanValidator.validateDurable(
                        graph(new SpoofedJdbcInput()),
                        ObjectTrustContext.localTrusted(getClass().getClassLoader())));
        assertTrue(spoofed.getMessage().contains(
                "RESERVED_JDBC_SNAPSHOT_SOURCE"), spoofed.getMessage());
        assertEquals(0, ProviderA.openCalls);
    }

    @Test
    void rejectsProviderEndpointDatasetQueryAndSnapshotGroupDriftBeforeOpen() {
        JdbcSnapshotSource baseline = input(
                new ProviderA(), "endpoint", "dataset", QUERY_A,
                "partition-0", PARTITION_A, SNAPSHOT_A);
        assertGroupFailure(baseline, input(
                new ProviderB(), "endpoint", "dataset", QUERY_A,
                "partition-1", PARTITION_B, SNAPSHOT_A));
        assertGroupFailure(baseline, input(
                new ProviderA(), "endpoint-other", "dataset", QUERY_A,
                "partition-1", PARTITION_B, SNAPSHOT_A));
        assertGroupFailure(baseline, input(
                new ProviderA(), "endpoint", "dataset-other", QUERY_A,
                "partition-1", PARTITION_B, SNAPSHOT_A));
        assertGroupFailure(baseline, input(
                new ProviderA(), "endpoint", "dataset", QUERY_B,
                "partition-1", PARTITION_B, SNAPSHOT_A));
        assertGroupFailure(baseline, input(
                new ProviderA(), "endpoint", "dataset", QUERY_A,
                "partition-1", PARTITION_B, SNAPSHOT_B));
        assertEquals(0, ProviderA.openCalls);
        assertEquals(0, ProviderB.openCalls);
    }

    @Test
    void duplicatePartitionDigestIsRejectedWhileBindingTheSource() {
        JdbcSnapshotSource first = input(
                new ProviderA(), "endpoint", "dataset", QUERY_A,
                "partition-alias-a", PARTITION_A, SNAPSHOT_A);
        JdbcSnapshotSource duplicate = input(
                new ProviderA(), "endpoint", "dataset", QUERY_A,
                "partition-alias-b", PARTITION_A, SNAPSHOT_A);

        IllegalArgumentException rejected = assertThrows(
                IllegalArgumentException.class, () -> graph(first, duplicate));
        assertTrue(rejected.getMessage().contains("duplicate splitId"),
                rejected.getMessage());
        assertEquals(0, ProviderA.openCalls);
    }

    private void assertGroupFailure(
            JdbcSnapshotSource first, JdbcSnapshotSource second) {
        IllegalArgumentException rejected = assertThrows(
                IllegalArgumentException.class,
                () -> BatchSourcePlanValidator.validateDurable(
                        graph(first, second),
                        ObjectTrustContext.localTrusted(getClass().getClassLoader())));
        assertTrue(rejected.getMessage().contains(
                "INCOHERENT_JDBC_SNAPSHOT_GROUP"), rejected.getMessage());
    }

    private static JdbcSnapshotSource input(
            JdbcSnapshotSourceProvider provider, String endpointRef,
            String datasetRef, String queryDigest, String partitionRef,
            String partitionDigest, String snapshotId) {
        return new JdbcSnapshotSource(
                META, provider, endpointRef, "credential", datasetRef,
                "query", partitionRef, "snapshot", queryDigest,
                partitionDigest, snapshotId, 10, 10, 4096);
    }

    private static PlanGraph graph(Source... inputs) {
        return new PlanGraph(Collections.singletonList(
                PlanNode.sourceFromInputs(META, Arrays.asList(inputs))
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "jdbc-source")));
    }

    private static String digest(char value) {
        char[] chars = new char[64];
        Arrays.fill(chars, value);
        return new String(chars);
    }

    public static final class ProviderA implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        static int openCalls;
        public ProviderA() { }
        @Override public String providerType() { return "provider-a"; }
        @Override public int providerVersion() { return 1; }
        @Override public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            openCalls++;
            throw new AssertionError("admission must not open JDBC");
        }
    }

    public static final class ProviderB implements JdbcSnapshotSourceProvider {
        private static final long serialVersionUID = 1L;
        static int openCalls;
        public ProviderB() { }
        @Override public String providerType() { return "provider-b"; }
        @Override public int providerVersion() { return 2; }
        @Override public JdbcSnapshotReadHandle open(JdbcSnapshotReadRequest request) {
            openCalls++;
            throw new AssertionError("admission must not open JDBC");
        }
    }

    private static final class SpoofedJdbcInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor =
                new BatchSourceSplitDescriptor(
                        1, "rowweave.jdbc-snapshot/forged", 1, "forged-split",
                        SchemaFingerprint.of(META), "forged-snapshot",
                        BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() {
            return Collections.emptyIterator();
        }
    }
}
