package io.rowweave.cluster;

import io.rowweave.BatchSource;
import io.rowweave.BatchSourceSplitDescriptor;
import io.rowweave.CollectionSource;
import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.Source;
import io.rowweave.BatchSession;
import io.rowweave.ManifestFileSource;
import io.rowweave.DataRowClassification;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.TypedQualityManifest;
import io.rowweave.artifact.PlanArtifact;
import io.rowweave.cluster.registry.InProcessRegistry;
import io.rowweave.commit.CommitManifest;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.commit.SchemaFingerprint;
import io.rowweave.ops.RowValidation;
import io.rowweave.run.ManifestMemberReceipt;
import io.rowweave.run.QualityGateCommitJournalSnapshot;
import io.rowweave.run.RunAttemptFence;
import io.rowweave.run.RunSubmission;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** local/cluster scale and atomic failure evidence. */
class TypedQualityPublicationDurabilityTest {

    private static final DataSchema META = new DataSchema(
            new Field("adaptor", DataType.StringType),
            new Field("period", DataType.IntegerType));
    private static final DataSchema REJECTED_META = new DataSchema(
            new Field("source_operator_uid", DataType.StringType),
            new Field("split_identity_sha256", DataType.StringType),
            new Field("split_local_ordinal", DataType.LongType),
            new Field("quality_error_code", DataType.StringType),
            new Field("quality_rule_id", DataType.StringType),
            new Field("quality_field_index", DataType.IntegerType),
            new Field("quality_detail", DataType.StringType));

    @Test
    void localAndInProcessP1P2P4PublishTheSameExactOracle(@TempDir java.nio.file.Path tmp) {
        for (int parallelism : new int[]{1, 2, 4}) {
            java.nio.file.Path local = tmp.resolve("local-p" + parallelism);
            run(null, parallelism, local, inputs());
            assertOracle(local);

            java.nio.file.Path clusterDomain = tmp.resolve("inprocess-p" + parallelism);
            File workerDir = tmp.resolve("inprocess-worker-p" + parallelism).toFile();
            try (InProcessCluster cluster = new InProcessCluster(parallelism, workerDir)) {
                run(cluster, parallelism, clusterDomain, inputs());
            }
            assertOracle(clusterDomain);
        }
    }

    @Test
    void independentMasterAndTwoWorkersPublishOneThreeMemberGeneration(
            @TempDir java.nio.file.Path tmp) throws Exception {
        java.nio.file.Path domain = tmp.resolve("m-plus-two-workers");
        File w0 = tmp.resolve("quality-w0").toFile();
        File w1 = tmp.resolve("quality-w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        InProcessRegistry registry = new InProcessRegistry();
        RunSubmission submission = new RunSubmission(
                "quality-multi-worker", "attempt-1", "publication-1",
                1, 0L, 10L);
        RunAttemptFence fence = new RunAttemptFence(
                submission.runId(), submission.attemptId(), 1L);
        AtomicReference<Set<String>> durableReceiptWorkers =
                new AtomicReference<>();
        try (MasterServer master = new MasterServer(10_000L, 0, registry);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "quality-w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "quality-w1", 32, "127.0.0.1", master.port(), w1)) {
            awaitWorkers(registry, 2);
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());
            master.setManifestPublishHooks(() -> {
                QualityGateCommitJournalSnapshot journal = registry
                        .durableStageAssignmentAuthority()
                        .qualityGateCommitJournal(fence);
                assertNotNull(journal);
                assertTrue(journal.allMembersSealed());
                Set<String> workerIds = new HashSet<>();
                for (ManifestMemberReceipt receipt : journal.members()) {
                    assertNotNull(receipt);
                    workerIds.add(receipt.token().workerId());
                }
                durableReceiptWorkers.set(workerIds);
            }, null);
            client.submitRun(artifact(4, domain, inputs()), submission);
            assertEquals(new HashSet<>(Arrays.asList("quality-w0", "quality-w1")),
                    durableReceiptWorkers.get(),
                    "three durable member receipts must span both workers; plan="
                            + master.lastPhysicalPlan().instances());
            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }
        assertOracle(domain);
    }

    private static void awaitWorkers(InProcessRegistry registry, int expected)
            throws InterruptedException {
        long deadline = System.nanoTime()
                + java.util.concurrent.TimeUnit.SECONDS.toNanos(5L);
        while (registry.liveWorkers().size() < expected
                && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertEquals(expected, registry.liveWorkers().size(),
                "all workers must be registered before durable planning");
    }

    @Test
    void clusterRuntimeFailureAbortsAllThreeMembersAndKeepsPreviousGeneration(
            @TempDir java.nio.file.Path tmp) {
        java.nio.file.Path domain = tmp.resolve("failure-keeps-old");
        run(null, 1, domain, inputs());
        CommitManifest original = new ManifestFileStore(domain).latest();
        List<String> originalSummary = summaryLines(read(
                TypedQualityManifest.summarySchema(), domain, "summary", original.generation()));

        File w0 = tmp.resolve("failure-w0").toFile();
        File w1 = tmp.resolve("failure-w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        try (MasterServer master = new MasterServer(10_000L);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "failure-w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "failure-w1", 32, "127.0.0.1", master.port(), w1)) {
            ClusterJobClient client = new ClusterJobClient(
                    "127.0.0.1", master.port());
            assertThrows(RuntimeException.class,
                    () -> run(client, 2, domain,
                            new Source[]{new FailingExactInput()}));
            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }

        CommitManifest visible = new ManifestFileStore(domain).latest();
        assertEquals(original.generation(), visible.generation());
        assertEquals(original.attemptId(), visible.attemptId());
        assertEquals(originalSummary, summaryLines(read(
                TypedQualityManifest.summarySchema(), domain, "summary",
                visible.generation())));
        java.nio.file.Path staging = domain.resolve("staging");
        if (java.nio.file.Files.exists(staging)) {
            try (java.util.stream.Stream<java.nio.file.Path> paths =
                         java.nio.file.Files.walk(staging)) {
                assertFalse(paths.anyMatch(java.nio.file.Files::isRegularFile),
                        "failed publication must not retain staging data");
            } catch (java.io.IOException failure) {
                throw new RuntimeException(failure);
            }
        }
    }

    private static void run(
            JobClient client, int parallelism, java.nio.file.Path domain, Source[] sources) {
        BatchSession session = client == null
                ? new BatchSession("quality-durable", "quality durable")
                : new BatchSession("quality-durable", "quality durable", client);
        session.setParallelism(parallelism);
        DataRowClassification classified = session.fromSource(sources)
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(validation());
        classified.writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary"));
        session.execute();
    }

    private static PlanArtifact artifact(
            int parallelism, java.nio.file.Path domain, Source[] sources) {
        BatchSession session = new BatchSession(
                "quality-durable", "quality durable");
        session.setParallelism(parallelism);
        DataRowClassification classified = session.fromSource(sources)
                .uid("ledger.source")
                .classifyRowsWithSourcePosition(validation());
        classified.writeToManifest(TypedQualityManifest.of(
                domain.toString(), "valid", "rejected", "summary"));
        return session.toPlanArtifact();
    }

    private static Source[] inputs() {
        return new Source[]{
                new CollectionSource(META, "split-a", Arrays.asList(
                        row("A", 1), row(null, 2))),
                new CollectionSource(META, "split-b", Collections.singletonList(
                        row("B", 13))),
                new CollectionSource(META, "split-c", Collections.singletonList(
                        row("C", 3))),
                new CollectionSource(META, "split-d", Collections.singletonList(
                        row("D", 4)))
        };
    }

    private static RowValidation validation() {
        return RowValidation.builder(META)
                .notNull("adaptor.required", 0)
                .integerRange("period.range", 1, 1, 12)
                .build();
    }

    private static void assertOracle(java.nio.file.Path domain) {
        CommitManifest manifest = new ManifestFileStore(domain).latest();
        assertEquals(3, manifest.members().size());
        assertEquals(3, read(META, domain, "valid", manifest.generation()).size());
        assertEquals(2, read(REJECTED_META, domain, "rejected", manifest.generation()).size());
        assertEquals(Arrays.asList(
                "ACCEPTED_TOTAL|||-1||3",
                "REJECTED_REASON|QUALITY_RULE_VIOLATION|adaptor.required|0|NOT_NULL|1",
                "REJECTED_REASON|QUALITY_RULE_VIOLATION|period.range|1|INTEGER_RANGE_CLOSED|1",
                "REJECTED_TOTAL|||-1||2"), summaryLines(read(
                TypedQualityManifest.summarySchema(), domain, "summary",
                manifest.generation())));
    }

    private static List<DataRow> read(
            DataSchema meta, java.nio.file.Path domain, String member, long generation) {
        List<DataRow> result = new ArrayList<>();
        try (io.rowweave.FileSource.Reader reader = new ManifestFileSource(
                meta, domain.toString(), member, generation).openReader()) {
            while (reader.hasNext()) result.add(reader.next());
        }
        return result;
    }

    private static List<String> summaryLines(List<DataRow> rows) {
        List<String> result = new ArrayList<>();
        for (DataRow row : rows) {
            result.add(row.getString(0) + "|" + row.getString(1) + "|"
                    + row.getString(2) + "|" + row.getInteger(3) + "|"
                    + row.getString(4) + "|" + row.getLong(5));
        }
        return result;
    }

    private static DataRow row(Object... values) {
        DataRow row = new DataRow(values.length);
        row.setValues(values);
        return row;
    }

    private static final class FailingExactInput implements BatchSource {
        private static final long serialVersionUID = 1L;
        private final BatchSourceSplitDescriptor descriptor = new BatchSourceSplitDescriptor(
                BatchSourceSplitDescriptor.CURRENT_VERSION,
                "typed-quality-failure", 1, "failing-split",
                SchemaFingerprint.of(META), "snapshot-v1",
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);

        @Override public DataSchema getSchema() { return META; }
        @Override public BatchSourceSplitDescriptor batchSourceSplit() { return descriptor; }
        @Override public Iterator<DataRow> iterator() {
            return new Iterator<DataRow>() {
                @Override public boolean hasNext() {
                    throw new IllegalStateException("injected quality source failure");
                }
                @Override public DataRow next() { throw new java.util.NoSuchElementException(); }
            };
        }
    }
}
