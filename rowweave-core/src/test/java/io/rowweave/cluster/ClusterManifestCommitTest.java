package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.ManifestFileSource;
import io.rowweave.ManifestFileSink;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.commit.ManifestCommitOutcome;
import io.rowweave.commit.ManifestFileStore;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PartitionRole;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ClusterManifestCommitTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));

    @Test
    void multiWorkerManifestJobPublishesOneGenerationAndExposesQueryableOutcome(
            @TempDir Path tmp) {
        Path domain = tmp.resolve("manifest-domain");
        PlanNode source = PlanNode.source(META, Arrays.asList(row(1L), row(2L), row(3L)));
        PlanNode partition = PlanNode.unary("partitionByHash", META, 0)
                .listParam("keys", Collections.singletonList("id"))
                .build();
        PlanNode left = PlanNode.sink(
                META, 1, new ManifestFileSink(META, domain.toString(), "left")).build();
        PlanNode right = PlanNode.sink(
                META, 1, new ManifestFileSink(META, domain.toString(), "right")).build();
        PlanGraph graph = new PlanGraph(Arrays.asList(source, partition, left, right), 2);

        File w0 = tmp.resolve("w0").toFile();
        File w1 = tmp.resolve("w1").toFile();
        assertTrue(w0.mkdirs());
        assertTrue(w1.mkdirs());
        try (MasterServer master = new MasterServer(5_000L);
             WorkerServer worker0 = WorkerServer.newUnguarded(
                     "manifest-w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer worker1 = WorkerServer.newUnguarded(
                     "manifest-w1", 32, "127.0.0.1", master.port(), w1)) {
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", master.port());
            ExecutionReport report = client.submitAttempt(graph, "cluster-attempt-1");

            assertEquals(6L, report.rowsOut());
            assertEquals(TelemetryCoverage.COMPLETE,
                    report.resourceTelemetryCoverage());
            assertTrue(report.peakReservedBytes() > 0L,
                    "manifest output buffering must be reported as a measured fact");
            assertTrue(report.acquiredBytes() >= report.peakReservedBytes());
            assertEquals(0L, report.pressureSpillCount());
            assertEquals(0L, report.spillRunCount());
            assertEquals(TelemetryCoverage.COMPLETE,
                    report.partitionTelemetryCoverage());
            assertEquals(1, report.partitionDistributions().size());
            assertEquals(PartitionRole.KEYED_OP,
                    report.partitionDistributions().get(0).role());
            assertEquals(3L, Arrays.stream(
                    report.partitionDistributions().get(0).rowCounts()).sum());
            List<DataRow> leftRows = readAll(new ManifestFileSource(
                    META, domain.toString(), "left"));
            List<DataRow> rightRows = readAll(new ManifestFileSource(
                    META, domain.toString(), "right"));
            assertEquals(3, leftRows.size());
            for (int i = 0; i < leftRows.size(); i++) {
                assertEquals(leftRows.get(i).get(0), rightRows.get(i).get(0));
            }

            ManifestCommitOutcome outcome = client.queryManifestOutcome(
                    domain.toString(), "cluster-attempt-1");
            assertEquals(ManifestCommitOutcome.State.COMMITTED, outcome.state());
            assertNotNull(outcome.manifestHash());
            assertEquals(0, worker0.preparedOutputActiveCount());
            assertEquals(0, worker1.preparedOutputActiveCount());
        }
    }

    @Test
    void singleManifestSinkStillUsesMasterGroupProtocol(@TempDir Path tmp) {
        Path domain = tmp.resolve("single-domain");
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Arrays.asList(row(7L), row(8L))),
                PlanNode.sink(META, 0,
                        new ManifestFileSink(META, domain.toString(), "only")).build()));
        File spill = tmp.resolve("single-worker").toFile();
        assertTrue(spill.mkdirs());

        try (MasterServer master = new MasterServer(5_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "single-manifest-worker", 8, "127.0.0.1", master.port(), spill)) {
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", master.port());
            ExecutionReport report = client.submitAttempt(graph, "single-attempt");

            assertEquals(2L, report.rowsOut());
            assertEquals(2, readAll(new ManifestFileSource(
                    META, domain.toString(), "only")).size());
            assertEquals(ManifestCommitOutcome.State.COMMITTED,
                    client.queryManifestOutcome(domain.toString(), "single-attempt").state());
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    @Test
    void sameAttemptRetryIsIdempotentAndDifferentContentConflicts(@TempDir Path tmp) {
        Path domain = tmp.resolve("retry-domain");
        File spill = tmp.resolve("retry-worker").toFile();
        assertTrue(spill.mkdirs());
        try (MasterServer master = new MasterServer(5_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "retry-manifest-worker", 8, "127.0.0.1", master.port(), spill)) {
            ClusterJobClient client = new ClusterJobClient("127.0.0.1", master.port());
            PlanGraph original = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Arrays.asList(row(31L))),
                    PlanNode.sink(META, 0,
                            new ManifestFileSink(META, domain.toString(), "ledger")).build()));

            client.submitAttempt(original, "stable-attempt");
            long generation = new ManifestFileStore(domain).latest().generation();
            client.submitAttempt(original, "stable-attempt");
            assertEquals(generation, new ManifestFileStore(domain).latest().generation());

            PlanGraph conflict = new PlanGraph(Arrays.asList(
                    PlanNode.source(META, Arrays.asList(row(32L))),
                    PlanNode.sink(META, 0,
                            new ManifestFileSink(META, domain.toString(), "ledger")).build()));
            RuntimeException failure = assertThrows(RuntimeException.class,
                    () -> client.submitAttempt(conflict, "stable-attempt"));
            assertTrue(failure.getMessage().contains("conflict"), failure.getMessage());
            assertEquals(31L, readAll(new ManifestFileSource(
                    META, domain.toString(), "ledger")).get(0).get(0));
            assertEquals(ManifestCommitOutcome.State.COMMITTED,
                    client.queryManifestOutcome(domain.toString(), "stable-attempt").state());
            assertEquals(0, worker.preparedOutputActiveCount());
        }
    }

    private static DataRow row(long id) {
        DataRow row = new DataRow(1);
        row.set(0, id);
        return row;
    }

    private static List<DataRow> readAll(ManifestFileSource input) {
        List<DataRow> rows = new ArrayList<>();
        try (io.rowweave.FileSource.Reader reader = input.openReader()) {
            while (reader.hasNext()) {
                rows.add(reader.next());
            }
        }
        return rows;
    }
}

