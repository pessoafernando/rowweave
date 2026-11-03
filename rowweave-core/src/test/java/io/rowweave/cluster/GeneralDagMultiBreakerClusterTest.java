package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.Field;
import io.rowweave.FileSink;
import io.rowweave.IterableSource;
import io.rowweave.DataSchema;
import io.rowweave.DataRow;
import io.rowweave.ops.ParallelGroupByAgg;
import io.rowweave.ops.SortField;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.PlanOps;
import io.rowweave.plan.PushDagExecutor;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.PartitionRole;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@Tag("multiprocess")
class GeneralDagMultiBreakerClusterTest {

    private static final DataSchema META = new DataSchema(
            new Field("k", DataType.StringType), new Field("amt", DataType.BigDecimalType));
    private static final DataSchema GB = ParallelGroupByAgg.of(META, "k").sum("amt").getOutputSchema();

    private static DataRow row(String k, int amt) {
        DataRow r = new DataRow(2);
        r.set(0, k);
        r.set(1, new BigDecimal(amt));
        return r;
    }

    private static PlanGraph chain(List<DataRow> rows, io.rowweave.Sink out, int parallelism) {
        PlanNode src = PlanNode.source(META, rows);
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode dist = PlanNode.unary("distinct", GB, 1)
                .listParam("keys", Arrays.asList("k", "sum0")).build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 2).listParam("sort", Arrays.asList(
                PlanOps.encodeSortField(new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST))
        )).build();
        PlanNode sink = PlanNode.sink(GB, 3, out).build();
        return new PlanGraph(Arrays.asList(src, gb, dist, ob, sink), parallelism);
    }

    private static List<DataRow> oracle(List<DataRow> rows) {
        List<DataRow> out = new ArrayList<>();
        PlanNode src = PlanNode.sourceFromInputs(META, Collections.singletonList(
                IterableSource.of(META, rows)));
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode dist = PlanNode.unary("distinct", GB, 1)
                .listParam("keys", Arrays.asList("k", "sum0")).build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 2).listParam("sort", Arrays.asList(
                PlanOps.encodeSortField(new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST))
        )).build();
        new PushDagExecutor(1).execute(new PlanGraph(Arrays.asList(src, gb, dist, ob)), out::add);
        return out;
    }

    @Test
    void multiBreakerChainP2(@TempDir Path tmp) throws Exception {
        runChain(tmp, 2);
    }

    @Test
    void multiBreakerChainP3(@TempDir Path tmp) throws Exception {
        runChain(tmp, 3);
    }

    @Test
    void millionRowMultiBreakerNoOom(@TempDir Path tmp) throws Exception {
        String prev = System.getProperty("rowweave.memory.bytes");
        System.setProperty("rowweave.memory.bytes", Long.toString(4L * 1024 * 1024));
        try {
            List<DataRow> rows = new ArrayList<>();
            for (int i = 0; i < 1_000_000; i++) {
                rows.add(row("k" + (i % 50_000), i % 13));
            }
            int parallelism = 3;
            List<DataRow> single = oracle(rows);
            assertTrue(single.size() > 0, "the reference is non-empty");

            File outFile = tmp.resolve("million.tsv").toFile();
            FileSink output = new FileSink(GB, outFile.getAbsolutePath());
            PlanGraph graph = chain(
                    ClusterTestFileInput.source(tmp.resolve("million-source.tsv"), META, rows),
                    output, parallelism);
            File w0 = tmp.resolve("w0").toFile(); w0.mkdirs();
            File w1 = tmp.resolve("w1").toFile(); w1.mkdirs();
            try (MasterServer master = new MasterServer(30_000);
                 WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
                 WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
                ExecutionReport report = new ClusterJobClient(
                        "127.0.0.1", master.port()).submitAttempt(
                        graph, "tiny-memory-telemetry");
                long rowCount = report.rowsOut();
                assertEquals(TelemetryCoverage.COMPLETE,
                        report.resourceTelemetryCoverage());
                assertTrue(report.acquiredBytes() > 0L,
                        "tiny managed budget must be reported as measured, not legacy zero");
                assertTrue(report.peakReservedBytes() > 0L);
                assertTrue(report.spillRunCount() > 0L
                                || report.pressureSpillCount() > 0L,
                        "tiny budget high-cardinality breakers must expose a spill fact");
                List<DataRow> cluster = FileSink.read(GB, outFile.getAbsolutePath());
                assertEquals(single.size(), cluster.size(), "a million-row multi-hop DAG matches the single-process row count");
                assertEquals(single.size(), rowCount, "JobResult rowCount");
                for (int i = 0; i < single.size(); i++) {
                    assertEquals(single.get(i).get(0), cluster.get(i).get(0), "row " + i + " k");
                    assertEquals(0, ((BigDecimal) single.get(i).get(1)).compareTo((BigDecimal) cluster.get(i).get(1)),
                            "row " + i + " sum");
                }
            }
        } finally {
            if (prev == null) {
                System.clearProperty("rowweave.memory.bytes");
            } else {
                System.setProperty("rowweave.memory.bytes", prev);
            }
        }
    }

    private void runChain(Path tmp, int parallelism) throws Exception {
        List<DataRow> rows = new ArrayList<>();
        for (int i = 0; i < 6000; i++) {
            rows.add(row("k" + (i % 40), i % 9));
        }
        List<DataRow> single = oracle(rows);
        assertTrue(single.size() > 0, "the reference is non-empty");

        File outFile = tmp.resolve("multibreaker.tsv").toFile();
        FileSink output = new FileSink(GB, outFile.getAbsolutePath());
        PlanGraph graph = chain(rows, output, parallelism);

        File w0 = tmp.resolve("w0").toFile(); w0.mkdirs();
        File w1 = tmp.resolve("w1").toFile(); w1.mkdirs();
        try (MasterServer master = new MasterServer(5000);
             WorkerServer ws0 = WorkerServer.newUnguarded("w0", 32, "127.0.0.1", master.port(), w0);
             WorkerServer ws1 = WorkerServer.newUnguarded("w1", 32, "127.0.0.1", master.port(), w1)) {
            ExecutionReport report = new ClusterJobClient(
                    "127.0.0.1", master.port()).submitAttempt(
                    graph, "multi-keyed-p" + parallelism);
            long rowCount = report.rowsOut();
            assertEquals(TelemetryCoverage.COMPLETE,
                    report.resourceTelemetryCoverage());
            assertTrue(report.acquiredBytes() > 0L);
            assertEquals(TelemetryCoverage.COMPLETE,
                    report.partitionTelemetryCoverage());
            assertEquals(2, report.partitionDistributions().size(),
                    "groupBy and distinct must remain separate logical distributions");
            assertTrue(report.partitionDistributions().stream()
                    .allMatch(distribution -> distribution.role() == PartitionRole.KEYED_OP));
            assertTrue(report.partitionDistributions().stream()
                    .allMatch(distribution -> distribution.parallelism() == parallelism));
            List<DataRow> cluster = FileSink.read(GB, outFile.getAbsolutePath());
            assertEquals(single.size(), cluster.size(), "P=" + parallelism + " row count matches the single-process run");
            assertEquals(single.size(), rowCount, "P=" + parallelism + " JobResult rowCount");
            assertEquals(2 * parallelism + 2, master.lastPhysicalPlan().instances().size(),
                    "P=" + parallelism + " total instances=source 1+groupBy P+distinct P+sink 1");
            for (int i = 0; i < single.size(); i++) {
                assertEquals(single.get(i).get(0), cluster.get(i).get(0), "P=" + parallelism + " row " + i + " k");
                assertEquals(0, ((BigDecimal) single.get(i).get(1)).compareTo((BigDecimal) cluster.get(i).get(1)),
                        "P=" + parallelism + " row " + i + " sum");
            }
        }
    }

    private static PlanGraph chain(PlanNode src, io.rowweave.Sink out, int parallelism) {
        PlanNode gb = PlanNode.unary("groupBySum", GB, 0)
                .listParam("keys", Arrays.asList("k")).param("measure", "amt").build();
        PlanNode dist = PlanNode.unary("distinct", GB, 1)
                .listParam("keys", Arrays.asList("k", "sum0")).build();
        PlanNode ob = PlanNode.unary("orderBy", GB, 2).listParam("sort", Arrays.asList(
                PlanOps.encodeSortField(new SortField(0, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST)),
                PlanOps.encodeSortField(new SortField(1, SortField.Direction.ASC, SortField.NullOrder.NULLS_LAST))
        )).build();
        PlanNode sink = PlanNode.sink(GB, 3, out).build();
        return new PlanGraph(Arrays.asList(src, gb, dist, ob, sink), parallelism);
    }
}

