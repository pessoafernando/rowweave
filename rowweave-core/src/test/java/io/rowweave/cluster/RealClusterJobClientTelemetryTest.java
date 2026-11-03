package io.rowweave.cluster;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.cluster.rpc.RpcMessage;
import io.rowweave.plan.ExecutionReport;
import io.rowweave.plan.ExecutionTelemetrySummary;
import io.rowweave.plan.ExecutionTelemetrySummaryCodec;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RealClusterJobClientTelemetryTest {

    @Test
    void reportedSingleSinkRouteDoesNotDiscardClusterTelemetry(@TempDir Path temp) {
        DataSchema meta = new DataSchema(new Field("v", DataType.IntegerType));
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(meta, Collections.emptyList()),
                PlanNode.sink(meta, 0, new DiscardSink(meta)).build()));
        try (MasterServer master = new MasterServer(60_000L);
             WorkerServer worker = WorkerServer.newUnguarded(
                     "reported-telemetry-worker", 1, "127.0.0.1", master.port(),
                     temp.toFile());
             RealClusterJobClient client = new RealClusterJobClient(
                     "127.0.0.1", master.port())) {
            ExecutionReport report = client.submitToSinkReported(graph);

            assertEquals(0L, report.rowsOut());
            assertEquals(TelemetryCoverage.COMPLETE,
                    report.resourceTelemetryCoverage());
            assertEquals(TelemetryCoverage.NOT_APPLICABLE,
                    report.partitionTelemetryCoverage());
        }
    }

    @Test
    void validJobSummarySurvivesClientRepack() {
        ExecutionTelemetrySummary summary = new ExecutionTelemetrySummary(
                TelemetryCoverage.COMPLETE, 10, 20, 3, 4, 50, 16,
                TelemetryCoverage.NOT_APPLICABLE, Collections.emptyList());
        RpcMessage result = RpcMessage.jobResult(
                true, 17L, null, ExecutionTelemetrySummaryCodec.encode(summary));

        ExecutionReport report = RealClusterJobClient.executionReport(result);

        assertEquals(17L, report.rowsOut());
        assertEquals(TelemetryCoverage.COMPLETE, report.resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.NOT_APPLICABLE, report.partitionTelemetryCoverage());
        assertEquals(10L, report.peakReservedBytes());
        assertEquals(4L, report.spillRunCount());
    }

    @Test
    void missingOrDamagedSummaryOnlyDegradesCoverageAndPreservesRows() {
        byte[] damaged = ExecutionTelemetrySummaryCodec.encode(
                ExecutionTelemetrySummary.unavailable());
        damaged[15] ^= 1;

        ExecutionReport bad = RealClusterJobClient.executionReport(
                RpcMessage.jobResult(true, 23L, null, damaged));
        ExecutionReport legacy = RealClusterJobClient.executionReport(
                RpcMessage.jobResult(true, 29L, null));

        assertEquals(23L, bad.rowsOut());
        assertEquals(TelemetryCoverage.UNAVAILABLE, bad.resourceTelemetryCoverage());
        assertEquals(TelemetryCoverage.UNAVAILABLE, bad.partitionTelemetryCoverage());
        assertEquals(29L, legacy.rowsOut());
        assertEquals(TelemetryCoverage.UNAVAILABLE, legacy.resourceTelemetryCoverage());
    }
}
