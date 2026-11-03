package io.rowweave.monitor.http;

import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.ArtifactRetentionView;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.RunRetentionView;
import io.rowweave.monitor.RetryRecoveryView;
import io.rowweave.monitor.StageView;
import io.rowweave.monitor.WorkerView;
import io.rowweave.plan.ExecutionTelemetrySummary;
import io.rowweave.plan.PartitionDistribution;
import io.rowweave.plan.PartitionRole;
import io.rowweave.plan.TelemetryCoverage;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Endpoint tests for the Netty HTTP monitor server. Start the server, request /api/jobs,
 * /api/jobs/{id}, /api/workers and the page at /, then assert on the JSON and HTML. The client
 * is JDK HttpURLConnection, which keeps the test free of extra dependencies.
 */
class MonitorHttpServerTest {

    private static String httpGet(int port, String path) throws Exception {
        HttpURLConnection c = (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
        c.setRequestMethod("GET");
        c.setConnectTimeout(3000);
        c.setReadTimeout(3000);
        int code = c.getResponseCode();
        StringBuilder sb = new StringBuilder();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(
                code < 400 ? c.getInputStream() : c.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = r.readLine()) != null) sb.append(line).append('\n');
        }
        return code + "|" + sb;
    }

    private InMemoryJobMonitor sampleMonitor() {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        m.onJobSubmit(JobView.builder("job-1", "myjob")
                .addStage(StageView.builder(0).role("SOURCE").state("RUNNING").opChain("source>map").build())
                .addStage(StageView.builder(1).role("SINK").state("RUNNING").opChain("groupBy>sink").addInput(0).build())
                .dagJson("{\"nodes\":[{\"id\":0},{\"id\":1}],\"edges\":[{\"from\":0,\"to\":1}]}")
                .build());
        m.onJobComplete("job-1", "FINISHED", null,
                new ExecutionTelemetrySummary(
                        TelemetryCoverage.COMPLETE,
                        7L, 8L, 1L, 2L, 3L, 4L,
                        TelemetryCoverage.PARTIAL,
                        Arrays.asList(new PartitionDistribution(
                                3, PartitionRole.KEYED_OP, 2,
                                TelemetryCoverage.PARTIAL,
                                new boolean[] { true, false },
                                new long[] { 0L, 0L })))
                        .toExecutionReport(11L));
        m.setWorkers(Arrays.asList(
                new WorkerView("w0", "127.0.0.1", 5001, 4, true),
                new WorkerView("w1", "127.0.0.1", 5002, 4, false)));
        m.onRunRetention(new RunRetentionView(
                7, 4096, 128, 60_000L, 5L, 3L, 1L,
                false, java.util.Collections.singletonList("run-a:CORRUPT")));
        m.onArtifactRetention(new ArtifactRetentionView(
                9, 64, "SHARD", 17, 6, 4096L, 256, 1L << 30,
                120_000L, 7L, 5L, 3L, 2L, 4L,
                false, "READY", "BACKFILLING",
                java.util.Collections.singletonList("artifact-a:AUTHORITY_FAILURE")));
        m.onRetryRecovery(new RetryRecoveryView(
                "EMPTY_READY", 0, 3, "LE_5S",
                7L, 2L, 4L, "CONTINUING", 128,
                java.util.Collections.singletonList("run-a:RETRY_WAIT")));
        return m;
    }

    @Test
    void apiJobsReturnsJsonArray() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String resp = httpGet(port, "/api/jobs");
            assertTrue(resp.startsWith("200|"), "200 OK, actual:" + resp);
            assertTrue(resp.contains("\"jobId\":\"job-1\""), "includes jobId");
            assertTrue(resp.contains("\"state\":\"FINISHED\""), "includes state");
            assertTrue(resp.contains("\"name\":\"myjob\""));
        } finally {
            server.stop();
        }
    }

    @Test
    void apiJobDetailReturnsDagAndStages() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String resp = httpGet(port, "/api/jobs/job-1");
            assertTrue(resp.startsWith("200|"), resp);
            assertTrue(resp.contains("\"dag\""), "includes dag (embedding dagJson)");
            assertTrue(resp.contains("\"edges\""), "DAG includes edges");
            assertTrue(resp.contains("\"stages\""), "includes the stages array");
            assertTrue(resp.contains("\"role\":\"SINK\""), "stage includes role");
            assertTrue(resp.contains("\"opChain\":\"groupBy>sink\""));
            assertTrue(resp.contains("\"executionTelemetry\""));
            assertTrue(resp.contains("\"resourceCoverage\":\"COMPLETE\""));
            assertTrue(resp.contains("\"partitionCoverage\":\"PARTIAL\""));
            assertTrue(resp.contains("\"budgetStatus\":\"WITHIN_BUDGET\""));
            assertTrue(resp.contains("\"presence\":[true,false]"),
                    "monitor must distinguish a measured zero from an unknown slot");
            assertTrue(resp.contains("\"rowCounts\":[0,0]"));
        } finally {
            server.stop();
        }
    }

    @Test
    void apiJobDetailDoesNotPresentUnavailableZerosAsHealthy() throws Exception {
        InMemoryJobMonitor monitor = new InMemoryJobMonitor(2);
        monitor.onJobSubmit(JobView.builder("unknown", "unknown").build());
        monitor.onJobComplete("unknown", "FINISHED", null,
                ExecutionTelemetrySummary.unavailable().toExecutionReport(0L));
        MonitorHttpServer server = new MonitorHttpServer(monitor, 0, null);
        int port = server.start();
        try {
            String response = httpGet(port, "/api/jobs/unknown");
            assertTrue(response.startsWith("200|"), response);
            assertTrue(response.contains("\"resourceCoverage\":\"UNAVAILABLE\""));
            assertTrue(response.contains("\"partitionCoverage\":\"UNAVAILABLE\""));
            assertTrue(response.contains("\"budgetStatus\":\"UNKNOWN\""));
            assertFalse(response.contains("\"budgetStatus\":\"WITHIN_BUDGET\""));
            assertTrue(response.contains("\"partitionDistributions\":[]"));
        } finally {
            server.stop();
        }
    }

    @Test
    void apiJobDetailUnknownReturns404() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String resp = httpGet(port, "/api/jobs/nonexistent");
            assertTrue(resp.startsWith("404|"), "unknown job yields 404, actual:" + resp);
        } finally {
            server.stop();
        }
    }

    @Test
    void apiWorkersReturnsList() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String resp = httpGet(port, "/api/workers");
            assertTrue(resp.startsWith("200|"), resp);
            assertTrue(resp.contains("\"workerId\":\"w0\""));
            assertTrue(resp.contains("\"alive\":true"));
            assertTrue(resp.contains("\"alive\":false"));
        } finally {
            server.stop();
        }
    }

    @Test
    void apiRetentionReturnsBoundedJanitorHealth() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String response = httpGet(port, "/api/retention");
            assertTrue(response.startsWith("200|"), response);
            assertTrue(response.contains("\"backlog\":7"));
            assertTrue(response.contains("\"lifecycleCapacityPerShard\":4096"));
            assertTrue(response.contains("\"oldestTerminalAgeMillis\":60000"));
            assertTrue(response.contains("\"claimedRuns\":5"));
            assertTrue(response.contains("\"purgedRuns\":3"));
            assertTrue(response.contains("\"errorRuns\":1"));
            assertTrue(response.contains("run-a:CORRUPT"));
        } finally {
            server.stop();
        }
    }

    @Test
    void apiArtifactRetentionReturnsFencedLifecycleHealth() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String response = httpGet(port, "/api/artifact-retention");
            assertTrue(response.startsWith("200|"), response);
            assertTrue(response.contains("\"backlog\":9"));
            assertTrue(response.contains("\"scanPageCapacity\":64"));
            assertTrue(response.contains("\"inventoryScope\":\"SHARD\""));
            assertTrue(response.contains("\"inventoryObservedShard\":17"));
            assertTrue(response.contains("\"inventoryEntries\":6"));
            assertTrue(response.contains("\"inventoryBytes\":4096"));
            assertTrue(response.contains("\"inventoryEntryCapacity\":256"));
            assertTrue(response.contains("\"inventoryByteCapacity\":1073741824"));
            assertTrue(response.contains("\"oldestCandidateAgeMillis\":120000"));
            assertTrue(response.contains("\"deletedArtifacts\":7"));
            assertTrue(response.contains("\"retainedReferenced\":5"));
            assertTrue(response.contains("\"staleOwners\":3"));
            assertTrue(response.contains("\"errorArtifacts\":2"));
            assertTrue(response.contains("\"notReadyPasses\":4"));
            assertTrue(response.contains("\"cycleComplete\":false"));
            assertTrue(response.contains("\"referenceStatus\":\"READY\""));
            assertTrue(response.contains("\"inventoryStatus\":\"BACKFILLING\""));
            assertTrue(response.contains("artifact-a:AUTHORITY_FAILURE"));
        } finally {
            server.stop();
        }
    }

    @Test
    void apiRetryRecoveryReturnsBoundedCoordinatorHealth() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String response = httpGet(port, "/api/retry-recovery");
            assertTrue(response.startsWith("200|"), response);
            assertTrue(response.contains("\"state\":\"EMPTY_READY\""));
            assertTrue(response.contains("\"waitingRetries\":3"));
            assertTrue(response.contains("\"nextDueDelayBucket\":\"LE_5S\""));
            assertTrue(response.contains("\"scheduledRetries\":7"));
            assertTrue(response.contains("\"exhaustedRetries\":2"));
            assertTrue(response.contains("\"notRetryableClosures\":4"));
            assertTrue(response.contains("\"maintenanceCursorHealth\":\"CONTINUING\""));
            assertTrue(response.contains("\"scanPageCapacity\":128"));
            assertTrue(response.contains("run-a:RETRY_WAIT"));
        } finally {
            server.stop();
        }
    }

    @Test
    void rootReturnsHtmlPage() throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String resp = httpGet(port, "/");
            assertTrue(resp.startsWith("200|"), resp);
            assertTrue(resp.toLowerCase().contains("<!doctype html") || resp.toLowerCase().contains("<html"),
                    "returns the HTML page");
            assertTrue(resp.contains("RowWeave"), "page includes the title");
        } finally {
            server.stop();
        }
    }

    @Test
    void rootRendersBoundedSinkSummaryAndJobExplorerControls()
            throws Exception {
        MonitorHttpServer server = new MonitorHttpServer(sampleMonitor(), 0, null);
        int port = server.start();
        try {
            String response = httpGet(port, "/");
            assertTrue(response.startsWith("200|"), response);
            assertTrue(response.contains("j.batchSink"), response);
            assertTrue(response.contains("locatorClosureCount"), response);
            assertTrue(response.contains("outcomeState"), response);
            assertTrue(response.contains("descriptorDigestPrefixes"), response);
            for (String control : Arrays.asList("jobSearch", "jobFilter",
                    "pauseUpdates", "refreshNow", "detailsToggle")) {
                assertEquals(1, occurrences(response, "id=\"" + control + "\""),
                        "the dashboard serves one accessible control: " + control);
            }
            assertTrue(response.contains("aria-label=\"Live execution graph\""));
            assertTrue(response.contains("aria-label=\"Job details\""));
        } finally {
            server.stop();
        }
    }

    @Test
    void jsonEscapesSpecialChars() throws Exception {
        InMemoryJobMonitor m = new InMemoryJobMonitor(100);
        m.onJobSubmit(JobView.builder("j\"x", "na\"me").build());
        m.onJobComplete("j\"x", "FAILED", "err with \"quote\" and \\ backslash");
        MonitorHttpServer server = new MonitorHttpServer(m, 0, null);
        int port = server.start();
        try {
            String resp = httpGet(port, "/api/jobs");
            assertTrue(resp.startsWith("200|"), resp);
            // Contains an escaped quote and is therefore valid JSON.
            assertTrue(resp.contains("\\\"quote\\\""), "special characters escaped, actual:" + resp);
        } finally {
            server.stop();
        }
    }

    private static int occurrences(String value, String needle) {
        int count = 0;
        int offset = 0;
        while ((offset = value.indexOf(needle, offset)) >= 0) {
            count++;
            offset += needle.length();
        }
        return count;
    }
}
