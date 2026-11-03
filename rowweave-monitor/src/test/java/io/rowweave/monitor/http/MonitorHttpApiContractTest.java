package io.rowweave.monitor.http;

import io.rowweave.monitor.InMemoryJobMonitor;
import io.rowweave.monitor.JobView;
import io.rowweave.monitor.MetricSet;
import io.rowweave.monitor.StageView;
import io.rowweave.monitor.WorkerView;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** HTTP API v2 contract: fixed resource shapes, metric exposition, and advisory read-only behavior. */
class MonitorHttpApiContractTest {
    private InMemoryJobMonitor monitor;
    private MonitorHttpServer server;
    private int port;

    @BeforeEach
    void startServer() {
        monitor = new InMemoryJobMonitor(32);
        monitor.onJobSubmit(JobView.builder("job-running", "dag job")
                .addStage(StageView.builder(0).role("SOURCE").state("RUNNING")
                        .opChain("csv>map").addInput(0).build())
                .addStage(StageView.builder(1).role("SINK").state("RUNNING")
                        .opChain("sink").addInput(0)
                        .addInstance(new StageView.InstanceView(0, "worker-1", "RUNNING", 7L))
                        .build())
                .dagJson("{\"nodes\":[{\"id\":0},{\"id\":1}],"
                        + "\"edges\":[{\"from\":0,\"to\":1}]}")
                .build());
        monitor.onMetrics("job-running", MetricSet.builder("stage")
                .tag("jobId", "job-running")
                .tag("stageIndex", "1")
                .gauge("stage.records_per_second", 12.5, "rows/second")
                .build());
        monitor.setWorkers(Collections.singletonList(
                new WorkerView("worker-1", "127.0.0.1", 7001, 2, true)));
        server = new MonitorHttpServer(monitor, 0, null);
        port = server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop();
    }

    @Test
    void overviewSummarizesJobsAndWorkers() throws Exception {
        Response response = get("/api/overview");
        assertEquals(200, response.status);
        assertEquals("application/json; charset=UTF-8", response.contentType);
        assertTrue(response.body.contains("\"jobsTotal\":1"));
        assertTrue(response.body.contains("\"jobsRunning\":1"));
        assertTrue(response.body.contains("\"workersAlive\":1"));
    }

    @Test
    void jobDetailKeepsRuntimeDagAndInstances() throws Exception {
        Response response = get("/api/jobs/job-running");
        assertEquals(200, response.status);
        assertTrue(response.body.contains("\"nodes\""));
        assertTrue(response.body.contains("\"edges\""));
        assertTrue(response.body.contains("\"instances\""));
        assertTrue(response.body.contains("\"state\":\"RUNNING\""));
        assertTrue(response.body.contains("\"outputRows\":7"));
    }

    @Test
    void exceptionsReturnBoundedFailureList() throws Exception {
        monitor.onJobComplete("job-running", "FAILED", "operator stopped", null);
        Response response = get("/api/jobs/job-running/exceptions");
        assertEquals(200, response.status);
        assertTrue(response.body.contains("\"jobId\":\"job-running\""));
        assertTrue(response.body.contains("\"message\":\"operator stopped\""));
    }

    @Test
    void configExposesSanitizedRuntimeSettings() throws Exception {
        Response response = get("/api/config");
        assertEquals(200, response.status);
        assertTrue(response.body.contains("\"bindAddress\":\"127.0.0.1\""));
        assertTrue(response.body.contains("\"port\":" + port));
        assertTrue(response.body.contains("\"history\":32"));
        assertTrue(response.body.contains("\"allowedOrigins\":[\"http://127.0.0.1:" + port + "\"]"));
    }

    @Test
    void healthIsCheapAndReadOnly() throws Exception {
        Response response = get("/api/health");
        assertEquals(200, response.status);
        assertTrue(response.body.contains("\"status\":\"UP\""));
        assertTrue(response.body.contains("\"jobs\":1"));
        assertTrue(response.body.contains("\"workers\":1"));
    }

    @Test
    void metricsUsePrometheusTextFormat() throws Exception {
        Response response = get("/api/metrics");
        assertEquals(200, response.status);
        assertEquals("text/plain; version=0.0.4; charset=utf-8", response.contentType);
        assertTrue(response.body.startsWith("# TYPE rowweave_stage_records_per_second gauge\n"));
        assertTrue(response.body.contains("rowweave_stage_records_per_second{"));
        assertTrue(response.body.contains("stageIndex=\"1\""));
        assertTrue(response.body.contains("} 12.5\n"));
    }

    @Test
    void unknownRoutesAreJsonNotFound() throws Exception {
        Response response = get("/api/not-a-route");
        assertEquals(404, response.status);
        assertEquals("application/json; charset=UTF-8", response.contentType);
        assertTrue(response.body.contains("\"error\":\"not found\""));
    }

    @Test
    void fakeSseEndpointIsRemoved() throws Exception {
        Response response = get("/api/stream");
        assertEquals(404, response.status);
    }

    private Response get(String path) throws Exception {
        HttpURLConnection connection =
                (HttpURLConnection) new URL("http://127.0.0.1:" + port + path).openConnection();
        connection.setRequestMethod("GET");
        connection.setConnectTimeout(2000);
        connection.setReadTimeout(2000);
        int status = connection.getResponseCode();
        InputStream stream = status < 400
                ? connection.getInputStream() : connection.getErrorStream();
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        try (InputStream input = stream) {
            int read;
            while ((read = input.read(buffer)) != -1) output.write(buffer, 0, read);
        }
        return new Response(status,
                connection.getContentType(), new String(output.toByteArray(), StandardCharsets.UTF_8));
    }

    private static final class Response {
        private final int status;
        private final String contentType;
        private final String body;

        private Response(int status, String contentType, String body) {
            this.status = status;
            this.contentType = contentType;
            this.body = body;
        }
    }
}
