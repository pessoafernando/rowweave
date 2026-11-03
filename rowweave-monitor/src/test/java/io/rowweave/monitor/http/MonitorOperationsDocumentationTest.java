package io.rowweave.monitor.http;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Operations monitoring stays aligned with the public HTTP and metric contract. */
class MonitorOperationsDocumentationTest {

    @Test
    void operationsPageCoversLifecycleViewsSnapshotsSecurityAndTriage()
            throws Exception {
        String page = read("docs/product/operations/monitoring.md");

        require(page, "## Start and locate the monitor");
        require(page, "## HTTP API and JSON contract");
        require(page, "## Worker observations");
        require(page, "## Retention and recovery observations");
        require(page, "## Security boundaries");
        require(page, "## Prometheus operations");
        require(page, "## Troubleshoot absent or stale data");

        for (String route : new String[] {
                "/api/overview", "/api/jobs", "/api/jobs/{jobId}",
                "/api/jobs/{jobId}/exceptions", "/api/workers", "/api/metrics",
                "/api/config", "/api/health", "/api/retention",
                "/api/artifact-retention", "/api/retry-recovery"}) {
            require(page, route);
        }

        for (String field : new String[] {
                "jobId", "effectiveP", "instances", "dag", "stream",
                "workerId", "freeSlots", "managedMemoryUsedBytes", "backlog",
                "cycleComplete", "waitingRetries", "maintenanceCursorHealth"}) {
            require(page, field);
        }

        for (String status : new String[] {
                "`405 Method Not Allowed`", "`404 Not Found`",
                "`302 Found`", "`503 Service Unavailable`"}) {
            require(page, status);
        }

        for (String setting : new String[] {
                "rowweave.monitor.enable", "rowweave.monitor.bindAddress",
                "rowweave.monitor.port", "rowweave.monitor.ioThreads",
                "rowweave.monitor.history", "rowweave.monitor.allowedOrigins"}) {
            require(page, setting);
        }

        require(page, "text/plain; version=0.0.4");
        require(page, "scrape_interval: 15s");
        require(page, "rowweave_monitor_port");
        require(page, "Monitoring failures are advisory");
        require(page, "do not treat a missed scrape as job failure evidence");
    }

    private static String read(String relative) throws Exception {
        Path root = Paths.get("").toAbsolutePath().normalize();
        while (root != null && !Files.exists(root.resolve("docs/product/index.md"))) {
            root = root.getParent();
        }
        if (root == null) {
            throw new IllegalStateException("product documentation root not found");
        }
        return new String(Files.readAllBytes(root.resolve(relative)),
                StandardCharsets.UTF_8);
    }

    private static void require(String value, String expected) {
        assertTrue(value.contains(expected),
                "missing monitoring operations contract: " + expected);
    }
}
