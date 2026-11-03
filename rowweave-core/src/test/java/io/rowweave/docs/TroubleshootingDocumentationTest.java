package io.rowweave.docs;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Operational troubleshooting keeps typed diagnostics and escalation boundaries explicit. */
class TroubleshootingDocumentationTest {

    @Test
    void troubleshootingPageCoversRequiredFailureTreesAndEvidence()
            throws Exception {
        String page = read("docs/product/operations/troubleshooting.md");

        require(page, "## Diagnostic protocol");
        require(page, "## Failure tree");
        require(page, "## Startup failures");
        require(page, "## Submission failures");
        require(page, "## Execution failures");
        require(page, "## Resource pressure");
        require(page, "## Connector failures");
        require(page, "## Monitor failures");
        require(page, "## Registry and HA failures");
        require(page, "## Checkpoint and recovery failures");
        require(page, "## Cluster network failures");
        require(page, "## Evidence to preserve");

        require(page, "[error and refusal reference](../reference/errors-and-refusals.md)");
        require(page, "`code()`");
        require(page, "`ChannelPoisonedException`");
        require(page, "`ProcessCapacityException`");
        require(page, "`DeploymentValidationException`");
        require(page, "`ProtocolNegotiationException`");
        require(page, "`StreamCheckpointException`");
        require(page, "`IncrementalAuthorityDecision`");
        require(page, "STORE_UPGRADE_ADMISSION_FROZEN");

        for (String evidence : new String[] {
                "MASTER_PORT=", "WORKER_READY=", "STREAM_MASTER_READY=",
                "STREAM_WORKER_READY=", "/api/overview", "/api/jobs/{jobId}",
                "/api/workers", "/api/config", "/api/retention",
                "/api/retry-recovery"}) {
            require(page, evidence);
        }

        require(page, "fail closed");
        require(page, "do not delete");
        require(page, "escalation boundary");
        require(page, "Preserve");
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
                "missing troubleshooting contract: " + expected);
    }
}
