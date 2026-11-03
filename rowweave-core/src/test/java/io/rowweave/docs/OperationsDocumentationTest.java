package io.rowweave.docs;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Operations pages keep the supported startup and stop procedures explicit. */
class OperationsDocumentationTest {

    @Test
    void deploymentPageDescribesCompleteLifecycleAndReadyProtocol()
            throws Exception {
        String deployment = read("docs/product/operations/deployment.md");

        require(deployment, "## Supported deployment paths");
        require(deployment, "## Local embedding");
        require(deployment, "## In-process cluster deployment");
        require(deployment, "## Cluster prerequisites");
        require(deployment, "## Registry setup");
        require(deployment, "## Artifact security");
        require(deployment, "## Start Batch Master and Worker processes");
        require(deployment, "## Validate the cluster");
        require(deployment, "## Ports and network admission");
        require(deployment, "## Logging directories");
        require(deployment, "## 6. Stream product bootstrap");
        require(deployment, "## Shutdown and rollback checks");

        require(deployment, "`Equivalent result sets: true`");
        require(deployment, "MASTER_PORT=");
        require(deployment, "WORKER_READY=");
        require(deployment, "STREAM_MASTER_READY=");
        require(deployment, "STREAM_WORKER_READY=");
        require(deployment, "SHUTDOWN");
        require(deployment, "rowweave.process.memoryLimitBytes");
        require(deployment, "rowweave.worker.spill.dir");
        require(deployment, "rowweave.registry=zk");
        require(deployment, "rowweave.zk.connectString");
        require(deployment, "rowweave.deployment.count");
        require(deployment, "sha256sum --check");
        require(deployment, "SIGTERM");

        assertFalse(deployment.contains("rowweave-cluster.jar"));
        assertFalse(deployment.contains("sleep 30"));
    }

    private static String read(String relative) throws Exception {
        Path root = Paths.get("").toAbsolutePath();
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
                "missing operations contract: " + expected);
    }
}
