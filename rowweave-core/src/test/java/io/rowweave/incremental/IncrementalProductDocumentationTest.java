package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** production support and operational limits stay explicit. */
class IncrementalProductDocumentationTest {

    @Test
    void capabilityMatrixStatesTheExactBoundedModesAndFailureSemantics()
            throws Exception {
        String matrix = read("docs/product/reference/capability-matrix.md");
        require(matrix, "## Bounded incremental CDC support matrix");
        require(matrix, "`SNAPSHOT_V1`");
        require(matrix, "`INCREMENTAL_V1`");
        require(matrix, "`BACKFILL_V1`");
        require(matrix, "whole-attempt");
        require(matrix, "frontier does not advance");
        require(matrix, "after intent");
        require(matrix, "same-build");
        require(matrix, "not the Stream Runtime");
    }

    @Test
    void operationsDocumentFrontierDiagnosisAndAllSourceResourceLanes()
            throws Exception {
        String monitoring = read("docs/product/operations/monitoring.md");
        require(monitoring, "### Bounded incremental frontier");
        require(monitoring, "queryIncrementalFrontier(feedId)");
        require(monitoring, "activeRunId");
        require(monitoring, "do not use it to advance or reset the frontier");

        String deployment = read("docs/product/operations/deployment.md");
        require(deployment, "64 ordinary source-control actions");
        require(deployment, "8 large source-control actions");
        require(deployment, "64 MiB/192 MiB");
        require(deployment, "4096 splits");
        require(deployment, "24 MiB");
    }

    private static String read(String relative) throws Exception {
        Path root = projectRoot();
        return new String(Files.readAllBytes(root.resolve(relative)),
                StandardCharsets.UTF_8);
    }

    private static Path projectRoot() {
        Path current = Paths.get("").toAbsolutePath().normalize();
        if (Files.isDirectory(current.resolve("docs/product"))) return current;
        Path parent = current.getParent();
        if (parent != null
                && Files.isDirectory(parent.resolve("docs/product"))) {
            return parent;
        }
        throw new IllegalStateException(
                "docs/product directory not found from " + current);
    }

    private static void require(String text, String fragment) {
        assertTrue(text.contains(fragment), "missing product contract: " + fragment);
    }
}
