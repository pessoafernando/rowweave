package io.rowweave.docs;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Upgrade and rollback runbooks stay consistent with recovery and capability contracts. */
class UpgradeRunbookDocumentationTest {

    @Test
    void rollingRunbookCoversCompatibilityRecoveryRollbackAndEvidence()
            throws Exception {
        String rolling = read("docs/runbooks/rolling-upgrade-v1.md");

        require(rolling, "# Rolling upgrade and rollback runbook");
        require(rolling, "## Preconditions");
        require(rolling, "## Compatibility and connector qualification");
        require(rolling, "## Checkpoint and savepoint compatibility");
        require(rolling, "## Rolling sequence");
        require(rolling, "## Validation after each step");
        require(rolling, "## Rollback decision");
        require(rolling, "## Stop conditions");
        require(rolling, "## Required evidence");

        require(rolling, "N-1");
        require(rolling, "same-build");
        require(rolling, "conformance");
        require(rolling, "writer-revision intersection");
        require(rolling, "completed checkpoint");
        require(rolling, "savepoint");
        require(rolling, "FREEZE_ADMISSION");
        require(rolling, "CUTOVER");
        require(rolling, "RESUME_WRITES");
        require(rolling, "DATA_ROLLBACK");
        require(rolling, "MARK_NEW_SEMANTICS");
        require(rolling, "store-migration.md");
        require(rolling, "../product/reference/capability-matrix.md");
        require(rolling, "stream-runtime.md");
        require(rolling, "fail closed");
        require(rolling, "Preserve");

        assertFalse(rolling.contains("# RowWeave v1 rolling"));
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
                "missing upgrade runbook contract: " + expected);
    }
}
