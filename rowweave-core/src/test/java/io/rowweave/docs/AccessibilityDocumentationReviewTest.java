package io.rowweave.docs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Representative pages pass the documented accessibility and density checklist. */
class AccessibilityDocumentationReviewTest {

    @Test
    void styleDefinesTheAccessibilityAndDensityChecklist() throws Exception {
        String page = read("docs/product/reference/documentation-style.md");
        require(page, "## Accessibility and density review");
        require(page, "one first-level title");
        require(page, "does not skip a heading level");
        require(page, "skip link");
        require(page, "descriptive link text");
        require(page, "`scope=\"col\"`");
        require(page, "`tabindex=\"0\"`");
        require(page, "text alternative");
        require(page, "not identified by color alone");
        require(page, "focus indicator");
        require(page, "no more than 75 words");
        assertFalse(page.contains("TODO"));
    }

    @Test
    void representativePagesStayTaskOrientedAndAvoidImageOnlyMeaning() throws Exception {
        String quickstart = read("docs/product/quickstart/first-job.md");
        require(quickstart, "## Prerequisites");
        require(quickstart, "## Run the tested example");
        require(quickstart, "## Expected output");
        require(quickstart, "## Common first-job problems");

        String reference = read("docs/product/reference/api-overview.md");
        require(reference, "## Batch `DataSet`");
        require(reference, "## StreamSession / DataStream");
        require(reference, "## Error model");

        String architecture = read("docs/product/architecture/pure-push-model.md");
        require(architecture, "## End-to-end supported shape");
        require(architecture, "## Backpressure and failure propagation");
        require(architecture, "## Deterministic semantics");

        String operations = read("docs/product/operations/monitoring.md");
        require(operations, "## Dashboard");
        require(operations, "## Security boundaries");
        require(operations, "## Troubleshoot absent or stale data");
        assertFalse(quickstart.contains("!["));
        assertFalse(reference.contains("!["));
        assertFalse(architecture.contains("!["));
        assertFalse(operations.contains("!["));
    }

    private static Path repositoryRoot() {
        Path root = Paths.get("").toAbsolutePath().normalize();
        while (root != null && !Files.exists(root.resolve("docs/product/index.md"))) {
            root = root.getParent();
        }
        if (root == null) {
            throw new IllegalStateException("repository documentation root not found");
        }
        return root;
    }

    private static String read(String relative) throws IOException {
        return Files.readString(repositoryRoot().resolve(relative), StandardCharsets.UTF_8);
    }

    private static void require(String value, String expected) {
        assertTrue(value.contains(expected), "missing accessibility contract: " + expected);
    }
}
