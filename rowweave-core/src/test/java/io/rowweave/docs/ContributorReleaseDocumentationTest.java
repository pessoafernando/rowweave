package io.rowweave.docs;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Contributor instructions carry the supported build, test, and release-candidate path. */
class ContributorReleaseDocumentationTest {

    @Test
    void contributingPageDescribesLayoutTestsAndReleaseQualification()
            throws Exception {
        String page = read("CONTRIBUTING.md");

        require(page, "## Repository layout");
        require(page, "## Test strategy");
        require(page, "## Documentation requirements");
        require(page, "## Release candidate packaging");
        require(page, "## Publication prerequisites");

        require(page, "rowweave-core");
        require(page, "rowweave-pipeline");
        require(page, "rowweave-connector-s3");
        require(page, "rowweave-connector-postgres-cdc");
        require(page, "rowweave-registry-zk");
        require(page, "rowweave-monitor");
        require(page, "rowweave-soak");
        require(page, "rowweave-examples");

        require(page, "mvn -q test");
        require(page, "mvn -Prelease verify");
        require(page, "ROWWEAVE_JAVA8_HOME");
        require(page, "bash tools/oss_release_gate.sh");
        require(page, "rowweave-core-");
        require(page, "-sources.jar");
        require(page, "-javadoc.jar");
        require(page, "bom.json");
        require(page, "bom.xml");
        require(page, ".SHA-256");
        require(page, "CycloneDX");
        require(page, "release-manifest.hex");
        require(page, "release-manifest.sig.hex");
        require(page, "release-public-key.der.hex");
        require(page, "Central Portal");
        require(page, "never published automatically");

        require(page, "release packet");
        require(page, "clean commit");
        require(page, "do not publish");

        assertFalse(page.contains("TODO"));
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
                "missing contributor/release contract: " + expected);
    }
}
