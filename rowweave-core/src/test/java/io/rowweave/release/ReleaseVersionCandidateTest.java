package io.rowweave.release;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The qualified candidate is versioned as 0.2.0 everywhere users can select it. */
class ReleaseVersionCandidateTest {
    private static final List<String> POMS = List.of(
            "pom.xml",
            "rowweave-core/pom.xml",
            "rowweave-connector-s3/pom.xml",
            "rowweave-connector-postgres-cdc/pom.xml",
            "rowweave-registry-zk/pom.xml",
            "rowweave-soak/pom.xml",
            "rowweave-pipeline/pom.xml",
            "rowweave-monitor/pom.xml",
            "rowweave-examples/pom.xml");

    @Test
    void releaseCandidateUsesOneVersionAcrossProjectAndUserExamples()
            throws Exception {
        for (String relative : POMS) {
            String pom = read(relative);
            assertTrue(pom.contains("<version>0.2.0</version>"),
                    "release candidate version missing from " + relative);
            assertFalse(pom.contains("<version>0.1.1</version>") || pom.contains("<version>0.1.0</version>"),
                    "stale 0.1.x selector remains in " + relative);
        }

        String readme = read("README.md");
        assertTrue(readme.contains("Version `0.2.0` is the **initial public preview**"));
        assertFalse(readme.contains("<version>0.1.0</version>"));

        String installation = read("docs/product/quickstart/installation.md");
        assertTrue(installation.contains("<version>0.2.0</version>"));
        assertFalse(installation.contains("<version>0.1.0</version>"));
    }

    private static String read(String relative) {
        try {
            return new String(Files.readAllBytes(projectRoot().resolve(relative)),
                    StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }

    private static Path projectRoot() {
        Path current = Paths.get("").toAbsolutePath().normalize();
        if (Files.isRegularFile(current.resolve("README.md"))) {
            return current;
        }
        Path parent = current.getParent();
        if (parent != null && Files.isRegularFile(parent.resolve("README.md"))) {
            return parent;
        }
        throw new IllegalStateException("project root not found");
    }
}
