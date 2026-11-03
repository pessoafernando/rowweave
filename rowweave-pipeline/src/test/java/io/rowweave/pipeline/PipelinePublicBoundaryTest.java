package io.rowweave.pipeline;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PipelinePublicBoundaryTest {
    @Test
    void productionModuleDoesNotImportEngineInternalsOrRuntimeImplementations()
            throws IOException {
        Path root = Paths.get("src/main/java");
        if (!Files.isDirectory(root)) {
            // Surefire runs in the module directory; a second shape covers a
            // build invoked from the repository root.
            root = Paths.get("rowweave-pipeline/src/main/java");
        }
        assertTrue(Files.isDirectory(root),
                "the ownership guard has no sources to read from " + root.toAbsolutePath());
        java.util.List<Path> walked = new java.util.ArrayList<>();
        try (java.util.stream.Stream<Path> files = Files.walk(root)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .forEach(walked::add);
        }
        assertTrue(walked.size() >= 38,
                "the ownership guard read " + walked.size() + " sources from "
                        + root.toAbsolutePath());
        for (Path path : walked) {
            try {
                String source = new String(
                        Files.readAllBytes(path), StandardCharsets.UTF_8);
                assertFalse(source.contains("import io.rowweave.internal."), path.toString());
                assertFalse(source.contains("import io.rowweave.cluster."), path.toString());
                assertFalse(source.contains("import io.rowweave.stream.cluster."), path.toString());
                assertFalse(source.contains("import io.rowweave.ops."), path.toString());
            } catch (IOException failure) {
                throw new AssertionError(failure);
            }
        }
    }
}
