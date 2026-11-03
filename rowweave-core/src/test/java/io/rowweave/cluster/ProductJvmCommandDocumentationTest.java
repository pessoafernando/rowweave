package io.rowweave.cluster;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** published ClusterMain commands must remain capacity-complete. */
class ProductJvmCommandDocumentationTest {

    private static final List<String> REQUIRED_JVM_OPTIONS = Arrays.asList(
            "-Xms", "-Xmx", "-XX:MaxDirectMemorySize=",
            "-XX:MaxMetaspaceSize=", "-XX:ReservedCodeCacheSize=",
            "-Xss", "-XX:ActiveProcessorCount=",
            "-Drowweave.process.memoryLimitBytes=");

    @Test
    void everyPublishedClusterMainCommandDeclaresTheCertifiedEnvelope()
            throws Exception {
        List<String> violations = new ArrayList<>();
        Path product = productDocs();
        try (Stream<Path> files = Files.walk(product)) {
            files.filter(path -> path.toString().endsWith(".md"))
                    .sorted()
                    .forEach(path -> inspect(path, product, violations));
        }
        assertEquals(new ArrayList<String>(), violations);
    }

    private static void inspect(Path path, Path product,
                                List<String> violations) {
        try {
            List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
            StringBuilder block = null;
            int blockStart = 0;
            for (int index = 0; index < lines.size(); index++) {
                String line = lines.get(index);
                if (line.trim().startsWith("```")) {
                    if (block == null) {
                        block = new StringBuilder();
                        blockStart = index + 1;
                    } else {
                        inspectBlock(product.relativize(path).toString(), blockStart,
                                block.toString(), violations);
                        block = null;
                    }
                } else if (block != null) {
                    block.append(line).append('\n');
                }
            }
        } catch (java.io.IOException failure) {
            throw new java.io.UncheckedIOException(failure);
        }
    }

    private static void inspectBlock(String file, int line, String block,
                                     List<String> violations) {
        if (!block.contains("java ")
                || !block.contains("io.rowweave.cluster.ClusterMain")) return;
        for (String required : REQUIRED_JVM_OPTIONS) {
            if (!block.contains(required)) {
                violations.add(file + ':' + line + " missing " + required);
            }
        }
        if (block.contains("-Drowweave.worker.enable=true")) {
            require(block, file, line, "-Drowweave.worker.memory=", violations);
            require(block, file, line, "-Drowweave.worker.maxStageThreads=", violations);
            require(block, file, line, "-Drowweave.shuffle.ioThreads=", violations);
        }
        if (block.contains("-Drowweave.monitor.enable=true")) {
            require(block, file, line, "-Drowweave.monitor.ioThreads=", violations);
        }
    }

    private static void require(String block, String file, int line,
                                String required, List<String> violations) {
        if (!block.contains(required)) {
            violations.add(file + ':' + line + " missing " + required);
        }
    }

    private static Path productDocs() {
        Path current = Paths.get("").toAbsolutePath();
        Path direct = current.resolve("docs/product");
        if (Files.isDirectory(direct)) return direct;
        Path parent = current.getParent();
        if (parent != null && Files.isDirectory(parent.resolve("docs/product"))) {
            return parent.resolve("docs/product");
        }
        throw new IllegalStateException("docs/product directory not found from " + current);
    }
}
