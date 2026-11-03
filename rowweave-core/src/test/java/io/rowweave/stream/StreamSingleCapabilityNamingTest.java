package io.rowweave.stream;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** production and current-tree tests describe one Stream capability, not generations. */
class StreamSingleCapabilityNamingTest {
    private static final Pattern PROCESS_GENERATION = Pattern.compile(
            "(?i)Stream(?:[ ]Runtime)?[ ]v[0-9]|StreamV[0-9]");

    @Test
    void productionAndCurrentTestsUseOneStreamCapabilityNarrative()
            throws Exception {
        List<String> offenders = new ArrayList<>();
        Path root = projectRoot();
        scan(root.resolve("src/main/java"), offenders);
        scan(root.resolve("src/test/java/io/rowweave"), offenders);

        assertTrue(offenders.isEmpty(),
                "Stream generation naming must stay out of the one public capability: "
                        + offenders);
    }

    private static void scan(Path root, List<String> offenders)
            throws IOException {
        if (!Files.isDirectory(root)) {
            throw new IOException("source root not found: " + root);
        }
        try (Stream<Path> paths = Files.walk(root)) {
            paths.filter(path -> path.toString().endsWith(".java"))
                    .filter(path -> !containsSegment(path, "compat"))
                    .forEach(path -> {
                        String source = read(path);
                        if (PROCESS_GENERATION.matcher(source).find()) {
                            offenders.add(root.relativize(path).toString());
                        }
                    });
        }
    }

    private static Path projectRoot() {
        Path current = Paths.get("").toAbsolutePath().normalize();
        if (Files.isDirectory(current.resolve("src/main/java"))) {
            return current;
        }
        Path parent = current.getParent();
        if (parent != null
                && Files.isDirectory(parent.resolve("rowweave-core/src/main/java"))) {
            return parent.resolve("rowweave-core");
        }
        throw new IllegalStateException("rowweave-core source root not found");
    }

    private static boolean containsSegment(Path path, String segment) {
        for (Path part : path) {
            if (part.toString().equals(segment)) {
                return true;
            }
        }
        return false;
    }

    private static String read(Path path) {
        try {
            return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
        } catch (IOException exception) {
            throw new UncheckedIOException(exception);
        }
    }
}
