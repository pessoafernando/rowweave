package io.rowweave.stream.cluster;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** the legacy shape-gate surface is explicit and cannot grow. */
class StreamShapeGateDebtManifestTest {
    @Test
    void everyProductPlanSingletonConsumerIsInTheRetirementManifest()
            throws Exception {
        assertEquals(
                StreamShapeGateDebtManifest.productPlanSingletonConsumers(),
                sourcesContaining("StreamSourceSinkProductPlan"));
    }

    @Test
    void everyFixedTwoNodeExecutorIsInTheRetirementManifest()
            throws Exception {
        assertEquals(
                StreamShapeGateDebtManifest.fixedTwoNodeExecutors(),
                sourcesContaining("plan.nodes().size() != 2"));
    }

    @Test
    void shapePredicateCallsAreConfinedToTheDeclaredLegacyDispatchers()
            throws Exception {
        Set<String> actual = new LinkedHashSet<>();
        for (String token : StreamShapeGateDebtManifest.shapePredicateTokens()) {
            for (String path : sourcesContaining(token)) {
                actual.add(path + "::" + token);
            }
        }
        assertEquals(StreamShapeGateDebtManifest.shapePredicateSites(), actual);
    }

    private static Set<String> sourcesContaining(String token)
            throws Exception {
        Set<String> matches = new LinkedHashSet<>();
        Path root = sourceRoot();
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(path -> path.toString().endsWith(".java"))
                    .sorted()
                    .forEach(path -> {
                        try {
                            String source = new String(Files.readAllBytes(path),
                                    StandardCharsets.UTF_8);
                            if (source.contains(token)) {
                                matches.add(root.relativize(path).toString()
                                        .replace('\\', '/'));
                            }
                        } catch (Exception failure) {
                            throw new IllegalStateException(failure);
                        }
                    });
        }
        return matches;
    }

    private static Path sourceRoot() {
        Path cwd = Paths.get("").toAbsolutePath();
        Path local = cwd.resolve("src/main/java/io/rowweave/stream");
        return Files.isDirectory(local) ? local : cwd.resolve(
                "rowweave-core/src/main/java/io/rowweave/stream");
    }
}
