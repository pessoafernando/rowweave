package io.rowweave.docs;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Curated pages declare one visible documentation contract. */
class DocumentationReleaseVisibilityTest {

    private static final String PREVIEW =
            "doc-contract: preview-0.2.0\ndoc-channel: default-branch";
    private static final String RELEASE =
            "doc-contract: rowweave-1.0.0\ndoc-channel: frozen-release";

    @Test
    void releaseVisibilityModelDefinesContractsRoutesAndDevelopmentBoundary()
            throws Exception {
        String page = read("docs/product/reference/release-visibility.md");
        require(page, "# Release and documentation visibility");
        require(page, "## Contract taxonomy");
        require(page, "| `preview-0.2.0` | `default-branch` | Current documentation, quickstarts, references, operations, and current runbooks. |");
        require(page, "| `rowweave-1.0.0` | `frozen-release` | The immutable release packet, portable integration boundary, rolling upgrade, and store migration procedures. |");
        require(page, "## Required metadata");
        require(page, "doc-contract: preview-0.2.0");
        require(page, "doc-channel: default-branch");
        require(page, "doc-contract: rowweave-1.0.0");
        require(page, "doc-channel: frozen-release");
        require(page, "doc-status: development-planning");
        require(page, "Development planning is not a support claim.");
        require(page, "[release contract](versioned-release-v1.md)");
        require(page, "[rolling upgrade](../../runbooks/rolling-upgrade-v1.md)");
        require(page, "[store migration](../../runbooks/store-migration.md)");
        require(page, "[Roadmap](../../../ROADMAP.md)");
        require(page, "[capability matrix](capability-matrix.md)");
        assertFalse(page.contains("TODO"));
    }

    @Test
    void everyCuratedPageDeclaresAnAllowedContract() throws Exception {
        Path root = repositoryRoot();
        List<Path> pages;
        try (Stream<Path> paths = Files.walk(root)) {
            pages = paths.filter(Files::isRegularFile)
                    .filter(path -> path.toString().endsWith(".md"))
                    .filter(path -> !path.toString().contains("/.git/"))
                    .filter(path -> !path.toString().contains("/target/"))
                    .filter(path -> isCurated(root.relativize(path)))
                    .sorted()
                    .collect(java.util.stream.Collectors.toList());
        }
        assertEquals(60, pages.size(), "curated Markdown inventory changed");
        for (Path page : pages) {
            String value = read(root.relativize(page).toString());
            assertTrue(value.startsWith("---\n"), "missing contract metadata: " + page);
            String metadata = frontMatter(value);
            boolean preview = metadata.contains(PREVIEW);
            boolean release = metadata.contains(RELEASE);
            assertTrue(preview ^ release, "page must declare preview or frozen release: " + page);
            if (value.contains("doc-status: development-planning")) {
                assertTrue(preview, "development planning must target the preview contract: " + page);
            }
        }
    }

    @Test
    void releasePacketAndDevelopmentPlanningHaveExplicitVisibility() throws Exception {
        String release = read("docs/product/reference/versioned-release-v1.md");
        require(release, "doc-contract: rowweave-1.0.0");
        require(release, "doc-channel: frozen-release");
        require(release, "This page is the human-readable index");

        String external = read("docs/product/guides/external-platform-integration.md");
        require(external, RELEASE);
        String rolling = read("docs/runbooks/rolling-upgrade-v1.md");
        require(rolling, RELEASE);
        String store = read("docs/runbooks/store-migration.md");
        require(store, RELEASE);

        String roadmap = read("ROADMAP.md");
        require(roadmap, PREVIEW);
        require(roadmap, "doc-status: development-planning");
        String audit = read("docs/product/reference/documentation-audit.md");
        require(audit, PREVIEW);
        require(audit, "doc-status: development-planning");

        String capability = read("docs/product/reference/capability-matrix.md");
        require(capability, PREVIEW);
        require(capability, "This page states what the public release supports today.");
        String stream = read("docs/runbooks/stream-runtime.md");
        require(stream, PREVIEW);
    }

    private static String frontMatter(String value) {
        String[] lines = value.split("\n", -1);
        StringBuilder metadata = new StringBuilder();
        for (int index = 1; index < lines.length && !lines[index].equals("---"); index++) {
            metadata.append(lines[index]).append("\n");
        }
        return metadata.toString();
    }

    private static boolean isCurated(Path path) {
        String relative = path.toString().replace('\\', '/');
        return relative.equals("README.md")
                || relative.equals("CHANGELOG.md")
                || relative.equals("CONTRIBUTING.md")
                || relative.equals("ROADMAP.md")
                || relative.equals("SECURITY.md")
                || relative.equals("SUPPORT.md")
                || relative.equals("THIRD-PARTY-NOTICES.md")
                || (relative.startsWith("docs/") && relative.endsWith(".md"));
    }

    private Path repositoryRoot() throws IOException {
        Path root = Paths.get("").toAbsolutePath().normalize();
        while (root != null && !Files.exists(root.resolve("docs/product/index.md"))) {
            root = root.getParent();
        }
        if (root == null) {
            throw new IllegalStateException("repository documentation root not found");
        }
        return root;
    }

    private static String read(String relative) throws Exception {
        Path root = Paths.get("").toAbsolutePath().normalize();
        while (root != null && !Files.exists(root.resolve("docs/product/index.md"))) {
            root = root.getParent();
        }
        if (root == null) {
            throw new IllegalStateException("repository documentation root not found");
        }
        return new String(Files.readAllBytes(root.resolve(relative)),
                StandardCharsets.UTF_8);
    }

    private static void require(String value, String expected) {
        assertTrue(value.contains(expected), "missing release visibility contract: " + expected);
    }
}
