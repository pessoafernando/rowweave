package io.rowweave.docs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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

/** The documentation site converts the curated source set without copying Markdown. */
class DocumentationSiteBuildTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void buildsCompleteResponsiveSiteFromFixtureSourceTree() throws Exception {
        Path source = temporaryDirectory.resolve("source");
        Path output = temporaryDirectory.resolve("site");
        write(source, "README.md", "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n# RowWeave\n\nBuild data pipelines with tested APIs.\n");
        write(source, "docs/product/index.md", "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n# Documentation\n\nStart at [API](../reference/api.md) and [source](../../src/App.java).\n");
        write(source, "src/App.java", "public class App {\n}\n");
        write(source, "docs/reference/api.md", "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n# API\n\nUse `BatchSession`.\n\n```bash\nmvn -q test\n```\n");
        write(source, "CHANGELOG.md", "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n# Changelog\n\n## 0.2.0\n\nInitial public contract.\n");

        run(source, output);

        List<Path> generated;
        try (Stream<Path> paths = Files.walk(output)) {
            generated = paths.filter(Files::isRegularFile).sorted()
                    .collect(java.util.stream.Collectors.toList());
        }
        assertTrue(generated.stream().noneMatch(path -> path.toString().endsWith(".md")));
        assertEquals(7, generated.size());

        String home = read(output, "index.html");
        assertTrue(home.contains("name=\"viewport\""));
        assertTrue(home.contains("assets/site.css"));
        assertTrue(home.contains("assets/site.js"));
        assertTrue(home.contains("assets/search-index.js"));
        assertTrue(home.contains("aria-label=\"Documentation sections\""));
        assertTrue(home.contains("Documentation"));
        assertTrue(home.contains("Documentation contract:"));
        assertTrue(home.contains("0.2.0 source preview"));

        String guide = read(output, "docs/product/index.html");
        assertTrue(guide.contains("href=\"../reference/api.html\""));
        assertTrue(guide.contains("href=\"https://github.com/pessoafernando/rowweave/blob/main/src/App.java\""));
        assertFalse(guide.contains("src/App.java.html"));
        assertTrue(guide.contains("aria-controls=\"sections\""));
        String stylesheet = read(output, "assets/site.css");
        assertTrue(stylesheet.contains(":focus-visible"));
        assertTrue(stylesheet.contains("prefers-reduced-motion"));

        String api = read(output, "docs/reference/api.html");
        assertTrue(api.contains("<pre tabindex=\"0\">"));
        assertTrue(api.contains("<code class=\"language-bash\">"));
        assertTrue(api.contains("BatchSession"));
        assertTrue(api.contains("id=\"api\""));
        String release = writeReleaseFixture(output);
        assertTrue(release.contains("<div class=\"table-wrap\" tabindex=\"0\" role=\"region\" aria-label=\"Scrollable table\">"));
        assertTrue(release.contains("<th scope=\"col\""));
        String searchIndex = read(output, "assets/search-index.js");
        assertTrue(searchIndex.contains("\"url\": \"index.html\""));
        assertTrue(searchIndex.contains("BatchSession"));
        assertTrue(searchIndex.contains("0.2.0 source preview"));
    }

    @Test
    void rejectsCuratedPageWithoutContractMetadata() throws Exception {
        Path source = temporaryDirectory.resolve("unclassified");
        Path output = temporaryDirectory.resolve("unclassified-site");
        write(source, "README.md", "# RowWeave\n\nMissing contract metadata.\n");
        runExpectingFailure(source, output, "missing contract metadata");
    }

    @Test
    void buildsEveryCuratedPublicMarkdownPageFromRepositorySource() throws Exception {
        Path output = temporaryDirectory.resolve("repository-site");
        run(repositoryRoot(), output);

        try (Stream<Path> paths = Files.walk(output)) {
            long pages = paths.filter(path -> path.toString().endsWith(".html")).count();
            assertEquals(60, pages, "expected curated documentation pages");
            try (Stream<Path> all = Files.walk(output)) {
                List<Path> generatedPages = all.filter(Files::isRegularFile)
                        .collect(java.util.stream.Collectors.toList());
                assertTrue(generatedPages.stream().noneMatch(path -> path.toString().endsWith(".md")));
                for (Path page : generatedPages) {
                    if (!page.toString().endsWith(".html")) {
                        continue;
                    }
                    String value = Files.readString(page);
                    assertTrue(value.contains("Documentation contract:"),
                            "missing contract badge: " + page);
                    assertFalse(value.contains("<p>---</p>"),
                            "front matter leaked into HTML: " + page);
                }
            }
        }
        assertTrue(Files.exists(output.resolve("assets/site.css")));
        assertTrue(Files.exists(output.resolve("assets/site.js")));
        assertTrue(Files.exists(output.resolve("assets/search-index.js")));
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

    private void run(Path source, Path output) throws IOException, InterruptedException {
        Path root = repositoryRoot();
        ProcessBuilder builder = new ProcessBuilder("python3",
                root.resolve("tools/oss_docs_site.py").toString(),
                "--source", source.toString(), "--out", output.toString());
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String diagnostics;
        try (java.io.InputStream input = process.getInputStream()) {
            diagnostics = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertEquals(0, process.waitFor(), diagnostics);
    }

    private String writeReleaseFixture(Path output) throws IOException {
        Path source = temporaryDirectory.resolve("release-source");
        Path releaseOutput = temporaryDirectory.resolve("release-site");
        write(source, "README.md", "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n# RowWeave\n\nBuild pipelines.\n");
        write(source, "docs/reference/release.md", "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n# Release\n\n| Contract | Status |\n|---|---|\n| Preview | Current |\n");
        try {
            run(source, releaseOutput);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IOException(exception);
        }
        return read(releaseOutput, "docs/reference/release.html");
    }

    private void runExpectingFailure(Path source, Path output, String expected)
            throws IOException, InterruptedException {
        Path root = repositoryRoot();
        ProcessBuilder builder = new ProcessBuilder("python3",
                root.resolve("tools/oss_docs_site.py").toString(),
                "--source", source.toString(), "--out", output.toString());
        builder.redirectErrorStream(true);
        Process process = builder.start();
        String diagnostics;
        try (java.io.InputStream input = process.getInputStream()) {
            diagnostics = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        assertTrue(process.waitFor() != 0, diagnostics);
        assertTrue(diagnostics.contains(expected), diagnostics);
    }

    private static void write(Path root, String relative, String content) throws IOException {
        Path target = root.resolve(relative);
        Files.createDirectories(target.getParent());
        Files.write(target, content.getBytes(StandardCharsets.UTF_8));
    }

    private static String read(Path root, String relative) throws IOException {
        return new String(Files.readAllBytes(root.resolve(relative)), StandardCharsets.UTF_8);
    }
}
