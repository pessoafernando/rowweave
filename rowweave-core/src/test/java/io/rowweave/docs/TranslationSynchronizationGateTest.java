package io.rowweave.docs;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Translation manifests bind every localized page to one canonical revision. */
class TranslationSynchronizationGateTest {

    @TempDir
    Path temporaryDirectory;

    @Test
    void acceptsReviewedTranslationBoundToCanonicalContentRevision() throws Exception {
        Path root = temporaryDirectory.resolve("repository");
        String sourceBody = "# Documentation home\n\nUse the capability matrix for support.\n";
        write(root, "README.md", previewMetadata() + sourceBody);
        String translationMetadata = "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n"
                + "doc-source: README.md\n"
                + "source-sha256: " + sha256(sourceBody) + "\n"
                + "translation-status: reviewed\n---\n";
        write(root, "docs/i18n/zh-Hans/README.md", translationMetadata + "# \u6587\u6863\u9996\u9875\n\n\u652F\u6301\u8303\u56F4\u4EE5\u80FD\u529B\u77E9\u9635\u4E3A\u51C6\u3002\n");
        write(root, "docs/i18n/zh-Hans/manifest.tsv", "source\tstatus\nREADME.md\treviewed\n");

        run(root, "TRANSLATION PARITY: PASS");
    }

    @Test
    void rejectsStaleSourceRevision() throws Exception {
        Path root = temporaryDirectory.resolve("stale");
        String sourceBody = "# Documentation home\n\nUse the capability matrix for support.\n";
        write(root, "README.md", previewMetadata() + sourceBody);
        String translationMetadata = "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n"
                + "doc-source: README.md\n"
                + "source-sha256: " + sha256(sourceBody) + "\n"
                + "translation-status: reviewed\n---\n";
        write(root, "docs/i18n/zh-Hans/README.md", translationMetadata + "# \u6587\u6863\u9996\u9875\n");
        write(root, "docs/i18n/zh-Hans/manifest.tsv", "source\tstatus\nREADME.md\treviewed\n");
        write(root, "README.md", previewMetadata() + "# Documentation home\n\nUse the updated capability matrix for support.\n");

        runExpectingFailure(root, "source revision changed");
    }

    @Test
    void rejectsUnmanifestedTranslation() throws Exception {
        Path root = temporaryDirectory.resolve("unmanifested");
        String sourceBody = "# Documentation home\n\nUse the capability matrix for support.\n";
        write(root, "README.md", previewMetadata() + sourceBody);
        String translationMetadata = "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n"
                + "doc-source: README.md\n"
                + "source-sha256: " + sha256(sourceBody) + "\n"
                + "translation-status: reviewed\n---\n";
        write(root, "docs/i18n/zh-Hans/README.md", translationMetadata + "# \u6587\u6863\u9996\u9875\n");
        write(root, "docs/i18n/zh-Hans/manifest.tsv", "source\tstatus\n");

        runExpectingFailure(root, "translation is not listed in the locale manifest");
    }

    @Test
    void rejectsForbiddenIdentityInTranslation() throws Exception {
        Path root = temporaryDirectory.resolve("identity");
        String sourceBody = "# Documentation home\n\nUse supported APIs.\n";
        write(root, "README.md", previewMetadata() + sourceBody);
        String translationMetadata = "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n"
                + "doc-source: README.md\n"
                + "source-sha256: " + sha256(sourceBody) + "\n"
                + "translation-status: reviewed\n---\n";
        String identity = new String(
                new byte[]{65, 108, 103, 111, 88}, StandardCharsets.US_ASCII);
        write(root, "docs/i18n/zh-Hans/README.md",
                translationMetadata + "# \u6587\u6863\u9996\u9875\n\n" + identity + " \u4E0D\u662F\u53D7\u652F\u6301\u7684\u540D\u79F0\u3002\n");
        write(root, "docs/i18n/zh-Hans/manifest.tsv", "source\tstatus\nREADME.md\treviewed\n");

        runExpectingFailure(root, "forbidden predecessor identity");
    }

    @Test
    void translationWorkflowDocumentDefinesManifestBindingAndReview() throws Exception {
        String page = read("docs/product/reference/translation-synchronization.md");
        require(page, "# Translation synchronization");
        require(page, "docs/i18n/<locale>/<canonical-source-path>");
        require(page, "docs/i18n/<locale>/manifest.tsv");
        require(page, "source\tstatus");
        require(page, "source-sha256: 64-lowercase-hex-characters");
        require(page, "translation-status: reviewed");
        require(page, "python3 tools/oss_docs_translation_gate.py");
        require(page, "fails as stale");
        require(page, "fails as unmanifested");
        require(page, "fails as missing");
        require(page, "No locale is shipped in the `0.2.0` source preview.");
    }

    private static String read(String relative) throws IOException {
        return Files.readString(repositoryRoot().resolve(relative), StandardCharsets.UTF_8);
    }

    private static void require(String value, String expected) {
        assertTrue(value.contains(expected), "missing translation workflow contract: " + expected);
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

    private static String previewMetadata() {
        return "---\ndoc-contract: preview-0.2.0\ndoc-channel: default-branch\n---\n";
    }

    private static String sha256(String value) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256")
                .digest(value.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(digest);
    }

    private void run(Path root, String expected) throws IOException, InterruptedException {
        Process process = start(root);
        String diagnostics = output(process);
        assertEquals(0, process.waitFor(), diagnostics);
        assertTrue(diagnostics.contains(expected), diagnostics);
    }

    private void runExpectingFailure(Path root, String expected)
            throws IOException, InterruptedException {
        Process process = start(root);
        String diagnostics = output(process);
        assertTrue(process.waitFor() != 0, diagnostics);
        assertTrue(diagnostics.contains(expected), diagnostics);
    }

    private Process start(Path root) throws IOException {
        Path repository = repositoryRoot();
        ProcessBuilder builder = new ProcessBuilder("python3",
                repository.resolve("tools/oss_docs_translation_gate.py").toString(),
                "--root", root.toString());
        builder.redirectErrorStream(true);
        return builder.start();
    }

    private static String output(Process process) throws IOException {
        try (java.io.InputStream input = process.getInputStream()) {
            return new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    private static void write(Path root, String relative, String content) throws IOException {
        Path target = root.resolve(relative);
        Files.createDirectories(target.getParent());
        Files.write(target, content.getBytes(StandardCharsets.UTF_8));
    }
}
