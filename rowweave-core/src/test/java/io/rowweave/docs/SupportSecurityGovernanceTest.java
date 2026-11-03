package io.rowweave.docs;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Governance pages agree on support, issue intake, and security reporting. */
class SupportSecurityGovernanceTest {

    @Test
    void supportAndSecurityPagesAgreeOnPreviewLifecycleAndReporting()
            throws Exception {
        String support = read("SUPPORT.md");
        require(support, "# Support");
        require(support, "Version `0.2.0` is a source-only public preview");
        require(support, "fixes target the default branch");
        require(support, "[Bug report](.github/ISSUE_TEMPLATE/bug_report.yml)");
        require(support, "[Feature request](.github/ISSUE_TEMPLATE/feature_request.yml)");
        require(support, "[capability matrix](docs/product/reference/capability-matrix.md)");
        require(support, "[SECURITY.md](SECURITY.md)");

        String security = read("SECURITY.md");
        require(security, "## Supported versions");
        require(security, "| `0.2.0` source preview | Evaluation only; no artifact is published | Default branch |");
        require(security, "security advisory form");
        require(security, "https://github.com/pessoafernando/rowweave/security/advisories/new");
        require(security, "version or source commit");
        require(security, "runtime and deployment mode");
        require(security, "host JDK and operating system");
        require(security, "self-contained reproduction");
        require(security, "Omit credentials");

        String readme = read("README.md");
        require(readme, "Support is evaluation-only during the source preview");
        require(readme, "[Support](SUPPORT.md)");

        String index = read("docs/product/index.md");
        require(index, "| Support and issue intake | [Support policy](../../SUPPORT.md) |");
    }

    @Test
    void contributorAndTemplatesDefineIntakeAndReviewBoundaries()
            throws Exception {
        String contributing = read("CONTRIBUTING.md");
        require(contributing, "## Issue intake and support boundary");
        require(contributing, "## Review boundaries");
        require(contributing, "[SUPPORT.md](SUPPORT.md)");
        require(contributing, "[SECURITY.md](SECURITY.md)");
        require(contributing, "Roadmap items are not support claims");
        require(contributing, "publication is a separate owner-authorized action");
        assertFalse(contributing.contains("TODO"));

        String bug = read(".github/ISSUE_TEMPLATE/bug_report.yml");
        require(bug, "id: environment");
        require(bug, "Host JDK, operating system, and topology");
        require(bug, "self-contained reproduction");
        require(bug, "Report vulnerabilities privately");

        String feature = read(".github/ISSUE_TEMPLATE/feature_request.yml");
        require(feature, "id: capability-boundary");
        require(feature, "capability matrix");
        require(feature, "stable refusal or unsupported outcome");
        require(feature, "id: outcome");

        String pullRequest = read(".github/pull_request_template.md");
        require(pullRequest, "bash tools/oss_docs_quality_gate.sh");
        require(pullRequest, "Support and security claims agree with the capability matrix");
    }

    @Test
    void roadmapAndCapabilityMatrixKeepLifecycleSeparate()
            throws Exception {
        String matrix = read("docs/product/reference/capability-matrix.md");
        require(matrix, "Lifecycle and reporting routes are outside this matrix");
        require(matrix, "[Support policy](../../../SUPPORT.md)");
        require(matrix, "[Security policy](../../../SECURITY.md)");

        String roadmap = read("ROADMAP.md");
        require(roadmap, "Before publication, the repository owner confirms the canonical URL and release identity");
        assertFalse(roadmap.contains("placeholder repository URL"));
        assertFalse(roadmap.contains("TODO"));
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
        Pattern whitespaceIndependent = Pattern.compile(Arrays
                .stream(expected.split(" ", -1))
                .map(Pattern::quote)
                .collect(Collectors.joining("\\s+")));
        assertTrue(whitespaceIndependent.matcher(value).find(),
                "missing governance contract: " + expected);
    }
}
