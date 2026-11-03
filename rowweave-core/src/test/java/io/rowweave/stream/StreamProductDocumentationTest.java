package io.rowweave.stream;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** public Stream support and refusal boundaries stay aligned. */
class StreamProductDocumentationTest {

    @Test
    void publicApiAndProductIndexDescribeBothSupportedModes()
            throws Exception {
        String readme = read("README.md");
        requireCurrentStreamNaming(readme);
        require(readme, "public `StreamSession/DataStream`");
        require(readme, "cross-host M+NW");
        require(readme, "dynamic split");
        assertFalse(readme.contains("phase 5 productization under way"));

        String index = read("docs/product/index.md");
        requireCurrentStreamNaming(index);
        require(index, "dual Runtime");
        require(index, "bounded-resource Stream");
        require(index, "cross-host M+NW");

        String api = read("docs/product/reference/api-overview.md");
        requireCurrentStreamNaming(api);
        require(api, "## StreamSession / DataStream");
        require(api, "createLocalStreamJobClient");
        require(api, "createClusterStreamJobClient");
        require(api, "StreamClusterMain");
        require(api, "StreamElasticExample");
        require(api, "stopWithSavepoint");
        require(api, "StreamConnectorConformance");
        require(api, "StreamAtomicCompositionExample");
        require(api, "chained stateful operators");
    }

    @Test
    void runModesDeploymentSecurityAndRunbookShareTheSameClusterBoundary()
            throws Exception {
        String modes = read("docs/product/guides/run-modes.md");
        requireCurrentStreamNaming(modes);
        require(modes, "## Stream: Local / InProcess / cross-host cluster");
        require(modes, "Master-only");
        require(modes, "Worker-only");
        require(modes, "ZooKeeper");

        String deployment = read("docs/product/operations/deployment.md");
        requireCurrentStreamNaming(deployment);
        require(deployment, "## 6. Stream product bootstrap");
        require(deployment, "StreamClusterMain");
        require(deployment, "rowweave.stream.master.enable");
        require(deployment, "rowweave.stream.worker.enable");
        require(deployment, "rowweave.registry");
        require(deployment, "RowWeave.startStreamWorker");
        require(deployment, "RowWeave.startStreamMaster");
        require(deployment, "StreamElasticExample");
        require(deployment, "external Stream process roles require MTLS");

        String security = read("docs/product/operations/security.md");
        requireCurrentStreamNaming(security);
        require(security, "## Stream security boundary");
        require(security, "cross-host");
        require(security, "mTLS");
        require(security, "ZooKeeper");
        require(security, "plaintext fallback");

        String runbook = read("docs/runbooks/stream-runtime.md");
        require(runbook, "## Public deployment modes");
        require(runbook, "cross-host M+NW");
        require(runbook, "dynamic split");
        require(runbook, "unaligned checkpoint");
        require(runbook, "live rescale");
        require(runbook, "RETRACT");
        require(runbook, "StreamConnectorConformance");
        require(runbook, "stable run/attempt/checkpoint identities");
        require(runbook, "`COMMIT_DECIDED`");
        require(runbook, "`StreamCdcStatusView`");
        require(runbook, "80%");
    }

    @Test
    void capabilityMatrixSeparatesSupportedStreamFromRejectedWideSemantics()
            throws Exception {
        String matrix = read("docs/product/reference/capability-matrix.md");
        requireCurrentStreamNaming(matrix);
        require(matrix, "## Stream product capabilities");
        require(matrix, "cross-host M+NW");
        require(matrix, "dynamic split discovery");
        require(matrix, "RETRACT");
        require(matrix, "unaligned checkpoint");
        require(matrix, "live rescale");
        require(matrix, "cross-host");
        require(matrix, "CALC");
        require(matrix, "FLAT_MAP");
        require(matrix, "UNION");
        require(matrix, "ASSIGN_EVENT_TIME");
        require(matrix, "KEYED_PROCESS");
        require(matrix, "KEYED_CO_PROCESS");
        require(matrix, "GENERIC_WINDOW");
        require(matrix, "ASYNC_WAIT");
        require(matrix, "Batch and pipeline capability map");
        require(matrix, "process→window→co-process");
        require(matrix, "without replaying acknowledged output");

        String api = read("docs/product/reference/api-overview.md");
        requireCurrentStreamNaming(api);
        require(api, "keyedProcessWithOutputs");
        require(api, "keyedCoProcess");
        require(api, "genericWindowWithLateOutput");
        require(api, "asyncWait");
        require(api, "StreamJobHandle.rescale");
        require(api, "periodicUnaligned");
        require(api, "StreamDynamicSourceProvider");
        require(api, "same task lifecycle");
    }

    @Test
    void capabilityMatrixIsTheSingleAuthorityForCapabilityBoundaries()
            throws Exception {
        String matrix = read("docs/product/reference/capability-matrix.md");
        require(matrix, "## Capability boundaries");
        require(matrix, "not part of the current public contract");
        assertFalse(matrix.contains("historical package and class compatibility"));
        require(matrix, "Typed ordering and ranked-top convenience methods.");
        require(matrix, "Per-node single-parallelism controls and public diagnostic/naming APIs.");
        require(matrix, "Explicit public hash-join key declaration.");
        require(matrix, "Session-level memory-budget setters.");
        require(matrix, "Public cleanup-status query APIs.");
        require(matrix, "Embedded Batch session timeout, asynchronous submission, progress and");
        require(matrix, "cancellation parity with remote run handles.");
        require(matrix, "not a promise of support");
        require(matrix, "## Stable refusals");

        requireDeploymentAndConnectorRows(matrix);

        String installation = read("docs/product/quickstart/installation.md");
        require(installation, "`rowweave-connector-s3` | S3-compatible object storage snapshot source provider");
        assertFalse(installation.contains("S3-compatible object storage input/output"));

        String roadmap = read("ROADMAP.md");
        require(roadmap, "[capability matrix](docs/product/reference/capability-matrix.md)");
        require(roadmap, "not a promise of support");
        assertFalse(roadmap.contains("Typed ordering and ranked-top convenience methods."));

        String readme = read("README.md");
        require(readme, "Roadmap](ROADMAP.md) — work that is open in `0.2.0`");
    }

    private static void requireDeploymentAndConnectorRows(String matrix) {
        require(matrix, "Batch deployment modes are Local, InProcess, and real cluster.");
        require(matrix, "## Connector capabilities");
        require(matrix, "| JDBC snapshot source |");
        require(matrix, "| PostgreSQL logical-replication CDC source |");
        require(matrix, "| S3-compatible object storage snapshot source |");
        require(matrix, "| Custom source and sink providers |");
        require(matrix, "exact-snapshot reads only");
        require(matrix, "| HTTP monitoring |");
    }

    @Test
    void releaseNarrativeDescribesOnePublicCapabilitySet()
            throws Exception {
        String changelog = read("CHANGELOG.md");
        requireCurrentStreamNaming(changelog);
        require(changelog, "## 0.2.0 (initial public preview)");

        String roadmap = read("ROADMAP.md");
        requireCurrentStreamNaming(roadmap);
        require(roadmap, "Deferred API boundaries");
        require(roadmap, "not a promise of support");
        require(roadmap, "The public 0.2.0 surface is complete for the Batch and Stream contracts listed in the capability matrix.");
        require(roadmap, "Canonical Markdown documentation is English.");
        require(roadmap, "reviewed translation mirrors remain open");
        assertFalse(roadmap.contains("The engine itself is feature-complete"));
        assertFalse(roadmap.contains("deeper guides under `docs/`"));
    }

    private static void requireCurrentStreamNaming(String text) {
        Pattern processGeneration = Pattern.compile(
                "(?i)stream(?:[ ]runtime)?[ ]v[12]|Stream" + "V2");
        assertFalse(processGeneration.matcher(text).find(),
                "public Stream content uses an internal generation name");
    }

    private static String read(String relative) throws Exception {
        return new String(Files.readAllBytes(projectRoot().resolve(relative)),
                StandardCharsets.UTF_8);
    }

    private static Path projectRoot() {
        Path current = Paths.get("").toAbsolutePath().normalize();
        if (Files.isDirectory(current.resolve("docs/product"))) return current;
        Path parent = current.getParent();
        if (parent != null
                && Files.isDirectory(parent.resolve("docs/product"))) {
            return parent;
        }
        throw new IllegalStateException(
                "docs/product directory not found from " + current);
    }

    private static void require(String text, String fragment) {
        assertTrue(text.contains(fragment),
                "missing Stream product contract: " + fragment);
    }
}
