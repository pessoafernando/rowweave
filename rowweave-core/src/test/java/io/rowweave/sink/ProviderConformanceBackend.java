package io.rowweave.sink;

import io.rowweave.cluster.ProviderRealClusterConformanceHarness;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

/** Backend-neutral adapter over the public client/Master/Worker conformance driver. */
final class ProviderConformanceBackend implements DurableSinkConformanceBackend {
    private final boolean manifestBackend;
    private final Path workspace;

    ProviderConformanceBackend(boolean manifestBackend) throws Exception {
        this.manifestBackend = manifestBackend;
        this.workspace = Files.createTempDirectory(
                manifestBackend ? "rowweave-manifest-conformance-"
                        : "rowweave-adversarial-conformance-");
    }

    @Override
    public DurableSinkConformanceObservation run(
            DurableSinkConformanceScenario scenario) throws Exception {
        ProviderRealClusterConformanceHarness.ScenarioEvidence evidence =
                ProviderRealClusterConformanceHarness.runScenario(
                        manifestBackend, workspace, scenario);
        return new DurableSinkConformanceObservation(
                scenario, evidence.outcome(), evidence.events(),
                evidence.publicationCount(), evidence.visibleMemberCount(),
                evidence.providerActionThreadCount(), evidence.detailCode());
    }

    @Override
    public void close() throws Exception {
        if (!Files.exists(workspace)) return;
        try (java.util.stream.Stream<Path> paths = Files.walk(workspace)) {
            paths.sorted(Comparator.reverseOrder()).forEach(path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (java.io.IOException failure) {
                    throw new java.io.UncheckedIOException(failure);
                }
            });
        }
    }
}
