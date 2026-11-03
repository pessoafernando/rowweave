package io.rowweave.sink;

/** Test-only factory backed by the production manifest provider path. */
public final class ManifestConformanceBackendFactory
        implements DurableSinkConformanceBackendFactory {
    @Override
    public String backendId() { return "manifest"; }

    @Override
    public boolean productionSideEffectfulBackend() { return true; }

    @Override
    public DurableSinkConformanceBackend open() throws Exception {
        return new ProviderConformanceBackend(true);
    }
}
