package io.rowweave.sink;

/** Test-only factory backed by a signed test deployment, never direct injection. */
public final class AdversarialConformanceBackendFactory
        implements DurableSinkConformanceBackendFactory {
    @Override
    public String backendId() { return "signed-adversarial"; }

    @Override
    public boolean productionSideEffectfulBackend() { return false; }

    @Override
    public DurableSinkConformanceBackend open() throws Exception {
        return new ProviderConformanceBackend(false);
    }
}
