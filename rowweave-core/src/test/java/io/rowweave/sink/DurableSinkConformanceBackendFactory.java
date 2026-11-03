package io.rowweave.sink;

/** Factory boundary shared by the production Manifest and signed adversarial backends. */
public interface DurableSinkConformanceBackendFactory {
    String backendId();

    boolean productionSideEffectfulBackend();

    DurableSinkConformanceBackend open() throws Exception;
}
