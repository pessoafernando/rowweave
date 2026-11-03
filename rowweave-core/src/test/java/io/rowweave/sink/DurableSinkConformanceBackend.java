package io.rowweave.sink;

/** One black-box backend session. Production tests must implement this through public cluster APIs. */
public interface DurableSinkConformanceBackend extends AutoCloseable {
    DurableSinkConformanceObservation run(DurableSinkConformanceScenario scenario) throws Exception;

    @Override
    void close() throws Exception;
}
