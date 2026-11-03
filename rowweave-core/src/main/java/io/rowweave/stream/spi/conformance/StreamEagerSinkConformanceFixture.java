package io.rowweave.stream.spi.conformance;

import io.rowweave.stream.spi.StreamSinkContext;
import io.rowweave.stream.spi.StreamSinkProvider;

/** Connector-owned fixture used by the published eager Sink conformance kit. */
public interface StreamEagerSinkConformanceFixture {
    StreamSinkProvider provider();
    StreamSinkContext context();
    byte[] recordFrame();
    int observedWrites();
    int observedCancels();
    int observedCloses();
}
