package io.rowweave.stream.spi.conformance;

import io.rowweave.stream.spi.StreamSourceContext;
import io.rowweave.stream.spi.StreamSourceProvider;

/** Connector-owned fixture used by the published source conformance kit. */
public interface StreamSourceConformanceFixture {
    StreamSourceProvider provider();
    StreamSourceContext initialContext();
    StreamSourceContext restoredContext(
            byte[] positionFrame, long logicalPosition);
    int comparePositions(byte[] left, byte[] right);
    long wakeupTimeoutMillis();
}
