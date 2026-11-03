package io.rowweave.stream.spi.conformance;

import io.rowweave.stream.spi.StreamTransactionalSinkContext;
import io.rowweave.stream.spi.StreamTransactionalSinkProvider;

/** Connector-owned fixture used by the published transactional Sink kit. */
public interface StreamTransactionalSinkConformanceFixture {
    StreamTransactionalSinkProvider provider();
    StreamTransactionalSinkContext context();
    byte[] recordFrame();
    String commitTransactionIdentity();
    String abortTransactionIdentity();
}
