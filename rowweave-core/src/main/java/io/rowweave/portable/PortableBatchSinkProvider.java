package io.rowweave.portable;

import io.rowweave.Sink;
import io.rowweave.DataSchema;

/** Stateless Worker-side factory for one unopened portable batch sink. */
public interface PortableBatchSinkProvider {
    PortableConnectorProviderContract contract();
    Sink create(
            byte[] canonicalDescriptor, byte[] canonicalConfiguration,
            DataSchema inputSchema);
}
