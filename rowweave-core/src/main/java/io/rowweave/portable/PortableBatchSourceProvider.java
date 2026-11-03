package io.rowweave.portable;

import io.rowweave.Source;

/** Stateless Worker-side factory for one unopened portable bounded source split. */
public interface PortableBatchSourceProvider {
    PortableConnectorProviderContract contract();
    Source create(byte[] canonicalConfiguration, byte[] canonicalSplitFrame);
}
