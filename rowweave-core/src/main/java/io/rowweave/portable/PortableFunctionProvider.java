package io.rowweave.portable;

import java.io.Serializable;

/** Worker-side factory paired with a signed static function contract resource. */
public interface PortableFunctionProvider {
    PortableFunctionProviderContract contract();
    Serializable create(byte[] canonicalConfiguration);
}
