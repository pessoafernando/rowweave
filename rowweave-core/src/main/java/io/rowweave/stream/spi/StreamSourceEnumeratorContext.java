package io.rowweave.stream.spi;

import io.rowweave.stream.api.StreamSourceDiscoverySpec;

/** Data-free Source enumerator open context with optional ephemeral secrets. */
public final class StreamSourceEnumeratorContext {
    private static final int MAX_CONFIGURATION_BYTES = 4 * 1024 * 1024;
    private final String operatorUid;
    private final byte[] configuration;
    private final StreamSourceDiscoverySpec discoverySpec;
    private final StreamSecretAccess secrets;

    public StreamSourceEnumeratorContext(String operatorUid,
            byte[] configurationFrame,
            StreamSourceDiscoverySpec discoverySpec) {
        this(operatorUid, configurationFrame, discoverySpec,
                StreamSecretAccess.unavailable());
    }

    public StreamSourceEnumeratorContext(String operatorUid,
            byte[] configurationFrame,
            StreamSourceDiscoverySpec discoverySpec,
            StreamSecretAccess secrets) {
        if (operatorUid == null || operatorUid.isEmpty()
                || configurationFrame == null
                || configurationFrame.length > MAX_CONFIGURATION_BYTES
                || discoverySpec == null || secrets == null) {
            throw new IllegalArgumentException(
                    "Stream source enumerator context is invalid");
        }
        this.operatorUid = operatorUid;
        this.configuration = configurationFrame.clone();
        this.discoverySpec = discoverySpec;
        this.secrets = secrets;
    }

    public String operatorUid() { return operatorUid; }
    public byte[] configurationFrame() { return configuration.clone(); }
    public StreamSourceDiscoverySpec discoverySpec() { return discoverySpec; }
    public StreamSecretAccess secrets() { return secrets; }
}
