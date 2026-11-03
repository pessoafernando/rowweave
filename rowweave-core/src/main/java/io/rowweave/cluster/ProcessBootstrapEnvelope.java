package io.rowweave.cluster;

import java.util.Properties;

/** Earliest-process immutable proof for both transport identity and JVM capacity. */
public final class ProcessBootstrapEnvelope {

    private final ProcessTlsEnvelope tlsEnvelope;
    private final JvmCapacityEnvelope capacityEnvelope;

    ProcessBootstrapEnvelope(ProcessTlsEnvelope tlsEnvelope,
                             JvmCapacityEnvelope capacityEnvelope) {
        if (tlsEnvelope == null || capacityEnvelope == null) {
            throw new IllegalArgumentException(
                    "TLS and JVM capacity envelopes are required");
        }
        this.tlsEnvelope = tlsEnvelope;
        this.capacityEnvelope = capacityEnvelope;
    }

    public ProcessTlsEnvelope tlsEnvelope() {
        return tlsEnvelope;
    }

    public JvmCapacityEnvelope capacityEnvelope() {
        return capacityEnvelope;
    }

    void requireCurrentConfiguration(Properties properties) {
        tlsEnvelope.requireCurrentBootstrap();
        tlsEnvelope.requireConfigurationMatch(properties);
    }
}
