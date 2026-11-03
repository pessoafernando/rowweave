package io.rowweave.cluster;

import java.util.Properties;

/** The single point where the switch for publishing the shuffle data plane is resolved. */
final class ShuffleTransportConfig {

    static final String MUX_PROPERTY = "rowweave.shuffle.mux";

    private ShuffleTransportConfig() {
    }

    /** Read at process start-up: multiplexing is on unless configured, and an explicit false rolls back to the legacy transport. */
    static boolean muxEnabled() {
        return muxEnabled(System.getProperties());
    }

    static boolean muxEnabled(Properties properties) {
        return ClusterPropertyParser.booleanValue(properties, MUX_PROPERTY, true);
    }
}
