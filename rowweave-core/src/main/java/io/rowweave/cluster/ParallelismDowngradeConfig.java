package io.rowweave.cluster;

import java.util.Properties;

/** The single place the configuration for graceful parallelism downgrade under slot pressure is read. */
final class ParallelismDowngradeConfig {

    static final String DOWNGRADE_PROPERTY = "rowweave.cluster.downgrade";
    static final String GRACE_PROPERTY = "rowweave.cluster.downgradeGraceMs";

    private ParallelismDowngradeConfig() {
    }

    static boolean enabled() {
        return enabled(System.getProperties());
    }

    static boolean enabled(Properties properties) {
        return ClusterPropertyParser.booleanValue(properties, DOWNGRADE_PROPERTY, true);
    }

    static long graceMs(long admitTimeoutMs) {
        return graceMs(System.getProperties(), admitTimeoutMs);
    }

    static long graceMs(Properties properties, long admitTimeoutMs) {
        long defaultGrace = Math.min(Math.max(0L, admitTimeoutMs / 4L), 5_000L);
        return ClusterPropertyParser.nonNegativeLong(properties, GRACE_PROPERTY, defaultGrace);
    }
}
