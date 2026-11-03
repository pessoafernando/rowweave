package io.rowweave.cluster;

import java.util.Locale;
import java.util.Properties;

/** Strict parser for cluster start-up properties: the Boolean and Long APIs would turn a typo silently into different scheduling behaviour. */
final class ClusterPropertyParser {

    private ClusterPropertyParser() {
    }

    static boolean booleanValue(Properties properties, String key, boolean defaultValue) {
        String configured = properties.getProperty(key);
        if (configured == null || configured.trim().isEmpty()) {
            return defaultValue;
        }
        String normalized = configured.trim().toLowerCase(Locale.ROOT);
        if ("true".equals(normalized)) {
            return true;
        }
        if ("false".equals(normalized)) {
            return false;
        }
        throw new IllegalArgumentException(key
                + " must be 'true' or 'false', but was '" + configured + "'");
    }

    static long nonNegativeLong(Properties properties, String key, long defaultValue) {
        String configured = properties.getProperty(key);
        if (configured == null || configured.trim().isEmpty()) {
            return defaultValue;
        }
        final long parsed;
        try {
            parsed = Long.parseLong(configured.trim());
        } catch (NumberFormatException invalid) {
            throw new IllegalArgumentException(key
                    + " must be a non-negative long, but was '" + configured + "'", invalid);
        }
        if (parsed < 0) {
            throw new IllegalArgumentException(key
                    + " must be >= 0, but was " + parsed);
        }
        return parsed;
    }
}
