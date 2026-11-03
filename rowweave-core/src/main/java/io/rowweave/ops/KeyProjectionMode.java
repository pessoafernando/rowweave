package io.rowweave.ops;

/** The internal A/B switch for the direct projection serialization of a grouping key. */
final class KeyProjectionMode {

    static final String PROPERTY = "rowweave.key.serialization.projected";

    private KeyProjectionMode() {
    }

    static boolean enabled() {
        return Boolean.parseBoolean(System.getProperty(PROPERTY, "true"));
    }
}
