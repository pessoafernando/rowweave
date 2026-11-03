package io.rowweave.cluster.rpc;

import java.util.Properties;

/** Startup-only loader for one canonical public-v1 hello frame. */
public final class ProtocolHelloConfig {
    public static final String HELLO_HEX_PROPERTY =
            "rowweave.control.protocolHelloHex";

    private ProtocolHelloConfig() { }

    /** Returns null only when the process explicitly remains on the pre-v1 transport. */
    public static ProtocolHelloV1 fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException("protocol properties are required");
        }
        String encoded = properties.getProperty(HELLO_HEX_PROPERTY);
        if (encoded == null) return null;
        if (encoded.isEmpty() || (encoded.length() & 1) != 0
                || encoded.length() > ProtocolHelloV1Codec.MAX_FRAME_BYTES * 2) {
            throw new IllegalArgumentException(
                    "protocol hello hex length is invalid");
        }
        byte[] frame = new byte[encoded.length() / 2];
        for (int index = 0; index < frame.length; index++) {
            int high = lowerHex(encoded.charAt(index * 2));
            int low = lowerHex(encoded.charAt(index * 2 + 1));
            if (high < 0 || low < 0) {
                throw new IllegalArgumentException(
                        "protocol hello must be canonical lowercase hex");
            }
            frame[index] = (byte) ((high << 4) | low);
        }
        return ProtocolHelloV1Codec.decode(frame);
    }

    public static ProtocolHelloV1 fromSystemProperties() {
        Properties snapshot = new Properties();
        snapshot.putAll(System.getProperties());
        return fromProperties(snapshot);
    }

    private static int lowerHex(char value) {
        if (value >= '0' && value <= '9') return value - '0';
        if (value >= 'a' && value <= 'f') return value - 'a' + 10;
        return -1;
    }
}
