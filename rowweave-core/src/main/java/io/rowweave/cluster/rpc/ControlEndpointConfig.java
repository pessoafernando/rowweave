package io.rowweave.cluster.rpc;

import java.net.InetAddress;
import java.util.Properties;

/** Pure control-plane profile and endpoint snapshot; never reads TLS material. */
public final class ControlEndpointConfig {

    private final ControlSecurityProfile profile;
    private final String advertisedHost;
    private final InetAddress bindAddress;

    private ControlEndpointConfig(ControlSecurityProfile profile,
                                  String advertisedHost,
                                  InetAddress bindAddress) {
        this.profile = profile;
        this.advertisedHost = advertisedHost;
        this.bindAddress = bindAddress;
    }

    public static ControlEndpointConfig fromProperties(Properties properties) {
        if (properties == null) {
            throw new IllegalArgumentException(
                    "control endpoint properties must not be null");
        }
        ControlSecurityProfile profile = ControlSecurityProfile.parse(
                properties.getProperty(ControlSecurityConfig.PROFILE_PROPERTY));
        String configuredHost = trimOrDefault(
                properties.getProperty(ControlSecurityConfig.HOST_PROPERTY),
                "127.0.0.1");
        InetAddress bindAddress = ControlEndpointPolicy.resolveBind(
                profile, configuredHost);
        String advertisedHost = profile == ControlSecurityProfile.LOOPBACK
                ? bindAddress.getHostAddress() : configuredHost;
        return new ControlEndpointConfig(profile, advertisedHost, bindAddress);
    }

    public ControlSecurityProfile profile() {
        return profile;
    }

    public String advertisedHost() {
        return advertisedHost;
    }

    public InetAddress bindAddress() {
        return bindAddress;
    }

    private static String trimOrDefault(String value, String defaultValue) {
        if (value == null) return defaultValue;
        String trimmed = value.trim();
        return trimmed.isEmpty() ? defaultValue : trimmed;
    }
}
