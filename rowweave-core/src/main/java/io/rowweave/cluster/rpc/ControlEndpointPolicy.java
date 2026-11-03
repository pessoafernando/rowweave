package io.rowweave.cluster.rpc;

import java.net.InetAddress;
import java.net.UnknownHostException;

/** Resolves and validates control plane addresses before a socket is created or an endpoint published. */
public final class ControlEndpointPolicy {

    private ControlEndpointPolicy() {
    }

    /** Resolves the bind address; under LOOPBACK a wildcard, a non-local loopback and mixed results are rejected. */
    public static InetAddress resolveBind(ControlSecurityProfile profile, String host) {
        return resolve(profile, host, "bind");
    }

    /** Resolves and pins the connect target; the caller uses the returned address and never repeats the lookup by host. */
    public static InetAddress resolveTarget(ControlSecurityProfile profile, String host) {
        return resolve(profile, host, "target");
    }

    private static InetAddress resolve(ControlSecurityProfile profile, String host, String purpose) {
        if (profile == null) {
            throw new IllegalArgumentException("control security profile must not be null");
        }
        String candidate = host == null ? "" : host.trim();
        if (candidate.isEmpty()) {
            throw new IllegalArgumentException("control " + purpose + " host must not be blank");
        }
        final InetAddress[] resolved;
        try {
            resolved = InetAddress.getAllByName(candidate);
        } catch (UnknownHostException unknown) {
            throw new IllegalArgumentException(
                    "cannot resolve control " + purpose + " host: " + candidate, unknown);
        }
        if (resolved.length == 0) {
            throw new IllegalArgumentException(
                    "control " + purpose + " host resolved to no addresses: " + candidate);
        }
        if (profile == ControlSecurityProfile.LOOPBACK) {
            for (InetAddress address : resolved) {
                if (!address.isLoopbackAddress()) {
                    throw new IllegalArgumentException(
                            "LOOPBACK control profile rejects " + purpose + " host " + candidate
                                    + " resolved as " + address.getHostAddress());
                }
            }
        }
        return resolved[0];
    }
}
