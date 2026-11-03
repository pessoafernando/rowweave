package io.rowweave.cluster.rpc;

import java.util.Locale;

/** The control plane allows exactly two profiles: loopback plaintext and production MTLS. */
public enum ControlSecurityProfile {
    LOOPBACK,
    MTLS;

    static ControlSecurityProfile parse(String value) {
        String normalized = value == null ? "LOOPBACK" : value.trim();
        if (normalized.isEmpty()) {
            normalized = "LOOPBACK";
        }
        try {
            return valueOf(normalized.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException unknown) {
            throw new IllegalArgumentException(
                    "unsupported rowweave.control.securityProfile: " + normalized
                            + "; expected LOOPBACK or MTLS",
                    unknown);
        }
    }
}
