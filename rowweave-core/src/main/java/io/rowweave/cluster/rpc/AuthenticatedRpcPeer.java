package io.rowweave.cluster.rpc;

import java.util.EnumSet;

/**
 * Server-owned identity extracted from the authenticated control TLS session.
 * It is deliberately not serializable and has no public construction path.
 */
public final class AuthenticatedRpcPeer {
    private final byte[] certificateFingerprint;
    private final EnumSet<ControlRole> roles;

    AuthenticatedRpcPeer(byte[] certificateFingerprint, EnumSet<ControlRole> roles) {
        if (certificateFingerprint == null
                || certificateFingerprint.length
                != DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES) {
            throw new IllegalArgumentException(
                    "authenticated peer fingerprint must be exactly 32 bytes");
        }
        if (roles == null || roles.isEmpty()) {
            throw new IllegalArgumentException("authenticated peer roles must not be empty");
        }
        this.certificateFingerprint = certificateFingerprint.clone();
        this.roles = EnumSet.copyOf(roles);
    }

    public byte[] certificateFingerprint() {
        return certificateFingerprint.clone();
    }

    public boolean hasRole(ControlRole role) {
        return role != null && roles.contains(role);
    }

    public EnumSet<ControlRole> roles() {
        return EnumSet.copyOf(roles);
    }
}
