package io.rowweave.cluster.rpc;

import java.security.MessageDigest;

/** Two-way, fail-closed negotiation between the control profile and the data capability carried by REGISTER. */
public final class DataPlaneRegistrationNegotiator {

    private DataPlaneRegistrationNegotiator() {
    }

    public static DataPlaneRegistration requireMasterCompatible(
            ControlSecurityProfile profile, DataPlaneRegistration registration) {
        if (profile == null) {
            throw new IllegalArgumentException("control security profile is required");
        }
        if (profile == ControlSecurityProfile.MTLS) {
            if (registration == null
                    || registration.protocol()
                    != DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1) {
                throw new IllegalArgumentException(
                        "MTLS worker registration requires SHUFFLE_MTLS_V1 capability");
            }
            return registration;
        }
        if (registration != null
                && registration.protocol()
                != DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0) {
            throw new IllegalArgumentException(
                    "LOOPBACK worker registration rejects non-loopback data capability");
        }
        return registration;
    }

    public static void requireWorkerAccepted(
            ControlSecurityProfile profile,
            DataPlaneRegistration expected,
            String expectedIncarnation,
            RpcMessage response) {
        if (profile == null || expected == null || expectedIncarnation == null) {
            throw new IllegalArgumentException(
                    "profile, expected registration and incarnation are required");
        }
        if (profile == ControlSecurityProfile.LOOPBACK) {
            if (expected.protocol()
                    != DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0
                    || response == null || response.type() != RpcMessage.Type.ACK) {
                throw rejected("LOOPBACK registration requires ordinary ACK");
            }
            return;
        }
        if (expected.protocol() != DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1
                || response == null
                || response.type() != RpcMessage.Type.WORKER_REGISTERED
                || !expectedIncarnation.equals(response.incarnationId())) {
            throw rejected("MTLS registration requires exact WORKER_REGISTERED echo");
        }
        DataPlaneRegistration accepted = response.dataPlaneRegistration();
        if (accepted == null
                || accepted.protocol() != expected.protocol()
                || !MessageDigest.isEqual(expected.certificateFingerprint(),
                accepted.certificateFingerprint())) {
            throw rejected("MTLS registration data identity echo mismatch");
        }
    }

    private static IllegalStateException rejected(String reason) {
        return new IllegalStateException("active master rejected worker data registration: " + reason);
    }
}
