package io.rowweave.cluster.rpc;

import java.util.Arrays;

/** Immutable group of data plane negotiation fields shared by REGISTER and WORKER_REGISTERED. */
public final class DataPlaneRegistration {

    public static final int CERTIFICATE_FINGERPRINT_BYTES = 32;

    public enum Protocol {
        LOOPBACK_PLAIN_V0(0),
        SHUFFLE_MTLS_V1(1);

        private final int wireId;

        Protocol(int wireId) {
            this.wireId = wireId;
        }

        int wireId() {
            return wireId;
        }

        static Protocol fromWireId(int wireId) {
            for (Protocol value : values()) {
                if (value.wireId == wireId) return value;
            }
            throw new IllegalArgumentException("unknown data protocol wire id: " + wireId);
        }
    }

    private final Protocol protocol;
    private final byte[] certificateFingerprint;

    private DataPlaneRegistration(Protocol protocol, byte[] certificateFingerprint) {
        this.protocol = protocol;
        this.certificateFingerprint = certificateFingerprint;
    }

    public static DataPlaneRegistration loopbackPlainV0() {
        return new DataPlaneRegistration(Protocol.LOOPBACK_PLAIN_V0, null);
    }

    public static DataPlaneRegistration shuffleMtlsV1(byte[] certificateFingerprint) {
        if (certificateFingerprint == null
                || certificateFingerprint.length != CERTIFICATE_FINGERPRINT_BYTES) {
            throw new IllegalArgumentException(
                    "MTLS data certificate fingerprint must be exactly 32 bytes");
        }
        return new DataPlaneRegistration(Protocol.SHUFFLE_MTLS_V1,
                certificateFingerprint.clone());
    }

    static DataPlaneRegistration fromWire(int protocolWireId, byte[] certificateFingerprint) {
        Protocol protocol = Protocol.fromWireId(protocolWireId);
        if (protocol == Protocol.LOOPBACK_PLAIN_V0) {
            if (certificateFingerprint != null && certificateFingerprint.length != 0) {
                throw new IllegalArgumentException(
                        "LOOPBACK data registration must not carry a certificate fingerprint");
            }
            return loopbackPlainV0();
        }
        return shuffleMtlsV1(certificateFingerprint);
    }

    public Protocol protocol() {
        return protocol;
    }

    public byte[] certificateFingerprint() {
        return certificateFingerprint == null ? null : certificateFingerprint.clone();
    }

    int fingerprintLength() {
        return certificateFingerprint == null ? 0 : certificateFingerprint.length;
    }

    void writeFingerprint(java.io.DataOutputStream output) throws java.io.IOException {
        if (certificateFingerprint != null) output.write(certificateFingerprint);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof DataPlaneRegistration)) return false;
        DataPlaneRegistration that = (DataPlaneRegistration) other;
        return protocol == that.protocol
                && Arrays.equals(certificateFingerprint, that.certificateFingerprint);
    }

    @Override
    public int hashCode() {
        return 31 * protocol.hashCode() + Arrays.hashCode(certificateFingerprint);
    }
}
