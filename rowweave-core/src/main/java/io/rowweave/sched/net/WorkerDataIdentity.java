package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.DataPlaneRegistration;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;

/**
 * one immutable, exact Worker data-plane generation.
 *
 * <p>The value is carried by an authenticated {@link ShuffleEndpoint} and by the TLS-internal
 * Worker hello. It deliberately has no secret-bearing diagnostic representation.</p>
 */
public final class WorkerDataIdentity implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String workerId;
    private final String incarnationId;
    private final DataPlaneRegistration.Protocol protocol;
    private final String deploymentSetId;
    private final String authzSetId;
    private final byte[] certificateFingerprint;

    private WorkerDataIdentity(String workerId, String incarnationId,
                               DataPlaneRegistration.Protocol protocol,
                               String deploymentSetId, String authzSetId,
                               byte[] certificateFingerprint) {
        this.workerId = bounded(workerId, "worker id");
        this.incarnationId = bounded(incarnationId, "worker incarnation");
        this.deploymentSetId = bounded(deploymentSetId, "deployment set id");
        this.authzSetId = bounded(authzSetId, "authorization set id");
        if (protocol != DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1) {
            throw new IllegalArgumentException(
                    "authenticated Worker identity requires SHUFFLE_MTLS_V1");
        }
        if (certificateFingerprint == null
                || certificateFingerprint.length
                != DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES) {
            throw new IllegalArgumentException(
                    "Worker certificate fingerprint must be exactly 32 bytes");
        }
        this.protocol = protocol;
        this.certificateFingerprint = certificateFingerprint.clone();
    }

    public static WorkerDataIdentity authenticated(
            String workerId, String incarnationId,
            DataPlaneRegistration.Protocol protocol,
            String deploymentSetId, String authzSetId,
            byte[] certificateFingerprint) {
        return new WorkerDataIdentity(workerId, incarnationId, protocol,
                deploymentSetId, authzSetId, certificateFingerprint);
    }

    private static String bounded(String value, String name) {
        if (value == null || value.isEmpty()) {
            throw new IllegalArgumentException(name + " is required");
        }
        if (!value.equals(value.trim())) {
            throw new IllegalArgumentException(name + " must not contain surrounding whitespace");
        }
        if (value.getBytes(StandardCharsets.UTF_8).length
                > WorkerDataHello.MAX_STRING_BYTES) {
            throw new IllegalArgumentException(name + " exceeds Worker hello limit");
        }
        return value;
    }

    public String workerId() { return workerId; }
    public String incarnationId() { return incarnationId; }
    public DataPlaneRegistration.Protocol protocol() { return protocol; }
    public String deploymentSetId() { return deploymentSetId; }
    public String authzSetId() { return authzSetId; }
    public byte[] certificateFingerprint() { return certificateFingerprint.clone(); }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        if (!(other instanceof WorkerDataIdentity)) return false;
        WorkerDataIdentity that = (WorkerDataIdentity) other;
        return workerId.equals(that.workerId)
                && incarnationId.equals(that.incarnationId)
                && protocol == that.protocol
                && deploymentSetId.equals(that.deploymentSetId)
                && authzSetId.equals(that.authzSetId)
                && Arrays.equals(certificateFingerprint, that.certificateFingerprint);
    }

    @Override
    public int hashCode() {
        return 31 * Objects.hash(workerId, incarnationId, protocol,
                deploymentSetId, authzSetId)
                + Arrays.hashCode(certificateFingerprint);
    }

    @Override
    public String toString() {
        return "WorkerDataIdentity{" + workerId + '@' + incarnationId
                + ", protocol=" + protocol + "}";
    }
}
