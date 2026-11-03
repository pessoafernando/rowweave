package io.rowweave.sched.net;

import java.io.Serializable;
import java.util.Objects;
import java.util.UUID;

/**
 * A shuffle network endpoint, an immutable {@code host:port} value object. Once {@link
 * MultiSocketChannelSource} on the consumer side has its ServerSocket up, it registers this endpoint with the
 * {@link ShuffleRegistry}, and the producer connects to it.
 *
 * <p><b>Multiplexing</b>: in mux mode the endpoint also carries a {@code targetId}, the UUID routing key, and
 * a {@code dataPort}, the single data port of the worker. The upstream connects to the worker dataPort and
 * frames carry the targetId, so the server demuxes by targetId. The older {@code (host,port)} constructor
 * defaults {@code targetId} to {@link Frame#NO_TARGET} and {@code dataPort} to -1, which mux off never reads.
 */
public final class ShuffleEndpoint implements Serializable {

    private static final long serialVersionUID = 4L; // attempt-local producer authorization

    private final String host;
    private final int port;  // legacy: one port per stage; mux: equal to dataPort, which is the connect target
    private final int dataPort;  // mux: the single data port of the worker, or -1 when mux is not in use
    private final UUID targetId;  // mux: the routing key, where NO_TARGET means not in use or unset
    private final WorkerDataIdentity destinationIdentity;
    private final ShuffleProducerGrant producerGrant;
    private final int producerOrdinal;
    private final byte[] cohortId;
    private final int expectedOrdinalCount;

    /** The older constructor, where each stage had its own port: targetId is NO_TARGET and dataPort is -1. */
    public ShuffleEndpoint(String host, int port) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = port;
        this.dataPort = -1;
        this.targetId = Frame.NO_TARGET;
        this.destinationIdentity = null;
        this.producerGrant = null;
        this.producerOrdinal = 0;
        this.cohortId = null;
        this.expectedOrdinalCount = 0;
    }

    private ShuffleEndpoint(String host, int dataPort, UUID targetId) {
        this(host, dataPort, targetId, null, null, 0, null, 0);
    }

    private ShuffleEndpoint(String host, int dataPort, UUID targetId,
                            WorkerDataIdentity destinationIdentity) {
        this(host, dataPort, targetId, destinationIdentity, null, 0, null, 0);
    }

    private ShuffleEndpoint(String host, int dataPort, UUID targetId,
                            WorkerDataIdentity destinationIdentity,
                            ShuffleProducerGrant producerGrant, int producerOrdinal,
                            byte[] cohortId, int expectedOrdinalCount) {
        this.host = Objects.requireNonNull(host, "host");
        this.port = dataPort;  // The connect target is the dataPort
        this.dataPort = dataPort;
        this.targetId = targetId == null ? Frame.NO_TARGET : targetId;
        this.destinationIdentity = destinationIdentity;
        if (producerGrant == null) {
            if (producerOrdinal != 0 || cohortId != null || expectedOrdinalCount != 0) {
                throw new IllegalArgumentException("partial producer authorization is invalid");
            }
        } else {
            if (destinationIdentity == null || Frame.NO_TARGET.equals(this.targetId)) {
                throw new IllegalArgumentException(
                        "authorized endpoint requires identity and non-zero target");
            }
            ShuffleProducerGrant.requireOrdinal(producerOrdinal);
            if (cohortId == null || cohortId.length != 32) {
                throw new IllegalArgumentException("shuffle cohort id must be exactly 32 bytes");
            }
            if (expectedOrdinalCount < 1
                    || expectedOrdinalCount > ShuffleProducerGrant.MAX_PRODUCERS_PER_TARGET) {
                throw new IllegalArgumentException("invalid expected ordinal count");
            }
        }
        this.producerGrant = producerGrant;
        this.producerOrdinal = producerOrdinal;
        this.cohortId = cohortId == null ? null : cohortId.clone();
        this.expectedOrdinalCount = expectedOrdinalCount;
    }

    /** A mux endpoint: the upstream connects to {@code host:dataPort} and frames carry {@code targetId}. */
    public static ShuffleEndpoint mux(String host, int dataPort, UUID targetId) {
        return new ShuffleEndpoint(host, dataPort, targetId);
    }

    /** MTLS mux endpoint bound to the exact destination Worker generation. */
    public static ShuffleEndpoint muxAuthenticated(
            String host, int dataPort, UUID targetId,
            WorkerDataIdentity destinationIdentity) {
        return new ShuffleEndpoint(host, dataPort, targetId,
                Objects.requireNonNull(destinationIdentity, "destinationIdentity"));
    }

    /** MTLS endpoint carrying the producer-only half of an attempt-local grant. */
    public static ShuffleEndpoint muxAuthorized(
            String host, int dataPort, UUID targetId,
            WorkerDataIdentity destinationIdentity,
            ShuffleProducerGrant producerGrant, int producerOrdinal,
            byte[] cohortId, int expectedOrdinalCount) {
        return new ShuffleEndpoint(host, dataPort, targetId,
                Objects.requireNonNull(destinationIdentity, "destinationIdentity"),
                Objects.requireNonNull(producerGrant, "producerGrant"),
                producerOrdinal, cohortId, expectedOrdinalCount);
    }

    public String host() {
        return host;
    }

    public int port() {
        return port;
    }

    /** The single data port of the worker, or -1 when mux is not in use and the legacy port applies. */
    public int dataPort() {
        return dataPort;
    }

    /** The multiplexing routing key, where NO_TARGET means not in use. */
    public UUID targetId() {
        return targetId;
    }

    public WorkerDataIdentity destinationIdentity() { return destinationIdentity; }
    public ShuffleProducerGrant producerGrant() { return producerGrant; }
    public int producerOrdinal() { return producerOrdinal; }
    public byte[] cohortId() { return cohortId == null ? null : cohortId.clone(); }
    public int expectedOrdinalCount() { return expectedOrdinalCount; }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ShuffleEndpoint)) {
            return false;
        }
        ShuffleEndpoint e = (ShuffleEndpoint) o;
        return port == e.port && dataPort == e.dataPort && host.equals(e.host)
                && targetId.equals(e.targetId)
                && Objects.equals(destinationIdentity, e.destinationIdentity)
                && producerOrdinal == e.producerOrdinal
                && expectedOrdinalCount == e.expectedOrdinalCount
                && java.util.Arrays.equals(cohortId, e.cohortId)
                && grantsEqual(producerGrant, e.producerGrant);
    }

    @Override
    public int hashCode() {
        // Never derive a diagnostic/hash value from the bearer secret.
        return 31 * Objects.hash(host, port, dataPort, targetId, destinationIdentity,
                producerOrdinal, expectedOrdinalCount) + java.util.Arrays.hashCode(cohortId);
    }

    @Override
    public String toString() {
        if (targetId.equals(Frame.NO_TARGET)) return host + ":" + port;
        return producerGrant == null ? host + ":" + dataPort + "#" + targetId
                : host + ":" + dataPort + "#authorized-redacted";
    }

    private static boolean grantsEqual(ShuffleProducerGrant left,
                                       ShuffleProducerGrant right) {
        return left == null ? right == null : left.constantTimeEquals(right);
    }
}
