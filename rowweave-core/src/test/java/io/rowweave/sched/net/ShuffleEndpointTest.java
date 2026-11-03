package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.DataPlaneRegistration;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ShuffleEndpoint gained an optional targetId and dataPort, so a multiplexed upstream connects
 * to the worker data port and carries the targetId for routing. These are added fields: the old
 * host and port constructor leaves targetId at NO_TARGET and dataPort at -1, which a disabled
 * mux path never reads, so nothing regresses.
 */
class ShuffleEndpointTest {

/** The old host and port constructor gives the NO_TARGET placeholder and dataPort -1, meaning mux is not enabled. */
    @Test
    void legacyConstructorDefaults() {
        ShuffleEndpoint ep = new ShuffleEndpoint("10.0.0.1", 5001);
        assertEquals("10.0.0.1", ep.host());
        assertEquals(5001, ep.port());
        assertEquals(Frame.NO_TARGET, ep.targetId(), "the constructor without a target uses the NO_TARGET placeholder");
        assertEquals(-1, ep.dataPort(), "the constructor without a data port reports -1 because mux is off");
    }

/** The mux constructor takes the data port and targetId, so the upstream connects there and frames carry the targetId. */
    @Test
    void muxConstructorCarriesTargetIdAndDataPort() {
        UUID tid = UUID.randomUUID();
        ShuffleEndpoint ep = ShuffleEndpoint.mux("10.0.0.2", 9000, tid);
        assertEquals("10.0.0.2", ep.host());
        assertEquals(9000, ep.dataPort(), "dataPort is the single worker data port");
        assertEquals(tid, ep.targetId(), "targetId is the routing key");
        // port() reports the data port as the connection target, so openTarget going through ep.port() reaches the right port.
        assertEquals(9000, ep.port());
    }

/** equals and hashCode include targetId, so different targets at the same host and data port are different endpoints. */
    @Test
    void equalsIncludesTargetId() {
        UUID a = UUID.randomUUID();
        UUID b = UUID.randomUUID();
        assertEquals(ShuffleEndpoint.mux("h", 9000, a), ShuffleEndpoint.mux("h", 9000, a));
        assertNotEquals(ShuffleEndpoint.mux("h", 9000, a), ShuffleEndpoint.mux("h", 9000, b));
    }

    /** DP4: endpoint carries the exact destination generation used by TLS + hello and pool reuse. */
    @Test
    void authenticatedMuxEndpointIncludesExactDestinationGeneration() {
        UUID target = UUID.randomUUID();
        WorkerDataIdentity first = identity("incarnation-1", 1);
        WorkerDataIdentity replacement = identity("incarnation-2", 2);

        ShuffleEndpoint endpoint = ShuffleEndpoint.muxAuthenticated(
                "worker.example", 9443, target, first);

        assertEquals(first, endpoint.destinationIdentity());
        assertNotEquals(endpoint, ShuffleEndpoint.muxAuthenticated(
                "worker.example", 9443, target, replacement));
        assertFalse(endpoint.toString().contains(hex(first.certificateFingerprint())),
                "endpoint diagnostics must not disclose the full certificate fingerprint");
    }

    private static WorkerDataIdentity identity(String incarnation, int seed) {
        byte[] fingerprint = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
        for (int i = 0; i < fingerprint.length; i++) fingerprint[i] = (byte) (seed + i);
        return WorkerDataIdentity.authenticated(
                "worker-a", incarnation,
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-set-a", "authz-set-a", fingerprint);
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) result.append(String.format("%02x", b & 0xff));
        return result.toString();
    }
}
