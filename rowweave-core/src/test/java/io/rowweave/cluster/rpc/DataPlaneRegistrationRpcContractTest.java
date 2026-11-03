package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DataPlaneRegistrationRpcContractTest {

    @Test
    void loopbackRegistrationRoundTripsWithoutCertificateIdentity() throws Exception {
        RpcMessage decoded = roundTrip(RpcMessage.registerWorker(
                "worker-0", "127.0.0.1", 7001, 4, 7002, "inc-0",
                DataPlaneRegistration.loopbackPlainV0()));

        assertEquals(DataPlaneRegistration.Protocol.LOOPBACK_PLAIN_V0,
                decoded.dataPlaneRegistration().protocol());
        assertNull(decoded.dataPlaneRegistration().certificateFingerprint());
    }

    @Test
    void mtlsRegistrationAndDedicatedEchoRoundTripExactFingerprint() throws Exception {
        byte[] fingerprint = fingerprint(7);
        RpcMessage register = roundTrip(RpcMessage.registerWorker(
                "worker-1", "worker.example", 7101, 8, 7102, "inc-1",
                DataPlaneRegistration.shuffleMtlsV1(fingerprint)));
        RpcMessage accepted = roundTrip(RpcMessage.workerRegistered(
                "inc-1", DataPlaneRegistration.shuffleMtlsV1(fingerprint)));

        assertEquals(RpcMessage.Type.WORKER_REGISTERED, accepted.type());
        assertEquals("inc-1", accepted.incarnationId());
        assertArrayEquals(fingerprint,
                register.dataPlaneRegistration().certificateFingerprint());
        assertArrayEquals(fingerprint,
                accepted.dataPlaneRegistration().certificateFingerprint());
    }

    @Test
    void capabilityDefensivelyOwnsFingerprintAndRejectsIllegalCombinations() {
        byte[] fingerprint = fingerprint(11);
        DataPlaneRegistration registration = DataPlaneRegistration.shuffleMtlsV1(fingerprint);
        fingerprint[0] = 99;
        byte[] returned = registration.certificateFingerprint();
        returned[1] = 98;

        assertEquals(11, registration.certificateFingerprint()[0]);
        assertEquals(12, registration.certificateFingerprint()[1]);
        assertThrows(IllegalArgumentException.class,
                () -> DataPlaneRegistration.shuffleMtlsV1(new byte[31]));
    }

    @Test
    void legacyRegisterRemainsDecodableButHasNoInventedCapability() throws Exception {
        RpcMessage decoded = roundTrip(RpcMessage.registerWorker(
                "legacy", "127.0.0.1", 7201, 2, -1, "legacy-inc"));
        assertNull(decoded.dataPlaneRegistration());
    }

    @Test
    void unknownCapabilityVersionAndTruncationAreRejected() {
        byte[] valid = RpcCodec.encode(RpcMessage.registerWorker(
                "worker-2", "127.0.0.1", 7301, 2, 7302, "inc-2",
                DataPlaneRegistration.loopbackPlainV0()));
        int extensionStart = valid.length - RpcCodec.DATA_REGISTRATION_LOOPBACK_BYTES;

        byte[] unknownVersion = valid.clone();
        ByteBuffer.wrap(unknownVersion,
                extensionStart + RpcCodec.DATA_REGISTRATION_MAGIC_BYTES,
                Integer.BYTES).putInt(99);
        assertThrows(IOException.class, () -> decode(unknownVersion));

        byte[] truncated = Arrays.copyOf(valid, valid.length - 1);
        ByteBuffer.wrap(truncated).putInt(truncated.length - Integer.BYTES);
        assertThrows(IOException.class, () -> decode(truncated));
    }

    private static RpcMessage roundTrip(RpcMessage message) throws Exception {
        return decode(RpcCodec.encode(message));
    }

    private static RpcMessage decode(byte[] frame) throws Exception {
        return RpcCodec.decode(new DataInputStream(new ByteArrayInputStream(frame)));
    }

    private static byte[] fingerprint(int seed) {
        byte[] value = new byte[32];
        for (int i = 0; i < value.length; i++) value[i] = (byte) (seed + i);
        return value;
    }
}

