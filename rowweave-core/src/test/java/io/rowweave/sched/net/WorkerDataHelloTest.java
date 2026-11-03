package io.rowweave.sched.net;

import io.rowweave.cluster.rpc.DataPlaneRegistration;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The worker hello inside TLS must be a fixed-size, exact-identity control preamble. */
class WorkerDataHelloTest {

    @Test
    void roundTripsExactWorkerGenerationAndDeploymentIdentityWithinOneKiB() {
        byte[] fingerprint = fingerprint(7);
        WorkerDataIdentity identity = WorkerDataIdentity.authenticated(
                "worker-a", "incarnation-a",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-set-a", "authz-set-a", fingerprint);

        byte[] encoded = WorkerDataHello.encode(identity);

        assertTrue(encoded.length <= WorkerDataHello.MAX_FRAME_BYTES);
        assertEquals(identity, WorkerDataHello.decode(encoded));
        assertArrayEquals(fingerprint, identity.certificateFingerprint());
        fingerprint[0] ^= 0x7f;
        assertFalse(Arrays.equals(fingerprint, identity.certificateFingerprint()),
                "identity must own a defensive fingerprint copy");
    }

    @Test
    void malformedLengthTrailingBytesAndUnknownProtocolFailClosed() {
        WorkerDataIdentity identity = WorkerDataIdentity.authenticated(
                "worker-a", "incarnation-a",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-set-a", "authz-set-a", fingerprint(11));
        byte[] valid = WorkerDataHello.encode(identity);

        byte[] oversizedLength = valid.clone();
        oversizedLength[8] = 0x7f;
        oversizedLength[9] = (byte) 0xff;
        oversizedLength[10] = (byte) 0xff;
        oversizedLength[11] = (byte) 0xff;
        assertThrows(IllegalArgumentException.class,
                () -> WorkerDataHello.decode(oversizedLength));

        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
        assertThrows(IllegalArgumentException.class, () -> WorkerDataHello.decode(trailing));

        byte[] unknownProtocol = valid.clone();
        unknownProtocol[12] = 0x7f;
        assertThrows(IllegalArgumentException.class,
                () -> WorkerDataHello.decode(unknownProtocol));
    }

    @Test
    void overlongIdentityIsRejectedBeforeAHelloCanBeEncoded() {
        String overlong = new String(new byte[WorkerDataHello.MAX_STRING_BYTES + 1],
                StandardCharsets.ISO_8859_1).replace('\0', 'x');
        assertThrows(IllegalArgumentException.class, () -> WorkerDataIdentity.authenticated(
                overlong, "incarnation-a",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-set-a", "authz-set-a", fingerprint(13)));
    }

    @Test
    void diagnosticTextNeverContainsFullCertificateFingerprint() {
        byte[] fingerprint = fingerprint(19);
        WorkerDataIdentity identity = WorkerDataIdentity.authenticated(
                "worker-a", "incarnation-a",
                DataPlaneRegistration.Protocol.SHUFFLE_MTLS_V1,
                "deployment-set-a", "authz-set-a", fingerprint);
        String fullHex = hex(fingerprint);

        assertFalse(identity.toString().contains(fullHex));
    }

    private static byte[] fingerprint(int seed) {
        byte[] value = new byte[DataPlaneRegistration.CERTIFICATE_FINGERPRINT_BYTES];
        for (int i = 0; i < value.length; i++) value[i] = (byte) (seed + i);
        return value;
    }

    private static String hex(byte[] value) {
        StringBuilder result = new StringBuilder(value.length * 2);
        for (byte b : value) result.append(String.format("%02x", b & 0xff));
        return result.toString();
    }
}
