package io.rowweave.cluster.registry;

import io.rowweave.cluster.rpc.RpcSecurityMetadata;
import io.rowweave.cluster.rpc.DataPlaneRegistration;
import io.rowweave.cluster.rpc.ControlMessageId;
import io.rowweave.cluster.rpc.ProtocolHelloV1;
import io.rowweave.cluster.rpc.ProtocolRevisionRange;
import io.rowweave.cluster.rpc.RpcMessage;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** registry identity has one bounded legacy/secure wire codec. */
class RegistryWorkerCodecTest {
    private static final String SET_A = repeat('a', 64);
    private static final String SET_B = repeat('b', 64);

    @Test
    void currentLegacyAndSecureRecordsRoundTripAtExactBoundaries() {
        RegistryWorker legacy = new RegistryWorker("w-1", "127.0.0.1", 9000, 8, 9001, "inc-1");
        RegistryWorker legacyBack = RegistryWorkerCodec.decode(
                RegistryWorkerCodec.encode(legacy), false);
        assertEquals("inc-1", legacyBack.incarnationId());
        assertNull(legacyBack.securityMetadata());

        RegistryWorker secure = new RegistryWorker("w-1", "worker.example", 9000, 8, 9001,
                "inc-1", RpcSecurityMetadata.of(SET_A, SET_B));
        byte[] encoded = RegistryWorkerCodec.encode(secure);
        RegistryWorker decoded = RegistryWorkerCodec.decode(encoded, true);
        assertEquals(SET_A, decoded.securityMetadata().deploymentSetId());
        assertEquals(SET_B, decoded.securityMetadata().authzSetId());
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(RegistryWorkerCodec.encode(legacy), true));

        byte[] fingerprint = new byte[32];
        Arrays.fill(fingerprint, (byte) 0x5a);
        DataPlaneRegistration dataIdentity =
                DataPlaneRegistration.shuffleMtlsV1(fingerprint);
        RegistryWorker secureData = new RegistryWorker(
                "w-data", "worker.example", 9010, 4, 9011, "inc-data",
                RpcSecurityMetadata.of(SET_A, SET_B), dataIdentity);
        RegistryWorker secureDataBack = RegistryWorkerCodec.decode(
                RegistryWorkerCodec.encode(secureData), true);
        assertEquals(dataIdentity, secureDataBack.dataPlaneRegistration());
        assertNull(decoded.dataPlaneRegistration(),
                "pre-DP3 secure registry records remain readable as unproved candidates");
    }

    @Test
    void dataIdentityExtensionIsAtomicAndRejectsTrailingOrTruncatedGroups() {
        byte[] fingerprint = new byte[32];
        Arrays.fill(fingerprint, (byte) 0x6b);
        RegistryWorker worker = new RegistryWorker(
                "w", "host", 1, 2, 3, "inc",
                RpcSecurityMetadata.of(SET_A, SET_B),
                DataPlaneRegistration.shuffleMtlsV1(fingerprint));
        byte[] encoded = RegistryWorkerCodec.encode(worker);
        int oldSecureLength = RegistryWorkerCodec.encode(new RegistryWorker(
                "w", "host", 1, 2, 3, "inc",
                RpcSecurityMetadata.of(SET_A, SET_B))).length;
        for (int length = oldSecureLength + 1; length < encoded.length; length++) {
            int candidateLength = length;
            assertThrows(IllegalArgumentException.class,
                    () -> RegistryWorkerCodec.decode(
                            Arrays.copyOf(encoded, candidateLength), true),
                    "accepted data identity truncation at " + candidateLength);
        }
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(
                        Arrays.copyOf(encoded, encoded.length + 1), true));
    }

    @Test
    void historicalAtomicLegacyGroupsRemainReadableOnlyWhenComplete() {
        RegistryWorker full = new RegistryWorker("w", "host", 1, 2, 3, "inc");
        byte[] current = RegistryWorkerCodec.encode(full);
        int base = 4 + 1 + 4 + 4 + 4 + 4;
        RegistryWorker l0 = RegistryWorkerCodec.decode(Arrays.copyOf(current, base), false);
        assertEquals(-1, l0.dataPort());
        assertNull(l0.incarnationId());
        RegistryWorker l1 = RegistryWorkerCodec.decode(Arrays.copyOf(current, base + 4), false);
        assertEquals(3, l1.dataPort());
        assertNull(l1.incarnationId());
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(Arrays.copyOf(current, base + 5), false));
    }

    @Test
    void malformedLengthsUtf8VersionTruncationAndTrailingBytesFailClosed() {
        RegistryWorker secure = new RegistryWorker("w", "host", 1, 2, 3, "inc",
                RpcSecurityMetadata.of(SET_A, SET_B));
        byte[] encoded = RegistryWorkerCodec.encode(secure);
        for (int length = 0; length < encoded.length; length++) {
            int candidateLength = length;
            assertThrows(IllegalArgumentException.class,
                    () -> RegistryWorkerCodec.decode(Arrays.copyOf(encoded, candidateLength), true),
                    "accepted secure truncation at " + candidateLength);
        }
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(Arrays.copyOf(encoded, encoded.length + 1), true));

        byte[] negative = RegistryWorkerCodec.encode(new RegistryWorker("w", "host", 1, 2));
        ByteBuffer.wrap(negative).putInt(0, -2);
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(negative, false));

        byte[] malformed = RegistryWorkerCodec.encode(new RegistryWorker("aa", "host", 1, 2));
        malformed[4] = (byte) 0xc3;
        malformed[5] = 0x28;
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(malformed, false));

        byte[] badVersion = encoded.clone();
        int versionOffset = encoded.length - 128 - 4;
        ByteBuffer.wrap(badVersion).putInt(versionOffset, 99);
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(badVersion, true));
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(new byte[4097], false));
    }

    @Test
    void negotiatedCapabilityIsPersistedWithTheExactWorkerIncarnation() {
        ProtocolHelloV1 hello = hello();
        RegistryWorker worker = new RegistryWorker(
                "w-v1", "host", 1, 2, 3, "inc-v1", null, null, hello);

        byte[] encoded = RegistryWorkerCodec.encode(worker);
        RegistryWorker decoded = RegistryWorkerCodec.decode(encoded, false);
        assertEquals("inc-v1", decoded.incarnationId());
        assertEquals(hello, decoded.protocolHello());

        int frameStart = encoded.length
                - io.rowweave.cluster.rpc.ProtocolHelloV1Codec.encode(hello).length;
        for (int length = encoded.length - 3; length < encoded.length; length++) {
            int candidate = length;
            assertThrows(IllegalArgumentException.class,
                    () -> RegistryWorkerCodec.decode(
                            Arrays.copyOf(encoded, candidate), false));
        }
        byte[] corrupt = encoded.clone();
        corrupt[frameStart + 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> RegistryWorkerCodec.decode(corrupt, false));
    }

    private static ProtocolHelloV1 hello() {
        return new ProtocolHelloV1(SET_A, 1,
                new ProtocolRevisionRange(0, 1),
                new ProtocolRevisionRange(0, 1),
                Collections.singleton(ControlMessageId.wireId(
                        RpcMessage.Type.REGISTER_WORKER)),
                Collections.singleton(3), Collections.singleton(2),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton("runtime.batch"),
                Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap(), 1024, 4096L);
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
