package io.rowweave.incremental;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** source deployment metadata is canonical, bounded and inert. */
class IncrementalSourceProviderContractTest {

    @Test
    void contractRoundTripIsCanonicalBoundedAndDigestStable() throws Exception {
        IncrementalSourceProviderContract contract = contract();
        byte[] encoded = IncrementalSourceProviderContractCodec.encode(contract);

        assertTrue(encoded.length
                <= IncrementalSourceProviderContractCodec.MAX_FRAME_BYTES);
        assertEquals(contract,
                IncrementalSourceProviderContractCodec.decode(encoded));
        assertArrayEquals(encoded, IncrementalSourceProviderContractCodec.encode(
                IncrementalSourceProviderContractCodec.decode(encoded)));
        assertArrayEquals(sha256(encoded),
                IncrementalSourceProviderContractCodec.digest(contract));

        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceProviderContractCodec.decode(corrupt));
        assertThrows(IllegalArgumentException.class,
                () -> IncrementalSourceProviderContractCodec.decode(
                        Arrays.copyOf(encoded, encoded.length + 1)));
    }

    @Test
    void contractClosesFrontierOrderingSnapshotAndResourceBounds() {
        IncrementalSourceProviderContract valid = contract();
        assertEquals(IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                valid.frontierKind());
        assertEquals(IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                valid.orderingGuarantee());
        assertEquals(IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                valid.snapshotGuarantee());

        assertThrows(IllegalArgumentException.class,
                () -> contract(IncrementalFrontier.Kind.BEGIN_V1,
                        valid.actionBudget()));
        assertThrows(IllegalArgumentException.class,
                () -> contract(IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                        new IncrementalSourceActionBudget(
                                1024L * 1024L + 1L, 1L, 1, 1L)));
        assertThrows(IllegalArgumentException.class,
                () -> contract(IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                        new IncrementalSourceActionBudget(
                                1L, 24L * 1024L * 1024L + 1L, 1, 1L)));
        assertThrows(IllegalArgumentException.class,
                () -> contract(IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                        new IncrementalSourceActionBudget(1L, 1L, 2, 1L)));
        assertThrows(IllegalArgumentException.class,
                () -> contract(IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                        new IncrementalSourceActionBudget(
                                1L, 1L, 1, 60_001L)));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceCodecDescriptor(
                        "capture", 1,
                        IncrementalSourceProviderContract
                                .MAX_CAPTURE_RESPONSE_BYTES + 1));
    }

    @Test
    void planConfigIsExactFinalBoundedDefensiveAndContainsNoProviderObject()
            throws Exception {
        IncrementalSourceProviderContract contract = contract();
        byte[] config = new byte[]{1, 2, 3};
        IncrementalSourceConfig carrier = new IncrementalSourceConfig(
                contract.providerKey(),
                IncrementalSourceProviderContractCodec.digest(contract), config);

        assertTrue(java.lang.reflect.Modifier.isFinal(
                IncrementalSourceConfig.class.getModifiers()));
        assertEquals(contract.providerKey(), carrier.providerKey());
        assertEquals(3, carrier.configurationSize());
        assertArrayEquals(config, carrier.configurationBytes());
        assertArrayEquals(
                IncrementalSourceProviderContractCodec.digest(contract),
                carrier.contractDigest());
        config[0] = 99;
        byte[] returned = carrier.configurationBytes();
        returned[1] = 99;
        assertArrayEquals(new byte[]{1, 2, 3}, carrier.configurationBytes());
        for (Field field : IncrementalSourceConfig.class.getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                continue;
            }
            assertTrue(field.getType() == IncrementalSourceProviderKey.class
                            || field.getType() == byte[].class
                            || field.getType() == int.class,
                    "unexpected executable/provider field: " + field);
        }

        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceConfig(
                        contract.providerKey(), new byte[31], new byte[0]));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceConfig(
                        contract.providerKey(), new byte[32],
                        new byte[IncrementalSourceProviderContract
                                .MAX_CONFIGURATION_BYTES + 1]));
    }

    @Test
    void sourceIdentityAndCodecTextAreStrictCanonicalUtf8() {
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceProviderKey("", 1));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceProviderKey("source\0type", 1));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceProviderKey("\ud800", 1));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceProviderKey("source", 0));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceCodecDescriptor("", 1, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceCodecDescriptor("codec", 0, 1));
        assertThrows(IllegalArgumentException.class,
                () -> new IncrementalSourceCodecDescriptor("codec", 1, 0));
    }

    private static IncrementalSourceProviderContract contract() {
        return contract(IncrementalFrontier.Kind.INT64_SEQUENCE_V1,
                new IncrementalSourceActionBudget(
                        1024L * 1024L, 24L * 1024L * 1024L,
                        1, 60_000L));
    }

    private static IncrementalSourceProviderContract contract(
            IncrementalFrontier.Kind frontierKind,
            IncrementalSourceActionBudget budget) {
        return new IncrementalSourceProviderContract(
                IncrementalSourceProviderContract.CURRENT_SPI_VERSION,
                new IncrementalSourceProviderKey("test.embedded", 1),
                new IncrementalSourceCodecDescriptor("test.config", 1,
                        IncrementalSourceProviderContract.MAX_CONFIGURATION_BYTES),
                new IncrementalSourceCodecDescriptor("test.capture", 1,
                        IncrementalSourceProviderContract
                                .MAX_CAPTURE_RESPONSE_BYTES),
                new IncrementalSourceCodecDescriptor("test.materialize", 1,
                        IncrementalSourceProviderContract
                                .MAX_MATERIALIZATION_RESPONSE_BYTES),
                IncrementalFrontierCodec.MAX_FRAME_BYTES,
                frontierKind,
                IncrementalSourceProviderContract.OrderingGuarantee
                        .TOTAL_ORDER_NO_LATE_LEFT_V1,
                IncrementalSourceProviderContract.SnapshotGuarantee
                        .EXACT_REOPENABLE_V1,
                true, budget,
                IncrementalSourceProviderContract.DeploymentCapability
                        .SIGNED_TEST_CONFORMANCE_V1);
    }

    private static byte[] sha256(byte[] value) throws Exception {
        return MessageDigest.getInstance("SHA-256").digest(value);
    }
}
