package io.rowweave.portable;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PortableBindingTest {

    private static final String DIGEST_A = repeat('a', 64);
    private static final String DIGEST_B = repeat('b', 64);
    private static final String CONFIG_DIGEST =
            "73907589101a7e8ab83178e7db2997aab7272cd02d364e8e3ecc2beccda4b631";

    @Test
    void sourceAndSinkBindingsAreTypedBoundedAndDefensivelyCopied() {
        byte[] sourceConfig = {1, 2};
        byte[] split = {3, 4};
        PortableSourceBinding source = new PortableSourceBinding(
                "jdbc-v1", DIGEST_A, sourceConfig,
                Collections.singletonList(split));
        sourceConfig[0] = 9;
        split[0] = 9;

        assertEquals(PortableOperatorCode.BindingKind.SOURCE, source.kind());
        assertArrayEquals(new byte[]{1, 2}, source.configurationFrame());
        assertArrayEquals(new byte[]{3, 4}, source.splitFrames().get(0));
        byte[] returned = source.splitFrames().get(0);
        returned[0] = 8;
        assertArrayEquals(new byte[]{3, 4}, source.splitFrames().get(0));

        PortableSinkBinding sink = new PortableSinkBinding(
                "manifest-file-v1", DIGEST_B,
                new byte[]{5}, new byte[]{6, 7});
        assertEquals(PortableOperatorCode.BindingKind.SINK, sink.kind());
        assertArrayEquals(new byte[]{5}, sink.descriptorFrame());
        assertArrayEquals(new byte[]{6, 7}, sink.configurationFrame());
    }

    @Test
    void functionReferenceBindsSignedDeploymentAbiConfigAndResources() {
        FunctionResourceProfile profile = new FunctionResourceProfile(
                64L * 1024 * 1024, 16L * 1024 * 1024, 1024);
        byte[] config = {8, 9};
        FunctionDeploymentReference reference = new FunctionDeploymentReference(
                DIGEST_A, "domain-functions", DIGEST_B,
                "example.functions.NormalizeAmount",
                FunctionAbi.MAP_V1, "json-v1", CONFIG_DIGEST, config, profile);
        config[0] = 0;

        assertEquals(PortableOperatorCode.BindingKind.FUNCTION, reference.kind());
        assertEquals(2, FunctionAbi.MAP_V1.wireId());
        assertEquals(FunctionAbi.MAP_V1, FunctionAbi.fromWireId(2));
        assertArrayEquals(new byte[]{8, 9}, reference.configurationFrame());
        assertEquals(profile, reference.resourceProfile());
        assertNotEquals(reference, new FunctionDeploymentReference(
                DIGEST_A, "domain-functions", DIGEST_B,
                "example.functions.NormalizeAmount",
                FunctionAbi.FILTER_V1, "json-v1", CONFIG_DIGEST,
                new byte[]{8, 9}, profile));
    }

    @Test
    void malformedIdentityDigestAndFramesFailClosedWithoutPayloadEcho() {
        assertCode("INVALID_BINDING", () -> new PortableSourceBinding(
                "bad key", DIGEST_A, new byte[]{1},
                Collections.singletonList(new byte[]{2})));
        assertCode("INVALID_DIGEST", () -> new PortableSinkBinding(
                "sink-v1", "not-a-digest", new byte[]{1}, new byte[]{2}));
        assertCode("LIMIT_EXCEEDED", () -> new PortableSinkBinding(
                "sink-v1", DIGEST_A, new byte[0], new byte[]{2}));
        assertCode("UNKNOWN_FUNCTION_ABI", () -> FunctionAbi.fromWireId(0));
        assertCode("CONFIG_DIGEST_MISMATCH",
                () -> new FunctionDeploymentReference(
                        DIGEST_A, "domain-functions", DIGEST_B,
                        "example.functions.NormalizeAmount",
                        FunctionAbi.MAP_V1, "json-v1", DIGEST_A,
                        new byte[]{8, 9},
                        new FunctionResourceProfile(1, 0, 1)));

        byte[] secret = new byte[PortableBindingLimits.MAX_CONFIGURATION_BYTES + 1];
        Arrays.fill(secret, (byte) 's');
        PortablePlanException failure = assertThrows(
                PortablePlanException.class,
                () -> new PortableSourceBinding(
                        "source-v1", DIGEST_A, secret,
                        Collections.singletonList(new byte[]{1})));
        assertEquals("LIMIT_EXCEEDED", failure.code());
        assertFalse(failure.getMessage().contains("ssss"));
    }

    @Test
    void functionResourceProfileRejectsUnboundedClaims() {
        assertCode("INVALID_RESOURCE_PROFILE",
                () -> new FunctionResourceProfile(0, 1, 1));
        assertCode("LIMIT_EXCEEDED",
                () -> new FunctionResourceProfile(
                        FunctionResourceProfile.MAX_HEAP_BYTES + 1,
                        1, 1));
    }

    private static void assertCode(String code, Runnable action) {
        PortablePlanException failure = assertThrows(
                PortablePlanException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
