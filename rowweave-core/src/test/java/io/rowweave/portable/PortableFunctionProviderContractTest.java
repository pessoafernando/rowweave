package io.rowweave.portable;

import org.junit.jupiter.api.Test;

import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PortableFunctionProviderContractTest {

    @Test
    void contractCodecIsCanonicalChecksummedAndDigestable() {
        PortableFunctionProviderContract contract = contract();

        byte[] encoded = PortableFunctionProviderContractCodec.encode(contract);
        PortableFunctionProviderContract decoded =
                PortableFunctionProviderContractCodec.decode(encoded);

        assertEquals(contract, decoded);
        assertArrayEquals(encoded,
                PortableFunctionProviderContractCodec.encode(decoded));
        assertEquals(64,
                PortableFunctionProviderContractCodec.digest(contract).length());
        assertEquals("RWXXFPC1", new String(encoded, 0, 8,
                java.nio.charset.StandardCharsets.US_ASCII));
    }

    @Test
    void unknownVersionCorruptionAndUnboundedConfigurationFailClosed() {
        assertCode("UNSUPPORTED_FUNCTION_CONTRACT", () ->
                new PortableFunctionProviderContract(
                        2, "example.functions.NormalizeAmount",
                        FunctionAbi.MAP_V1, "json-v1", 1024,
                        new FunctionResourceProfile(1024, 0, 1)));
        assertCode("LIMIT_EXCEEDED", () ->
                new PortableFunctionProviderContract(
                        1, "example.functions.NormalizeAmount",
                        FunctionAbi.MAP_V1, "json-v1",
                        PortableBindingLimits.MAX_CONFIGURATION_BYTES + 1,
                        new FunctionResourceProfile(1024, 0, 1)));

        byte[] corrupted = PortableFunctionProviderContractCodec.encode(contract());
        corrupted[corrupted.length - 1] ^= 1;
        PortablePlanException failure = assertThrows(
                PortablePlanException.class,
                () -> PortableFunctionProviderContractCodec.decode(corrupted));
        assertEquals("FUNCTION_CONTRACT_CHECKSUM_MISMATCH", failure.code());
        assertTrue(failure.getMessage().indexOf("NormalizeAmount") < 0);
    }

    @Test
    void referenceMustFitTheExactStaticContract() throws Exception {
        PortableFunctionProviderContract contract = contract();
        byte[] config = {8, 9};
        String digest = hex(MessageDigest.getInstance("SHA-256").digest(config));
        FunctionDeploymentReference valid = new FunctionDeploymentReference(
                repeat('a', 64), "domain-functions", repeat('b', 64),
                contract.functionClassName(), FunctionAbi.MAP_V1, "json-v1",
                digest, config, new FunctionResourceProfile(512, 0, 1));

        contract.validateReference(valid);

        FunctionDeploymentReference wrongAbi = new FunctionDeploymentReference(
                repeat('a', 64), "domain-functions", repeat('b', 64),
                contract.functionClassName(), FunctionAbi.FILTER_V1, "json-v1",
                digest, config, new FunctionResourceProfile(512, 0, 1));
        assertCode("FUNCTION_CONTRACT_MISMATCH",
                () -> contract.validateReference(wrongAbi));
    }

    private static PortableFunctionProviderContract contract() {
        return new PortableFunctionProviderContract(
                1, "example.functions.NormalizeAmount", FunctionAbi.MAP_V1,
                "json-v1", 1024,
                new FunctionResourceProfile(2048, 256, 4));
    }

    private static void assertCode(String code, Runnable action) {
        PortablePlanException failure = assertThrows(
                PortablePlanException.class, action::run);
        assertEquals(code, failure.code());
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        java.util.Arrays.fill(chars, value);
        return new String(chars);
    }

    private static String hex(byte[] bytes) {
        StringBuilder value = new StringBuilder(bytes.length * 2);
        for (byte current : bytes) {
            value.append(Character.forDigit((current >>> 4) & 0xf, 16));
            value.append(Character.forDigit(current & 0xf, 16));
        }
        return value.toString();
    }
}
