package io.rowweave.portable;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PortableConnectorProviderContractTest {

    @Test
    void sourceAndSinkContractsHaveCanonicalStableFrames() {
        PortableConnectorProviderContract source =
                new PortableConnectorProviderContract(
                        1, PortableOperatorCode.BindingKind.SOURCE,
                        "jdbc-v1", "rowweave.opaque-bytes.v1",
                        4096, 8192, 64);
        PortableConnectorProviderContract sink =
                new PortableConnectorProviderContract(
                        1, PortableOperatorCode.BindingKind.SINK,
                        "manifest-file-v1", "rowweave.opaque-bytes.v1",
                        4096, 8192, 1);

        assertRoundTrip(source);
        assertRoundTrip(sink);
        assertEquals(64,
                PortableConnectorProviderContractCodec.digest(source).length());
    }

    @Test
    void kindLimitsAndChecksumFailClosed() {
        assertCode("INVALID_CONNECTOR_CONTRACT", () ->
                new PortableConnectorProviderContract(
                        1, PortableOperatorCode.BindingKind.FUNCTION,
                        "bad-v1", "rowweave.opaque-bytes.v1", 1, 1, 1));
        assertCode("LIMIT_EXCEEDED", () ->
                new PortableConnectorProviderContract(
                        1, PortableOperatorCode.BindingKind.SOURCE,
                        "bad-v1", "rowweave.opaque-bytes.v1",
                        PortableBindingLimits.MAX_CONFIGURATION_BYTES + 1,
                        1, 1));

        byte[] corrupt = PortableConnectorProviderContractCodec.encode(
                new PortableConnectorProviderContract(
                        1, PortableOperatorCode.BindingKind.SINK,
                        "sink-v1", "rowweave.opaque-bytes.v1", 8, 8, 1));
        corrupt[corrupt.length - 1] ^= 1;
        assertCode("CONNECTOR_CONTRACT_CHECKSUM_MISMATCH",
                () -> PortableConnectorProviderContractCodec.decode(corrupt));
    }

    private static void assertRoundTrip(
            PortableConnectorProviderContract contract) {
        byte[] encoded = PortableConnectorProviderContractCodec.encode(contract);
        PortableConnectorProviderContract decoded =
                PortableConnectorProviderContractCodec.decode(encoded);
        assertEquals(contract, decoded);
        assertArrayEquals(encoded,
                PortableConnectorProviderContractCodec.encode(decoded));
    }

    private static void assertCode(String code, Runnable action) {
        PortablePlanException failure = assertThrows(
                PortablePlanException.class, action::run);
        assertEquals(code, failure.code());
    }
}
