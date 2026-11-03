package io.rowweave.stream.api;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamConnectorConfigurationTest {
    @Test
    void canonicalFrameCarriesOptionsAndSecretReferencesOnly() {
        Map<String, String> options = new LinkedHashMap<>();
        options.put("region", "eu-west-1");
        options.put("bucket", "orders");
        Map<String, String> references = new LinkedHashMap<>();
        references.put("password", "vault://production/postgres");
        StreamConnectorConfiguration configuration =
                StreamConnectorConfiguration.of(options, references);
        byte[] encoded = configuration.encode();
        assertArrayEquals("ALSCCFG1".getBytes(StandardCharsets.US_ASCII),
                Arrays.copyOf(encoded, 8));
        assertArrayEquals(encoded, StreamConnectorConfiguration.of(
                new java.util.TreeMap<>(options),
                new java.util.TreeMap<>(references)).encode());
        assertEquals("orders", StreamConnectorConfiguration.decode(encoded)
                .options().get("bucket"));
        assertEquals("vault://production/postgres",
                StreamConnectorConfiguration.decode(encoded)
                        .secretReferences().get("password"));
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorConfiguration.decode(
                        Arrays.copyOf(encoded, encoded.length + 1)));
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorConfiguration.decode(
                        Arrays.copyOf(encoded, encoded.length - 1)));
    }

    @Test
    void invalidNamesAndUnboundedValuesDoNotEnterPlan() {
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorConfiguration.of(
                        Collections.singletonMap("bad name", "x"),
                        Collections.emptyMap()));
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorConfiguration.of(
                        Collections.emptyMap(),
                        Collections.singletonMap("password", "")));
        assertThrows(IllegalArgumentException.class,
                () -> StreamConnectorConfiguration.of(
                        Collections.singletonMap("value", "x\u0000y"),
                        Collections.emptyMap()));
    }
}
