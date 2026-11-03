package io.rowweave.cluster.rpc;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolHelloConfigTest {

    @Test
    void absentConfigurationKeepsTheLegacyTransportExplicit() {
        assertNull(ProtocolHelloConfig.fromProperties(new Properties()));
    }

    @Test
    void canonicalHelloFrameIsFrozenFromTheStartupSnapshot() {
        ProtocolHelloV1 expected = hello();
        Properties properties = new Properties();
        properties.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY,
                hex(ProtocolHelloV1Codec.encode(expected)));

        assertEquals(expected, ProtocolHelloConfig.fromProperties(properties));
        properties.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY, "00");
        assertEquals(expected, ProtocolHelloConfig.fromProperties(copyWithExpected(expected)));
    }

    @Test
    void malformedOrOversizedConfigurationFailsClosed() {
        Properties malformed = new Properties();
        malformed.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY, "0g");
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolHelloConfig.fromProperties(malformed));

        Properties oversized = new Properties();
        char[] hex = new char[(ProtocolHelloV1Codec.MAX_FRAME_BYTES + 1) * 2];
        Arrays.fill(hex, '0');
        oversized.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY,
                new String(hex));
        assertThrows(IllegalArgumentException.class,
                () -> ProtocolHelloConfig.fromProperties(oversized));
    }

    private static Properties copyWithExpected(ProtocolHelloV1 expected) {
        Properties copy = new Properties();
        copy.setProperty(ProtocolHelloConfig.HELLO_HEX_PROPERTY,
                hex(ProtocolHelloV1Codec.encode(expected)));
        return copy;
    }

    private static ProtocolHelloV1 hello() {
        return new ProtocolHelloV1(repeat('a', 64), 1,
                new ProtocolRevisionRange(0, 1),
                new ProtocolRevisionRange(0, 1),
                Collections.singleton(ControlMessageId.wireId(
                        RpcMessage.Type.ACK)),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton(1), Collections.singleton(1),
                Collections.singleton("rowweave.runtime.batch.v1"),
                Collections.<Integer>emptySet(),
                Collections.<Integer, byte[]>emptyMap(),
                1024 * 1024, 8L * 1024 * 1024);
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
