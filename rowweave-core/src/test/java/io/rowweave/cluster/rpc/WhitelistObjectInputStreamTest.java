package io.rowweave.cluster.rpc;

import example.extension.ExternalConnectorDescriptor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InvalidClassException;
import java.io.ObjectOutputStream;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class WhitelistObjectInputStreamTest {

    @Test
    void allowsExactlyTheSupportedSqlTimestampValueType() throws Exception {
        Timestamp expected = Timestamp.valueOf("2026-08-31 12:34:56.123456789");
        Object decoded = decode(encode(expected));
        assertEquals(expected, decoded);
        assertEquals(expected.getNanos(), ((Timestamp) decoded).getNanos());
    }

    @Test
    void stillRejectsUnrelatedSerializableJdkTypes() throws Exception {
        byte[] encoded = encode(new File("must-not-cross-rpc"));
        assertThrows(InvalidClassException.class, () -> decode(encoded));
    }

    @Test
    void serializableApplicationClassIsNotAClusterDeploymentContract() throws Exception {
        byte[] encoded = encode(new ExternalConnectorDescriptor("app://orders"));

        InvalidClassException failure = assertThrows(
                InvalidClassException.class, () -> decode(encoded));
        assertEquals("example.extension.ExternalConnectorDescriptor", failure.classname);
    }

    private static byte[] encode(Object value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
            out.writeObject(value);
        }
        return bytes.toByteArray();
    }

    private static Object decode(byte[] encoded) throws Exception {
        try (WhitelistObjectInputStream in = new WhitelistObjectInputStream(
                new ByteArrayInputStream(encoded))) {
            return in.readObject();
        }
    }
}
