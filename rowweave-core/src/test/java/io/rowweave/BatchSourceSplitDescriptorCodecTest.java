package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical and bounded batch source split identity. */
class BatchSourceSplitDescriptorCodecTest {

    private static final int MAGIC = 0x52575344; // RWSD
    private static final DataSchema META = new DataSchema(
            new Field("id", "identifier", DataType.LongType),
            new Field("amount", DataType.BigDecimalType));
    private static final String SCHEMA = SchemaFingerprint.of(META);

    @Test
    void exactAndLegacyDescriptorsRoundTripCanonically() throws Exception {
        BatchSourceSplitDescriptor exact = descriptor(
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START);
        BatchSourceSplitDescriptor legacy = new BatchSourceSplitDescriptor(
                1, "legacy-input", 1, "legacy:0", SCHEMA,
                "artifact-position:0",
                BatchSourceSplitDescriptor.ReplayGuarantee.LEGACY_SINGLE_ATTEMPT);

        byte[] first = BatchSourceSplitDescriptorCodec.encode(exact);
        byte[] second = BatchSourceSplitDescriptorCodec.encode(descriptor(
                BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START));
        assertArrayEquals(first, second, "same logical descriptor must have one wire form");
        assertEquals(exact, BatchSourceSplitDescriptorCodec.decode(first));
        assertEquals(legacy, BatchSourceSplitDescriptorCodec.decode(
                BatchSourceSplitDescriptorCodec.encode(legacy)));
        assertTrue(first.length <= BatchSourceSplitDescriptorCodec.MAX_FRAME_BYTES);

        byte[] digest = BatchSourceSplitDescriptorCodec.digest(exact);
        assertEquals(32, digest.length);
        digest[0] ^= 1;
        assertFalse(Arrays.equals(digest, BatchSourceSplitDescriptorCodec.digest(exact)),
                "digest getter must be defensive");
        assertNotEquals(exact, legacy);

        ByteArrayOutputStream serialized = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(serialized)) {
            out.writeObject(exact);
        }
        try (ObjectInputStream in = new ObjectInputStream(
                new ByteArrayInputStream(serialized.toByteArray()))) {
            assertEquals(exact, in.readObject());
        }
        assertTrue(Source.class.isAssignableFrom(BatchSource.class));
    }

    @Test
    void constructorRejectsInvalidFieldsByUtf8Bytes() {
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                0, "manifest-file", 1, "ledger-0", SCHEMA, "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                2, "manifest-file", 1, "ledger-0", SCHEMA, "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "", 1, "ledger-0", SCHEMA, "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, repeat("\u754C", 43), 1, "ledger-0", SCHEMA, "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "manifest-file", 0, "ledger-0", SCHEMA, "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "", SCHEMA, "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, repeat("\u754C", 86), SCHEMA, "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "ledger-0", SCHEMA.toUpperCase(), "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "ledger-0", SCHEMA.substring(1), "domain:1:ledger", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "ledger-0", SCHEMA, "", exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "ledger-0", SCHEMA, repeat("\u754C", 342), exact()));
        assertInvalid(() -> new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "ledger-0", SCHEMA, "domain:1:ledger", null));
    }

    @Test
    void codecRejectsChecksumVersionEnumUtf8AndBoundaryAttacks() throws Exception {
        byte[] valid = BatchSourceSplitDescriptorCodec.encode(descriptor(exact()));
        byte[] corrupt = valid.clone();
        corrupt[20] ^= 1;
        assertDecodeFails(corrupt, "checksum");
        assertDecodeFails(Arrays.copyOf(valid, valid.length - 1), "length");
        byte[] trailing = Arrays.copyOf(valid, valid.length + 1);
        assertDecodeFails(trailing, "checksum");
        assertDecodeFails(null, "length");
        assertDecodeFails(new byte[BatchSourceSplitDescriptorCodec.MAX_FRAME_BYTES + 1], "length");

        assertDecodeFails(frame(body(0x01020304, 1, 1,
                bytes("manifest-file"), 1, bytes("ledger-0"), bytes(SCHEMA),
                bytes("domain:1:ledger"), 1)), "magic");
        assertDecodeFails(frame(body(MAGIC, 2, 1,
                bytes("manifest-file"), 1, bytes("ledger-0"), bytes(SCHEMA),
                bytes("domain:1:ledger"), 1)), "codec version");
        assertDecodeFails(frame(body(MAGIC, 1, 2,
                bytes("manifest-file"), 1, bytes("ledger-0"), bytes(SCHEMA),
                bytes("domain:1:ledger"), 1)), "descriptorVersion");
        assertDecodeFails(frame(body(MAGIC, 1, 1,
                bytes("manifest-file"), 1, bytes("ledger-0"), bytes(SCHEMA),
                bytes("domain:1:ledger"), 99)), "replayGuarantee");
        assertDecodeFails(frame(body(MAGIC, 1, 1,
                new byte[]{(byte) 0xc3, 0x28}, 1, bytes("ledger-0"), bytes(SCHEMA),
                bytes("domain:1:ledger"), 1)), "UTF-8");

        ByteArrayOutputStream hostile = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(hostile)) {
            out.writeInt(MAGIC);
            out.writeInt(1);
            out.writeInt(1);
            out.writeInt(Integer.MAX_VALUE);
        }
        assertDecodeFails(frame(hostile.toByteArray()), "length");
    }

    @Test
    void oneByteChangesProduceDifferentIdentityDigest() {
        BatchSourceSplitDescriptor left = descriptor(exact());
        BatchSourceSplitDescriptor right = new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "ledger-1", SCHEMA,
                "domain:7:ledger", exact());
        assertFalse(MessageDigest.isEqual(
                BatchSourceSplitDescriptorCodec.digest(left),
                BatchSourceSplitDescriptorCodec.digest(right)));
    }

    private static BatchSourceSplitDescriptor descriptor(
            BatchSourceSplitDescriptor.ReplayGuarantee guarantee) {
        return new BatchSourceSplitDescriptor(
                1, "manifest-file", 1, "ledger-0", SCHEMA,
                "domain:7:ledger", guarantee);
    }

    private static BatchSourceSplitDescriptor.ReplayGuarantee exact() {
        return BatchSourceSplitDescriptor.ReplayGuarantee.EXACT_ORDERED_FROM_START;
    }

    private static byte[] body(int magic, int codecVersion, int descriptorVersion,
                               byte[] connectorType, int connectorVersion,
                               byte[] splitId, byte[] schema, byte[] snapshot,
                               int replay) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(magic);
            out.writeInt(codecVersion);
            out.writeInt(descriptorVersion);
            writeBytes(out, connectorType);
            out.writeInt(connectorVersion);
            writeBytes(out, splitId);
            writeBytes(out, schema);
            writeBytes(out, snapshot);
            out.writeByte(replay);
        }
        return bytes.toByteArray();
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws Exception {
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] frame(byte[] body) throws Exception {
        byte[] digest = MessageDigest.getInstance("SHA-256").digest(body);
        byte[] frame = Arrays.copyOf(body, body.length + digest.length);
        System.arraycopy(digest, 0, frame, body.length, digest.length);
        return frame;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String repeat(String value, int count) {
        StringBuilder result = new StringBuilder(value.length() * count);
        for (int i = 0; i < count; i++) result.append(value);
        return result.toString();
    }

    private static void assertInvalid(Runnable action) {
        assertThrows(IllegalArgumentException.class, action::run);
    }

    private static void assertDecodeFails(byte[] bytes, String messagePart) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> BatchSourceSplitDescriptorCodec.decode(bytes));
        assertTrue(failure.getMessage().contains(messagePart), failure::getMessage);
    }
}
