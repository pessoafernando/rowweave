package io.rowweave;

import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** canonical, bounded and non-self-authorizing Sink capability. */
class BatchSinkDescriptorCodecTest {

    private static final DataSchema META = new DataSchema(
            new Field("id", "identifier", DataType.LongType));
    private static final String SCHEMA = SchemaFingerprint.of(META);

    @Test
    void allProfilesRoundTripWithOneCanonicalIdentity() throws Exception {
        for (BatchSinkDescriptor.CapabilityProfile profile
                : BatchSinkDescriptor.CapabilityProfile.values()) {
            BatchSinkDescriptor descriptor = descriptor(profile);
            byte[] first = BatchSinkDescriptorCodec.encode(descriptor);
            byte[] second = BatchSinkDescriptorCodec.encode(descriptor(profile));
            assertArrayEquals(first, second);
            assertEquals(descriptor, BatchSinkDescriptorCodec.decode(first));
            assertEquals(32, BatchSinkDescriptorCodec.digest(descriptor).length);
            assertTrue(first.length <= BatchSinkDescriptorCodec.MAX_FRAME_BYTES);

            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
                out.writeObject(descriptor);
            }
            try (ObjectInputStream in = new ObjectInputStream(
                    new ByteArrayInputStream(bytes.toByteArray()))) {
                assertEquals(descriptor, in.readObject());
            }
        }
        assertTrue(Sink.class.isAssignableFrom(BatchSink.class));
    }

    @Test
    void constructorAndCodecRejectInvalidOrNonCanonicalFields() {
        assertInvalid(() -> new BatchSinkDescriptor(
                0, "rowweave.manifest-file", 1, SCHEMA, "dest", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, "", 1, SCHEMA, "dest", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, repeat("\u754C", 43), 1, SCHEMA, "dest", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 0, SCHEMA, "dest", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SCHEMA.toUpperCase(), "dest", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SCHEMA, "", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SCHEMA, "dest", "",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SCHEMA, repeat("\u754C", 342), "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SCHEMA, "dest", repeat("\u754C", 171),
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        assertInvalid(() -> new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 1, SCHEMA, "dest", "domain", null));

        byte[] valid = BatchSinkDescriptorCodec.encode(descriptor(
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
        byte[] corrupt = valid.clone();
        corrupt[20] ^= 1;
        assertDecodeFails(corrupt, "checksum");
        assertDecodeFails(Arrays.copyOf(valid, valid.length - 1), "length");
        assertDecodeFails(null, "length");
        assertDecodeFails(new byte[BatchSinkDescriptorCodec.MAX_FRAME_BYTES + 1], "length");
    }

    @Test
    void builtInsExposeExactProfilesWithoutLeakingDestinationInToString() {
        DiscardSink discard = new DiscardSink(META);
        assertTrue(discard instanceof BatchSink);
        BatchSinkDescriptor discardDescriptor =
                ((BatchSink) discard).batchSinkDescriptor();
        assertEquals(BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1,
                discardDescriptor.capabilityProfile());
        assertEquals(SCHEMA, discardDescriptor.schemaFingerprint());

        ManifestFileSink manifest = new ManifestFileSink(
                META, "target/bk1-manifest", "ledger");
        assertTrue(manifest instanceof BatchSink);
        BatchSinkDescriptor manifestDescriptor =
                ((BatchSink) manifest).batchSinkDescriptor();
        assertEquals(BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                manifestDescriptor.capabilityProfile());
        assertEquals(manifest.domainId(), manifestDescriptor.commitDomainId());
        assertEquals(manifest.exclusiveDestinationId(), manifestDescriptor.destinationId());
        assertFalse(manifestDescriptor.toString().contains(manifest.domainPath()));
        assertFalse(manifestDescriptor.toString().contains(manifestDescriptor.destinationId()));
    }

    @Test
    void oneFieldChangeChangesDescriptorDigest() {
        BatchSinkDescriptor left = descriptor(
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        BatchSinkDescriptor right = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 2, SCHEMA,
                "manifest-domain:member:ledger", "manifest-domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        assertFalse(MessageDigest.isEqual(
                BatchSinkDescriptorCodec.digest(left),
                BatchSinkDescriptorCodec.digest(right)));
    }

    private static BatchSinkDescriptor descriptor(
            BatchSinkDescriptor.CapabilityProfile profile) {
        return new BatchSinkDescriptor(
                1, profile == BatchSinkDescriptor.CapabilityProfile.LEGACY_PROCESS_LOCAL
                        ? "legacy-output" : "rowweave.manifest-file",
                1, SCHEMA, "manifest-domain:member:ledger", "manifest-domain", profile);
    }

    private static void assertInvalid(Runnable action) {
        assertThrows(IllegalArgumentException.class, action::run);
    }

    private static void assertDecodeFails(byte[] frame, String message) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> BatchSinkDescriptorCodec.decode(frame));
        assertTrue(failure.getMessage().contains(message), failure::getMessage);
    }

    private static String repeat(String value, int count) {
        StringBuilder result = new StringBuilder(value.length() * count);
        for (int i = 0; i < count; i++) result.append(value);
        return result.toString();
    }
}
