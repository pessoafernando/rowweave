package io.rowweave.run;

import io.rowweave.BatchSinkDescriptor;
import io.rowweave.DataType;
import io.rowweave.DurableSinkCodecDescriptor;
import io.rowweave.DurableSinkProviderKey;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.commit.SchemaFingerprint;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Sink attempt identity and prepared locator are canonical authority data. */
class SinkAttemptIdentityCodecTest {

    private static final DataSchema META = new DataSchema(new Field("id", DataType.LongType));
    private static final BatchSinkDescriptor DESCRIPTOR = new BatchSinkDescriptor(
            1, "rowweave.manifest-file", 1, SchemaFingerprint.of(META),
            "domain:member:ledger", "domain",
            BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);

    @Test
    void identityBindsArtifactFenceAssignmentOperatorAndDescriptor() {
        Assignment fixture = assignment("artifact-a", "run-a", "attempt-a", 7L, 11L, 0);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "ledger-sink", DESCRIPTOR);
        SinkAttemptIdentity same = SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "ledger-sink", DESCRIPTOR);

        assertEquals(identity, same);
        assertArrayEquals(identity.identityDigest(), same.identityDigest());
        assertEquals(32, identity.descriptorDigest().length);
        assertEquals(32, identity.identityDigest().length);
        assertEquals("ledger-sink", identity.sinkOperatorId());

        assertDigestDiffers(identity, identity(
                assignment("artifact-b", "run-a", "attempt-a", 7L, 11L, 0)));
        assertDigestDiffers(identity, identity(
                assignment("artifact-a", "run-a", "attempt-b", 8L, 11L, 0)));
        assertDigestDiffers(identity, identity(
                assignment("artifact-a", "run-a", "attempt-a", 7L, 12L, 0)));
        assertNotEquals(identity, SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "other-sink", DESCRIPTOR));

        BatchSinkDescriptor changedDescriptor = new BatchSinkDescriptor(
                1, "rowweave.manifest-file", 2, SchemaFingerprint.of(META),
                "domain:member:ledger", "domain",
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        assertNotEquals(identity, SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "ledger-sink", changedDescriptor));
    }

    @Test
    void identityCodecIsCanonicalBoundedAndRejectsCorruption() {
        Assignment fixture = assignment("artifact-a", "run-a", "attempt-a", 7L, 11L, 0);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "ledger-sink", DESCRIPTOR);
        byte[] first = SinkAttemptIdentityCodec.encode(identity);
        byte[] second = SinkAttemptIdentityCodec.encode(identity);
        assertArrayEquals(first, second);
        assertEquals(identity, SinkAttemptIdentityCodec.decode(first));
        assertTrue(first.length <= SinkAttemptIdentityCodec.MAX_FRAME_BYTES);

        byte[] corrupt = first.clone();
        corrupt[20] ^= 1;
        assertDecodeFails(corrupt, "checksum");
        assertDecodeFails(Arrays.copyOf(first, first.length - 1), "length");
        assertDecodeFails(new byte[SinkAttemptIdentityCodec.MAX_FRAME_BYTES + 1], "length");

        assertThrows(IllegalArgumentException.class,
                () -> SinkAttemptIdentity.forAssignment(
                        sourceAssignment(), sourceAssignment().assignment(0),
                        "not-a-sink", DESCRIPTOR));
    }

    @Test
    void preparedLocatorRoundTripsDefensivelyAndNeverPrintsPayload() {
        Assignment fixture = assignment("artifact-a", "run-a", "attempt-a", 7L, 11L, 0);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "ledger-sink", DESCRIPTOR);
        byte[] payload = new byte[]{1, 2, 3, 4};
        byte[] dataHash = sha256(new byte[]{9, 8, 7});
        PreparedSinkLocator locator = new PreparedSinkLocator(
                1, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", payload, 3L, dataHash, 1234L);

        byte[] encoded = PreparedSinkLocatorCodec.encode(locator);
        assertEquals(locator, PreparedSinkLocatorCodec.decode(encoded));
        assertTrue(encoded.length <= PreparedSinkLocatorCodec.MAX_FRAME_BYTES);
        assertFalse(locator.toString().contains("1, 2, 3, 4"));
        assertFalse(locator.toString().contains("domain"));

        payload[0] = 99;
        dataHash[0] ^= 1;
        assertArrayEquals(new byte[]{1, 2, 3, 4}, locator.payload());
        assertArrayEquals(sha256(new byte[]{9, 8, 7}), locator.dataHash());
        byte[] returned = locator.payload();
        returned[0] = 88;
        assertEquals(1, locator.payload()[0]);

        byte[] corrupt = encoded.clone();
        corrupt[25] ^= 1;
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> PreparedSinkLocatorCodec.decode(corrupt));
        assertTrue(failure.getMessage().contains("checksum"), failure::getMessage);
    }

    @Test
    void preparedLocatorRejectsUnboundedOrContradictoryClaims() {
        Assignment fixture = assignment("artifact-a", "run-a", "attempt-a", 7L, 11L, 0);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "ledger-sink", DESCRIPTOR);
        byte[] hash = sha256(new byte[0]);

        assertThrows(IllegalArgumentException.class, () -> new PreparedSinkLocator(
                1, identity, BatchSinkDescriptor.CapabilityProfile.SIDE_EFFECT_FREE_V1,
                "domain", new byte[]{1}, 0L, hash, 0L));
        assertThrows(IllegalArgumentException.class, () -> new PreparedSinkLocator(
                1, identity, BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "other-domain", new byte[]{1}, 0L, hash, 0L));
        assertThrows(IllegalArgumentException.class, () -> new PreparedSinkLocator(
                1, identity, BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", new byte[PreparedSinkLocator.MAX_PAYLOAD_BYTES + 1],
                0L, hash, 0L));
        assertThrows(IllegalArgumentException.class, () -> new PreparedSinkLocator(
                1, identity, BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", new byte[]{1}, -1L, hash, 0L));
    }

    @Test
    void preparedLocatorV2BindsProviderCodecAndCanonicalPayloadDigest() throws Exception {
        assertEquals(2, PreparedSinkLocator.CURRENT_VERSION);
        Assignment fixture = assignment("artifact-a", "run-a", "attempt-a", 7L, 11L, 0);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "ledger-sink", DESCRIPTOR);
        DurableSinkProviderKey providerKey = new DurableSinkProviderKey(
                "test.adversarial", 1,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1);
        DurableSinkCodecDescriptor locatorCodec = new DurableSinkCodecDescriptor(
                "test.locator", 3, PreparedSinkLocator.MAX_PAYLOAD_BYTES);
        byte[] payload = new byte[]{7, 4, 2};
        byte[] payloadDigest = sha256(payload);

        java.lang.reflect.Constructor<PreparedSinkLocator> constructor =
                PreparedSinkLocator.class.getConstructor(
                        int.class, SinkAttemptIdentity.class,
                        BatchSinkDescriptor.CapabilityProfile.class, String.class,
                        DurableSinkProviderKey.class, DurableSinkCodecDescriptor.class,
                        byte[].class, byte[].class, long.class, byte[].class, long.class);
        PreparedSinkLocator locator = constructor.newInstance(
                2, identity,
                BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                "domain", providerKey, locatorCodec, payload, payloadDigest,
                3L, sha256(new byte[]{9, 8, 7}), 1234L);

        assertEquals(providerKey,
                PreparedSinkLocator.class.getMethod("providerKey").invoke(locator));
        assertEquals(locatorCodec,
                PreparedSinkLocator.class.getMethod("locatorCodec").invoke(locator));
        assertArrayEquals(payloadDigest, (byte[]) PreparedSinkLocator.class
                .getMethod("payloadDigest").invoke(locator));
        byte[] encoded = PreparedSinkLocatorCodec.encode(locator);
        assertEquals(locator, PreparedSinkLocatorCodec.decode(encoded));
        assertArrayEquals(encoded,
                PreparedSinkLocatorCodec.encode(PreparedSinkLocatorCodec.decode(encoded)));

        java.lang.reflect.InvocationTargetException invalidDigest = assertThrows(
                java.lang.reflect.InvocationTargetException.class,
                () -> constructor.newInstance(
                        2, identity,
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1,
                        "domain", providerKey, locatorCodec, payload, sha256(new byte[]{1}),
                        3L, sha256(new byte[]{9, 8, 7}), 1234L));
        assertTrue(invalidDigest.getCause() instanceof IllegalArgumentException);
    }

    @Test
    void preparedLocatorV1ReaderMapsOnlyToBuiltInManifestAndPreservesBytes() throws Exception {
        Assignment fixture = assignment("artifact-a", "run-a", "attempt-a", 7L, 11L, 0);
        SinkAttemptIdentity identity = SinkAttemptIdentity.forAssignment(
                fixture.manifest, fixture.record, "ledger-sink", DESCRIPTOR);
        byte[] legacy = legacyV1Locator(identity, new byte[]{1, 2, 3},
                sha256(new byte[]{5, 6}), 1234L);

        PreparedSinkLocator decoded = PreparedSinkLocatorCodec.decode(legacy);
        assertEquals(1, decoded.locatorVersion());
        assertEquals(new DurableSinkProviderKey(
                        "rowweave.manifest-file", 1,
                        BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1),
                PreparedSinkLocator.class.getMethod("providerKey").invoke(decoded));
        DurableSinkCodecDescriptor codec = (DurableSinkCodecDescriptor)
                PreparedSinkLocator.class.getMethod("locatorCodec").invoke(decoded);
        assertEquals("rowweave.manifest-member", codec.codecId());
        assertEquals(1, codec.codecVersion());
        assertArrayEquals(sha256(new byte[]{1, 2, 3}), (byte[])
                PreparedSinkLocator.class.getMethod("payloadDigest").invoke(decoded));
        assertArrayEquals(legacy, PreparedSinkLocatorCodec.encode(decoded));
    }

    private static byte[] legacyV1Locator(
            SinkAttemptIdentity identity, byte[] payload,
            byte[] dataHash, long preparedAtMillis) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(0x5257504c);
            out.writeInt(1);
            out.writeInt(1);
            SinkAttemptIdentityCodec.writeBytes(
                    out, SinkAttemptIdentityCodec.encode(identity));
            out.writeByte(SinkAttemptIdentityCodec.profileId(
                    BatchSinkDescriptor.CapabilityProfile.MANIFEST_ATOMIC_GROUP_V1));
            RunWire.writeText(out, "domain");
            SinkAttemptIdentityCodec.writeBytes(out, payload);
            out.writeLong(2L);
            out.write(dataHash);
            out.writeLong(preparedAtMillis);
        }
        byte[] body = bytes.toByteArray();
        byte[] checksum = SinkAttemptIdentityCodec.sha256(body, 0, body.length);
        byte[] frame = Arrays.copyOf(body, body.length + checksum.length);
        System.arraycopy(checksum, 0, frame, body.length, checksum.length);
        return frame;
    }

    private static Assignment assignment(
            String artifactId, String runId, String attemptId,
            long fenceToken, long generation, int ordinal) {
        RunAttemptFence fence = new RunAttemptFence(runId, attemptId, fenceToken);
        StageAssignmentRecord record = StageAssignmentRecord.create(
                fence, generation, ordinal, 100L + ordinal, 2, 0,
                StageAssignmentRole.SINK, "worker", "127.0.0.1",
                9001, 9002, "incarnation");
        StageAssignmentManifest manifest = new StageAssignmentManifest(
                fence, artifactId, generation, Collections.singletonList(record));
        return new Assignment(manifest, record);
    }

    private static StageAssignmentManifest sourceAssignment() {
        RunAttemptFence fence = new RunAttemptFence("run", "attempt", 1L);
        StageAssignmentRecord source = StageAssignmentRecord.create(
                fence, 1L, 0, 101L, 0, 0, StageAssignmentRole.SOURCE_STAGE,
                "worker", "127.0.0.1", 9001, 9002, "incarnation");
        return new StageAssignmentManifest(
                fence, "artifact", 1L, Collections.singletonList(source));
    }

    private static void assertDigestDiffers(
            SinkAttemptIdentity expected, Assignment changed) {
        assertDigestDiffers(expected, identity(changed));
    }

    private static SinkAttemptIdentity identity(Assignment assignment) {
        return SinkAttemptIdentity.forAssignment(
                assignment.manifest, assignment.record, "ledger-sink", DESCRIPTOR);
    }

    private static void assertDigestDiffers(
            SinkAttemptIdentity expected, SinkAttemptIdentity changed) {
        assertFalse(MessageDigest.isEqual(
                expected.identityDigest(), changed.identityDigest()));
    }

    private static void assertDecodeFails(byte[] bytes, String message) {
        IllegalArgumentException failure = assertThrows(
                IllegalArgumentException.class,
                () -> SinkAttemptIdentityCodec.decode(bytes));
        assertTrue(failure.getMessage().contains(message), failure::getMessage);
    }

    private static byte[] sha256(byte[] value) {
        try {
            return MessageDigest.getInstance("SHA-256").digest(value);
        } catch (Exception impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static final class Assignment {
        final StageAssignmentManifest manifest;
        final StageAssignmentRecord record;
        Assignment(StageAssignmentManifest manifest, StageAssignmentRecord record) {
            this.manifest = manifest;
            this.record = record;
        }
    }
}
