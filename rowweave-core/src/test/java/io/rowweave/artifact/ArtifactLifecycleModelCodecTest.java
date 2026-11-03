package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.Normalizer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArtifactLifecycleModelCodecTest {

    private static final String ARTIFACT_A = repeat('a', 64);
    private static final String ARTIFACT_A2 = "aa" + repeat('c', 62);
    private static final String ARTIFACT_B = repeat('b', 64);

    @Test
    void ownerTokenInventoryAndReferenceRoundTripDeterministically() {
        ArtifactGcOwner owner = new ArtifactGcOwner("master-Ω", -7L);
        ArtifactGcToken token = ArtifactGcToken.create(
                ARTIFACT_A, owner, 3L, 11L);
        byte[] encodedToken = ArtifactLifecycleCodec.encodeGcToken(token);
        assertArrayEquals(encodedToken, ArtifactLifecycleCodec.encodeGcToken(token));
        ArtifactGcToken decodedToken =
                ArtifactLifecycleCodec.decodeGcToken(encodedToken);
        assertEquals(token, decodedToken);
        byte[] digest = decodedToken.tokenDigest();
        digest[0] ^= 1;
        assertFalse(Arrays.equals(digest, decodedToken.tokenDigest()));

        ArtifactInventoryEntry publishing = ArtifactInventoryEntry.publishing(
                ARTIFACT_A2, 101L, 1_000L, 9L);
        ArtifactInventoryEntry live = ArtifactInventoryEntry.live(
                ARTIFACT_B, 202L, 2_000L, 10L);
        ArtifactInventoryEntry deleting = ArtifactInventoryEntry.deleting(
                ARTIFACT_A, 101L, 1_000L, 11L, token);
        assertNull(publishing.gcToken());
        assertNull(live.gcToken());
        assertEquals(token, deleting.gcToken());

        ArtifactInventorySnapshot inventory = ArtifactInventorySnapshot.of(
                0xaa, 17L, Arrays.asList(deleting, publishing));
        byte[] encodedInventory = ArtifactLifecycleCodec.encodeInventory(inventory);
        assertArrayEquals(encodedInventory,
                ArtifactLifecycleCodec.encodeInventory(inventory));
        ArtifactInventorySnapshot decodedInventory =
                ArtifactLifecycleCodec.decodeInventory(encodedInventory);
        assertEquals(0xaa, decodedInventory.shard());
        assertEquals(17L, decodedInventory.generation());
        assertEquals(2, decodedInventory.entries().size());
        assertEquals(202L, decodedInventory.storedBytes());
        assertEquals(ArtifactInventoryState.DELETING,
                decodedInventory.entries().get(0).state());
        assertEquals(token, decodedInventory.entries().get(0).gcToken());
        assertThrows(UnsupportedOperationException.class,
                () -> decodedInventory.entries().add(live));

        ArtifactReferenceRef ref = new ArtifactReferenceRef(
                "run-Φ", ARTIFACT_A, 19L);
        byte[] encodedRef = ArtifactLifecycleCodec.encodeReference(ref);
        assertArrayEquals(encodedRef, ArtifactLifecycleCodec.encodeReference(ref));
        ArtifactReferenceRef decodedRef =
                ArtifactLifecycleCodec.decodeReference(encodedRef);
        assertEquals(ref, decodedRef);

        ArtifactCandidate candidate = ArtifactCandidate.from(live);
        assertEquals(ARTIFACT_B, candidate.artifactId());
        assertEquals(202L, candidate.encodedBytes());
        assertEquals(2_000L, candidate.createdAtMillis());
        assertEquals(10L, candidate.inventoryGeneration());
    }

    @Test
    void modelRejectsAliasesInvalidStateAndInconsistentInventory() {
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactInventoryEntry.live(ARTIFACT_A.toUpperCase(),
                        1L, 1L, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new ArtifactGcOwner("", 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new ArtifactGcOwner("master", 0L));
        assertThrows(IllegalArgumentException.class, () -> new ArtifactGcOwner(
                Normalizer.normalize("é", Normalizer.Form.NFD), 1L));
        ArtifactGcOwner owner = new ArtifactGcOwner("master", 1L);
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactGcToken.create(ARTIFACT_A, owner, 0L, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactGcToken.create(ARTIFACT_A, owner, 1L, 0L));
        ArtifactGcToken wrong = ArtifactGcToken.create(
                ARTIFACT_B, owner, 1L, 1L);
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactInventoryEntry.deleting(
                        ARTIFACT_A, 1L, 1L, 1L, wrong));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactInventorySnapshot.of(0xbb, 1L,
                        Collections.singletonList(ArtifactInventoryEntry.live(
                                ARTIFACT_A, 1L, 1L, 1L))));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactInventorySnapshot.of(0xaa, 1L, Arrays.asList(
                        ArtifactInventoryEntry.live(ARTIFACT_A, 1L, 1L, 1L),
                        ArtifactInventoryEntry.live(ARTIFACT_A, 1L, 1L, 2L))));
        assertThrows(IllegalArgumentException.class,
                () -> new ArtifactReferenceRef("", ARTIFACT_A, 1L));
        assertThrows(IllegalArgumentException.class,
                () -> new ArtifactReferenceRef("run", ARTIFACT_A, 0L));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactCandidate.from(ArtifactInventoryEntry.publishing(
                        ARTIFACT_A, 1L, 1L, 1L)));
    }

    @Test
    void codecRejectsCorruptionTruncationTrailingAndUnknownState() {
        ArtifactGcToken token = ArtifactGcToken.create(ARTIFACT_A,
                new ArtifactGcOwner("master", 1L), 1L, 3L);
        byte[] encodedToken = ArtifactLifecycleCodec.encodeGcToken(token);
        assertMalformedToken(mutate(encodedToken, 8));
        assertMalformedToken(Arrays.copyOf(encodedToken, encodedToken.length - 1));
        assertMalformedToken(Arrays.copyOf(encodedToken, encodedToken.length + 1));

        ArtifactInventorySnapshot inventory = ArtifactInventorySnapshot.of(
                0xaa, 1L, Collections.singletonList(
                        ArtifactInventoryEntry.deleting(
                                ARTIFACT_A, 7L, 9L, 3L, token)));
        byte[] encodedInventory = ArtifactLifecycleCodec.encodeInventory(inventory);
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCodec.decodeInventory(mutate(encodedInventory, 9)));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCodec.decodeInventory(
                        Arrays.copyOf(encodedInventory, encodedInventory.length - 1)));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCodec.decodeInventory(
                        Arrays.copyOf(encodedInventory, encodedInventory.length + 1)));

        // one-entry envelope: magic(4), version(1), shard(1), generation(8), count(4),
        // storedBytes(8), then state(1).
        byte[] unknownState = encodedInventory.clone();
        unknownState[26] = (byte) 127;
        refreshChecksum(unknownState);
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCodec.decodeInventory(unknownState));

        byte[] encodedRef = ArtifactLifecycleCodec.encodeReference(
                new ArtifactReferenceRef("run-Φ", ARTIFACT_A, 4L));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCodec.decodeReference(mutate(encodedRef, 7)));
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCodec.decodeReference(
                        Arrays.copyOf(encodedRef, encodedRef.length + 1)));
    }

    @Test
    void referenceAndBackfillResultsCannotMisrepresentZeroReadiness() {
        assertEquals(ArtifactReferenceView.Code.READY_REFERENCED,
                ArtifactReferenceView.referenced(2L).code());
        assertEquals(2L, ArtifactReferenceView.referenced(2L).referenceCount());
        assertEquals(ArtifactReferenceView.Code.READY_UNREFERENCED,
                ArtifactReferenceView.unreferenced().code());
        assertEquals(0L, ArtifactReferenceView.unreferenced().referenceCount());
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactReferenceView.referenced(0L));

        ArtifactBackfillStatus ready = ArtifactBackfillStatus.ready();
        assertEquals(ArtifactBackfillStatus.Code.READY, ready.code());
        assertNull(ready.cursor());
        ArtifactBackfillStatus backfilling =
                ArtifactBackfillStatus.backfilling("opaque", 7L);
        assertEquals(ArtifactBackfillStatus.Code.BACKFILLING, backfilling.code());
        assertEquals("opaque", backfilling.cursor());
        assertEquals(7L, backfilling.processedEntries());
        assertEquals(ArtifactBackfillStatus.Code.CORRUPT,
                ArtifactBackfillStatus.corrupt("bad ref").code());
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactBackfillStatus.backfilling("", 0L));
    }

    private static void assertMalformedToken(byte[] encoded) {
        assertThrows(IllegalArgumentException.class,
                () -> ArtifactLifecycleCodec.decodeGcToken(encoded));
    }

    private static byte[] mutate(byte[] encoded, int index) {
        byte[] corrupt = encoded.clone();
        corrupt[index] ^= 1;
        return corrupt;
    }

    private static void refreshChecksum(byte[] encoded) {
        int bodyLength = encoded.length - 32;
        try {
            byte[] checksum = MessageDigest.getInstance("SHA-256")
                    .digest(Arrays.copyOf(encoded, bodyLength));
            System.arraycopy(checksum, 0, encoded, bodyLength, checksum.length);
        } catch (NoSuchAlgorithmException impossible) {
            throw new AssertionError(impossible);
        }
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
