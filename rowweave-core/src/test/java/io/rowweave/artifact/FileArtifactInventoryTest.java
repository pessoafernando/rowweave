package io.rowweave.artifact;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.rowweave.artifact.ArtifactRepositoryContract.encoded;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileArtifactInventoryTest {

    @TempDir
    Path directory;

    @Test
    void durableIntentFinalLiveAndCallbackOrderIsExplicit() {
        List<String> order = new ArrayList<>();
        ArtifactRetentionPolicy policy = ArtifactRetentionPolicy.testing(
                8, 1_000_000L, 10L, 8, 4, 1L);
        FileArtifactRepository repository = new FileArtifactRepository(
                directory,
                point -> {
                    if (point == FileArtifactRepository.PublicationPoint.AFTER_DIRECTORY_FORCE) {
                        order.add("final");
                    }
                }, policy, () -> 1_000L,
                point -> {
                    if (point == FileArtifactRepository.InventoryPoint.AFTER_PUBLISHING_FORCE) {
                        order.add("intent");
                    } else if (point == FileArtifactRepository.InventoryPoint.AFTER_LIVE_FORCE) {
                        order.add("live");
                    }
                });

        byte[] bytes = encoded("inventory-order", new byte[]{1});
        repository.publishReferenced(bytes, value -> { }, artifactId -> {
            order.add("callback");
            assertArrayEquals(bytes, repository.get(artifactId));
            return artifactId;
        });
        assertEquals(Arrays.asList("intent", "final", "live", "callback"), order);
    }

    @Test
    void entryAndByteQuotaAreDurableAndReplayDoesNotDoubleCharge() {
        ArtifactPair pair = sameShardPair("quota");
        ArtifactRetentionPolicy oneEntry = ArtifactRetentionPolicy.testing(
                1, pair.first.length + pair.second.length + 1L,
                10L, 8, 4, 1L);
        FileArtifactRepository entries = new FileArtifactRepository(
                directory.resolve("entries"), point -> { }, oneEntry,
                () -> 1_000L, point -> { });
        String firstId = entries.put(pair.first);
        assertEquals(firstId, entries.put(pair.first));
        assertThrows(IllegalStateException.class, () -> entries.put(pair.second));
        assertEquals(1, entries.inventoryForTest(firstId).entries().size());
        assertEquals(pair.first.length,
                entries.inventoryForTest(firstId).storedBytes());

        ArtifactRetentionPolicy byteBound = ArtifactRetentionPolicy.testing(
                2, pair.first.length, 10L, 8, 4, 1L);
        FileArtifactRepository bytes = new FileArtifactRepository(
                directory.resolve("bytes"), point -> { }, byteBound,
                () -> 1_000L, point -> { });
        bytes.put(pair.first);
        assertThrows(IllegalStateException.class, () -> bytes.put(pair.second));
        assertEquals(pair.first.length,
                bytes.inventoryForTest(firstId).storedBytes());
    }

    @Test
    void inventoryUsageReadsExactlyOneBoundedShard() {
        ArtifactPair pair = sameShardPair("usage");
        ArtifactRetentionPolicy policy = ArtifactRetentionPolicy.testing(
                8, 1_000_000L, 10L, 8, 4, 1L);
        FileArtifactRepository repository = new FileArtifactRepository(
                directory.resolve("usage"), point -> { }, policy,
                () -> 1_000L, point -> { });
        repository.put(pair.first);
        repository.put(pair.second);
        int shard = Integer.parseInt(
                PlanArtifactCodec.artifactId(pair.first).substring(0, 2), 16);

        ArtifactInventoryUsage usage = repository.inventoryUsage(shard);

        assertEquals(256, repository.inventoryUsageShardCount());
        assertEquals(ArtifactInventoryUsage.Scope.SHARD, usage.scope());
        assertEquals(shard, usage.shard());
        assertEquals(2, usage.entries());
        assertEquals(pair.first.length + pair.second.length, usage.bytes());
        assertEquals(8, usage.entryCapacity());
        assertEquals(1_000_000L, usage.byteCapacity());
        assertThrows(IllegalArgumentException.class,
                () -> repository.inventoryUsage(-1));
        assertThrows(IllegalArgumentException.class,
                () -> repository.inventoryUsage(256));
    }

    @Test
    void reopenCompletesBoundedMigrationAndScansCanonicalCandidates() {
        ArtifactRetentionPolicy policy = ArtifactRetentionPolicy.testing(
                8, 1_000_000L, 10L, 2, 1, 1L);
        FileArtifactRepository repository = new FileArtifactRepository(
                directory, point -> { }, policy, () -> 1_000L, point -> { });
        byte[] first = encoded("candidate-one", new byte[]{1});
        byte[] second = encoded("candidate-two", new byte[]{2});
        repository.put(first);
        repository.put(second);
        assertEquals(ArtifactBackfillStatus.Code.BACKFILLING,
                repository.inventoryStatus().code());

        ArtifactBackfillStatus status = repository.inventoryStatus();
        for (int i = 0; i < 256 && status.code() != ArtifactBackfillStatus.Code.READY; i++) {
            status = repository.advanceInventoryBackfill(2);
        }
        assertEquals(ArtifactBackfillStatus.Code.READY, status.code());

        FileArtifactRepository reopened = new FileArtifactRepository(
                directory, point -> { }, policy, () -> 2_000L, point -> { });
        assertEquals(ArtifactBackfillStatus.Code.READY,
                reopened.inventoryStatus().code());
        List<String> ids = new ArrayList<>();
        String cursor = null;
        boolean complete = false;
        for (int i = 0; i < 512 && !complete; i++) {
            ArtifactCandidatePage page = reopened.scanCandidates(cursor, 1, 1_000L);
            for (ArtifactCandidate candidate : page.candidates()) {
                ids.add(candidate.artifactId());
            }
            cursor = page.nextCursor();
            complete = page.cycleComplete();
        }
        List<String> expected = Arrays.asList(
                PlanArtifactCodec.artifactId(first), PlanArtifactCodec.artifactId(second));
        expected.sort(String::compareTo);
        ids.sort(String::compareTo);
        assertEquals(expected, ids);
    }

    @Test
    void legacyFinalIsValidatedAndMigratedButOverCapacityShardFailsClosed()
            throws Exception {
        ArtifactPair pair = sameShardPair("legacy");
        ArtifactRetentionPolicy policy = ArtifactRetentionPolicy.testing(
                1, 1_000_000L, 10L, 8, 4, 1L);
        FileArtifactRepository repository = new FileArtifactRepository(
                directory, point -> { }, policy, () -> 1_000L, point -> { });
        String firstId = PlanArtifactCodec.artifactId(pair.first);
        Path firstPath = repository.artifactPathForTest(firstId);
        Files.createDirectories(firstPath.getParent());
        Files.write(firstPath, pair.first);
        assertEquals(firstId, repository.put(pair.first));
        assertEquals(1, repository.inventoryForTest(firstId).entries().size());

        Path over = directory.resolve("over");
        FileArtifactRepository overCapacity = new FileArtifactRepository(
                over, point -> { }, policy, () -> 1_000L, point -> { });
        String secondId = PlanArtifactCodec.artifactId(pair.second);
        Path overFirst = overCapacity.artifactPathForTest(firstId);
        Path overSecond = overCapacity.artifactPathForTest(secondId);
        Files.createDirectories(overFirst.getParent());
        Files.write(overFirst, pair.first);
        Files.write(overSecond, pair.second);
        assertThrows(IllegalStateException.class,
                () -> overCapacity.put(pair.first));
        assertFalse(Files.exists(overCapacity.inventoryPathForTest(firstId)));
        assertTrue(Files.exists(overFirst));
        assertTrue(Files.exists(overSecond));
    }

    private static ArtifactPair sameShardPair(String prefix) {
        byte[] first = encoded(prefix + "-0", new byte[]{0});
        String shard = PlanArtifactCodec.artifactId(first).substring(0, 2);
        for (int i = 1; i < 20_000; i++) {
            byte[] candidate = encoded(prefix + "-" + i, new byte[]{(byte) i});
            if (PlanArtifactCodec.artifactId(candidate).startsWith(shard)) {
                return new ArtifactPair(first, candidate);
            }
        }
        throw new AssertionError("failed to find same-shard Artifact pair");
    }

    private static final class ArtifactPair {
        private final byte[] first;
        private final byte[] second;

        private ArtifactPair(byte[] first, byte[] second) {
            this.first = first;
            this.second = second;
        }
    }
}
