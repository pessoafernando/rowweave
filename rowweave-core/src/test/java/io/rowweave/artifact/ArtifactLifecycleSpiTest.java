package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArtifactLifecycleSpiTest {

    private static final String ARTIFACT_A = repeat('a', 64);
    private static final String ARTIFACT_A2 = "aa" + repeat('b', 62);

    @Test
    void lifecycleSpiKeepsGuardedPublisherAndMigrationExplicit() {
        AtomicInteger publications = new AtomicInteger();
        ArtifactLifecycleRepository repository = new StubLifecycleRepository(publications);
        String result = repository.publishReferenced(new byte[]{1}, value -> { },
                artifactId -> {
                    assertEquals(ARTIFACT_A, artifactId);
                    return "published";
                });
        assertEquals("published", result);
        assertEquals(1, publications.get());
        assertEquals(ArtifactBackfillStatus.Code.BACKFILLING,
                repository.inventoryStatus().code());
        assertEquals(ArtifactBackfillStatus.Code.READY,
                repository.advanceInventoryBackfill(7).code());
        ArtifactCandidatePage page = repository.scanCandidates(null, 2, 100L);
        assertEquals(1, page.candidates().size());
        assertEquals(ARTIFACT_A, page.candidates().get(0).artifactId());
        assertNull(page.nextCursor());
    }

    @Test
    void referenceAuthorityNeverUsesNotReadyAsZero() {
        ArtifactReferenceAuthority authority = new ArtifactReferenceAuthority() {
            @Override
            public ArtifactReferenceView inspect(String artifactId) {
                return ArtifactReferenceView.referenced(1L);
            }

            @Override
            public ArtifactBackfillStatus backfillStatus() {
                return ArtifactBackfillStatus.backfilling("cursor", 3L);
            }

            @Override
            public ArtifactBackfillStatus advanceBackfill(int maxEntries) {
                return ArtifactBackfillStatus.ready();
            }
        };
        assertEquals(ArtifactReferenceView.Code.READY_REFERENCED,
                authority.inspect(ARTIFACT_A).code());
        assertEquals(ArtifactBackfillStatus.Code.BACKFILLING,
                authority.backfillStatus().code());

        ArtifactReferenceAuthorityProvider provider = () -> authority;
        assertEquals(authority, provider.artifactReferenceAuthority());
    }

    @Test
    void candidatePageIsImmutableBoundedAndCanonical() {
        ArtifactCandidate first = ArtifactCandidate.from(
                ArtifactInventoryEntry.live(ARTIFACT_A, 1L, 1L, 1L));
        ArtifactCandidate second = ArtifactCandidate.from(
                ArtifactInventoryEntry.live(ARTIFACT_A2, 2L, 2L, 2L));
        ArtifactCandidatePage page = new ArtifactCandidatePage(
                Arrays.asList(second, first), "next", false);
        assertEquals(ARTIFACT_A, page.candidates().get(0).artifactId());
        assertThrows(UnsupportedOperationException.class,
                () -> page.candidates().add(first));
        assertThrows(IllegalArgumentException.class,
                () -> new ArtifactCandidatePage(
                        Arrays.asList(first, first), null, true));
        assertThrows(IllegalArgumentException.class,
                () -> new ArtifactCandidatePage(
                        Collections.nCopies(1001, first), null, true));
        assertThrows(IllegalArgumentException.class,
                () -> new ArtifactCandidatePage(
                        Collections.singletonList(first), "", false));
    }

    private static final class StubLifecycleRepository
            implements ArtifactLifecycleRepository {
        private final AtomicInteger publications;

        private StubLifecycleRepository(AtomicInteger publications) {
            this.publications = publications;
        }

        @Override
        public <T> T publishReferenced(byte[] encodedArtifact,
                java.util.function.LongConsumer reservation,
                ArtifactReferencePublisher<T> publisher) {
            publications.incrementAndGet();
            return publisher.publish(ARTIFACT_A);
        }

        @Override
        public ArtifactBackfillStatus inventoryStatus() {
            return ArtifactBackfillStatus.backfilling("cursor", 0L);
        }

        @Override
        public ArtifactBackfillStatus advanceInventoryBackfill(int maxEntries) {
            return ArtifactBackfillStatus.ready();
        }

        @Override
        public int inventoryUsageShardCount() { return 1; }

        @Override
        public ArtifactInventoryUsage inventoryUsage(int shard) {
            return ArtifactInventoryUsage.global(1, 1L, 2, 2L);
        }

        @Override
        public ArtifactCandidatePage scanCandidates(
                String cursor, int limit, long cutoffMillis) {
            return new ArtifactCandidatePage(Collections.singletonList(
                    ArtifactCandidate.from(ArtifactInventoryEntry.live(
                            ARTIFACT_A, 1L, 1L, 1L))), null, true);
        }

        @Override
        public ArtifactRecoveryPage scanRecovering(String cursor, int limit) {
            return new ArtifactRecoveryPage(Collections.emptyList(), null, true);
        }

        @Override
        public ArtifactCollectResult collect(
                ArtifactCandidate candidate, ArtifactGcToken token,
                ArtifactReferenceAuthority references) {
            return ArtifactCollectResult.of(
                    ArtifactCollectResult.Code.STALE_CANDIDATE);
        }

        @Override
        public String put(byte[] encodedArtifact) { return ARTIFACT_A; }

        @Override
        public byte[] get(String artifactId) { return null; }
    }

    private static String repeat(char value, int count) {
        char[] chars = new char[count];
        Arrays.fill(chars, value);
        return new String(chars);
    }
}
