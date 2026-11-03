package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Age, per-round ceiling, cursor and item isolation of the Artifact janitor. */
class ArtifactJanitorTest {

    @Test
    void onePassBoundsCollectsAndResumesSameShardWithoutSkipping() {
        InMemoryArtifactRepository repository = repositoryWithShardZeroArtifacts(3);
        List<String> ids = artifactIds(repository);
        MutableReferences references = new MutableReferences();
        references.referenced.add(ids.get(0));
        ArtifactJanitor janitor = new ArtifactJanitor(
                repository, references,
                ArtifactRetentionPolicy.testing(
                        64, 64L * 1024L * 1024L, 1L, 3, 2, 1L),
                () -> System.currentTimeMillis() + 10_000L);
        ArtifactGcOwner owner = new ArtifactGcOwner("leader-a", 71L);

        ArtifactJanitor.PassResult first = janitor.runOnePass(owner, () -> true);
        ArtifactJanitor.PassResult second = janitor.runOnePass(owner, () -> true);

        assertEquals(3, first.scannedCandidates());
        assertEquals(2, first.collectAttempts());
        assertEquals(1, first.deletedArtifacts());
        assertEquals(1, first.retainedReferenced());
        assertFalse(first.cycleComplete());
        assertEquals(1, second.scannedCandidates());
        assertEquals(1, second.collectAttempts());
        assertEquals(1, second.deletedArtifacts());
        assertEquals(1, repository.size());
        assertEquals(2L, janitor.snapshot().deletedArtifacts());
        assertEquals(1L, janitor.snapshot().retainedReferenced());
        assertTrue(janitor.snapshot().oldestCandidateAgeMillis() >= 1L);
        assertEquals(ArtifactInventoryUsage.Scope.GLOBAL,
                janitor.snapshot().inventoryScope());
        assertEquals(-1, janitor.snapshot().inventoryObservedShard());
        assertEquals(1, janitor.snapshot().inventoryEntries());
        assertTrue(janitor.snapshot().inventoryBytes() > 0L);
        assertEquals(64, janitor.snapshot().inventoryEntryCapacity());
        assertEquals(64L * 1024L * 1024L,
                janitor.snapshot().inventoryByteCapacity());
        janitor.close();
        repository.close();
    }

    @Test
    void unavailableCandidateIsIsolatedAndLaterCandidateStillDeletes() {
        InMemoryArtifactRepository repository = repositoryWithShardZeroArtifacts(2);
        List<String> ids = artifactIds(repository);
        MutableReferences references = new MutableReferences();
        references.unavailable.add(ids.get(0));
        ArtifactJanitor janitor = new ArtifactJanitor(
                repository, references,
                ArtifactRetentionPolicy.testing(
                        64, 64L * 1024L * 1024L, 1L, 2, 2, 1L),
                () -> System.currentTimeMillis() + 10_000L);

        ArtifactJanitor.PassResult result = janitor.runOnePass(
                new ArtifactGcOwner("leader-b", -72L), () -> true);

        assertEquals(2, result.collectAttempts());
        assertEquals(1, result.unavailableArtifacts());
        assertEquals(1, result.deletedArtifacts());
        assertEquals(1, repository.size());
        assertTrue(result.diagnosticSamples().get(0).endsWith(":AUTHORITY_FAILURE"));
        janitor.close();
        repository.close();
    }

    @Test
    void leadershipLossBeforeItemPreventsCollection() {
        InMemoryArtifactRepository repository = repositoryWithShardZeroArtifacts(1);
        ArtifactJanitor janitor = new ArtifactJanitor(
                repository, new MutableReferences(),
                ArtifactRetentionPolicy.testing(
                        64, 64L * 1024L * 1024L, 1L, 1, 1, 1L),
                () -> System.currentTimeMillis() + 10_000L);

        ArtifactJanitor.PassResult result = janitor.runOnePass(
                new ArtifactGcOwner("leader-c", 73L), () -> false);

        assertTrue(result.leadershipLost());
        assertEquals(0, result.collectAttempts());
        assertEquals(1, repository.size());
        janitor.close();
        repository.close();
    }

    @Test
    void bothMigrationBarriersMustBeReadyBeforeAnyCandidateScan() {
        InMemoryArtifactRepository delegate = repositoryWithShardZeroArtifacts(1);
        BarrierRepository repository = new BarrierRepository(delegate);
        ArtifactJanitor janitor = new ArtifactJanitor(
                repository, new MutableReferences(),
                ArtifactRetentionPolicy.testing(
                        64, 64L * 1024L * 1024L, 1L, 1, 1, 1L),
                () -> System.currentTimeMillis() + 10_000L);
        ArtifactGcOwner owner = new ArtifactGcOwner("leader-d", 74L);

        ArtifactJanitor.PassResult blocked = janitor.runOnePass(owner, () -> true);
        assertEquals(1, blocked.notReadyPasses());
        assertEquals(ArtifactBackfillStatus.Code.BACKFILLING,
                blocked.inventoryStatus());
        assertEquals(0, repository.scans.get());
        assertEquals(1, delegate.size());

        ArtifactJanitor.PassResult ready = janitor.runOnePass(owner, () -> true);
        assertEquals(ArtifactBackfillStatus.Code.READY, ready.inventoryStatus());
        assertTrue(repository.scans.get() > 0);
        assertEquals(1, ready.deletedArtifacts());
        janitor.close();
        delegate.close();
    }

    @Test
    void corruptLiveShardIsIsolatedAndNextShardStillCollects() {
        InMemoryArtifactRepository delegate = repositoryWithArtifacts(1, "01");
        CorruptFirstLiveScanRepository repository =
                new CorruptFirstLiveScanRepository(delegate);
        ArtifactJanitor janitor = new ArtifactJanitor(
                repository, new MutableReferences(),
                ArtifactRetentionPolicy.testing(
                        64, 64L * 1024L * 1024L, 1L, 1, 1, 1L),
                () -> System.currentTimeMillis() + 10_000L);
        ArtifactGcOwner owner = new ArtifactGcOwner("leader-e", 75L);

        ArtifactJanitor.PassResult corrupt = janitor.runOnePass(owner, () -> true);
        ArtifactJanitor.PassResult recovered = janitor.runOnePass(owner, () -> true);

        assertEquals(1, corrupt.corruptArtifacts());
        assertEquals(1, recovered.deletedArtifacts());
        assertEquals(0, delegate.size());
        janitor.close();
        delegate.close();
    }

    private static InMemoryArtifactRepository repositoryWithShardZeroArtifacts(
            int count) {
        return repositoryWithArtifacts(count, "00");
    }

    private static InMemoryArtifactRepository repositoryWithArtifacts(
            int count, String prefix) {
        InMemoryArtifactRepository repository = new InMemoryArtifactRepository(
                64, 64L * 1024L * 1024L);
        for (int value = 0; repository.size() < count; value++) {
            byte[] encoded = ArtifactRepositoryContract.encoded(
                    "janitor-" + value, new byte[]{(byte) value});
            if (PlanArtifactCodec.artifactId(encoded).startsWith(prefix)) {
                repository.put(encoded);
            }
        }
        return repository;
    }

    private static List<String> artifactIds(
            InMemoryArtifactRepository repository) {
        ArtifactCandidatePage page = repository.scanCandidates(
                null, ArtifactCandidatePage.MAX_PAGE_SIZE, Long.MAX_VALUE);
        List<String> ids = new ArrayList<>();
        for (ArtifactCandidate candidate : page.candidates()) {
            ids.add(candidate.artifactId());
        }
        Collections.sort(ids);
        return ids;
    }

    private static final class MutableReferences
            implements ArtifactReferenceAuthority {
        private final Set<String> referenced = new HashSet<>();
        private final Set<String> unavailable = new HashSet<>();

        @Override public ArtifactReferenceView inspect(String artifactId) {
            if (unavailable.contains(artifactId)) {
                throw new ArtifactLifecycleUnavailableException(
                        "injected authority failure");
            }
            return referenced.contains(artifactId)
                    ? ArtifactReferenceView.referenced(1L)
                    : ArtifactReferenceView.unreferenced();
        }
        @Override public ArtifactBackfillStatus backfillStatus() {
            return ArtifactBackfillStatus.ready();
        }
        @Override public ArtifactBackfillStatus advanceBackfill(int maxEntries) {
            return ArtifactBackfillStatus.ready();
        }
    }

    private static final class BarrierRepository
            implements ArtifactLifecycleRepository {
        private final InMemoryArtifactRepository delegate;
        private final AtomicInteger advances = new AtomicInteger();
        private final AtomicInteger scans = new AtomicInteger();

        private BarrierRepository(InMemoryArtifactRepository delegate) {
            this.delegate = delegate;
        }

        @Override public ArtifactBackfillStatus inventoryStatus() {
            return advances.get() < 2
                    ? ArtifactBackfillStatus.backfilling(
                    ArtifactLifecycleCursor.start(
                            ArtifactLifecycleCursor.Kind.INVENTORY).encode(), 0L)
                    : ArtifactBackfillStatus.ready();
        }
        @Override public ArtifactBackfillStatus advanceInventoryBackfill(int maxEntries) {
            return advances.incrementAndGet() < 2
                    ? inventoryStatus() : ArtifactBackfillStatus.ready();
        }
        @Override public int inventoryUsageShardCount() {
            return delegate.inventoryUsageShardCount();
        }
        @Override public ArtifactInventoryUsage inventoryUsage(int shard) {
            return delegate.inventoryUsage(shard);
        }
        @Override public ArtifactCandidatePage scanCandidates(
                String cursor, int limit, long cutoffMillis) {
            scans.incrementAndGet();
            return delegate.scanCandidates(cursor, limit, cutoffMillis);
        }
        @Override public ArtifactRecoveryPage scanRecovering(
                String cursor, int limit) {
            scans.incrementAndGet();
            return delegate.scanRecovering(cursor, limit);
        }
        @Override public ArtifactCollectResult collect(
                ArtifactCandidate candidate, ArtifactGcToken token,
                ArtifactReferenceAuthority references) {
            return delegate.collect(candidate, token, references);
        }
        @Override public <T> T publishReferenced(
                byte[] encodedArtifact, java.util.function.LongConsumer reservation,
                ArtifactReferencePublisher<T> publisher) {
            return delegate.publishReferenced(encodedArtifact, reservation, publisher);
        }
        @Override public String put(byte[] encodedArtifact) {
            return delegate.put(encodedArtifact);
        }
        @Override public byte[] get(String artifactId) {
            return delegate.get(artifactId);
        }
    }

    private static final class CorruptFirstLiveScanRepository
            implements ArtifactLifecycleRepository {
        private final InMemoryArtifactRepository delegate;
        private boolean corrupt = true;

        private CorruptFirstLiveScanRepository(
                InMemoryArtifactRepository delegate) {
            this.delegate = delegate;
        }

        @Override public ArtifactBackfillStatus inventoryStatus() {
            return ArtifactBackfillStatus.ready();
        }
        @Override public ArtifactBackfillStatus advanceInventoryBackfill(int maxEntries) {
            return ArtifactBackfillStatus.ready();
        }
        @Override public int inventoryUsageShardCount() {
            return delegate.inventoryUsageShardCount();
        }
        @Override public ArtifactInventoryUsage inventoryUsage(int shard) {
            return delegate.inventoryUsage(shard);
        }
        @Override public ArtifactCandidatePage scanCandidates(
                String cursor, int limit, long cutoffMillis) {
            if (corrupt) {
                corrupt = false;
                throw new IllegalStateException("injected corrupt shard");
            }
            return delegate.scanCandidates(cursor, limit, cutoffMillis);
        }
        @Override public ArtifactRecoveryPage scanRecovering(
                String cursor, int limit) {
            return delegate.scanRecovering(cursor, limit);
        }
        @Override public ArtifactCollectResult collect(
                ArtifactCandidate candidate, ArtifactGcToken token,
                ArtifactReferenceAuthority references) {
            return delegate.collect(candidate, token, references);
        }
        @Override public <T> T publishReferenced(
                byte[] encodedArtifact, java.util.function.LongConsumer reservation,
                ArtifactReferencePublisher<T> publisher) {
            return delegate.publishReferenced(encodedArtifact, reservation, publisher);
        }
        @Override public String put(byte[] encodedArtifact) {
            return delegate.put(encodedArtifact);
        }
        @Override public byte[] get(String artifactId) {
            return delegate.get(artifactId);
        }
    }
}
