package io.rowweave.artifact;

import io.rowweave.DataType;
import io.rowweave.DiscardSink;
import io.rowweave.Field;
import io.rowweave.DataSchema;
import io.rowweave.plan.PlanGraph;
import io.rowweave.plan.PlanNode;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every valid-final delete must be guarded by publication, exact-zero and an owner token. */
abstract class ArtifactFencedGcContract {
    private static final DataSchema META =
            new DataSchema(new Field("id", DataType.IntegerType));

    abstract ArtifactLifecycleRepository createRepository();

    void closeRepository(ArtifactLifecycleRepository repository) throws Exception {
        if (repository instanceof AutoCloseable) {
            ((AutoCloseable) repository).close();
        }
    }

    @Test
    void referencedArtifactIsRetainedAndLastReferenceReleaseAllowsDeletion()
            throws Exception {
        ArtifactLifecycleRepository repository = createRepository();
        try {
            Published published = publish(repository, "referenced-then-released");
            MutableReferences references = new MutableReferences(2L);
            ArtifactGcToken token = token(published.candidate, "master-a", 7L, 1L);

            ArtifactCollectResult retained = repository.collect(
                    published.candidate, token, references);
            assertEquals(ArtifactCollectResult.Code.RETAINED_REFERENCED,
                    retained.code());
            assertArrayEquals(published.bytes, repository.get(published.artifactId));

            references.count = 0L;
            ArtifactCollectResult deleted = repository.collect(
                    published.candidate, token, references);
            assertEquals(ArtifactCollectResult.Code.DELETED, deleted.code());
            assertNull(repository.get(published.artifactId));
            assertEquals(ArtifactCollectResult.Code.ALREADY_DELETED,
                    repository.collect(published.candidate, token, references).code());
        } finally {
            closeRepository(repository);
        }
    }

    @Test
    void referenceAppearingAfterDeletingIntentRestoresLiveWithoutUnlink()
            throws Exception {
        ArtifactLifecycleRepository repository = createRepository();
        try {
            Published published = publish(repository, "reference-race");
            SequenceReferences references = new SequenceReferences(0L, 1L);
            ArtifactGcToken token = token(published.candidate, "master-a", 9L, 1L);

            ArtifactCollectResult retained = repository.collect(
                    published.candidate, token, references);

            assertEquals(ArtifactCollectResult.Code.RETAINED_REFERENCED,
                    retained.code());
            assertEquals(2, references.inspections.get());
            assertArrayEquals(published.bytes, repository.get(published.artifactId));
            ArtifactCandidate refreshed = candidate(repository, published.artifactId);
            assertNotNull(refreshed);
            assertEquals(ArtifactCollectResult.Code.STALE_CANDIDATE,
                    repository.collect(published.candidate, token,
                            new MutableReferences(0L)).code());
        } finally {
            closeRepository(repository);
        }
    }

    @Test
    void staleCandidateCannotDeleteRepublishedSameContent() throws Exception {
        ArtifactLifecycleRepository repository = createRepository();
        try {
            Published first = publish(repository, "republished-content");
            MutableReferences zero = new MutableReferences(0L);
            ArtifactGcToken firstToken = token(first.candidate, "master-a", 11L, 1L);
            assertEquals(ArtifactCollectResult.Code.DELETED,
                    repository.collect(first.candidate, firstToken, zero).code());

            repository.publishReferenced(first.bytes, ignored -> { }, id -> id);
            ArtifactCandidate replacement = candidate(repository, first.artifactId);
            assertNotNull(replacement);
            assertEquals(ArtifactCollectResult.Code.STALE_CANDIDATE,
                    repository.collect(first.candidate, firstToken, zero).code());
            assertArrayEquals(first.bytes, repository.get(first.artifactId));
            assertEquals(ArtifactCollectResult.Code.DELETED,
                    repository.collect(replacement,
                            token(replacement, "master-b", 12L, 1L), zero).code());
        } finally {
            closeRepository(repository);
        }
    }

    @Test
    void unavailableSecondProofLeavesDeletingForHigherEpochReplacement()
            throws Exception {
        ArtifactLifecycleRepository repository = createRepository();
        try {
            Published published = publish(repository, "owner-replacement");
            ArtifactGcToken interrupted = token(
                    published.candidate, "old-master", 21L, 2L);

            assertThrows(IllegalStateException.class, () -> repository.collect(
                    published.candidate, interrupted,
                    new FailingSecondInspection()));
            assertArrayEquals(published.bytes, repository.get(published.artifactId));

            ArtifactGcToken stale = token(
                    published.candidate, "stale-master", 20L, 1L);
            assertEquals(ArtifactCollectResult.Code.STALE_OWNER,
                    repository.collect(published.candidate, stale,
                            new MutableReferences(0L)).code());
            assertArrayEquals(published.bytes, repository.get(published.artifactId));

            ArtifactGcToken replacement = token(
                    published.candidate, "new-master", 22L, 3L);
            assertEquals(ArtifactCollectResult.Code.DELETED,
                    repository.collect(published.candidate, replacement,
                            new MutableReferences(0L)).code());
            assertNull(repository.get(published.artifactId));
        } finally {
            closeRepository(repository);
        }
    }

    @Test
    void unavailableFirstProofKeepsLiveArtifactAndCandidate() throws Exception {
        ArtifactLifecycleRepository repository = createRepository();
        try {
            Published published = publish(repository, "unavailable-before-intent");
            ArtifactGcToken token = token(published.candidate, "master-a", 31L, 1L);

            assertThrows(IllegalStateException.class, () -> repository.collect(
                    published.candidate, token, new AlwaysUnavailableReferences()));

            assertArrayEquals(published.bytes, repository.get(published.artifactId));
            assertNotNull(candidate(repository, published.artifactId));
        } finally {
            closeRepository(repository);
        }
    }

    @Test
    void exactSuccessorEpochFencesOpaqueLeaderTermsAcrossProcessRestart()
            throws Exception {
        ArtifactLifecycleRepository repository = createRepository();
        try {
            Published published = publish(repository, "term-fenced-owner");
            ArtifactGcToken interrupted = token(
                    published.candidate, "old-master", 50L, 100L);
            assertThrows(IllegalStateException.class, () -> repository.collect(
                    published.candidate, interrupted,
                    new FailingSecondInspection()));

            ArtifactGcToken skippedEpoch = token(
                    published.candidate, "replacement-master", -49L, 102L);
            assertEquals(ArtifactCollectResult.Code.STALE_OWNER,
                    repository.collect(published.candidate, skippedEpoch,
                            new MutableReferences(0L)).code());

            ArtifactGcToken successor = token(
                    published.candidate, "replacement-master", -49L, 101L);
            assertThrows(IllegalStateException.class, () -> repository.collect(
                    published.candidate, successor,
                    new AlwaysUnavailableReferences()));
            assertEquals(ArtifactCollectResult.Code.STALE_OWNER,
                    repository.collect(published.candidate, interrupted,
                            new MutableReferences(0L)).code());
            assertEquals(ArtifactCollectResult.Code.DELETED,
                    repository.collect(published.candidate, successor,
                            new MutableReferences(0L)).code());
        } finally {
            closeRepository(repository);
        }
    }

    @Test
    void deletingIntentIsDiscoverableForExactSuccessorLeaderRecovery()
            throws Exception {
        ArtifactLifecycleRepository repository = createRepository();
        try {
            Published published = publish(repository, "discover-deleting");
            ArtifactGcToken interrupted = token(
                    published.candidate, "old-master", 61L, 7L);
            assertThrows(IllegalStateException.class, () -> repository.collect(
                    published.candidate, interrupted,
                    new FailingSecondInspection()));

            ArtifactRecoveryCandidate recovering = recoveryCandidate(
                    repository, published.artifactId);
            assertNotNull(recovering);
            assertEquals(interrupted, recovering.observedToken());
            assertEquals(published.candidate.artifactId(),
                    recovering.candidate().artifactId());
            ArtifactGcToken successor = token(
                    recovering.candidate(), "new-master", -62L, 8L);
            assertEquals(ArtifactCollectResult.Code.DELETED,
                    repository.collect(recovering.candidate(), successor,
                            new MutableReferences(0L)).code());
        } finally {
            closeRepository(repository);
        }
    }

    @Test
    void concurrentRepublishCannotEnterCallbackInsideCollectGuard()
            throws Exception {
        ArtifactLifecycleRepository repository = createRepository();
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Published published = publish(repository, "collect-publish-race");
            BlockingSecondProof references = new BlockingSecondProof();
            ArtifactGcToken token = token(
                    published.candidate, "gc-master", 81L, 1L);
            Future<ArtifactCollectResult> collect = workers.submit(
                    () -> repository.collect(
                            published.candidate, token, references));
            assertTrue(references.secondProofEntered.await(5, TimeUnit.SECONDS));

            CountDownLatch callbackEntered = new CountDownLatch(1);
            Future<String> republish = workers.submit(() ->
                    repository.publishReferenced(
                            published.bytes, ignored -> { }, artifactId -> {
                                callbackEntered.countDown();
                                return artifactId;
                            }));
            assertFalse(callbackEntered.await(200, TimeUnit.MILLISECONDS));

            references.releaseSecondProof.countDown();
            assertEquals(ArtifactCollectResult.Code.DELETED,
                    collect.get(5, TimeUnit.SECONDS).code());
            assertEquals(published.artifactId,
                    republish.get(5, TimeUnit.SECONDS));
            assertTrue(callbackEntered.await(5, TimeUnit.SECONDS));
            assertArrayEquals(published.bytes,
                    repository.get(published.artifactId));
            assertEquals(ArtifactCollectResult.Code.STALE_CANDIDATE,
                    repository.collect(published.candidate, token,
                            new MutableReferences(0L)).code());
        } finally {
            workers.shutdownNow();
            closeRepository(repository);
        }
    }

    private static Published publish(
            ArtifactLifecycleRepository repository, String definitionId) {
        byte[] bytes = artifact(definitionId);
        String artifactId = repository.publishReferenced(
                bytes, ignored -> { }, id -> id);
        ArtifactCandidate candidate = candidate(repository, artifactId);
        return new Published(bytes, artifactId, candidate);
    }

    private static ArtifactCandidate candidate(
            ArtifactLifecycleRepository repository, String artifactId) {
        for (int i = 0; i < 257
                && repository.inventoryStatus().code()
                != ArtifactBackfillStatus.Code.READY; i++) {
            repository.advanceInventoryBackfill(1);
        }
        assertEquals(ArtifactBackfillStatus.Code.READY,
                repository.inventoryStatus().code());
        String cursor = null;
        for (int i = 0; i < 257; i++) {
            ArtifactCandidatePage page = repository.scanCandidates(
                    cursor, ArtifactCandidatePage.MAX_PAGE_SIZE, Long.MAX_VALUE);
            for (ArtifactCandidate candidate : page.candidates()) {
                if (candidate.artifactId().equals(artifactId)) return candidate;
            }
            if (page.cycleComplete()) return null;
            cursor = page.nextCursor();
        }
        throw new AssertionError("candidate scan did not complete one bounded cycle");
    }

    private static ArtifactGcToken token(
            ArtifactCandidate candidate, String owner, long term, long epoch) {
        return ArtifactGcToken.create(candidate.artifactId(),
                new ArtifactGcOwner(owner, term), epoch,
                candidate.inventoryGeneration());
    }

    private static ArtifactRecoveryCandidate recoveryCandidate(
            ArtifactLifecycleRepository repository, String artifactId) {
        String cursor = null;
        for (int i = 0; i < 257; i++) {
            ArtifactRecoveryPage page = repository.scanRecovering(
                    cursor, ArtifactCandidatePage.MAX_PAGE_SIZE);
            for (ArtifactRecoveryCandidate candidate : page.candidates()) {
                if (candidate.candidate().artifactId().equals(artifactId)) {
                    return candidate;
                }
            }
            if (page.cycleComplete()) return null;
            cursor = page.nextCursor();
        }
        throw new AssertionError("recovery scan did not complete one bounded cycle");
    }

    private static byte[] artifact(String definitionId) {
        PlanGraph graph = new PlanGraph(Arrays.asList(
                PlanNode.source(META, Collections.emptyList())
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "source"),
                PlanNode.sink(META, 0, new DiscardSink(META)).build()
                        .withParam(PlanNode.OPERATOR_UID_PARAM, "output")));
        return PlanArtifactCodec.encode(
                LegacyPlanArtifactAdapter.wrap(definitionId, graph));
    }

    private static final class Published {
        final byte[] bytes;
        final String artifactId;
        final ArtifactCandidate candidate;

        private Published(byte[] bytes, String artifactId,
                          ArtifactCandidate candidate) {
            this.bytes = bytes;
            this.artifactId = artifactId;
            this.candidate = candidate;
        }
    }

    private static class MutableReferences implements ArtifactReferenceAuthority {
        volatile long count;

        private MutableReferences(long count) { this.count = count; }

        @Override public ArtifactReferenceView inspect(String artifactId) {
            return count == 0L
                    ? ArtifactReferenceView.unreferenced()
                    : ArtifactReferenceView.referenced(count);
        }
        @Override public ArtifactBackfillStatus backfillStatus() {
            return ArtifactBackfillStatus.ready();
        }
        @Override public ArtifactBackfillStatus advanceBackfill(int maxEntries) {
            return ArtifactBackfillStatus.ready();
        }
    }

    private static final class SequenceReferences extends MutableReferences {
        private final long[] sequence;
        private final AtomicInteger inspections = new AtomicInteger();

        private SequenceReferences(long... sequence) {
            super(0L);
            this.sequence = sequence;
        }

        @Override public ArtifactReferenceView inspect(String artifactId) {
            int index = inspections.getAndIncrement();
            long value = sequence[Math.min(index, sequence.length - 1)];
            return value == 0L
                    ? ArtifactReferenceView.unreferenced()
                    : ArtifactReferenceView.referenced(value);
        }
    }

    private static final class FailingSecondInspection extends MutableReferences {
        private final AtomicInteger inspections = new AtomicInteger();

        private FailingSecondInspection() { super(0L); }

        @Override public ArtifactReferenceView inspect(String artifactId) {
            if (inspections.getAndIncrement() == 0) {
                return ArtifactReferenceView.unreferenced();
            }
            throw new IllegalStateException("injected reference authority unavailable");
        }
    }

    private static final class AlwaysUnavailableReferences extends MutableReferences {
        private AlwaysUnavailableReferences() { super(0L); }

        @Override public ArtifactReferenceView inspect(String artifactId) {
            throw new IllegalStateException("injected reference authority unavailable");
        }
    }

    private static final class BlockingSecondProof extends MutableReferences {
        private final AtomicInteger inspections = new AtomicInteger();
        private final CountDownLatch secondProofEntered = new CountDownLatch(1);
        private final CountDownLatch releaseSecondProof = new CountDownLatch(1);

        private BlockingSecondProof() { super(0L); }

        @Override public ArtifactReferenceView inspect(String artifactId) {
            if (inspections.getAndIncrement() == 0) {
                return ArtifactReferenceView.unreferenced();
            }
            secondProofEntered.countDown();
            try {
                if (!releaseSecondProof.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("second proof release timeout");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "interrupted waiting to release second proof", interrupted);
            }
            return ArtifactReferenceView.unreferenced();
        }
    }
}
