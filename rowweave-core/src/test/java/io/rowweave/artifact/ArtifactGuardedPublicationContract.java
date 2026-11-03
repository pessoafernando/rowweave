package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static io.rowweave.artifact.ArtifactRepositoryContract.encoded;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Black-box contract for publication guards across lifecycle repositories. */
abstract class ArtifactGuardedPublicationContract {

    protected abstract ArtifactLifecycleRepository newRepository();

    @Test
    void callbackRunsAfterValidatedFinalPublicationAndReturnsItsResult() {
        ArtifactLifecycleRepository repository = newRepository();
        byte[] bytes = encoded("guarded-visible", new byte[]{1, 2, 3});
        String expected = PlanArtifactCodec.artifactId(bytes);
        String result = repository.publishReferenced(bytes, value -> { }, artifactId -> {
            assertEquals(expected, artifactId);
            assertArrayEquals(bytes, repository.get(artifactId));
            return "run-ref:" + artifactId;
        });
        assertEquals("run-ref:" + expected, result);
    }

    @Test
    void sameArtifactCallbacksAreSerializedAcrossRepositoryHandles() throws Exception {
        ArtifactLifecycleRepository first = newRepository();
        ArtifactLifecycleRepository second = newRepository();
        byte[] bytes = encoded("guarded-serial", new byte[]{4, 5, 6});
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        CountDownLatch secondEntered = new CountDownLatch(1);
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximum = new AtomicInteger();
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<String> one = pool.submit(() -> first.publishReferenced(
                    bytes, value -> { }, artifactId -> {
                        maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                        firstEntered.countDown();
                        await(releaseFirst);
                        active.decrementAndGet();
                        return artifactId;
                    }));
            assertTrue(firstEntered.await(5, TimeUnit.SECONDS));
            Future<String> two = pool.submit(() -> second.publishReferenced(
                    bytes, value -> { }, artifactId -> {
                        maximum.accumulateAndGet(active.incrementAndGet(), Math::max);
                        secondEntered.countDown();
                        active.decrementAndGet();
                        return artifactId;
                    }));
            assertFalse(secondEntered.await(200, TimeUnit.MILLISECONDS),
                    "second callback entered the same Artifact guard");
            releaseFirst.countDown();
            assertEquals(one.get(5, TimeUnit.SECONDS), two.get(5, TimeUnit.SECONDS));
            assertTrue(secondEntered.await(1, TimeUnit.SECONDS));
            assertEquals(1, maximum.get());
        } finally {
            releaseFirst.countDown();
            pool.shutdownNow();
        }
    }

    @Test
    void callbackFailureLeavesValidatedOrphanAndInvalidArgumentsNeverInvokeIt() {
        ArtifactLifecycleRepository repository = newRepository();
        byte[] bytes = encoded("guarded-failure", new byte[]{7, 8, 9});
        String artifactId = PlanArtifactCodec.artifactId(bytes);
        AtomicInteger callbacks = new AtomicInteger();
        IllegalStateException failure = assertThrows(IllegalStateException.class,
                () -> repository.publishReferenced(bytes, value -> { }, ignored -> {
                    callbacks.incrementAndGet();
                    throw new IllegalStateException("create ACK lost");
                }));
        assertEquals("create ACK lost", failure.getMessage());
        assertEquals(1, callbacks.get());
        assertArrayEquals(bytes, repository.get(artifactId));

        assertThrows(IllegalArgumentException.class,
                () -> repository.publishReferenced(bytes, null, ignored -> null));
        assertThrows(IllegalArgumentException.class,
                () -> repository.publishReferenced(bytes, value -> { }, null));
        assertThrows(PlanArtifactValidationException.class,
                () -> repository.publishReferenced(new byte[]{1}, value -> { },
                        ignored -> callbacks.incrementAndGet()));
        assertEquals(1, callbacks.get());
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                throw new AssertionError("timed out waiting for test gate");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError("interrupted waiting for test gate", interrupted);
        }
    }
}
