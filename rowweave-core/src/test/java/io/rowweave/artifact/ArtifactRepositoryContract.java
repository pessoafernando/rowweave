package io.rowweave.artifact;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.rowweave.artifact.PlanArtifactModelTest.artifact;
import static io.rowweave.artifact.PlanArtifactModelTest.requiredFeatures;
import static io.rowweave.artifact.PlanArtifactModelTest.source;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Black-box contract that every ArtifactRepository backend must pass. */
public abstract class ArtifactRepositoryContract {

    protected abstract ArtifactRepository newRepository();

    @Test
    void validatedContentAddressingNotFoundAndDefensiveCopies() {
        ArtifactRepository repository = newRepository();
        byte[] encoded = encoded("definition-one", new byte[]{1, 2, 3});
        String expectedId = PlanArtifactCodec.artifactId(encoded);

        assertNull(repository.get(expectedId));
        assertEquals(expectedId, repository.put(encoded));
        encoded[0] ^= 1;

        byte[] first = repository.get(expectedId);
        assertArrayEquals(encoded("definition-one", new byte[]{1, 2, 3}), first);
        first[0] ^= 1;
        assertArrayEquals(encoded("definition-one", new byte[]{1, 2, 3}),
                repository.get(expectedId));
    }

    @Test
    void malformedInputAndInvalidExpectedIdentityFailClosed() {
        ArtifactRepository repository = newRepository();
        byte[] encoded = encoded("definition-two", new byte[]{4, 5, 6});
        byte[] corrupt = encoded.clone();
        corrupt[corrupt.length - 1] ^= 1;

        assertThrows(PlanArtifactValidationException.class,
                () -> repository.put(corrupt));
        assertThrows(IllegalArgumentException.class, () -> repository.put(null));
        assertThrows(IllegalArgumentException.class, () -> repository.get(null));
        assertThrows(IllegalArgumentException.class, () -> repository.get("../escape"));
        assertThrows(IllegalArgumentException.class,
                () -> repository.get(PlanArtifactCodec.artifactId(encoded).toUpperCase()));
        assertNull(repository.get(PlanArtifactCodec.artifactId(encoded)));
    }

    @Test
    void concurrentIdenticalPutIsIdempotent() throws Exception {
        ArtifactRepository repository = newRepository();
        byte[] encoded = encoded("definition-concurrent", new byte[1024]);
        String expectedId = PlanArtifactCodec.artifactId(encoded);
        int writers = 8;
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(writers);
        List<Future<?>> results = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            results.add(pool.submit(() -> {
                start.await();
                assertEquals(expectedId, repository.put(encoded));
                return null;
            }));
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));
        for (Future<?> result : results) {
            result.get();
        }
        assertArrayEquals(encoded, repository.get(expectedId));
    }

    static byte[] encoded(String definitionId, byte[] payload) {
        return PlanArtifactCodec.encode(artifact(
                definitionId, requiredFeatures(), Collections.emptyList(),
                Collections.singletonList(source("source-1")), payload));
    }
}
