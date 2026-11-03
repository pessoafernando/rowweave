package io.rowweave.artifact;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static io.rowweave.artifact.ArtifactRepositoryContract.encoded;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileArtifactGuardedPublicationTest
        extends ArtifactGuardedPublicationContract {

    @TempDir
    Path directory;

    @Override
    protected ArtifactLifecycleRepository newRepository() {
        return new FileArtifactRepository(directory);
    }

    @Test
    void publicationUsesFixedStripeLockAndCreatesNoPerArtifactLock() {
        FileArtifactRepository repository = new FileArtifactRepository(directory);
        byte[] bytes = encoded("fixed-lock", new byte[]{1});
        String artifactId = repository.publishReferenced(
                bytes, value -> { }, id -> id);

        assertTrue(Files.isRegularFile(repository.stripeLockPathForTest(artifactId),
                LinkOption.NOFOLLOW_LINKS));
        Path legacyPerArtifactLock = repository.artifactPathForTest(artifactId)
                .resolveSibling(artifactId + ".lock");
        assertFalse(Files.exists(legacyPerArtifactLock, LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    void independentProcessHoldsPublicationGuardAcrossCallback() throws Exception {
        Path release = directory.resolve("release.guard");
        Path entered = directory.resolve("child-entered.guard");
        Process child = launchGuardHolder(release, entered);
        waitForFile(entered, child);

        FileArtifactRepository repository = new FileArtifactRepository(directory);
        byte[] bytes = encoded("process-crash", new byte[]{7, 8, 9});
        CountDownLatch parentEntered = new CountDownLatch(1);
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            Future<String> publication = pool.submit(() -> repository.publishReferenced(
                    bytes, value -> { }, artifactId -> {
                        parentEntered.countDown();
                        return artifactId;
                    }));
            assertFalse(parentEntered.await(250, TimeUnit.MILLISECONDS),
                    "parent callback bypassed child process FileLock");
            Files.write(release, new byte[]{1});
            assertTrue(parentEntered.await(5, TimeUnit.SECONDS));
            assertEquals(PlanArtifactCodec.artifactId(bytes),
                    publication.get(5, TimeUnit.SECONDS));
            assertTrue(child.waitFor(5, TimeUnit.SECONDS));
            assertEquals(0, child.exitValue());
        } finally {
            Files.write(release, new byte[]{1});
            pool.shutdownNow();
            if (child.isAlive()) child.destroyForcibly();
        }
    }

    private Process launchGuardHolder(Path release, Path entered) throws Exception {
        return new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                FileArtifactRepositoryCrashProbe.class.getName(), directory.toString(),
                "hold-guard", release.toString(), entered.toString()))
                .redirectErrorStream(true).start();
    }

    private static void waitForFile(Path path, Process child) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (!Files.exists(path, LinkOption.NOFOLLOW_LINKS)
                && child.isAlive() && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        assertTrue(Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS),
                "child did not enter guarded callback");
    }
}
