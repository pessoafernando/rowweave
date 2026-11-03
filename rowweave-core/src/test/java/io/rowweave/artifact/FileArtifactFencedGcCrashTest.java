package io.rowweave.artifact;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Every durable cut in DELETING, unlink and finalize can be converged by a new process. */
class FileArtifactFencedGcCrashTest {
    @TempDir
    Path temporary;

    @Test
    void realProcessCrashAtEveryGcCutConvergesWithoutUnsafeRepublish()
            throws Exception {
        verifyCut("after-intent", 81, true, true,
                ArtifactCollectResult.Code.DELETED);
        verifyCut("after-unlink", 82, false, true,
                ArtifactCollectResult.Code.DELETED);
        verifyCut("after-finalize", 83, false, false,
                ArtifactCollectResult.Code.ALREADY_DELETED);
    }

    private void verifyCut(String mode, int exitCode, boolean finalPresent,
                           boolean intentPresent,
                           ArtifactCollectResult.Code recoveryCode)
            throws Exception {
        Path root = temporary.resolve(mode);
        FileArtifactRepository repository = new FileArtifactRepository(root);
        byte[] encoded = ArtifactRepositoryContract.encoded(
                "gc-crash-" + mode, new byte[]{8, 1, 7});
        String artifactId = repository.put(encoded);
        makeInventoryReady(repository);
        ArtifactCandidate candidate = candidate(repository, artifactId);
        assertNotNull(candidate);

        Process child = launch(root, mode);
        assertTrue(child.waitFor(20, TimeUnit.SECONDS));
        assertEquals(exitCode, child.exitValue());

        FileArtifactRepository recovered = new FileArtifactRepository(root);
        assertEquals(finalPresent,
                Files.exists(recovered.artifactPathForTest(artifactId)));
        ArtifactInventoryEntry entry = findEntry(
                recovered.inventoryForTest(artifactId), artifactId);
        if (intentPresent) {
            assertNotNull(entry);
            assertEquals(ArtifactInventoryState.DELETING, entry.state());
        } else {
            assertNull(entry);
        }

        ArtifactGcToken token = ArtifactGcToken.create(
                artifactId, new ArtifactGcOwner("crash-probe", 41L),
                1L, candidate.inventoryGeneration());
        assertEquals(recoveryCode, recovered.collect(
                candidate, token, new ZeroReferences()).code());
        assertNull(recovered.get(artifactId));
        assertFalse(Files.exists(recovered.artifactPathForTest(artifactId)));
        assertNull(findEntry(recovered.inventoryForTest(artifactId), artifactId));
    }

    private Process launch(Path root, String mode) throws Exception {
        return new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                FileArtifactFencedGcCrashProbe.class.getName(),
                root.toString(), mode)).inheritIO().start();
    }

    private static void makeInventoryReady(FileArtifactRepository repository) {
        for (int i = 0; i < 257
                && repository.inventoryStatus().code()
                != ArtifactBackfillStatus.Code.READY; i++) {
            repository.advanceInventoryBackfill(1);
        }
        assertEquals(ArtifactBackfillStatus.Code.READY,
                repository.inventoryStatus().code());
    }

    private static ArtifactCandidate candidate(
            FileArtifactRepository repository, String artifactId) {
        String cursor = null;
        for (int page = 0; page < 257; page++) {
            ArtifactCandidatePage candidates = repository.scanCandidates(
                    cursor, ArtifactCandidatePage.MAX_PAGE_SIZE, Long.MAX_VALUE);
            for (ArtifactCandidate candidate : candidates.candidates()) {
                if (candidate.artifactId().equals(artifactId)) return candidate;
            }
            if (candidates.cycleComplete()) return null;
            cursor = candidates.nextCursor();
        }
        throw new AssertionError("candidate scan did not complete");
    }

    private static ArtifactInventoryEntry findEntry(
            ArtifactInventorySnapshot inventory, String artifactId) {
        for (ArtifactInventoryEntry entry : inventory.entries()) {
            if (entry.artifactId().equals(artifactId)) return entry;
        }
        return null;
    }

    private static final class ZeroReferences
            implements ArtifactReferenceAuthority {
        @Override public ArtifactReferenceView inspect(String artifactId) {
            return ArtifactReferenceView.unreferenced();
        }
        @Override public ArtifactBackfillStatus backfillStatus() {
            return ArtifactBackfillStatus.ready();
        }
        @Override public ArtifactBackfillStatus advanceBackfill(int maxEntries) {
            return ArtifactBackfillStatus.ready();
        }
    }
}
