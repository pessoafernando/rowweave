package io.rowweave.artifact;

import java.nio.file.Path;
import java.nio.file.Paths;

/** Crash probe for fenced Artifact GC in a real child JVM. */
public final class FileArtifactFencedGcCrashProbe {
    private FileArtifactFencedGcCrashProbe() {
    }

    public static void main(String[] args) {
        Path root = Paths.get(args[0]);
        String mode = args[1];
        FileArtifactRepository repository = new FileArtifactRepository(
                root, point -> { },
                ArtifactRetentionPolicy.fromProperties(System.getProperties()),
                System::currentTimeMillis, point -> { }, point -> {
                    if ("after-intent".equals(mode)
                            && point == FileArtifactRepository.GcPoint
                            .AFTER_DELETING_FORCE) {
                        Runtime.getRuntime().halt(81);
                    }
                    if ("after-unlink".equals(mode)
                            && point == FileArtifactRepository.GcPoint
                            .AFTER_UNLINK_DIRECTORY_FORCE) {
                        Runtime.getRuntime().halt(82);
                    }
                    if ("after-finalize".equals(mode)
                            && point == FileArtifactRepository.GcPoint
                            .AFTER_FINALIZE_FORCE) {
                        Runtime.getRuntime().halt(83);
                    }
                });
        ArtifactCandidate candidate = firstCandidate(repository);
        ArtifactGcToken token = ArtifactGcToken.create(
                candidate.artifactId(), new ArtifactGcOwner("crash-probe", 41L),
                1L, candidate.inventoryGeneration());
        repository.collect(candidate, token, new ZeroReferences());
        throw new IllegalStateException("crash hook did not halt child JVM");
    }

    private static ArtifactCandidate firstCandidate(
            ArtifactLifecycleRepository repository) {
        String cursor = null;
        for (int page = 0; page < 257; page++) {
            ArtifactCandidatePage candidates = repository.scanCandidates(
                    cursor, ArtifactCandidatePage.MAX_PAGE_SIZE, Long.MAX_VALUE);
            if (!candidates.candidates().isEmpty()) {
                return candidates.candidates().get(0);
            }
            if (candidates.cycleComplete()) break;
            cursor = candidates.nextCursor();
        }
        throw new IllegalStateException("crash probe found no Artifact candidate");
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
