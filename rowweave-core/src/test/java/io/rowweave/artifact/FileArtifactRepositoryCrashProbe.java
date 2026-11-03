package io.rowweave.artifact;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/** Probe for crashes in a real child JVM and for cross-process publication races. */
public final class FileArtifactRepositoryCrashProbe {
    private FileArtifactRepositoryCrashProbe() {
    }

    public static void main(String[] args) throws Exception {
        Path root = Paths.get(args[0]);
        String mode = args[1];
        Path gate = "-".equals(args[2]) ? null : Paths.get(args[2]);
        Path winners = "-".equals(args[3]) ? null : Paths.get(args[3]);
        byte[] encoded = ArtifactRepositoryContract.encoded(
                "process-crash", new byte[]{7, 8, 9});

        if (gate != null && !"hold-guard".equals(mode)) {
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (!Files.exists(gate) && System.nanoTime() < deadline) {
                Thread.yield();
            }
            if (!Files.exists(gate)) {
                throw new IllegalStateException("start gate timeout");
            }
        }

        FileArtifactRepository repository = new FileArtifactRepository(root, point -> {
            if ("before-move".equals(mode)
                    && point == FileArtifactRepository.PublicationPoint.AFTER_TEMP_FORCE) {
                Runtime.getRuntime().halt(71);
            }
            if ("after-publish".equals(mode)
                    && point == FileArtifactRepository.PublicationPoint.AFTER_DIRECTORY_FORCE) {
                Runtime.getRuntime().halt(72);
            }
            if (mode.startsWith("contend-")
                    && point == FileArtifactRepository.PublicationPoint.AFTER_DIRECTORY_FORCE) {
                try {
                    Files.write(winners, (mode + "\n").getBytes(java.nio.charset.StandardCharsets.UTF_8),
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                } catch (IOException failure) {
                    throw new UncheckedIOException(failure);
                }
            }
        }, ArtifactRetentionPolicy.fromProperties(System.getProperties()),
                System::currentTimeMillis, point -> {
                    if ("after-intent".equals(mode)
                            && point == FileArtifactRepository.InventoryPoint
                            .AFTER_PUBLISHING_FORCE) {
                        Runtime.getRuntime().halt(73);
                    }
                });
        if ("hold-guard".equals(mode)) {
            String published = repository.publishReferenced(
                    encoded, value -> { }, artifactId -> {
                        try {
                            Files.write(winners, new byte[]{1},
                                    StandardOpenOption.CREATE_NEW);
                            long deadline = System.nanoTime()
                                    + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
                            while (!Files.exists(gate) && System.nanoTime() < deadline) {
                                Thread.yield();
                            }
                            if (!Files.exists(gate)) {
                                throw new IllegalStateException("release gate timeout");
                            }
                            return artifactId;
                        } catch (IOException failure) {
                            throw new UncheckedIOException(failure);
                        }
                    });
            System.out.println(published);
        } else {
            System.out.println(repository.put(encoded));
        }
    }
}
