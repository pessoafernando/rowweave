package io.rowweave.soak;

import org.junit.jupiter.api.Test;

import java.nio.file.AccessDeniedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StreamSoakResourceSamplerTest {
    @Test
    void countsRegularFilesRecursively() throws Exception {
        Path root = Files.createTempDirectory("stream-soak-disk-bytes-");
        Path nested = Files.createDirectories(root.resolve("nested"));
        Files.write(root.resolve("a"), new byte[] {1, 2});
        Files.write(nested.resolve("b"), new byte[] {3, 4, 5});

        assertEquals(5L, StreamSoakResourceSampler.diskBytes(root));
    }

    @Test
    void skipsAPathThatDisappearsDuringTraversal() throws Exception {
        assertEquals(FileVisitResult.CONTINUE,
                StreamSoakResourceSampler.onVisitFailure(
                        new NoSuchFileException("minio-tmp")));
    }

    @Test
    void failsClosedForOtherTraversalErrors() {
        AccessDeniedException denied = new AccessDeniedException("state");

        AccessDeniedException thrown = assertThrows(
                AccessDeniedException.class,
                () -> StreamSoakResourceSampler.onVisitFailure(denied));
        assertEquals(denied, thrown);
    }
}
