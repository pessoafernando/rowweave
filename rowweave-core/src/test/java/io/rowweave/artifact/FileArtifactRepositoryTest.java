package io.rowweave.artifact;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FileArtifactRepositoryTest extends ArtifactRepositoryContract {

    @TempDir
    Path directory;

    @Override
    protected ArtifactRepository newRepository() {
        return new FileArtifactRepository(directory);
    }

    @Test
    void committedArtifactSurvivesReopenAndTempIsNeverVisible() throws Exception {
        byte[] committed = encoded("committed", new byte[]{1, 2, 3});
        String committedId = new FileArtifactRepository(directory).put(committed);
        assertArrayEquals(committed,
                new FileArtifactRepository(directory).get(committedId));

        byte[] interrupted = encoded("interrupted", new byte[]{4, 5, 6});
        String interruptedId = PlanArtifactCodec.artifactId(interrupted);
        FileArtifactRepository reopened = new FileArtifactRepository(directory);
        Path temp = reopened.tempPathForTest(interruptedId);
        Files.createDirectories(temp.getParent());
        Files.write(temp, interrupted);

        assertNull(reopened.get(interruptedId));
        assertFalse(Files.exists(reopened.artifactPathForTest(interruptedId),
                LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.exists(temp, LinkOption.NOFOLLOW_LINKS));
        assertArrayEquals(interrupted,
                reopened.get(reopened.put(interrupted)));
    }

    @Test
    void completeCorruptionAndContentAtWrongIdentityFailClosed() throws Exception {
        FileArtifactRepository repository = new FileArtifactRepository(directory);
        byte[] encoded = encoded("corrupt", new byte[]{9, 8, 7});
        String artifactId = repository.put(encoded);
        Path finalPath = repository.artifactPathForTest(artifactId);
        byte[] corrupt = Files.readAllBytes(finalPath);
        corrupt[corrupt.length - 1] ^= 1;
        Files.write(finalPath, corrupt, StandardOpenOption.TRUNCATE_EXISTING);
        assertThrows(IllegalStateException.class, () -> repository.get(artifactId));
        assertThrows(IllegalStateException.class, () -> repository.put(encoded));

        Path secondRoot = directory.resolve("identity-mismatch");
        FileArtifactRepository mismatched = new FileArtifactRepository(secondRoot);
        byte[] expected = encoded("expected", new byte[]{1});
        byte[] wrong = encoded("wrong", new byte[]{2});
        String expectedId = PlanArtifactCodec.artifactId(expected);
        Path expectedPath = mismatched.artifactPathForTest(expectedId);
        Files.createDirectories(expectedPath.getParent());
        Files.write(expectedPath, wrong);
        assertThrows(IllegalStateException.class, () -> mismatched.get(expectedId));
    }

    @Test
    void symlinkNonRegularAndOversizedFinalAreRejectedBeforeRead() throws Exception {
        FileArtifactRepository repository = new FileArtifactRepository(directory);

        byte[] linked = encoded("linked", new byte[]{1});
        String linkedId = PlanArtifactCodec.artifactId(linked);
        Path linkedPath = repository.artifactPathForTest(linkedId);
        Files.createDirectories(linkedPath.getParent());
        Path outside = directory.resolve("outside.artifact");
        Files.write(outside, linked);
        Files.createSymbolicLink(linkedPath, outside);
        assertThrows(IllegalStateException.class, () -> repository.get(linkedId));

        byte[] directoryArtifact = encoded("directory", new byte[]{2});
        String directoryId = PlanArtifactCodec.artifactId(directoryArtifact);
        Files.createDirectories(repository.artifactPathForTest(directoryId));
        assertThrows(IllegalStateException.class, () -> repository.get(directoryId));

        byte[] oversizedArtifact = encoded("oversized", new byte[]{3});
        String oversizedId = PlanArtifactCodec.artifactId(oversizedArtifact);
        Path oversized = repository.artifactPathForTest(oversizedId);
        Files.createDirectories(oversized.getParent());
        try (FileChannel channel = FileChannel.open(oversized,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            channel.position(PlanArtifactCodec.MAX_FRAME_BYTES);
            channel.write(ByteBuffer.wrap(new byte[]{0}));
        }
        assertThrows(IllegalStateException.class, () -> repository.get(oversizedId));
    }

    @Test
    void processHaltBeforeMoveLeavesInvisibleTempAndRetryCanPublish() throws Exception {
        byte[] encoded = encoded("process-crash", new byte[]{7, 8, 9});
        String artifactId = PlanArtifactCodec.artifactId(encoded);
        Process process = launch("before-move", null, null, null);
        assertTrue(process.waitFor(15, TimeUnit.SECONDS));
        assertEquals(71, process.exitValue());

        FileArtifactRepository reopened = new FileArtifactRepository(directory);
        assertEquals(ArtifactInventoryState.PUBLISHING,
                reopened.inventoryForTest(artifactId).entries().get(0).state());
        assertNull(reopened.get(artifactId));
        assertFalse(Files.exists(reopened.artifactPathForTest(artifactId),
                LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.isRegularFile(reopened.tempPathForTest(artifactId),
                LinkOption.NOFOLLOW_LINKS));
        assertEquals(artifactId, reopened.put(encoded));
        assertEquals(ArtifactInventoryState.LIVE,
                reopened.inventoryForTest(artifactId).entries().get(0).state());
        assertArrayEquals(encoded, reopened.get(artifactId));
    }

    @Test
    void processHaltAfterInventoryIntentReservesQuotaWithoutPublishingFinal()
            throws Exception {
        byte[] encoded = encoded("process-crash", new byte[]{7, 8, 9});
        String artifactId = PlanArtifactCodec.artifactId(encoded);
        Process process = launch("after-intent", null, null, null);
        assertTrue(process.waitFor(15, TimeUnit.SECONDS));
        assertEquals(73, process.exitValue());

        FileArtifactRepository reopened = new FileArtifactRepository(directory);
        ArtifactInventorySnapshot inventory = reopened.inventoryForTest(artifactId);
        assertEquals(1, inventory.entries().size());
        assertEquals(ArtifactInventoryState.PUBLISHING,
                inventory.entries().get(0).state());
        assertFalse(Files.exists(reopened.artifactPathForTest(artifactId),
                LinkOption.NOFOLLOW_LINKS));
        assertFalse(Files.exists(reopened.tempPathForTest(artifactId),
                LinkOption.NOFOLLOW_LINKS));
        assertEquals(artifactId, reopened.put(encoded));
        assertEquals(ArtifactInventoryState.LIVE,
                reopened.inventoryForTest(artifactId).entries().get(0).state());
    }

    @Test
    void processHaltAfterDirectoryForceLeavesDurableFinal() throws Exception {
        byte[] encoded = encoded("process-crash", new byte[]{7, 8, 9});
        String artifactId = PlanArtifactCodec.artifactId(encoded);
        Process process = launch("after-publish", null, null, null);
        assertTrue(process.waitFor(15, TimeUnit.SECONDS));
        assertEquals(72, process.exitValue());

        FileArtifactRepository reopened = new FileArtifactRepository(directory);
        assertArrayEquals(encoded, reopened.get(artifactId));
        assertEquals(ArtifactInventoryState.PUBLISHING,
                reopened.inventoryForTest(artifactId).entries().get(0).state());
        assertEquals(artifactId, reopened.put(encoded));
        assertEquals(ArtifactInventoryState.LIVE,
                reopened.inventoryForTest(artifactId).entries().get(0).state());
    }

    @Test
    void twoIndependentProcessesPublishOnePhysicalArtifact() throws Exception {
        Path gate = directory.resolve("start.gate");
        Path winners = directory.resolve("winners.log");
        Path firstOutput = directory.resolve("first.out");
        Path secondOutput = directory.resolve("second.out");
        Process first = launch("contend-first", gate, winners, firstOutput);
        Process second = launch("contend-second", gate, winners, secondOutput);
        Files.write(gate, new byte[]{1}, StandardOpenOption.CREATE_NEW);

        assertTrue(first.waitFor(20, TimeUnit.SECONDS));
        assertTrue(second.waitFor(20, TimeUnit.SECONDS));
        assertEquals(0, first.exitValue(), read(firstOutput));
        assertEquals(0, second.exitValue(), read(secondOutput));
        List<String> publications = Files.readAllLines(winners, StandardCharsets.UTF_8);
        assertEquals(1, publications.size(), publications.toString());

        byte[] encoded = encoded("process-crash", new byte[]{7, 8, 9});
        String artifactId = PlanArtifactCodec.artifactId(encoded);
        assertEquals(artifactId, read(firstOutput).trim());
        assertEquals(artifactId, read(secondOutput).trim());
        assertArrayEquals(encoded,
                new FileArtifactRepository(directory).get(artifactId));
    }

    private Process launch(String mode, Path gate, Path winners, Path output) throws Exception {
        ProcessBuilder builder = new ProcessBuilder(io.rowweave.test.ForkedJvm.command(
                FileArtifactRepositoryCrashProbe.class.getName(), directory.toString(), mode,
                gate == null ? "-" : gate.toString(),
                winners == null ? "-" : winners.toString()));
        builder.redirectErrorStream(true);
        if (output != null) {
            builder.redirectOutput(output.toFile());
        }
        return builder.start();
    }

    private static String read(Path path) throws Exception {
        return new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
    }
}
