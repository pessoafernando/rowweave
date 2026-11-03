package io.rowweave.stream.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded content-addressed checkpoint chunks. */
class FileCheckpointChunkStoreTest {

    @Test
    void chunkedPayloadRestoresExactlyAcrossReopen(@TempDir Path root)
            throws Exception {
        byte[] content = content(10_321);
        Path staged = root.resolve("staged.bin");
        Files.write(staged, content);
        FileCheckpointChunkStore store = new FileCheckpointChunkStore(
                root.resolve("store"), 1024, 32_768L, 64);

        FileCheckpointChunkStore.StoredChunkedPayload payload =
                store.store(staged);

        assertEquals(content.length, payload.contentBytes());
        assertEquals(11, payload.chunks().size());
        for (FileCheckpointChunkStore.ChunkHandle chunk : payload.chunks()) {
            assertTrue(chunk.contentBytes() > 0 && chunk.contentBytes() <= 1024);
            assertEquals("NONE", chunk.compressionId());
            assertEquals("NONE", chunk.encryptionId());
        }
        Path restored = root.resolve("restored.bin");
        new FileCheckpointChunkStore(root.resolve("store"),
                1024, 32_768L, 64).restore(payload, restored);
        assertArrayEquals(content, Files.readAllBytes(restored));
    }

    @Test
    void repeatedPublicationDeduplicatesAndCorruptionDoesNotReplaceTarget(
            @TempDir Path root) throws Exception {
        byte[] content = content(4096);
        Path first = root.resolve("first.bin");
        Path second = root.resolve("second.bin");
        Files.write(first, content);
        Files.write(second, content);
        FileCheckpointChunkStore store = new FileCheckpointChunkStore(
                root.resolve("store"), 1024, 8192L, 16);

        FileCheckpointChunkStore.StoredChunkedPayload one = store.store(first);
        FileCheckpointChunkStore.StoredChunkedPayload two = store.store(second);
        assertEquals(one, two);
        assertEquals(1L, Files.list(store.manifestDirectory()).count());
        assertEquals(one.chunks().size(),
                Files.list(store.chunkDirectory()).count());

        Path target = root.resolve("target.bin");
        byte[] old = "keep-old".getBytes(StandardCharsets.UTF_8);
        Files.write(target, old);
        Path corrupt = store.chunkPath(one.chunks().get(0));
        byte[] bytes = Files.readAllBytes(corrupt);
        bytes[0] ^= 1;
        Files.write(corrupt, bytes);

        assertCode(StreamCheckpointException.Code.CORRUPT_CHUNK,
                () -> store.restore(one, target));
        assertArrayEquals(old, Files.readAllBytes(target));
    }

    @Test
    void payloadAndChunkCountLimitsFailWithoutManifest(@TempDir Path root)
            throws Exception {
        Path staged = root.resolve("too-large.bin");
        Files.write(staged, content(4097));
        FileCheckpointChunkStore bytesBound = new FileCheckpointChunkStore(
                root.resolve("bytes"), 1024, 4096L, 8);
        assertCode(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                () -> bytesBound.store(staged));
        assertFalse(Files.list(bytesBound.manifestDirectory())
                .findAny().isPresent());

        FileCheckpointChunkStore chunkBound = new FileCheckpointChunkStore(
                root.resolve("chunks"), 1024, 8192L, 4);
        assertCode(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                () -> chunkBound.store(staged));
        assertFalse(Files.list(chunkBound.manifestDirectory())
                .findAny().isPresent());
    }

    @Test
    void corruptManifestFailsBeforeRestoreTargetMutation(@TempDir Path root)
            throws Exception {
        Path staged = root.resolve("payload.bin");
        Files.write(staged, content(2049));
        FileCheckpointChunkStore store = new FileCheckpointChunkStore(
                root.resolve("store"), 1024, 4096L, 8);
        FileCheckpointChunkStore.StoredChunkedPayload payload =
                store.store(staged);
        Path manifest = store.manifestPath(payload.digest());
        byte[] bytes = Files.readAllBytes(manifest);
        bytes[bytes.length - 1] ^= 1;
        Files.write(manifest, bytes);

        Path target = root.resolve("target.bin");
        byte[] old = "old".getBytes(StandardCharsets.UTF_8);
        Files.write(target, old);
        assertCode(StreamCheckpointException.Code.CORRUPT_CHUNK_MANIFEST,
                () -> store.restore(payload, target));
        assertArrayEquals(old, Files.readAllBytes(target));
    }

    private static byte[] content(int size) {
        byte[] bytes = new byte[size];
        for (int index = 0; index < bytes.length; index++) {
            bytes[index] = (byte) ((index * 31 + index / 7) & 0xff);
        }
        return bytes;
    }

    private static void assertCode(
            StreamCheckpointException.Code code,
            org.junit.jupiter.api.function.Executable action) {
        StreamCheckpointException failure = assertThrows(
                StreamCheckpointException.class, action);
        assertEquals(code, failure.code());
    }
}
