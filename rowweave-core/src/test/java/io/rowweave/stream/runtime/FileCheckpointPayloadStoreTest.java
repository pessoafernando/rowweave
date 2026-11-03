package io.rowweave.stream.runtime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** bounded streaming payload persistence for local restore. */
class FileCheckpointPayloadStoreTest {

    @Test
    void contentAddressedPayloadSurvivesReopenAndRestoresAtomically(
            @TempDir Path root) throws Exception {
        Path staged = root.resolve("staged-state");
        byte[] payload = "canonical-state-payload".getBytes(
                StandardCharsets.UTF_8);
        Files.write(staged, payload);
        FileCheckpointPayloadStore store =
                new FileCheckpointPayloadStore(root.resolve("store"), 1024L);

        FileCheckpointPayloadStore.StoredPayload first = store.store(staged);
        FileCheckpointPayloadStore.StoredPayload replay = store.store(staged);
        assertEquals(first.digest(), replay.digest());
        assertEquals(payload.length, first.contentBytes());

        Path restored = root.resolve("restore/state.bin");
        new FileCheckpointPayloadStore(root.resolve("store"), 1024L)
                .restore(first, restored);
        assertArrayEquals(payload, Files.readAllBytes(restored));
        assertFalse(Files.exists(restored.resolveSibling(
                restored.getFileName() + ".tmp")));
    }

    @Test
    void oversizedInputFailsBeforePublication(@TempDir Path root)
            throws Exception {
        Path staged = root.resolve("too-large");
        Files.write(staged, new byte[65]);
        FileCheckpointPayloadStore store =
                new FileCheckpointPayloadStore(root.resolve("store"), 64L);

        StreamCheckpointException limit = assertThrows(
                StreamCheckpointException.class, () -> store.store(staged));
        assertEquals(StreamCheckpointException.Code.PAYLOAD_LIMIT,
                limit.code());
        assertEquals(0L, Files.list(root.resolve("store/payloads")).count());
    }

    @Test
    void corruptDurablePayloadFailsBeforeRestoreTargetMutation(
            @TempDir Path root) throws Exception {
        Path staged = root.resolve("staged");
        Files.write(staged, new byte[] {1, 2, 3});
        FileCheckpointPayloadStore store =
                new FileCheckpointPayloadStore(root.resolve("store"), 1024L);
        FileCheckpointPayloadStore.StoredPayload handle = store.store(staged);
        Files.write(store.payloadPath(handle.digest()), new byte[] {9},
                StandardOpenOption.TRUNCATE_EXISTING);
        Path target = root.resolve("target");
        Files.write(target, new byte[] {7});

        StreamCheckpointException corrupt = assertThrows(
                StreamCheckpointException.class,
                () -> store.restore(handle, target));
        assertEquals(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                corrupt.code());
        assertArrayEquals(new byte[] {7}, Files.readAllBytes(target));
    }

    @Test
    void crashAfterDurablePublicationIsExactRetry(@TempDir Path root)
            throws Exception {
        Path staged = root.resolve("staged");
        Files.write(staged, new byte[] {4, 5, 6});
        RuntimeException crash = new RuntimeException("simulated crash");
        FileCheckpointPayloadStore crashing =
                new FileCheckpointPayloadStore(
                        root.resolve("store"), 1024L,
                        (step, ignored) -> { throw crash; });

        assertSame(crash, assertThrows(RuntimeException.class,
                () -> crashing.store(staged)));
        FileCheckpointPayloadStore reopened =
                new FileCheckpointPayloadStore(root.resolve("store"), 1024L);
        FileCheckpointPayloadStore.StoredPayload recovered =
                reopened.store(staged);
        Path restored = root.resolve("restored");
        reopened.restore(recovered, restored);
        assertArrayEquals(new byte[] {4, 5, 6},
                Files.readAllBytes(restored));
    }

    @Test
    void boundedChunksTransferIdempotentlyAcrossDisjointStores(
            @TempDir Path root) throws Exception {
        byte[] payload = new byte[
                FileCheckpointPayloadStore.TRANSFER_CHUNK_BYTES + 17];
        for (int index = 0; index < payload.length; index++) {
            payload[index] = (byte) (index * 31);
        }
        Path staged = root.resolve("source-state");
        Files.write(staged, payload);
        FileCheckpointPayloadStore source =
                new FileCheckpointPayloadStore(
                        root.resolve("source"), payload.length);
        FileCheckpointPayloadStore.StoredPayload handle =
                source.store(staged);
        FileCheckpointPayloadStore target =
                new FileCheckpointPayloadStore(
                        root.resolve("target"), payload.length);

        byte[] first = source.readChunk(handle, 0L,
                FileCheckpointPayloadStore.TRANSFER_CHUNK_BYTES);
        assertFalse(target.acceptChunk(handle, 0L, first));
        assertFalse(target.acceptChunk(handle, 0L, first),
                "ACK loss retry must be exact and idempotent");
        byte[] last = source.readChunk(handle, first.length,
                FileCheckpointPayloadStore.TRANSFER_CHUNK_BYTES);
        assertTrue(target.acceptChunk(handle, first.length, last));

        Path restored = root.resolve("target-restored");
        target.restore(handle, restored);
        assertArrayEquals(payload, Files.readAllBytes(restored));
        StreamCheckpointException gap = assertThrows(
                StreamCheckpointException.class,
                () -> new FileCheckpointPayloadStore(
                        root.resolve("gap"), payload.length)
                        .acceptChunk(handle, 1L, last));
        assertEquals(StreamCheckpointException.Code.CORRUPT_PAYLOAD,
                gap.code());
    }
}
